#!/usr/bin/env python3
"""Local PostgreSQL protocol proxy: disconnect before COMMIT or hide its response.

Only dedicated harness SQL connections use this proxy. Never logs SQL or secrets.
The after mode waits for PostgreSQL's successful COMMIT CommandComplete, proving
server completion before disconnecting the client without forwarding that reply.
"""
import argparse
import json
import pathlib
import time
import socket
import socketserver
import struct
import threading


def exact(stream, length):
    chunks = []
    while length:
        chunk = stream.recv(length)
        if not chunk:
            raise EOFError("connection closed")
        chunks.append(chunk)
        length -= len(chunk)
    return b"".join(chunks)


def frame(stream, startup=False):
    kind = b"" if startup else exact(stream, 1)
    header = exact(stream, 4)
    length = struct.unpack("!I", header)[0]
    if not 4 <= length <= 16 * 1024 * 1024:
        raise ValueError("invalid PostgreSQL frame length")
    body = exact(stream, length - 4)
    return kind, body, kind + header + body


def commit(kind, body):
    return kind == b"Q" and body == b"COMMIT\0"


class Proxy(socketserver.ThreadingTCPServer):
    allow_reuse_address = True
    daemon_threads = True

    def __init__(self, *args, **kwargs):
        self.once = False
        self.arm_query = None
        self.fault_taken = False
        self.fault_lock = threading.Lock()
        self.log_lock = threading.Lock()
        super().__init__(*args, **kwargs)

    def claim_fault(self):
        with self.fault_lock:
            if self.once and self.fault_taken:
                return False
            self.fault_taken = True
            return True

    def record(self, event):
        with self.log_lock:
            print(json.dumps(event), flush=True)


class Handler(socketserver.BaseRequestHandler):
    def handle(self):
        upstream = socket.create_connection(("127.0.0.1", self.server.upstream), timeout=10)
        upstream.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
        self.request.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
        upstream.settimeout(60)
        self.request.settimeout(60)
        stopping = threading.Event()
        committing = threading.Event()

        def close():
            stopping.set()
            for stream in (upstream, self.request):
                try:
                    stream.shutdown(socket.SHUT_RDWR)
                except OSError:
                    pass

        def forward_client():
            try:
                _, _, startup = frame(self.request, startup=True)
                upstream.sendall(startup)
                armed = self.server.arm_query is None
                while not stopping.is_set():
                    kind, body, message = frame(self.request)
                    if kind in (b'Q', b'P') and self.server.arm_query and self.server.arm_query in body:
                        armed = True
                    chosen = commit(kind, body) and armed and self.server.claim_fault()
                    if chosen:
                        committing.set()
                        if self.server.mode == "before":
                            self.server.record({"fault": "before_commit", "server_committed": False})
                            close()
                            return
                    if chosen and self.server.mode == "hold":
                        pathlib.Path(str(self.server.gate) + ".pending").write_text("pending")
                        self.server.record({"fault": "held_commit", "server_committed": False})
                        deadline = time.monotonic() + 30
                        while not self.server.gate.exists():
                            if time.monotonic() >= deadline:
                                raise TimeoutError("COMMIT release gate expired")
                            time.sleep(.02)
                    upstream.sendall(message)
                    if chosen and self.server.mode == "hold":
                        # Keep the upstream alive until server completion even if
                        # the client died while COMMIT was held by the gate.
                        while committing.is_set() and not stopping.is_set():
                            time.sleep(.005)
            except (EOFError, OSError, ValueError):
                close()

        thread = threading.Thread(target=forward_client, daemon=True)
        thread.start()
        try:
            while not stopping.is_set():
                kind, body, message = frame(upstream)
                if self.server.mode == "after" and committing.is_set() and kind == b"C" and body == b"COMMIT\0":
                    self.server.record({"fault": "lost_commit_response", "server_committed": True})
                    close()
                    break
                if self.server.mode == "hold" and committing.is_set() and kind == b"C" and body == b"COMMIT\0":
                    self.server.record({"fault": "released_commit", "server_committed": True})
                    committing.clear()
                try:
                    self.request.sendall(message)
                except OSError:
                    # A held transaction may still COMMIT after its client dies.
                    # Preserve the upstream until the witnessed COMMIT reply;
                    # forwarding earlier ReadyForQuery to a dead client must not
                    # turn this specific harness scenario into an early rollback.
                    if not (self.server.mode == "hold" and committing.is_set()):
                        raise
        except (EOFError, OSError, ValueError):
            close()
        finally:
            close()
            thread.join(timeout=2)
            upstream.close()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--mode", choices=("before", "after", "hold"), required=True)
    parser.add_argument("--upstream", type=int, default=55434)
    parser.add_argument("--gate", type=pathlib.Path)
    parser.add_argument("--once", action="store_true", help="inject one global COMMIT fault")
    parser.add_argument("--arm-query", help="arm after an SQL Parse/Query containing this literal")
    args = parser.parse_args()
    if args.mode == "hold" and args.gate is None:
        parser.error("hold mode requires --gate")
    with Proxy(("127.0.0.1", 0), Handler) as server:
        server.mode, server.upstream, server.gate = args.mode, args.upstream, args.gate
        server.once = args.once
        server.arm_query = args.arm_query.encode() if args.arm_query else None
        print(json.dumps({"ready": True, "port": server.server_address[1], "mode": args.mode}), flush=True)
        server.serve_forever()
