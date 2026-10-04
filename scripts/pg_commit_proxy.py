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


class Handler(socketserver.BaseRequestHandler):
    def handle(self):
        upstream = socket.create_connection(("127.0.0.1", self.server.upstream), timeout=10)
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
                while not stopping.is_set():
                    kind, body, message = frame(self.request)
                    if commit(kind, body):
                        committing.set()
                        if self.server.mode == "before":
                            print(json.dumps({"fault": "before_commit", "server_committed": False}), flush=True)
                            close()
                            return
                    if commit(kind, body) and self.server.mode == "hold":
                        pathlib.Path(str(self.server.gate) + ".pending").write_text("pending")
                        print(json.dumps({"fault": "held_commit", "server_committed": False}), flush=True)
                        deadline = time.monotonic() + 30
                        while not self.server.gate.exists():
                            if time.monotonic() >= deadline:
                                raise TimeoutError("COMMIT release gate expired")
                            time.sleep(.02)
                    upstream.sendall(message)
            except (EOFError, OSError, ValueError):
                close()

        thread = threading.Thread(target=forward_client, daemon=True)
        thread.start()
        try:
            while not stopping.is_set():
                kind, body, message = frame(upstream)
                if self.server.mode == "after" and committing.is_set() and kind == b"C" and body == b"COMMIT\0":
                    print(json.dumps({"fault": "lost_commit_response", "server_committed": True}), flush=True)
                    close()
                    break
                if self.server.mode == "hold" and committing.is_set() and kind == b"C" and body == b"COMMIT\0":
                    print(json.dumps({"fault": "released_commit", "server_committed": True}), flush=True)
                self.request.sendall(message)
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
    args = parser.parse_args()
    if args.mode == "hold" and args.gate is None:
        parser.error("hold mode requires --gate")
    with Proxy(("127.0.0.1", 0), Handler) as server:
        server.mode, server.upstream, server.gate = args.mode, args.upstream, args.gate
        print(json.dumps({"ready": True, "port": server.server_address[1], "mode": args.mode}), flush=True)
        server.serve_forever()
