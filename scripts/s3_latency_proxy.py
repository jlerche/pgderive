#!/usr/bin/env python3
"""Local S3 HTTP proxy with seeded, percentile-interpolated additive latency.

Anchors: TopicPartition raw upload/download tables, 2025-03-04, 500 KiB,
EC2/S3 eu-north-1, 100 sequential samples. The article's TL;DR swaps medians;
we use the labeled raw tables. This synthetic distribution is not an AWS SLA.
https://topicpartition.io/misc/AWS-S3-PUT-latency-benchmark
"""
import argparse
import hashlib
import http.client
import json
import math
import threading
import time
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from urllib.parse import urlsplit

ANCHORS = {"GET": (26.13, 38.86, 86.13), "PUT": (69.75, 101.10, 137.23)}
HOP_HEADERS = {"connection", "keep-alive", "proxy-authenticate", "proxy-authorization",
               "te", "trailer", "transfer-encoding", "upgrade"}


def quantile(method, probability):
    """Linear inverse CDF: (0,0), published p50/p95/p99, then clamp at p99."""
    if not 0 <= probability <= 1:
        raise ValueError("probability outside [0,1]")
    values = (0, *ANCHORS[method], ANCHORS[method][-1])
    points = (0, .5, .95, .99, 1)
    for i in range(1, len(points)):
        if probability <= points[i]:
            fraction = (probability - points[i - 1]) / (points[i] - points[i - 1])
            return values[i - 1] + fraction * (values[i] - values[i - 1])
    return values[-1]


class Model:
    def __init__(self, seed=42, scale=1, fail=None):
        if not math.isfinite(scale) or scale < 0:
            raise ValueError("scale must be finite and nonnegative")
        self.seed, self.scale, self.fail = seed, scale, fail
        self.counters = {"GET": 0, "PUT": 0}
        self.lock = threading.Lock()

    def sample(self, method):
        kind = "PUT" if method in ("PUT", "POST", "DELETE") else "GET"
        with self.lock:
            self.counters[kind] += 1
            ordinal = self.counters[kind]
        digest = hashlib.sha256(f"{self.seed}:{kind}:{ordinal}".encode()).digest()
        probability = int.from_bytes(digest[:8], "big") / 2**64
        return kind, ordinal, quantile(kind, probability) * self.scale, self.fail == f"{kind}:{ordinal}"


class Proxy(ThreadingHTTPServer):
    daemon_threads = True

    def __init__(self, address, upstream, model):
        parsed = urlsplit(upstream)
        if parsed.scheme != "http" or parsed.hostname not in ("127.0.0.1", "localhost") or parsed.path not in ("", "/"):
            raise ValueError("upstream must be a local HTTP origin")
        self.upstream = (parsed.hostname, parsed.port or 80)
        self.model = model
        self.log_lock = threading.Lock()
        super().__init__(address, Handler)

    def record(self, event):
        # print performs separate data/newline writes; serialize worker threads.
        with self.log_lock:
            print(json.dumps(event), flush=True)


class Handler(BaseHTTPRequestHandler):
    protocol_version = "HTTP/1.1"

    def log_message(self, *_):
        pass

    def forward(self):
        started = time.monotonic()
        kind, ordinal, delay_ms, fail = self.server.model.sample(self.command)
        time.sleep(delay_ms / 1000)
        self.close_connection = True
        connection = http.client.HTTPConnection(*self.server.upstream, timeout=30)
        status = 502
        try:
            if fail:
                self.send_error(503, "injected object request failure")
                status = 503
                return
            # Keep the original Host, signed headers, URI, range and conditionals.
            connection.putrequest(self.command, self.path, skip_host=True, skip_accept_encoding=True)
            named_hops = {name.strip().lower() for name in self.headers.get("Connection", "").split(",")}
            for name, value in self.headers.items():
                if name.lower() not in HOP_HEADERS | named_hops:
                    connection.putheader(name, value)
            chunked = self.headers.get("Transfer-Encoding", "").lower() == "chunked"
            if chunked:
                connection.putheader("Transfer-Encoding", "chunked")
            connection.putheader("Connection", "close")
            connection.endheaders()
            if chunked:
                self.forward_chunks(connection)
            else:
                remaining = int(self.headers.get("Content-Length", 0))
                while remaining:
                    chunk = self.rfile.read(min(remaining, 65536))
                    if not chunk:
                        raise EOFError("incomplete request body")
                    connection.send(chunk)
                    remaining -= len(chunk)
            response = connection.getresponse()
            status = response.status
            self.send_response_only(status, response.reason)
            for name, value in response.getheaders():
                if name.lower() not in HOP_HEADERS:
                    self.send_header(name, value)
            self.send_header("Connection", "close")
            self.end_headers()
            if self.command != "HEAD":
                while chunk := response.read(65536):
                    self.wfile.write(chunk)
        except (OSError, EOFError, ValueError, http.client.HTTPException) as error:
            # Request/response logs are retained by the harness. No retry in proxy.
            self.server.record({"error": str(error), "method": self.command})
            if status == 502:
                self.send_error(502, "upstream request failed")
        finally:
            connection.close()
            self.server.record({"method": self.command, "range": self.headers.get("Range"),
                              "kind": kind, "ordinal": ordinal, "delay_ms": delay_ms,
                              "status": status, "elapsed_ms": (time.monotonic() - started) * 1000})

    def forward_chunks(self, connection):
        while True:
            line = self.rfile.readline(8192)
            size = int(line.split(b";", 1)[0].strip(), 16)
            connection.send(line)
            if size == 0:
                while trailer := self.rfile.readline(8192):
                    connection.send(trailer)
                    if trailer == b"\r\n":
                        return
                raise EOFError("incomplete chunk trailers")
            remaining = size + 2
            while remaining:
                chunk = self.rfile.read(min(remaining, 65536))
                if not chunk:
                    raise EOFError("incomplete chunk")
                connection.send(chunk)
                remaining -= len(chunk)

    do_GET = do_HEAD = do_PUT = do_POST = do_DELETE = forward


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--port", type=int, default=8334)
    parser.add_argument("--upstream", default="http://127.0.0.1:8333")
    parser.add_argument("--seed", type=int, default=42)
    parser.add_argument("--scale", type=float, default=1)
    parser.add_argument("--fail", help="One-shot failure at GET:N or PUT:N (HEAD uses GET)")
    args = parser.parse_args()
    server = Proxy(("127.0.0.1", args.port), args.upstream, Model(args.seed, args.scale, args.fail))
    print(json.dumps({"ready": True, "port": server.server_port, "seed": args.seed,
                      "scale": args.scale, "anchors_ms": ANCHORS}), flush=True)
    try:
        server.serve_forever()
    finally:
        server.server_close()


if __name__ == "__main__":
    main()
