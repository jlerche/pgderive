import contextlib
import io
import json
import time
import http.client
import threading
import tempfile
import pathlib
import unittest
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from s3_latency_proxy import ANCHORS, Model, Proxy, quantile


class LatencyTests(unittest.TestCase):
    def test_request_gate_claims_only_armed_method_and_waits_for_release(self):
        with tempfile.TemporaryDirectory() as directory:
            gate = pathlib.Path(directory) / 'release'
            arm = pathlib.Path(str(gate) + '.arm')
            pending = pathlib.Path(str(gate) + '.pending')
            arm.write_text('PUT')
            proxy = Proxy(('127.0.0.1', 0), 'http://127.0.0.1:8333', Model())
            proxy.gate = gate
            try:
                proxy.hold('GET')
                self.assertTrue(arm.exists())
                thread = threading.Thread(target=proxy.hold, args=('PUT',))
                thread.start()
                deadline = time.monotonic() + 2
                while not pending.exists() and time.monotonic() < deadline:
                    time.sleep(.01)
                self.assertTrue(pending.exists())
                self.assertFalse(arm.exists())
                self.assertTrue(thread.is_alive())
                # A second request cannot consume the same gate reservation.
                proxy.hold('PUT')
                gate.touch()
                thread.join(timeout=2)
                self.assertFalse(thread.is_alive())
            finally:
                gate.touch()
                proxy.server_close()

    def test_concurrent_evidence_records_are_complete_json_lines(self):
        class YieldingOutput(io.StringIO):
            def write(self, value):
                result = super().write(value)
                time.sleep(.001)
                return result
        output = YieldingOutput()
        proxy = Proxy(("127.0.0.1", 0), "http://127.0.0.1:8333", Model())
        try:
            with contextlib.redirect_stdout(output):
                threads = [threading.Thread(target=proxy.record, args=({"record": i},)) for i in range(20)]
                for thread in threads:
                    thread.start()
                for thread in threads:
                    thread.join()
            records = [json.loads(line)["record"] for line in output.getvalue().splitlines()]
            self.assertEqual(sorted(records), list(range(20)))
        finally:
            proxy.server_close()

    def test_published_anchors_and_interpolation(self):
        for method, anchors in ANCHORS.items():
            for probability, expected in zip((.5, .95, .99), anchors):
                self.assertAlmostEqual(quantile(method, probability), expected)
            self.assertEqual(quantile(method, 1), anchors[-1])
            self.assertAlmostEqual(quantile(method, .725), (anchors[0] + anchors[1]) / 2)
        with self.assertRaises(ValueError):
            quantile("GET", -1)
        with self.assertRaises(ValueError):
            Model(scale=float("nan"))

    def test_seeded_empirical_percentiles(self):
        for method in ANCHORS:
            model = Model()
            samples = sorted(model.sample(method)[2] for _ in range(100000))
            for probability, expected in zip((.5, .95, .99), ANCHORS[method]):
                self.assertAlmostEqual(samples[int(probability * len(samples))], expected, delta=1)
        first, second = Model(), Model()
        self.assertEqual([first.sample("HEAD") for _ in range(10)], [second.sample("HEAD") for _ in range(10)])
        model = Model(fail="PUT:2")
        self.assertFalse(model.sample("PUT")[3])
        self.assertTrue(model.sample("PUT")[3])
        self.assertFalse(model.sample("PUT")[3])

    def test_http_forwarding_preserves_s3_body_headers_ranges_and_failure(self):
        observed = []
        class Upstream(BaseHTTPRequestHandler):
            def log_message(self, *_):
                pass
            def handle_request(self):
                body = self.rfile.read(int(self.headers.get("Content-Length", 0)))
                observed.append((self.command, self.path, dict(self.headers), body))
                self.send_response(206 if self.command == "GET" else 200)
                self.send_header("Content-Length", "3")
                self.send_header("ETag", '"immutable"')
                self.end_headers()
                if self.command != "HEAD":
                    self.wfile.write(b"abc")
            do_PUT = do_GET = do_HEAD = handle_request
        upstream = ThreadingHTTPServer(("127.0.0.1", 0), Upstream)
        proxy = Proxy(("127.0.0.1", 0), f"http://127.0.0.1:{upstream.server_port}", Model(scale=0, fail="GET:3"))
        threads = [threading.Thread(target=server.serve_forever) for server in (upstream, proxy)]
        for thread in threads:
            thread.start()
        try:
            for method, body, headers in [("PUT", b"payload", {"If-None-Match": "*"}),
                                          ("GET", None, {"Range": "bytes=2-4"}),
                                          ("HEAD", None, {}), ("GET", None, {})]:
                connection = http.client.HTTPConnection("127.0.0.1", proxy.server_port)
                connection.request(method, "/bucket/object?part=1", body, headers)
                response = connection.getresponse()
                self.assertEqual(response.status, 503 if len(observed) == 3 and method == "GET" else 206 if method == "GET" else 200)
                response.read()
                connection.close()
            self.assertEqual(observed[0][3], b"payload")
            self.assertEqual(observed[0][2]["If-None-Match"], "*")
            self.assertEqual(observed[1][2]["Range"], "bytes=2-4")
            self.assertEqual(observed[1][1], "/bucket/object?part=1")
            self.assertEqual(observed[0][2]["Host"], f"127.0.0.1:{proxy.server_port}")
            self.assertEqual(len(observed), 3)
        finally:
            for server in (proxy, upstream):
                server.shutdown()
                server.server_close()
            for thread in threads:
                thread.join()
