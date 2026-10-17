"""Recorded process readiness is bounded and preserves complete failure evidence."""
import json
from pathlib import Path
import sys
import tempfile
import unittest
from harness_control import start_recorded


class RecordedProcessTests(unittest.TestCase):
    def test_partial_ready_record_waits_for_complete_line(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory)/'process.jsonl'
            script = "import sys,time;sys.stdout.write('{\\\"ready\\\":');sys.stdout.flush();time.sleep(.1);print('true,\\\"port\\\":123}');time.sleep(30)"
            process, ready = start_recorded([sys.executable, '-u', '-c', script], path)
            try:
                self.assertEqual(ready, {'ready':True, 'port':123})
                self.assertEqual(json.loads(path.read_text()), ready)
            finally:
                process.terminate()
                process.wait(timeout=10)

    def test_failed_and_timed_out_processes_keep_evidence(self):
        for script, error in [("print('{\"failed\":true}')", RuntimeError),
                              ("import time;print('{\"waiting\":true}');time.sleep(30)", AssertionError)]:
            with self.subTest(error=error), tempfile.TemporaryDirectory() as directory:
                path = Path(directory)/'failure.jsonl'
                with self.assertRaises(error):
                    start_recorded([sys.executable, '-u', '-c', script], path, timeout=.2)
                self.assertTrue(json.loads(path.read_text()))
