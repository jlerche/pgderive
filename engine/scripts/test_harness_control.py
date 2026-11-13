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


class OwnedServerTests(unittest.TestCase):
    def test_startup_failures_keep_owned_database_and_failure_logs(self):
        from unittest.mock import Mock, patch
        from harness_control import Server
        info = {'Mounts':[], 'HostConfig':{'Tmpfs':{'/var/lib/postgresql/data':'rw'}},
                'NetworkSettings':{'Ports':{'5432/tcp':[{'HostPort':'123'}]}}}
        for kind in ['inspect', 'persistent', 'readiness']:
            with self.subTest(kind=kind), tempfile.TemporaryDirectory() as directory:
                output = Path(directory)
                inspected = dict(info, Mounts=[{}]) if kind=='persistent' else info
                with patch('harness_control.subprocess.run', return_value=Mock(returncode=1)) as run, \
                     patch('harness_control.subprocess.check_output', side_effect=RuntimeError('inspect') if kind=='inspect' else None, return_value=json.dumps([inspected])), \
                     patch('harness_control.time.monotonic', side_effect=[0,31]):
                    with self.assertRaises((RuntimeError, AssertionError)):
                        Server(output, 'owned', name='pgderive-unit-owned')
                commands = [call.args[0] for call in run.call_args_list]
                self.assertIn(['docker','logs','pgderive-unit-owned'], commands)
                self.assertFalse(any(command[:2]==['docker','rm'] for command in commands))
                self.assertTrue((output/'pgderive-unit-owned-failure.log').is_file())
