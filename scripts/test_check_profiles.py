import contextlib
import io
import json
import pathlib
import subprocess
import tempfile
import unittest
from unittest.mock import patch
from run_with_s3_proxy import timed_run
from verify_worker_core import verify


class CheckProfiles(unittest.TestCase):
    def test_invalid_profiles_fail_before_starting_live_checks(self):
        for script in ('check_worker.sh', 'check_coverage.sh'):
            result = subprocess.run(['bash', 'scripts/'+script],
                                    env={'PATH': '/usr/bin:/bin', 'PGDERIVE_CHECK_PROFILE': 'typo'},
                                    capture_output=True,text=True)
            self.assertEqual(result.returncode,2)
            self.assertIn('Invalid check profile',result.stderr)
        for arguments in (['--typo'], ['--full','--typo']):
            result = subprocess.run(['bash','scripts/check.sh',*arguments],capture_output=True,text=True)
            self.assertEqual(result.returncode,2)

    def test_core_evidence_rejects_missing_extra_and_malformed_results(self):
        with tempfile.TemporaryDirectory() as directory:
            root = pathlib.Path(directory)
            result = root/'compiler/regular/result.json'
            result.parent.mkdir(parents=True)
            rebuild = root/'query-rebuild/qualification.json'
            rebuild.parent.mkdir()
            rebuild.write_text('{}')
            with self.assertRaises(ValueError):
                verify(root,{'compiler':1})
            result.write_text('{}')
            self.assertEqual(verify(root,{'compiler':1}),1)
            extra = root/'result.json'
            extra.write_text('{}')
            with self.assertRaises(ValueError):
                verify(root,{'compiler':1})
            extra.unlink()
            result.write_text('[]')
            with self.assertRaises(ValueError):
                verify(root,{'compiler':1})
            result.write_text('invalid json')
            with self.assertRaises(ValueError):
                verify(root,{'compiler':1})

    def test_timings_record_success_failure_and_interruption(self):
        for code in (0,1,None):
            with tempfile.TemporaryDirectory() as directory, contextlib.redirect_stderr(io.StringIO()):
                output = pathlib.Path(directory)
                with patch('run_with_s3_proxy.run',return_value=code,side_effect=TimeoutError if code is None else None):
                    if code is None:
                        with self.assertRaises(TimeoutError):
                            timed_run(output,['command'])
                    else:
                        self.assertEqual(timed_run(output,['command']),code)
                recorded = json.loads((output/'timing.json').read_text())
                self.assertEqual(recorded['returncode'],code)
                self.assertEqual(recorded['completed'],code is not None)
                self.assertGreaterEqual(recorded['elapsed_seconds'],0)
