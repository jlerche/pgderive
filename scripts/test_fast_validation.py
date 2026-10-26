"""Benchmark executable identity survives another Cargo build."""
import pathlib
import stat
import json
import sys
import tempfile
import unittest
from unittest import mock
from fast_validation import execute, freeze_binary


class ExecutableTest(unittest.TestCase):
    def test_frozen_executable_survives_source_replacement(self):
        with tempfile.TemporaryDirectory() as directory:
            root = pathlib.Path(directory)
            binary = root / 'cargo-test-binary'
            binary.write_bytes(b'first build')
            binary.chmod(0o700)
            output = root / 'evidence'
            output.mkdir()
            frozen = pathlib.Path(freeze_binary(binary, output))
            binary.write_bytes(b'another checkout build')
            self.assertEqual(frozen.read_bytes(), b'first build')
            self.assertEqual(stat.S_IMODE(frozen.stat().st_mode), 0o700)

    def test_compiler_overlap_rejects_timings_but_keeps_completed_evidence(self):
        with tempfile.TemporaryDirectory() as directory:
            output = pathlib.Path(directory)
            with mock.patch('fast_maintenance.compiler_processes', return_value=['synthetic:rustc']):
                with self.assertRaisesRegex(RuntimeError, 'overlapped compilation'):
                    execute([sys.executable, '-c', 'import time; time.sleep(.12)'],
                            output, 'probe', isolate_compilers=True)
            evidence = json.loads((output/'probe.json').read_text())
            self.assertEqual(evidence['exit_code'], 0)
            self.assertEqual(evidence['compiler_overlap'], ['synthetic:rustc'])
            self.assertTrue((output/'probe.log').exists())
