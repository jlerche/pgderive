"""Benchmark executable identity survives another Cargo build."""
import pathlib
import stat
import tempfile
import unittest
from fast_validation import freeze_binary


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
