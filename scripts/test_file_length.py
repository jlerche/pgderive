"""Boundary and override coverage for the custom Rust file-length lint."""

import unittest
from pathlib import Path
import subprocess
import sys
import tempfile
from check_file_length import violations


class FileLengthTests(unittest.TestCase):
    def test_default_boundary(self):
        self.assertEqual(violations("// line\n" * 1000), [])
        self.assertTrue(violations("// line\n" * 1001))

    def test_blank_lines_and_final_newline(self):
        self.assertEqual(violations("\n" * 1000), [])
        self.assertTrue(violations("\n" * 1001))
        self.assertEqual(violations("// line\n" * 999 + "// last"), [])

    def test_override_boundary(self):
        header = "// pgderive: max-lines=1200 -- generated schema\n"
        self.assertEqual(violations(header + "// line\n" * 1199), [])
        self.assertTrue(violations(header + "// line\n" * 1200))

    def test_smaller_override(self):
        self.assertTrue(violations("// pgderive: max-lines=1 -- focused module\n// extra"))

    def test_header_boundary(self):
        directive = "// pgderive: max-lines=1200 -- generated schema\n"
        self.assertEqual(violations("\n" * 9 + directive), [])
        self.assertTrue(violations("\n" * 10 + directive))

    def test_duplicate_override(self):
        self.assertTrue(violations("// pgderive: max-lines=1200 -- schema\n" * 2))

    def test_malformed_override(self):
        for value in ("0 -- reason", "-1 -- reason", "lots -- reason", "1200", "1200 -- "):
            with self.subTest(value=value):
                self.assertTrue(violations(f"// pgderive: max-lines={value}\n"))

    def test_mention_in_code_is_not_a_directive(self):
        self.assertEqual(violations('let marker = "pgderive: max-lines=1200";'), [])

    def test_cli_failure_and_success(self):
        checker = Path(__file__).with_name("check_file_length.py").resolve()
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "source with spaces.rs"
            path.write_text("// line\n" * 1001)
            failed = subprocess.run([sys.executable, str(checker), str(path)], capture_output=True)
            self.assertEqual(failed.returncode, 1)
            path.write_text("// pgderive: max-lines=1001 -- boundary fixture\n" + "// line\n" * 1000)
            passed = subprocess.run([sys.executable, str(checker), str(path)], capture_output=True)
            self.assertEqual(passed.returncode, 0)

    def test_cli_missing_file_fails(self):
        checker = Path(__file__).with_name("check_file_length.py").resolve()
        with tempfile.TemporaryDirectory() as directory:
            missing = Path(directory) / "missing.rs"
            result = subprocess.run([sys.executable, str(checker), str(missing)], capture_output=True)
            self.assertEqual(result.returncode, 1)


if __name__ == "__main__":
    unittest.main()
