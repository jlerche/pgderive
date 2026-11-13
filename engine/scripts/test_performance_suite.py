"""Include pure performance-harness checks in the common repository gate."""
import importlib.util
import pathlib
import unittest


def load_tests(_loader, _tests, _pattern):
    tests = unittest.TestSuite()
    for path in sorted((pathlib.Path(__file__).parent / 'performance').glob('test_*.py')):
        spec = importlib.util.spec_from_file_location('pgderive_performance_' + path.stem, path)
        module = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(module)
        tests.addTests(unittest.defaultTestLoader.loadTestsFromModule(module))
    return tests
