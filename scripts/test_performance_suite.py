"""Include pure performance-harness checks in the common repository gate."""
import importlib.util
import pathlib
import unittest


def load_tests(_loader, _tests, _pattern):
    path = pathlib.Path(__file__).parent / 'performance' / 'test_suite.py'
    spec = importlib.util.spec_from_file_location('pgderive_performance_tests', path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return unittest.defaultTestLoader.loadTestsFromModule(module)
