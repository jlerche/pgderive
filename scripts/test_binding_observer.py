"""Observe execution of cached statements, rather than just SQL preparation."""
import struct
import unittest
from binding_observer import BindingObserver


class BindingObserverTests(unittest.TestCase):
    def test_prepared_reuse_and_close(self):
        observer = BindingObserver()
        self.assertIsNone(observer.observe(b'P', b's\0WITH RECURSIVE native AS (...) SELECT 1\0\0\0'))
        self.assertIsNone(observer.observe(b'B', b'p\0s\0secret parameters'))
        self.assertEqual(observer.observe(b'E', b'p\0'+struct.pack('!I', 0)), 'catalog')
        self.assertEqual(observer.observe(b'E', b'p\0'+struct.pack('!I', 0)), 'catalog')
        observer.observe(b'C', b'Pp\0')
        self.assertIsNone(observer.observe(b'E', b'p\0'+struct.pack('!I', 0)))
        observer.observe(b'C', b'Ss\0')
        observer.observe(b'B', b'p\0s\0parameters')
        self.assertIsNone(observer.observe(b'E', b'p\0'+struct.pack('!I', 0)))

    def test_only_inspection_categories_are_exposed(self):
        observer = BindingObserver()
        for query, expected in [
            (b'SELECT * FROM pg_catalog.pg_proc p JOIN pg_catalog.pg_language l', 'terminal_functions'),
            (b'SELECT * FROM pg_catalog.pg_collation c', 'terminal_collation'),
            (b'SELECT * FROM pg_catalog.pg_timezone_abbrevs', 'terminal_calendar'),
            (b'SELECT a.attname FROM pg_attribute a LEFT JOIN pg_collation c ON true', None),
            (b'SET search_path=pg_catalog; SET TimeZone=UTC', 'session_configuration'),
            (b'SET LOCAL search_path=pg_catalog; SET LOCAL TimeZone=UTC', 'session_configuration'),
            (b'SET LOCAL synchronous_commit=on', None),
            (b'UPDATE private SET secret=123', None),
        ]:
            self.assertEqual(observer.observe(b'Q', query+b'\0'), expected)
