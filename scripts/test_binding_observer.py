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

    def test_sink_observer_counts_executions_without_classifying_user_values(self):
        from sink_observer import category
        observer = BindingObserver(category)
        marker = b'/* pgderive:sink-delete-v1 */ WITH decoded AS MATERIALIZED (...) DELETE FROM owned.results'
        self.assertIsNone(observer.observe(b'P', b's\0'+marker+b'\0\0\0'))
        self.assertIsNone(observer.observe(b'B', b'p\0s\0private payload'))
        self.assertEqual(observer.observe(b'E', b'p\0'+struct.pack('!I',0)), 'delete')
        self.assertEqual(observer.observe(b'E', b'p\0'+struct.pack('!I',0)), 'delete')
        self.assertIsNone(category(b'SELECT 1 /* pgderive:sink-delete-v1 */'))
        self.assertEqual(category(b'/* pgderive:sink-insert-v1 */ INSERT INTO owned.results'), 'insert')

    def test_removed_per_row_sink_sql_is_not_classified(self):
        from sink_observer import category
        self.assertIsNone(category(b'INSERT INTO owned.results SELECT pg_catalog.generate_series(1::bigint,$2::bigint)'))
        self.assertIsNone(category(b'WITH selected AS (SELECT ctid FROM owned.results LIMIT $2 FOR UPDATE) DELETE FROM owned.results'))
