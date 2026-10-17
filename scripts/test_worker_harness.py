"""Failure evidence survives errors in both qualification and cleanup."""
import unittest
from worker_harness import Fixture


class BagVerificationTests(unittest.TestCase):
    def test_memory_bag_preserves_numeric_nulls_and_acknowledgement(self):
        from decimal import Decimal
        from unittest.mock import Mock
        fixture = Fixture.__new__(Fixture)
        fixture.query = 'independent oracle query'
        fixture.bag_relation = Mock(return_value='destination')
        fixture.sql = Mock(return_value='[{"tuple":[null,[null,0.1234567890123456789]],"weight":2}]')
        fixture.acknowledgement_ahead = Mock(return_value='0\n')
        expected = {(None, Decimal('0.1234567890123456789')): 2}
        fixture.verify_memory(expected, parse_float=Decimal)
        fixture.acknowledgement_ahead.assert_called_once()
        with self.assertRaises(AssertionError):
            fixture.verify_memory({})
        fixture.acknowledgement_ahead.return_value = '1'
        with self.assertRaisesRegex(AssertionError, 'acknowledged beyond'):
            fixture.verify_memory(expected, parse_float=Decimal)

    def test_sql_bag_uses_labels_or_explicit_oracle_and_context(self):
        from unittest.mock import Mock
        fixture = Fixture.__new__(Fixture)
        fixture.query = 'SELECT x FROM source'
        fixture.labels = ['x', '"Case Sensitive"']
        fixture.bag_difference = Mock(return_value='0\n')
        fixture.verify_sql(context='SET TimeZone=UTC;')
        query, = fixture.bag_difference.call_args.args
        self.assertIn('jsonb_build_array(x,"Case Sensitive")', query)
        self.assertIn('FROM (SELECT x FROM source) q GROUP BY 1', query)
        self.assertEqual(fixture.bag_difference.call_args.kwargs, {'context': 'SET TimeZone=UTC;'})
        fixture.verify_sql('SELECT oracle')
        fixture.bag_difference.assert_called_with('SELECT oracle', context='')
        fixture.bag_difference.return_value = '1'
        with self.assertRaises(AssertionError):
            fixture.verify_sql()


class ManagedFixtureTests(unittest.TestCase):
    def test_cleanup_and_failure_evidence_order(self):
        for failure in (None, 'body', 'cleanup'):
            events = []
            error = RuntimeError(failure)

            class OwnedFixture(Fixture):
                def __init__(self):
                    pass

                def cleanup(self):
                    events.append('cleanup')
                    if failure == 'cleanup':
                        raise error

                def preserve(self):
                    events.append('preserve')

            def run():
                with OwnedFixture().managed():
                    events.append('body')
                    if failure == 'body':
                        raise error

            with self.subTest(failure=failure):
                if failure is None:
                    run()
                    self.assertEqual(events, ['body', 'cleanup'])
                else:
                    with self.assertRaises(RuntimeError) as caught:
                        run()
                    self.assertIs(caught.exception, error)
                    expected = ['body', 'preserve'] if failure == 'body' else ['body', 'cleanup', 'preserve']
                    self.assertEqual(events, expected)


class HistoryTests(unittest.TestCase):
    def test_transactions_rejections_and_restart(self):
        from worker_harness import History
        events = []
        reopened = {'time': 12, 'slot': 'owned'}
        progress = ['prior']

        class Process:
            def __init__(self, resumed):
                self.resumed = resumed

            def event(self, kind, **options):
                events.append((kind, options))
                return reopened if self.resumed else {'time': 10, 'slot': 'owned'}

            def abort(self):
                events.append('abort')

            def finish(self, graceful=False):
                events.append(('finish', True) if graceful else 'finish')

        class OwnedFixture(Fixture):
            name = 'owned'
            query = 'original'

            def __init__(self):
                pass

            def start(self, command, label, maximum=0):
                events.append((label, maximum, self.query))
                return Process(label == 'resumed')

            def sql(self, sql):
                events.append(sql)
                return progress[0]

            def verify(self):
                events.append('verify')

            def rejected(self, command, label, query):
                events.append((label, query))
                if label == 'tamper':
                    progress[0] = 'changed'

        fixture = OwnedFixture()
        fixture.run_history([], History(['one', 'two'], 'final', 'normalized',
                                       [('changed', 'invalid')], session='SET x=y;'),
                            initial=lambda: events.append('initial'))
        self.assertEqual(events[0], ('first', 0, 'original'))
        self.assertIn('SET x=y;BEGIN;one;COMMIT', events)
        self.assertIn(('published', {'minimum_time': 12}), events)
        self.assertLess(events.index('abort'), events.index(('changed', 'invalid')))
        self.assertLess(events.index(('changed', 'invalid')), events.index(('resumed', 1, 'normalized')))
        self.assertEqual(events.count('verify'), 5)
        self.assertEqual(events[-3:], [('published', {}), 'finish', 'verify'])

        for field, value in [('time', 11), ('slot', 'replaced')]:
            with self.subTest(field=field):
                reopened.update(time=12, slot='owned')
                reopened[field] = value
                events.clear()
                with self.assertRaises(AssertionError):
                    fixture.run_history([], History(['one', 'two'], 'final', 'normalized', []))
                self.assertNotIn('final', events)

        reopened.update(time=12, slot='owned')
        events.clear()
        fixture.run_history([], History(['one', 'two'], 'final', 'normalized', [], graceful=True),
                            after=lambda: events.append('after'))
        self.assertLess(events.index('after'), events.index(('finish', True)))
        self.assertNotIn('abort', events)
        events.clear()
        with self.assertRaisesRegex(AssertionError, 'changed durable progress'):
            fixture.run_history([], History(['one', 'two'], 'final', 'normalized', [('tamper', '')]))
        self.assertFalse(any(isinstance(event, tuple) and event[0] == 'resumed' for event in events))


class ProxyFixtureLifecycleTests(unittest.TestCase):
    def test_proxy_readiness_failure_preserves_owned_fixture(self):
        from pathlib import Path
        from unittest.mock import patch
        import query_binding_harness
        evidence = []
        class OwnedFixture(Fixture):
            port = '123'
            def __init__(self, *args):
                pass
            def preserve(self):
                evidence.append('preserved')
        with patch.object(query_binding_harness, 'BindingFixture', OwnedFixture), \
             patch.object(query_binding_harness, 'commit_proxy', side_effect=RuntimeError('not ready')):
            with self.assertRaisesRegex(RuntimeError, 'not ready'):
                query_binding_harness.hot_path(Path('/unused'), [], 'hot-plain', 'owned')
        self.assertEqual(evidence, ['preserved'])
