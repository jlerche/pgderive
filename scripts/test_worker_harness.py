"""Failure evidence survives errors in both qualification and cleanup."""
import unittest
from worker_harness import Fixture


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
