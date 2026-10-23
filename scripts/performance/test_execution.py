"""Sequential pairing must tear down every fixture before starting the next."""
import contextlib
import copy
import json
import pathlib
import sys
import tempfile
import types
import unittest
from unittest import mock

sys.path.insert(0, str(pathlib.Path(__file__).resolve().parent))
import matrix
import run as benchmark
import fixture as fixture_module


class ExecutionTests(unittest.TestCase):
    def execute(self, root, order, *, cleanup_failure=False, work_failure=False):
        events, active = [], set()
        class Fixture:
            def __init__(self, name, directory, _args, _binary):
                self.name, self.directory = name, directory
                self.fingerprint = {'stable': True}
                events.append(('create', directory.name))
            def start(self):
                if active:
                    raise AssertionError('Previous fixture still running')
                active.add(self.name)
                events.append(('start', self.directory.name))
            def close(self, success):
                events.append(('close', self.directory.name, success))
                if cleanup_failure and sum(event[0] == 'close' for event in events) == 1:
                    return ['injected cleanup failure']
                active.remove(self.name)
                return []
        args = types.SimpleNamespace(baseline=root/'baseline', candidate=root/'candidate',
            output=root/'evidence', risingwave=False, pair_order=order,
            execution_mode='sequential', postgres_image='postgres:17')
        def work(fixture, *_args):
            events.append(('measure', fixture.directory.name))
            if work_failure:
                raise RuntimeError('injected workload failure')
            return {'passed': True, 'transactions': 10, 'sink_visibility_ms': {'p50': 1}}
        with contextlib.ExitStack() as stack:
            for name, value in [('arguments', lambda: args), ('docker', lambda *_args: 'ok'),
                                ('sha256', lambda _path: 'hash'), ('Fixture', Fixture), ('run', work),
                                ('host_observation', lambda _prefix: {'load_per_cpu': 0, 'external_performance_containers': []})]:
                stack.enter_context(mock.patch.object(benchmark, name, value))
            stack.enter_context(mock.patch.object(benchmark.signal, 'signal'))
            stack.enter_context(mock.patch.object(benchmark.importlib.util, 'find_spec', return_value=True))
            stack.enter_context(mock.patch.object(benchmark.subprocess, 'check_output', side_effect=
                lambda command, **_kwargs: 'commit' if 'rev-parse' in command else b''))
            if cleanup_failure or work_failure:
                with self.assertRaises(RuntimeError):
                    benchmark.main()
            else:
                benchmark.main()
        return events, json.loads((args.output/'report.json').read_text()), json.loads((args.output/'manifest.json').read_text())

    def test_both_orders_fully_close_before_next_fixture(self):
        for order, expected in [('baseline-first', ['baseline', 'candidate']),
                                ('candidate-first', ['candidate', 'baseline'])]:
            with self.subTest(order=order), tempfile.TemporaryDirectory() as name:
                events, report, manifest = self.execute(pathlib.Path(name), order)
                self.assertTrue(report['passed'])
                self.assertEqual(manifest['execution_order'], expected)
                self.assertLess(events.index(('close', expected[0], True)), events.index(('create', expected[1])))
                left, right = manifest['execution_intervals']
                self.assertLessEqual(left['closed_at'], right['started_at'])

    def test_cleanup_failure_never_starts_another_engine_or_claims_pass(self):
        with tempfile.TemporaryDirectory() as name:
            events, report, _ = self.execute(pathlib.Path(name), 'baseline-first', cleanup_failure=True)
        self.assertFalse(report['passed'])
        self.assertEqual([label for action, label, *_ in events if action == 'create'], ['baseline'])
        self.assertEqual(report['cleanup_errors']['baseline'], ['injected cleanup failure'])
        self.assertEqual(sum(event[0] == 'close' for event in events), 2)

    def test_workload_failure_closes_owned_fixture_and_preserves_failure(self):
        with tempfile.TemporaryDirectory() as name:
            events, report, _ = self.execute(pathlib.Path(name), 'baseline-first', work_failure=True)
        self.assertFalse(report['passed'])
        self.assertIn(('close', 'baseline', False), events)
        self.assertIn('injected workload failure', report['error'])

    def test_partial_container_cleanup_retries_only_remaining_owned_containers(self):
        with tempfile.TemporaryDirectory() as name:
            fixture = fixture_module.Fixture('owned', pathlib.Path(name)/'fixture', None)
            fixture.containers = ['owned-a', 'owned-b']
            removals = []
            def docker(*arguments):
                if arguments[0] == 'inspect':
                    return json.dumps([{'Mounts': [], 'Config': {'Labels': {'pgderive.performance': 'owned'}}}])
                removals.append(arguments[-1])
                if removals == ['owned-b']:
                    raise OSError('injected Docker removal failure')
                return ''
            with mock.patch.object(fixture_module, 'docker', side_effect=docker), mock.patch.object(fixture_module.subprocess, 'run'):
                self.assertTrue(fixture.close())
                self.assertEqual(fixture.containers, ['owned-b'])
                self.assertEqual(fixture.close(), [])
            self.assertEqual(fixture.containers, [])
            attempts = [json.loads(line) for line in (fixture.directory/'cleanup-attempts.jsonl').read_text().splitlines()]
            self.assertTrue(attempts[0]['errors'])
            self.assertEqual(attempts[1]['errors'], [])
            self.assertEqual(removals, ['owned-b', 'owned-a', 'owned-b'])

    def test_qualification_catalog_has_enough_complete_trend_windows(self):
        specification = json.loads((pathlib.Path(matrix.__file__).parent/'scenarios.json').read_text())
        matrix.validate_catalog(specification)
        self.assertEqual(len(specification['qualification_profiles']), 6)
        for replacement in [{'window_seconds': 60}, {'window_seconds': 32}, {'warmup_windows': 9}]:
            invalid = copy.deepcopy(specification)
            invalid['scenarios']['storage-stress'].update(replacement)
            with self.subTest(parameters=replacement), self.assertRaisesRegex(ValueError, 'storage-stress: qualification'):
                matrix.validate_catalog(invalid)

    def test_matrix_alternates_actual_pair_order(self):
        with tempfile.TemporaryDirectory() as name, mock.patch.object(matrix.subprocess, 'run') as invoke:
            with mock.patch.object(sys, 'argv', ['matrix.py', '--baseline', 'a', '--candidate', 'b',
                    '--output', str(pathlib.Path(name)/'matrix'), '--scenarios', 'smoke', '--repeats', '3']):
                matrix.main()
        commands = [call.args[0] for call in invoke.call_args_list]
        self.assertEqual([command[command.index('--pair-order') + 1] for command in commands],
                         ['baseline-first', 'candidate-first', 'baseline-first'])
        self.assertTrue(all(command[command.index('--execution-mode') + 1] == 'sequential' for command in commands))


if __name__ == '__main__':
    unittest.main()
