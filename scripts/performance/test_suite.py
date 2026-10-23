"""Pure harness tests; no infrastructure or application data touched."""
import pathlib
import json
import sys
import types
import copy
import tempfile
from unittest import mock
import unittest

sys.path.insert(0, str(pathlib.Path(__file__).resolve().parent))
from common import ROOT, distribution, wait
from compare import validate_runs, gate_failures, environment_mismatches, latency_verdict, throughput_check, verdict
from fixture import (Fixture, effective_proxy_profile,
                     effective_risingwave_sql, effective_worker_config)
from run import close_fixtures
from summarize import marker_progress, object_requests
from workload import observe_prefix, operation


class SuiteTests(unittest.TestCase):
    def test_object_request_accounting_separates_attempts_errors_and_unknown_posts(self):
        def request(method, listing=False, status=200, size=10):
            return {'ordinal': 1, 'method': method, 'listing': listing, 'status': status,
                    'request_body_bytes': size, 'response_body_bytes': 20}
        rows = [{'ready': True}, {'error': 'failed', 'method': 'PUT'},
                request('PUT', status=502), request('PUT'), request('GET', listing=True),
                request('GET'), request('HEAD'), request('DELETE'), request('POST', size=None)]
        with tempfile.TemporaryDirectory() as directory:
            root = pathlib.Path(directory)
            (root / 'proxy.jsonl').write_text(''.join(json.dumps(row)+'\n' for row in rows))
            result = object_requests(root)
        self.assertEqual(result['attempts'], 7)
        self.assertEqual(result['estimated_r2_classes'], {
            'class_a': 3, 'class_b': 2, 'free': 1, 'unclassified': 1})
        self.assertEqual(result['transport_errors'], 1)
        self.assertEqual(result['status_codes']['502'], 1)
        self.assertEqual(result['request_body_bytes'], 60)
        self.assertEqual(result['response_body_bytes'], 140)
        self.assertEqual(result['unknown_request_body_sizes'], 1)

    def test_gate_requires_distinct_and_matching_run_evidence(self):
        reports = [{'run_id': str(i)} for i in range(3)]
        paths = ['a', 'b', 'c']
        validate_runs(paths, reports, copy.deepcopy(reports))
        for files, runs, manifests in [
                (['a']*3, reports, reports),
                (paths, [reports[0]]*3, [reports[0]]*3),
                (paths, reports, list(reversed(reports))),
                (paths, [{'run_id': None}]*3, reports)]:
            with self.assertRaises(ValueError):
                validate_runs(files, runs, manifests)

    def test_verdict_rejects_invalid_metric_samples(self):
        for values in [[], [float('nan')], [float('inf')], [-1], [None], [True]]:
            with self.assertRaises(ValueError):
                verdict(values, [1, 1, 1])
            with self.assertRaises(ValueError):
                verdict([1, 1, 1], values)

    def test_gate_rejects_inconclusive_regressing_and_unmatched_evidence(self):
        result = {'exploratory': False, 'throughput': {'runs': [
            {'valid_for_latency_comparison': True}]},
            'latency': verdict([10, 10, 10], [10, 10, 10])}
        self.assertEqual(gate_failures(result), [])
        for evidence in [verdict([10], [10]), verdict([10]*3, [20]*3)]:
            self.assertTrue(gate_failures({**result, 'latency': evidence}))
        self.assertTrue(gate_failures({**result, 'exploratory': True}))
        self.assertTrue(gate_failures({**result, 'throughput': {'runs': [
            {'valid_for_latency_comparison': False}]}}))

    def test_child_exit_is_not_a_retryable_startup_error(self):
        predicate = mock.Mock(side_effect=ChildProcessError('engine exited'))
        with self.assertRaises(ChildProcessError):
            wait(predicate)
        predicate.assert_called_once()

    def test_effective_worker_fingerprint_preserves_policy_not_secrets_or_ports(self):
        first = '[postgres]\nhost="localhost"\nport=1000\npassword="private"\ntls="disable"\n[worker]\nmaintenance_ticks=16\n[execution]\nresident_bytes=100\n[listener]\nmax_transaction_changes=99\n'
        second = first.replace('1000', '2000').replace('private', 'other-secret')
        self.assertEqual(effective_worker_config(first), effective_worker_config(second))
        for before, after in [('maintenance_ticks=16', 'maintenance_ticks=32'),
                              ('resident_bytes=100', 'resident_bytes=200'),
                              ('max_transaction_changes=99', 'max_transaction_changes=199')]:
            self.assertNotEqual(effective_worker_config(first), effective_worker_config(first.replace(before, after)))
        self.assertNotIn('private', str(effective_worker_config(first)))

    def test_proxy_profile_and_rw_sql_fingerprints(self):
        profile = {'ready': True, 'port': 1234, 'upstream': 'localhost:3333',
                   'seed': 42, 'scale': 1, 'anchors_ms': {'PUT': [70, 100, 137]}}
        changed_port = {**profile, 'port': 9876, 'upstream': 'localhost:4444'}
        self.assertEqual(effective_proxy_profile(profile), effective_proxy_profile(changed_port))
        for key, value in [('seed', 43), ('scale', 3), ('anchors_ms', {'PUT': [90, 100, 137]})]:
            self.assertNotEqual(effective_proxy_profile(profile), effective_proxy_profile({**profile, key: value}))
        sql = "CREATE SOURCE x WITH(hostname='localhost',port='1234',password='private',transactional='true')"
        self.assertEqual(effective_risingwave_sql([sql]),
                         effective_risingwave_sql([sql.replace('1234', '9876').replace('private', 'other')]))
        self.assertNotEqual(effective_risingwave_sql([sql]),
                            effective_risingwave_sql([sql.replace("transactional='true'", "transactional='false'")]))
        self.assertNotIn('private', str(effective_risingwave_sql([sql])))

    def test_progress_distinguishes_source_acceptance_from_consumer_cutoff(self):
        timeline = {'start_monotonic': 100, 'transactions': [
            {'tick': 1, 'commit': 100.1, 'marker': True},
            {'tick': 2, 'commit': 100.2, 'marker': True},
            {'tick': 3, 'commit': 100.3, 'marker': True}]}
        result = marker_progress(timeline, 1, {'1': 100.5, '2': 101.1, '3': 101.8}, 'updates')
        self.assertEqual(result['source_commits_in_input_window'], 3)
        self.assertEqual(result['visible_marker_prefix_transactions_at_input_end'], 1)
        self.assertAlmostEqual(result['last_marker_drain_ms'], 800)
        self.assertFalse(result['prefix_is_lower_bound'])

    def test_progress_preserves_client_ack_race_without_inventing_commit_time(self):
        timeline = {'start_monotonic': 100, 'transactions': [
            {'tick': 1, 'commit': 100.9, 'marker': True},
            {'tick': 2, 'commit': 101.01, 'marker': True}]}
        result = marker_progress(timeline, 1, {'1': 100.95, '2': 100.99}, 'updates')
        self.assertEqual(result['source_commits_in_input_window'], 1)
        self.assertEqual(result['visible_marker_prefix_transactions_at_input_end'], 2)
        self.assertEqual(result['last_marker_drain_ms'], 0)

    def test_mixed_marker_progress_is_lower_bound_and_unknown_drain_stays_unknown(self):
        timeline = {'start_monotonic': 100, 'transactions': [
            {'tick': tick, 'commit': 100 + tick / 10, 'marker': tick % 3 == 1}
            for tick in range(1, 7)]}
        result = marker_progress(timeline, 1, {'1': 100.5}, 'mixed')
        self.assertTrue(result['prefix_is_lower_bound'])
        self.assertEqual(result['source_commits_in_input_window'], 6)
        self.assertEqual(result['visible_marker_prefix_transactions_at_input_end'], 1)
        self.assertIsNone(result['last_marker_drain_ms'])
        self.assertFalse(result['last_marker_observed'])

    def test_distribution(self):
        self.assertIsNone(distribution([]))
        self.assertEqual(distribution(range(1, 101))['p95'], 95)
        self.assertEqual(distribution([2])['p99'], 2)

    def test_noise_not_row_count(self):
        self.assertIn('inconclusive', verdict([10], [100])['status'])
        self.assertEqual(verdict([10, 10, 10], [20, 20, 20])['status'], 'possible regression')
        self.assertEqual(verdict([10, 20, 30], [20, 25, 30])['status'], 'within noise/practical margin')

    def test_slow_low_latency_producer_cannot_be_a_win(self):
        manifest = {'parameters': {'rate': 1000, 'duration': 60, 'window_seconds': 60}}
        def engine(count):
            return {'windows': [{'start_seconds': 0, 'transactions': count}]}
        report = {'engines': {'baseline': engine(60000), 'candidate': engine(6000)}}
        check = throughput_check(report, manifest)
        self.assertFalse(check['valid_for_latency_comparison'])
        result = latency_verdict([100, 100, 100], [1, 1, 1], check['valid_for_latency_comparison'])
        self.assertIn('invalid latency comparison', result['status'])
        report['engines']['candidate'] = engine(60000)
        self.assertTrue(throughput_check(report, manifest)['valid_for_latency_comparison'])

    def test_mutable_image_and_host_changes_rejected(self):
        manifest = {'host': {'cpu_affinity': [0, 1]},
                    'fixtures': {'baseline': {'images': {'pg': 'sha256:first'}, 'resources': {'Memory': 4000}}}}
        self.assertEqual(environment_mismatches([manifest, copy.deepcopy(manifest)]), [])
        image_changed = copy.deepcopy(manifest)
        image_changed['fixtures']['baseline']['images']['pg'] = 'sha256:second'
        self.assertTrue(environment_mismatches([manifest, image_changed]))
        host_changed = copy.deepcopy(manifest); host_changed['host']['cpu_affinity'] = [2, 3]
        self.assertTrue(environment_mismatches([manifest, host_changed]))
        resource_changed = copy.deepcopy(manifest)
        resource_changed['fixtures']['baseline']['resources']['Memory'] = 8000
        self.assertTrue(environment_mismatches([manifest, resource_changed]))
        self.assertTrue(environment_mismatches([{}]))

    def test_cleanup_continues_after_process_and_file_races(self):
        with tempfile.TemporaryDirectory() as temporary:
            fixture = Fixture('owned', pathlib.Path(temporary) / 'fixture', None)
            process = mock.Mock(returncode=0)
            process.poll.return_value = None
            process.send_signal.side_effect = ProcessLookupError('already gone')
            fixture.processes = [process]
            broken, remaining = mock.Mock(), mock.Mock()
            broken.close.side_effect = OSError('file close')
            fixture.files = [broken, remaining]
            errors = fixture.close()
            self.assertEqual(len(errors), 2)
            process.wait.assert_called_once()
            remaining.close.assert_called_once()

    def test_connections_bound_statement_and_lock_waits(self):
        with tempfile.TemporaryDirectory() as temporary:
            fixture = Fixture('owned', pathlib.Path(temporary) / 'fixture', None)
            fixture.pg_port = 1234
            with mock.patch('psycopg.connect') as connect:
                fixture.connect()
                options = connect.call_args.kwargs['options']
                self.assertIn('statement_timeout=10000', options)
                self.assertIn('lock_timeout=3000', options)
                self.assertEqual(connect.call_args.kwargs['connect_timeout'], 3)

    def test_failed_initialization_and_fixture_cleanup_do_not_skip_others(self):
        first, second = mock.Mock(), mock.Mock()
        first.directory.name = 'first'; second.directory.name = 'second'
        first.close.return_value = []
        second.close.side_effect = OSError('failed fixture cleanup')
        result = close_fixtures([first, second], False)
        first.close.assert_called_once_with(success=False)
        self.assertTrue(result['second'])
        with tempfile.TemporaryDirectory() as temporary:
            fixture = Fixture('owned', pathlib.Path(temporary) / 'fixture', None)
            self.assertEqual(fixture.close(), [])

    def test_autocommit_single_statement_and_mixed_identity(self):
        args = types.SimpleNamespace(workload='mixed', state_rows=12000)
        ids = set(range(1, args.state_rows + 2))
        updates = inserts = deletes = 0
        for tick in range(1, 10001):
            sql, params, marker = operation(tick, args)
            self.assertNotIn(';', sql)
            if sql.startswith('UPDATE'):
                updates += 1
                self.assertTrue(marker)
                self.assertEqual(params, (tick,))
            elif sql.startswith('INSERT'):
                inserts += 1
                self.assertNotIn(params[0], ids)
                ids.add(params[0]); self.assertGreater(params[1], 1)
            else:
                deletes += 1
                self.assertIn(params[0], ids); ids.remove(params[0])
        self.assertGreater(inserts, deletes)
        self.assertGreater(deletes, 0)
        self.assertGreater(updates, 0)

    def test_visible_before_client_ack_does_not_drop_marker(self):
        records, seen = [], {}
        index = observe_prefix(records, 0, 10, seen, 100)
        self.assertEqual(index, 0)
        records.extend([{'tick': 1, 'marker': True}, {'tick': 2, 'marker': False},
                        {'tick': 3, 'marker': True}])
        index = observe_prefix(records, index, 10, seen, 101)
        self.assertEqual(index, 3)
        self.assertEqual(seen, {1: 101, 3: 101})
        self.assertEqual(observe_prefix(records, index, 10, seen, 102), 3)
        self.assertEqual(seen[1], 101)

    def test_updates_have_every_transaction_marker(self):
        args = types.SimpleNamespace(workload='updates')
        for tick in range(1, 10):
            self.assertTrue(operation(tick, args)[2])


if __name__ == '__main__':
    unittest.main()
