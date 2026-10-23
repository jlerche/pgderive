"""Adversarial checks for canonical scope, trend regressions and paired controls."""
import copy
import gzip
import json
import pathlib
import sys
import tempfile
import unittest

sys.path.insert(0, str(pathlib.Path(__file__).resolve().parent))
from compare import compare_measurements, gate_failures, verdict
from metrics import execution_issues, extract_engine


def evidence(directory, *, late_latency=.01, late_processing=1, late_queue=0, late_requests=1):
    records, events, requests, samples, arrivals = [], [], [], [], {}
    for index in range(100):
        commit = 100 + index + .1
        records.append({'tick': index + 1, 'commit': commit, 'source_changes': 1, 'marker': True, 'source_wal_fence': f'0/{index + 1:X}'})
        arrivals[str(index + 1)] = commit + (late_latency if index >= 90 else .01)
        events.extend([{'event': 'published', '_observed_at': commit + .05,
                        'elapsed_ms': late_processing if index >= 90 else 1},
                       {'event': 'epoch', '_observed_at': commit + .05, 'received_to_sink_ms': 2}])
        if index % 5 == 0:
            events.append({'event': 'maintenance', '_observed_at': commit + .05, 'elapsed_us': 1000})
        samples.append({'seconds': index + .2, 'rss_bytes': 100, 'state': {'bytes': 200},
                        'retained_wal_bytes': 300,
                        'pending_source_changes_after_marker': late_queue if index >= 90 else 0})
        for attempt in range(late_requests if index >= 90 else 1):
            requests.append({'ordinal': len(requests) + 1, 'method': 'PUT', 'listing': False,
                             'completed_monotonic': commit + .05, 'status': 200,
                             'request_body_bytes': 10, 'response_body_bytes': 0})
    for at in [90, 210]:
        requests.append({'ordinal': len(requests) + 1, 'method': 'PUT', 'listing': False,
                         'completed_monotonic': at, 'status': 200,
                         'request_body_bytes': 1000000, 'response_body_bytes': 0})
    samples.append({**samples[-1], 'seconds': 100.2})
    with gzip.open(directory / 'timeline.json.gz', 'wt') as stream:
        json.dump({'start_monotonic': 100, 'transactions': records, 'sink_arrivals': arrivals}, stream)
    (directory / 'events.jsonl').write_text(''.join(json.dumps(event) + '\n' for event in events))
    (directory / 'proxy.jsonl').write_text(''.join(json.dumps(row) + '\n' for row in requests))
    engine = {'samples': samples, 'windows': [{'start_seconds': i, 'source_changes': 10} for i in range(0, 100, 10)],
              'transactions': 100, 'source_changes': 100, 'final_progress': {'source_wal_fence': '0/64', 'published_end_lsn': '0/64'}, 'process_cpu_seconds': .1, 'oracle_equal': True, 'ack_safe': True}
    parameters = {'duration': 100, 'window_seconds': 10, 'warmup_windows': 1, 'sample_seconds': 1}
    return engine, parameters


class MetricsTests(unittest.TestCase):
    def measured(self, directory, **kwargs):
        engine, parameters = evidence(directory, **kwargs)
        return extract_engine(directory, engine, parameters)

    def test_input_scope_excludes_bootstrap_shutdown_and_never_averages_percentiles(self):
        with tempfile.TemporaryDirectory() as name:
            result = self.measured(pathlib.Path(name), late_latency=.1)
        self.assertEqual(result['qualification_issues'], [])
        self.assertAlmostEqual(result['scalars']['sink_visibility_ms_p50'], 10)
        self.assertAlmostEqual(result['scalars']['sink_visibility_ms_p95'], 100)
        self.assertEqual(result['input_requests']['request_body_bytes'], 1000)
        self.assertEqual(result['lifecycle_requests']['request_body_bytes'], 2001000)
        self.assertEqual(result['scalars']['class_a_per_change'], 1)
        self.assertAlmostEqual(result['trends']['sink_visibility_ms_p95']['delta'], 90)

    def test_unchanged_global_median_cannot_hide_late_growth(self):
        with tempfile.TemporaryDirectory() as name:
            root = pathlib.Path(name)
            baseline = self.measured(root)
            candidate = self.measured(root, late_latency=.1)
        measurements = [{'baseline': baseline, 'candidate': candidate}] * 3
        result = compare_measurements(measurements, .1, 2, 1, throughput_valid=True)
        self.assertEqual(result['sink_visibility_ms_p50']['status'], 'within noise/practical margin')
        self.assertEqual(result['trend_sink_visibility_ms_p95']['status'], 'possible regression')

    def test_more_queue_and_operations_fail_independently_of_faster_freshness(self):
        with tempfile.TemporaryDirectory() as name:
            root = pathlib.Path(name)
            baseline = self.measured(root)
            candidate = self.measured(root, late_latency=.001, late_queue=20, late_requests=4)
        result = compare_measurements([{'baseline': baseline, 'candidate': candidate}] * 3, .1, 2, 1, throughput_valid=True)
        for name in ['peak_pending_changes', 'class_a_per_change', 'trend_class_a_per_change']:
            self.assertEqual(result[name]['status'], 'possible regression', name)

    def test_missing_events_duplicate_windows_nan_and_short_trends_fail_closed(self):
        with tempfile.TemporaryDirectory() as name:
            root = pathlib.Path(name)
            engine, parameters = evidence(root)
            engine['windows'].append(engine['windows'][0])
            with self.assertRaisesRegex(ValueError, 'duplicate'):
                extract_engine(root, engine, parameters)
            engine, parameters = evidence(root)
            engine['samples'][0]['rss_bytes'] = float('nan')
            with self.assertRaisesRegex(ValueError, 'finite'):
                extract_engine(root, engine, parameters)
            engine, parameters = evidence(root)
            (root / 'events.jsonl').write_text('')
            missing = extract_engine(root, engine, parameters)
            self.assertTrue(missing['qualification_issues'])
            self.assertIsNone(missing['scalars']['checkpoint_wall_ms_p95'])
            engine, parameters = evidence(root)
            parameters['warmup_windows'] = 8
            self.assertTrue(extract_engine(root, engine, parameters)['qualification_issues'])

    def test_overlapping_checkpoint_work_is_not_a_foreground_stall_fraction(self):
        with tempfile.TemporaryDirectory() as name:
            root = pathlib.Path(name)
            engine, parameters = evidence(root)
            events = [json.loads(line) for line in (root / 'events.jsonl').read_text().splitlines()]
            for event in events:
                if event['event'] == 'maintenance':
                    event['elapsed_us'] = 20000000
            (root / 'events.jsonl').write_text(''.join(json.dumps(event) + '\n' for event in events))
            result = extract_engine(root, engine, parameters)
        self.assertEqual(result['scalars']['checkpoint_wall_ms_per_change'], 4000)
        self.assertEqual(result['scalars']['processing_ms_p95'], 1)
        self.assertEqual(result['qualification_issues'], [])

    def test_execution_and_source_controls_reject_overlap_concurrency_and_different_state(self):
        reports = {label: {'source_digest': [100, 10, 'digest'], 'samples': []} for label in ['baseline', 'candidate']}
        manifest = {'parameters': {'execution_mode': 'sequential', 'pair_order': 'baseline-first', 'maximum_host_load_per_cpu': .25},
                    'execution_order': ['baseline', 'candidate'], 'execution_intervals': [
                        {'engine': 'baseline', 'started_at': 10, 'closed_at': 20, 'host_before': {'load_per_cpu': 0, 'external_performance_containers': []}, 'host_after': {'load_per_cpu': 0, 'external_performance_containers': []}},
                        {'engine': 'candidate', 'started_at': 20, 'closed_at': 30, 'host_before': {'load_per_cpu': 0, 'external_performance_containers': []}, 'host_after': {'load_per_cpu': 0, 'external_performance_containers': []}}]}
        self.assertEqual(execution_issues(manifest, reports), [])
        manifest['parameters']['pair_order'] = 'candidate-first'
        self.assertTrue(execution_issues(manifest, reports))
        manifest['parameters']['pair_order'] = 'baseline-first'
        manifest['execution_intervals'][0]['host_after']['external_performance_containers'] = ['competing-soak']
        self.assertTrue(execution_issues(manifest, reports))
        manifest['execution_intervals'][0]['host_after']['external_performance_containers'] = []
        manifest['execution_intervals'][1]['started_at'] = 19
        self.assertTrue(execution_issues(manifest, reports))
        manifest['execution_intervals'][1]['started_at'] = 20
        manifest['parameters']['execution_mode'] = 'concurrent'
        self.assertTrue(execution_issues(manifest, reports))
        manifest['parameters']['execution_mode'] = 'sequential'
        reports['candidate']['source_digest'][0] = 101
        self.assertTrue(execution_issues(manifest, reports))

    def test_nonfinite_arrival_and_request_times_are_rejected_before_clamping(self):
        for kind in ['arrival', 'request']:
            with tempfile.TemporaryDirectory() as name:
                root = pathlib.Path(name)
                engine, parameters = evidence(root)
                if kind == 'arrival':
                    with gzip.open(root / 'timeline.json.gz', 'rt') as stream:
                        timeline = json.load(stream)
                    timeline['sink_arrivals']['1'] = float('nan')
                    with gzip.open(root / 'timeline.json.gz', 'wt') as stream:
                        json.dump(timeline, stream)
                else:
                    row = json.loads((root / 'proxy.jsonl').read_text().splitlines()[0])
                    row['completed_monotonic'] = float('nan')
                    (root / 'proxy.jsonl').write_text(json.dumps(row) + '\n')
                with self.assertRaisesRegex(ValueError, 'finite'):
                    extract_engine(root, engine, parameters)

    def test_native_equality_cannot_hide_unconsumed_trailing_source_transactions(self):
        with tempfile.TemporaryDirectory() as name:
            root = pathlib.Path(name)
            engine, parameters = evidence(root)
            engine['final_progress']['published_end_lsn'] = '0/63'
            with self.assertRaisesRegex(ValueError, 'last source transaction'):
                extract_engine(root, engine, parameters)
            engine['final_progress']['published_end_lsn'] = '0/64'
            engine['source_changes'] = 99
            with self.assertRaisesRegex(ValueError, 'source changes differ'):
                extract_engine(root, engine, parameters)

    def test_drain_resource_peak_is_gated_even_when_input_peak_does_not_change(self):
        with tempfile.TemporaryDirectory() as name:
            root = pathlib.Path(name)
            engine, parameters = evidence(root)
            baseline = extract_engine(root, engine, parameters)
            engine['samples'][-1]['rss_bytes'] = 10000000
            engine['samples'][-1]['pending_source_changes_after_marker'] = 100
            candidate = extract_engine(root, engine, parameters)
        result = compare_measurements([{'baseline': baseline, 'candidate': candidate}] * 3, .1, 2, 1, throughput_valid=True)
        self.assertEqual(result['peak_rss_bytes']['status'], 'within noise/practical margin')
        self.assertEqual(result['measurement_and_drain_peak_rss_bytes']['status'], 'possible regression')
        self.assertEqual(result['drain_peak_pending_changes']['status'], 'possible regression')

    def test_signed_trends_preserve_improvements_and_gate_requires_both_orders(self):
        self.assertEqual(verdict([-10] * 3, [10] * 3, signed=True)['status'], 'possible regression')
        result = {'qualification': [{'issues': []}], 'pair_orders': ['baseline-first'],
                  'capacity': [{'passed': True}], 'exploratory': False,
                  'throughput': {'runs': [{'valid_for_latency_comparison': True}]}}
        self.assertIn('Both paired execution orders are required', gate_failures(result))
        result['pair_orders'].append('candidate-first')
        self.assertEqual(gate_failures(result), [])
        result['qualification'][0]['issues'].append('missing checkpoint observations')
        self.assertTrue(gate_failures(result))


if __name__ == '__main__':
    unittest.main()
