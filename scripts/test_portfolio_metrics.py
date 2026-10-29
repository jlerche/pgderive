"""Exercise the shared demo collector, per-query observer and retained histories."""
import importlib.util
import json
import pathlib
import shutil
import subprocess
import sys
import tempfile
import unittest
from unittest import mock

HERE = pathlib.Path(__file__).resolve().parents[1]/'examples/portfolio'


def module(name):
    with mock.patch.object(sys, 'path', [str(HERE), *sys.path]):
        spec = importlib.util.spec_from_file_location('portfolio_'+name, HERE/(name+'.py'))
        loaded = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(loaded)
        return loaded


class PortfolioMetrics(unittest.TestCase):
    @unittest.skipUnless(shutil.which('node'), 'optional Node.js executes the browser history regression')
    def test_production_history_shared_axes_and_query_errors(self):
        subprocess.run(['node', 'scripts/portfolio_history_probe.cjs'], cwd=HERE.parents[1], check=True, capture_output=True, text=True)

    def test_progress_observers_do_not_wait_for_result_changes(self):
        observer = module('observe_latency')
        totals, top = observer.QueryObserver(), observer.QueryObserver()
        event = {'tick': 1, 'committed_at_unix': 100, 'target_lsn': '0/20', 'source_changes': 2}
        for query in (totals, top):
            query.add([event])
        self.assertEqual(len(totals.advance('0/20', 100.01)), 1)
        self.assertEqual(top.advance('0/1F', 100.02), [])
        self.assertEqual(len(top.pending), 1)
        # This API has no sink-row input: unchanged top-ten output still publishes progress.
        self.assertEqual(len(top.advance('0/20', 100.03)), 1)
        self.assertEqual(top.advance('0/20', 100.04), [])
        self.assertAlmostEqual(totals.snapshot(101, 0)['write_to_sink']['p50_ms'], 10)
        self.assertAlmostEqual(top.snapshot(101, 0)['write_to_sink']['p50_ms'], 30)
        self.assertEqual(top.snapshot(162, 0)['write_to_sink']['samples'], 0)
        self.assertIsNone(top.snapshot(162, 0)['write_to_sink']['p50_ms'])

    def test_missing_progress_does_not_hide_other_query_and_expiry_is_bounded(self):
        observer = module('observe_latency')
        absent, healthy = observer.QueryObserver(), observer.QueryObserver()
        events = [{'tick': i, 'committed_at_unix': 100, 'target_lsn': '0/10', 'source_changes': 1} for i in range(4100)]
        for query in (absent, healthy):
            query.add(events)
        self.assertEqual(absent.dropped, 4)
        self.assertEqual(absent.advance(None, 110), [])
        self.assertIsNone(absent.snapshot(110, 0)['last_progress_poll_at'])
        self.assertEqual(len(healthy.advance('0/10', 110)), 4096)
        absent.advance(None, 221)
        self.assertEqual(absent.expired, 4096)
        self.assertEqual(len(absent.pending), 0)

    def test_observer_status_distinguishes_missing_stale_unavailable_and_zero_samples(self):
        collect = module('collect')
        self.assertEqual(collect.observer_status(None, 100), 'unconfigured')
        self.assertEqual(collect.observer_status({'observed_at': 80}, 100), 'stale')
        self.assertEqual(collect.observer_status({'observed_at': 100}, 100), 'unavailable')
        self.assertEqual(collect.observer_status({'observed_at': 100, 'last_progress_poll_at': 99}, 100), 'healthy')
        self.assertEqual(collect.observer_status({'observed_at': 101, 'last_progress_poll_at': 101}, 100), 'stale')
        with mock.patch.object(collect.subprocess, 'check_output', return_value=''), mock.patch.object(collect.time, 'time', return_value=100):
            missing = collect.latency('top', None)
            healthy = collect.latency('top', {'observed_at': 100, 'last_progress_poll_at': 100,
                                              'write_to_sink': {'samples': 0, 'p50_ms': None}})
        self.assertIsNone(missing['source_commit_to_sink']['samples'])
        self.assertEqual(healthy['source_commit_to_sink']['samples'], 0)
        self.assertEqual(healthy['published_source_transactions'], 0)

    def test_own_log_window_sums_source_transactions_and_keeps_latencies_separate(self):
        collect = module('collect')
        output = '\n'.join(json.dumps(row) for row in [
            {'event': 'published', 'elapsed_ms': 4, 'source_transactions': 3, 'phases_us': {'inputs': 1000}},
            {'event': 'published', 'elapsed_ms': 8, 'source_transactions': 2},
            {'event': 'epoch', 'received_to_sink_ms': 12}, {'event': 'maintenance', 'elapsed_us': 5000}])
        with mock.patch.object(collect.subprocess, 'check_output', return_value=output):
            result = collect.latency('top', None)
        self.assertEqual(result['published_epochs'], 2)
        self.assertEqual(result['published_source_transactions'], 5)
        self.assertEqual(result['engine']['p50_ms'], 4)
        self.assertEqual(result['write_to_sink']['p50_ms'], 12)
        self.assertEqual(result['checkpoint']['p50_ms'], 5)
        self.assertEqual(result['phases']['inputs']['p50_ms'], 1)

    def test_public_health_allowlist_preserves_zero_and_excludes_tokens(self):
        collect = module('collect')
        health = {'observed_at': 12, 'owner_live': True, 'fence_current': True, 'token': 'private',
                  'diagnostics': {'phase': 'ready', 'memtable_bytes': 0, 'runs': 2, 'cache': {'bytes': 123, 'token': 'private'},
                                  'object_service': {'foreground': {'requests': 0, 'written_bytes': 27, 'private': 9}, 'admission': {'private': 8}}}}
        result = collect.telemetry(health)
        self.assertEqual(result['memtable_bytes'], 0)
        self.assertEqual(result['cache'], {'bytes': 123})
        self.assertEqual(result['object_service'], {'foreground': {'requests': 0, 'written_bytes': 27}})
        self.assertNotIn('private', json.dumps(result))
        self.assertIsNone(result['memtable_limit'])
        with self.assertRaises(RuntimeError):
            collect.telemetry(health | {'fence_current': False})

    def test_query_failure_retains_original_age_and_does_not_borrow_other_measurements(self):
        collect = module('collect')
        spec = {'schema': 'demo_topk', 'table': 'book_top10', 'container': 'top'}
        prior = {'worker': {'observed_at': 10, 'rss_bytes': 99}, 'state': {'observed_at': 99, 'bytes': 7}}
        generator = {'transactions': 7, 'source_changes': 14}
        def sql(query):
            if 'pgderive_worker_health' in query:
                return json.dumps({'observed_at': 99, 'owner_live': True, 'fence_current': True,
                                   'diagnostics': {'cache': {'bytes': 456}, 'memtable_bytes': 0}})
            return json.dumps({'observed_at': 100, 'inserts': 1, 'updates': 2, 'deletes': 0, 'stats_reset': None})
        with mock.patch.object(collect.time, 'time', return_value=100), \
             mock.patch.object(collect, 'worker_memory', side_effect=RuntimeError('down')), \
             mock.patch.object(collect, 'latency', return_value={'observer_healthy': True, 'engine': {'p50_ms': 22}}):
            result = collect.collect_query(spec, prior, sql, {}, generator, {})
        self.assertEqual(result['worker'], prior['worker'])
        self.assertEqual(result['engine']['cache']['bytes'], 456)
        self.assertEqual(result['latency']['engine']['p50_ms'], 22)
        self.assertEqual(result['errors'], ['worker unavailable: RuntimeError'])
        self.assertEqual(result['flow']['transactions'], 7)

    def test_history_is_per_query_and_omits_stale_values_without_fabricating_zero(self):
        history = module('history')
        with tempfile.TemporaryDirectory() as directory:
            root = pathlib.Path(directory)
            (root/'stack-r2/web').mkdir(parents=True)
            (root/'metrics').mkdir()
            sample = {'collected_at': 100, 'queries': {
                'totals': {'worker': {'observed_at': 100, 'rss_bytes': 111}, 'errors': []},
                'top10': {'worker': {'observed_at': 100, 'rss_bytes': 222}, 'engine': {'observed_at': 1, 'memtable_bytes': 333},
                          'latency': {'observed_at': 100, 'published_epochs': 0}, 'errors': ['observer stale']}}}
            history.publish(root, sample)
            totals = json.loads((root/'stack-r2/web/metrics-history.json').read_text())
            top = json.loads((root/'stack-r2/web/metrics-history-top10.json').read_text())
            value = lambda document, key: document['rows'][0][document['columns'].index(key)+1]
            self.assertEqual(value(totals, 'worker.rss_bytes'), 111)
            self.assertEqual(value(top, 'worker.rss_bytes'), 222)
            self.assertEqual(value(top, 'latency.published_epochs'), 0)
            self.assertEqual(value(top, 'health.errors'), 1)
            self.assertNotIn('engine.memtable_bytes', top['columns'])
            self.assertNotIn('state.bytes', top['columns'])

    def test_first_publication_reuses_retained_per_query_history(self):
        history = module('history')
        with tempfile.TemporaryDirectory() as directory:
            root = pathlib.Path(directory)
            (root/'stack-r2/web').mkdir(parents=True)
            (root/'metrics').mkdir()
            retained = {'collected_at': 100, 'queries': {
                'totals': {'worker': {'observed_at': 100, 'rss_bytes': 111}},
                'top10': {'worker': {'observed_at': 100, 'rss_bytes': 222}}}}
            (root/'metrics/history.jsonl').write_text(json.dumps(retained)+'\n')
            history.publish(root, {'collected_at': 200, 'queries': {'totals': {}, 'top10': {}}})
            for name, expected in [('totals', 111), ('top10', 222)]:
                suffix = '' if name == 'totals' else '-top10'
                result = json.loads((root/('stack-r2/web/metrics-history'+suffix+'.json')).read_text())
                self.assertEqual(result['rows'][0][0], 100)
                self.assertEqual(result['rows'][0][result['columns'].index('worker.rss_bytes')+1], expected)
