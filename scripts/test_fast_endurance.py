"""Endurance qualification rejects incomplete work and worsening component costs."""
import copy
import json
import pathlib
import tempfile
import unittest
from fast_endurance import assess
from fast_replay import delta, endurance


class EnduranceTests(unittest.TestCase):
    def test_repeated_unchanged_updates_and_missing_deletes_count_affected_rows(self):
        row = {'id': 1, 'account_id': 2, 'amount': None}
        source = {'entry': {1: row.copy()}, 'account': {}}
        transaction = {'rollback': False, 'operations': [
            {'table': 'entry', 'kind': 'upsert', 'row': row.copy()},
            {'table': 'entry', 'kind': 'upsert', 'row': dict(row, amount=-3)},
            {'table': 'entry', 'kind': 'delete', 'id': 1},
            {'table': 'entry', 'kind': 'delete', 'id': 1}]}
        changes, count = delta(source, transaction)
        self.assertEqual(count, 3)
        self.assertEqual([item['weight'] for item in changes], [-1, 1, -1, 1, -1])
        self.assertEqual(source['entry'], {})
        self.assertEqual(delta(source, dict(transaction, rollback=True)), ([], 0))

    def test_generator_keeps_live_history_and_reuses_oracles(self):
        case = endurance(200, 32, batch_rows=40)
        self.assertFalse(any(step['restart'] for step in case['steps']))
        self.assertTrue(all(0 < step['source_rows'] <= 40 for step in case['steps'][1:]))
        self.assertIsNotNone(case['steps'][-1]['expected'])
        self.assertGreater(len(case['steps'][-1]['expected']), len(case['steps'][0]['expected']))
        changes = case['steps'][1]['updates']
        self.assertTrue(any(change['weight'] < 0 for change in changes))
        self.assertTrue(any(change['weight'] > 0 for change in changes))

    def test_fixed_churn_preserves_live_identity_domain_and_checks_oracles(self):
        case = endurance(200, 32, batch_rows=40, growing=False,
                         group_width=1, oracle_interval=8)
        live = {}
        for step in case['steps']:
            self.assertLessEqual(step['source_rows'], 40)
            for change in step['updates']:
                if change['table'] != 'entry':
                    continue
                key = change['row']['id']
                if change['weight'] == -1:
                    self.assertEqual(live.pop(key), change['row'])
                else:
                    self.assertNotIn(key, live)
                    live[key] = change['row']
            self.assertEqual(set(live), {str(key) for key in range(1, 201)})
        self.assertEqual(sum(step['expected'] is not None for step in case['steps']), 5)
        self.assertFalse(any(step['restart'] for step in case['steps']))
        self.assertTrue(any(change['row'].get('amount') is None
                            for step in case['steps'][1:] for change in step['updates']))
        for keyword in ('rows','epochs','batch_rows','group_width','oracle_interval'):
            arguments = dict(rows=200, epochs=32, batch_rows=40, group_width=1, oracle_interval=8)
            arguments[keyword] = 0
            with self.assertRaises(ValueError):
                endurance(**arguments)

    def test_growing_hot_working_set_leaves_retained_cold_rows_untouched(self):
        case = endurance(200, 32, batch_rows=40, growing=True, hot_rows=5,
                         group_width=1, oracle_interval=8)
        for step in case['steps'][1:]:
            self.assertEqual(step['source_rows'], 40)
            for change in step['updates']:
                key = int(change['row']['id'])
                self.assertTrue(key <= 5 or key > 200)
                if change['weight'] < 0:
                    self.assertLessEqual(key, 5)
        self.assertEqual(len(case['steps'][-1]['expected']), 328)
        for size in (0, 201):
            with self.assertRaises(ValueError):
                endurance(200, 32, hot_rows=size)

    def test_random_initial_amounts_control_keeps_transaction_sequence(self):
        arguments = dict(rows=1000, epochs=8, batch_rows=100, group_width=1,
                         oracle_interval=4, hot_rows=100)
        original = endurance(**arguments)
        control = endurance(**arguments, initial_random_amounts=True)
        self.assertEqual(control, endurance(**arguments, initial_random_amounts=True))
        self.assertIn('-random-initial-', control['name'])
        initial = [change['row'] for change in control['steps'][0]['updates']
                   if change['table'] == 'entry']
        self.assertGreater(len({row['amount'] for row in initial}), 500)
        for index, row in enumerate(initial):
            position = index % arguments['batch_rows']
            if position % 10 != 0 and position % 17 == 0:
                self.assertIsNone(row['amount'])
            else:
                self.assertTrue(-2500 <= int(row['amount']) <= 2500)
        oracle = {cells[0]: cells[1:] for cells, weight in control['steps'][0]['expected']
                  if weight == 1}
        self.assertEqual(oracle, {row['account_id']: ['1', row['amount']] for row in initial})
        self.assertNotEqual(original['steps'][0]['expected'], control['steps'][0]['expected'])
        for before, after in zip(original['steps'][1:], control['steps'][1:], strict=True):
            self.assertEqual(before['source_rows'], after['source_rows'])
            # New values and chosen keys are identical; only retractions of
            # untouched initial values and their corresponding oracles differ.
            positive = lambda step: [change for change in step['updates'] if change['weight'] > 0]
            self.assertEqual(positive(before), positive(after))
            self.assertEqual(before['expected'] is None, after['expected'] is None)

    def test_assessment_rejects_cost_growth_restarts_and_missing_cycles(self):
        samples = []
        for index in range(128):
            samples.append(dict(source_rows=index*1000, prepare_ns=1000, gets=index,
                                read_bytes=index*100, puts=index, put_bytes=index*100,
                                maintenance_ns=index*1000,
                                cpu_ticks=index*2, installed_checkpoints=index//4,
                                upload_pending=False, consolidations=index//8, consolidation_pending=False,
                                cache=dict(bytes=1, entries=1, live_bytes=1, index_bytes=1,
                                           filter_bytes=1, pins=0, merges=[index//8],
                                           hits=index, misses=index, decoded_bytes=index,
                                           decoded_rows=index, point_bytes=0, point_entries=0, index_reads=index,
                                           index_read_bytes=index, index_requested_bytes=index,
                                           io={'foreground':{'gets':index,'fetched_bytes':index}})))
        report = dict(case='synthetic-assessment-only', cold_reopen_equal=True,
                      lifecycle=dict(samples=samples, source_rows=127000, logical_ms=127000,
                                     restarts=0, checkpoints=25, rebased_uploads=24,
                                     maximum_runs=6, maximum_memtable_bytes=1024,
                                     retirement=[dict(rss_bytes=1024)]))
        with tempfile.TemporaryDirectory() as directory:
            path = pathlib.Path(directory)/'engine.json'
            def check(value, cache_mib=64):
                path.write_text(json.dumps([value]))
                return assess(path, cache_bytes=cache_mib*1024**2, cache_entries=512)['qualified']
            self.assertTrue(check(report))
            growing = copy.deepcopy(report)
            for sample in growing['lifecycle']['samples'][96:]:
                sample['prepare_ns'] *= 2
            self.assertFalse(check(growing))
            oversized = copy.deepcopy(report)
            oversized['lifecycle']['samples'][-1]['cache']['bytes'] = 65*1024**2
            self.assertFalse(check(oversized))
            self.assertFalse(check(oversized, cache_mib=16))
            self.assertTrue(check(oversized, cache_mib=128))
            sixteen = copy.deepcopy(report)
            sixteen['lifecycle']['samples'][-1]['cache']['bytes'] = 16*1024**2+1
            self.assertFalse(check(sixteen, cache_mib=16))
            entries = copy.deepcopy(report)
            entries['lifecycle']['samples'][-1]['cache']['entries'] = 513
            self.assertFalse(check(entries))
            for field, value in [('restarts', 1), ('checkpoints', 1)]:
                invalid = copy.deepcopy(report)
                invalid['lifecycle'][field] = value
                with self.assertRaises(ValueError):
                    check(invalid)
