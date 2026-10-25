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

    def test_assessment_rejects_cost_growth_restarts_and_missing_cycles(self):
        samples = []
        for index in range(128):
            samples.append(dict(source_rows=index*1000, prepare_ns=1000, gets=index,
                                read_bytes=index*100, puts=index, put_bytes=index*100,
                                maintenance_ns=index*1000,
                                cache=dict(bytes=1, live_bytes=1, index_bytes=1,
                                           filter_bytes=1, pins=0, merges=[4])))
        report = dict(case='synthetic-assessment-only', cold_reopen_equal=True,
                      lifecycle=dict(samples=samples, source_rows=127000, logical_ms=127000,
                                     restarts=0, checkpoints=25, rebased_uploads=24,
                                     maximum_runs=6, maximum_memtable_bytes=1024,
                                     retirement=[dict(rss_bytes=1024)]))
        with tempfile.TemporaryDirectory() as directory:
            path = pathlib.Path(directory)/'engine.json'
            def check(value):
                path.write_text(json.dumps([value]))
                return assess(path)['qualified']
            self.assertTrue(check(report))
            growing = copy.deepcopy(report)
            for sample in growing['lifecycle']['samples'][96:]:
                sample['prepare_ns'] *= 2
            self.assertFalse(check(growing))
            oversized = copy.deepcopy(report)
            oversized['lifecycle']['samples'][-1]['cache']['bytes'] = 65*1024**2
            self.assertFalse(check(oversized))
            for field, value in [('restarts', 1), ('checkpoints', 1)]:
                invalid = copy.deepcopy(report)
                invalid['lifecycle'][field] = value
                with self.assertRaises(ValueError):
                    check(invalid)
