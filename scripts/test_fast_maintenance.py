"""Acceptance evidence must expose cold work, deferred merges and uniform PUT inflation."""
import copy
import json
import pathlib
import tempfile
import unittest
from unittest import mock
import test_fast_cycles
from fast_maintenance import (cold_scaling, comparable_cycles, exact_work,
                              replay_costs, suite, validate_workload)
from performance.common import sha256


class MaintenanceTests(unittest.TestCase):
    def samples(self):
        rows = test_fast_cycles.CycleTests().samples()
        for index, sample in enumerate(rows):
            sample['cache'].update(dict.fromkeys(
                ('bytes', 'live_bytes', 'index_bytes', 'filter_bytes', 'point_bytes',
                 'point_entries', 'entries', 'pins'), 0))
            sample['cache']['merges'] = [0] * 64
            sample['cache']['merges'][6] = index // 10
            sample['merge_input_rows'] = [100] if index and index % 10 == 0 else []
            sample.update(checkpoint_lag_rows=0, checkpoint_age_ms=0, runs=4)
        return rows

    def report(self):
        samples = self.samples()
        final = dict.fromkeys(('gets', 'read_bytes', 'puts', 'put_bytes', 'cpu_ticks',
                               'elapsed_ns', 'checkpoint_lag_rows'), 0)
        return dict(case='synthetic-assessment-only', cold_reopen_equal=True,
                    stored_bytes=100, stored_objects=2, object_puts=samples[-1]['puts'],
                    lifecycle=dict(samples=samples, restarts=0, maintenance_quiescent=True,
                                   checkpoints=32, rebased_uploads=1, maximum_runs=4,
                                   maximum_memtable_bytes=4096, retirement=[dict(rss_bytes=1024)],
                                   final_merge_input_rows=[], final_maintenance=final,
                                   source_rows=159000, logical_ms=159000,
                                   durable_roots=2, durable_cohorts=1))

    def test_profile_reuses_endurance_with_declared_pressure_and_real_work(self):
        with mock.patch('fast_maintenance.endurance', side_effect=lambda *a, **k: (a, k)) as build:
            profile = suite()
        self.assertEqual(build.call_count, 3)
        self.assertEqual(profile['lifecycle']['source_rows_per_second'], 1000)
        self.assertEqual(profile['lifecycle']['checkpoint_interval_ms'], 128)
        self.assertEqual(profile['limits'], dict(cache_bytes=128*1024, cache_entries=64))
        self.assertEqual([case[0] for case in profile['cases']], [(512, 512), (8192, 512), (8192, 512)])
        self.assertEqual([case[1]['growing'] for case in profile['cases']], [False, False, True])
        self.assertTrue(all(case[1]['batch_rows'] == 64 for case in profile['cases']))

    def test_exact_work_rejects_missing_boolean_negative_and_mismatched_events(self):
        exact_work(self.samples())
        for events in (None, [True], [-1], [101, 101]):
            samples = self.samples()
            samples[10]['merge_input_rows'] = events
            with self.assertRaises(ValueError):
                exact_work(samples)

    def test_workload_change_cannot_silently_reuse_request_budget(self):
        with tempfile.TemporaryDirectory() as directory:
            path = pathlib.Path(directory)/'replay.json'
            path.write_text('{"source_changes":64}')
            digest = sha256(path)
            with mock.patch('fast_maintenance.request_budget', return_value={'trace_sha256': digest}):
                self.assertEqual(validate_workload(path), digest)
                path.write_text('{"source_changes":32}')
                with self.assertRaises(ValueError):
                    validate_workload(path)

    def test_same_histogram_tier_does_not_prove_comparable_large_merges(self):
        report = self.report()
        result = comparable_cycles(report)
        self.assertTrue(result['checks'][0]['passed'])
        for sample in report['lifecycle']['samples'][-30:]:
            if sample['merge_input_rows']:
                sample['merge_input_rows'] = [127]
        result = comparable_cycles(report)
        self.assertEqual(result['peak_input_rows'], 127)
        self.assertEqual(result['checks'][0]['actual'], 3)
        self.assertFalse(result['checks'][0]['passed'])

    def test_pending_results_are_not_installed_witnesses(self):
        report = self.report()
        for sample in report['lifecycle']['samples'][80:]:
            sample['upload_pending'] = True
        result = comparable_cycles(report)
        self.assertTrue(all(w['epoch'] < 80 for w in result['witnesses']))
        self.assertFalse(result['checks'][0]['passed'])

    def test_new_largest_merge_in_final_drain_cannot_be_hidden(self):
        report = self.report()
        report['lifecycle']['final_merge_input_rows'] = [1000]
        result = comparable_cycles(report)
        self.assertEqual(result['peak_input_rows'], 1000)
        self.assertFalse(result['checks'][0]['passed'])

    def test_uniform_put_inflation_fails_total_budget_despite_flat_trends(self):
        report = self.report()
        budget = dict(puts=report['object_puts'], put_bytes=report['lifecycle']['samples'][-1]['put_bytes'])
        self.assertTrue(replay_costs(report, budget)['qualified'])
        for sample in report['lifecycle']['samples']:
            sample['puts'] *= 2
            sample['put_bytes'] *= 2
        report['object_puts'] *= 2
        result = replay_costs(report, budget)
        self.assertFalse(result['qualified'])
        self.assertEqual({c['metric'] for c in result['checks'] if not c['passed']},
                         {'total_puts', 'total_put_bytes'})

    def test_final_drain_and_lag_cannot_move_cost_outside_samples(self):
        report = self.report()
        budget = dict(puts=10000, put_bytes=1000000)
        report['lifecycle']['final_maintenance']['puts'] = 100
        with self.assertRaises(ValueError):
            replay_costs(report, budget)
        report['object_puts'] += 100
        result = replay_costs(report, budget)
        self.assertIn('final_drain_puts', {c['metric'] for c in result['checks'] if not c['passed']})
        report['lifecycle']['maintenance_quiescent'] = False
        with self.assertRaises(ValueError):
            replay_costs(report, budget)

    def cold_rows(self):
        rows = []
        for size in (100, 1000, 10000):
            for layout in ('partitioned', 'broad'):
                rows.append(dict(layout=layout, cold_rows_per_side_per_cohort=size,
                                 source_row_changes=2, natural_merges=1, cold_oracle=True,
                                 merge_sizes=[1] + [0]*63,
                                 pending_upload=False, external_reloads=0,
                                 before_objects=['old'], before_cohorts=[1],
                                 after_objects=['new'], after_cohorts=[1],
                                 selected_durable_rows=10, selected_cold_rows=0,
                                 reads=2, read_bytes=1000, puts=2, put_bytes=1000,
                                 flush_ns=1, maximum_runs=2, final_runs=1))
        return rows

    def test_cold_scaling_flags_state_proportional_work_without_timing_gate(self):
        with tempfile.TemporaryDirectory() as directory:
            path = pathlib.Path(directory)/'cold.json'
            rows = self.cold_rows()
            path.write_text(json.dumps(rows))
            self.assertTrue(cold_scaling(path)['qualified'])
            rows[-1]['read_bytes'] = 100000
            rows[-1]['flush_ns'] = 0
            path.write_text(json.dumps(rows))
            result = cold_scaling(path)
            self.assertFalse(result['qualified'])
            self.assertEqual({c['metric'] for c in result['checks'] if not c['passed']}, {'read_bytes'})
            for invalid in (rows[:-1], rows + [copy.deepcopy(rows[0])]):
                path.write_text(json.dumps(invalid))
                with self.assertRaises(ValueError):
                    cold_scaling(path)
            for key, value in (('natural_merges', True), ('merge_sizes', [True]+[0]*63),
                               ('before_cohorts', [-1, 2])):
                invalid = copy.deepcopy(rows)
                invalid[0][key] = value
                path.write_text(json.dumps(invalid))
                with self.assertRaises(ValueError):
                    cold_scaling(path)
