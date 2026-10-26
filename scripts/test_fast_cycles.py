"""Cycle comparisons reject missing installation and preserve performance failures."""
import copy
import unittest
from fast_cycles import assess_cycles, boundary_indices


class CycleTests(unittest.TestCase):
    def samples(self):
        result = []
        for index in range(160):
            cache = dict(merges=[index//10], hits=index*3, misses=index,
                         decoded_bytes=index*100, decoded_rows=index*2,
                         index_reads=index, index_read_bytes=index*10,
                         index_requested_bytes=index*10,
                         io={'foreground': {'gets':index,'fetched_bytes':index*10}})
            result.append(dict(source_rows=index*1000, prepare_ns=10000, gets=index,
                               read_bytes=index*100, puts=index, put_bytes=index*50,
                               maintenance_ns=index*1000, installed_checkpoints=index//5,
                               upload_pending=False, cpu_ticks=index*2, cache=cache))
        return result

    def test_compares_complete_natural_intervals_without_preload(self):
        report = dict(case='assessment-only', lifecycle={'samples':self.samples()})
        result = assess_cycles(report)
        self.assertTrue(result['trends_passed'])
        self.assertEqual(result['early']['natural_merges'], 4)
        self.assertEqual(result['late']['natural_merges'], 4)
        self.assertLessEqual(result['early']['last_epoch'], result['late']['first_epoch'])
        self.assertEqual(result['early']['metrics']['gets_per_row'], .001)
        self.assertFalse(result['pending_upload_at_last_boundary'])
        self.assertTrue(result['largest_tier_repeat_coverage'])

    def test_many_small_merges_do_not_qualify_largest_tier_repetition(self):
        samples = self.samples()
        for index, sample in enumerate(samples):
            sample['cache']['merges'].append(int(index>=90))
        result = assess_cycles(dict(case='one-large-merge', lifecycle={'samples':samples}))
        self.assertTrue(result['trends_passed'])
        self.assertFalse(result['largest_tier_repeat_coverage'])

    def test_rejects_missing_cycles_pending_upload_and_regression(self):
        samples = self.samples()
        with self.assertRaises(ValueError):
            assess_cycles(dict(case='short', lifecycle={'samples':samples[:30]}))
        pending = copy.deepcopy(samples)
        for sample in pending:
            sample['upload_pending'] = True
        self.assertEqual(boundary_indices(pending), [])
        for key in ('installed_checkpoints',):
            invalid = copy.deepcopy(samples)
            invalid[-1][key] = 0
            with self.assertRaises(ValueError):
                boundary_indices(invalid)
        samples[-1]['cache']['merges'] = [0]
        with self.assertRaises(ValueError):
            boundary_indices(samples)

    def test_regressions_between_pending_samples_are_rejected(self):
        for field in ('installed_checkpoints', 'merges'):
            samples = self.samples()
            samples[110]['upload_pending'] = True
            samples[111]['upload_pending'] = True
            if field == 'merges':
                samples[110]['cache']['merges'] = [12]
                samples[111]['cache']['merges'] = [11]
            else:
                samples[110][field] = 23
                samples[111][field] = 22
            with self.assertRaises(ValueError):
                boundary_indices(samples)

    def test_worsening_cpu_and_io_remain_failed(self):
        samples = self.samples()
        for index in range(110,160):
            samples[index]['cpu_ticks'] += (index-109)*3
            samples[index]['gets'] += (index-109)*2
        result = assess_cycles(dict(case='worse', lifecycle={'samples':samples}))
        self.assertFalse(result['trends_passed'])
        failures = {check['metric'] for check in result['checks'] if not check['passed']}
        self.assertIn('cpu_ns_per_row', failures)
        self.assertIn('gets_per_row', failures)
