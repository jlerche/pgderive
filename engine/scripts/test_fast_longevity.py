"""Long-lived evidence must show actual repeated work and complete coverage."""
import json
import pathlib
import tempfile
import unittest
from fast_longevity import CYCLES, SIZES, WARMUP, assess


class LongevityTest(unittest.TestCase):
    def test_complete_bounded_evidence_and_regressions(self):
        rows = [dict(rows=size, cycle=cycle, warmup=cycle<WARMUP, coalesced=True, changes=4096,
                     reads=20, read_bytes=2000, decoded_bytes=10000, virtual_io_us=20000,
                     wall_us=1000, filter_bytes=100, filter_entries=1, compactions=1,
                     compaction_reads=1, compaction_read_bytes=100, compaction_wall_us=1000,
                     cache_bytes=1000, cache_entries=1, live_bytes=1000, pins=0, runs=1, state_rows=size,
                     index_bytes=1000, index_entries=1, compaction_virtual_io_us=20000,
                     resident_root_bytes=1000, index_reads=1, index_read_bytes=1000,
                     index_requested_bytes=2000, foreground_read_peak=8,
                     index_decoded_bytes=500, index_encoded_bytes=500, index_encoded_entries=1,
                     data_reads=19, data_read_bytes=1000,
                     puts=16, put_bytes=10000, compaction_puts=3, compaction_put_bytes=100000,
                     ranges=dict(active_batches=0, pinned_bytes=0, pinned_windows=0, resident_bytes=100, resident_charge=200, resident_windows=1),
                     retirement=dict(gets=3, read_bytes=1000, cache_bytes=1000, deleted_objects=3,
                                     state_objects=3, state_bytes=size*100, rss_bytes=size*100,
                                     peak_rss_bytes=size*100,
                                     virtual_io_us=60000, wall_us=1000),
                     logical_elapsed_seconds=(index * (WARMUP+CYCLES) + cycle + 1) * 43200)
                for index, size in enumerate(SIZES) for cycle in range(WARMUP+CYCLES)]
        with tempfile.TemporaryDirectory() as directory:
            path = pathlib.Path(directory) / 'work.log'

            def run(data):
                path.write_text('\n'.join(json.dumps(row) for row in data))
                return assess(path)

            result = run(rows)
            self.assertTrue(result['bounded_cold_regime'])
            self.assertFalse(result['full_worker_steady_state_proven'])
            transitioned = [r | {'reads':2, 'data_reads':1} if r['warmup'] else r for r in rows]
            qualified = run(transitioned)
            self.assertTrue(qualified['bounded_cold_regime'])
            self.assertFalse(qualified['broad_transition_plateau_passed'])
            warmup_overflow = [rows[0] | {'cache_bytes':32 * 1024**2}, *rows[1:]]
            self.assertFalse(run(warmup_overflow)['bounded_cold_regime'])
            self.assertEqual(result['executed_changes'], len(SIZES) * (WARMUP+CYCLES) * 4096)
            path.write_text('\n'.join(json.dumps(r | {'warmup':False}) for r in rows if r['rows'] in SIZES[-2:] and r['cycle'] < 4))
            short = assess(path, cold=True)
            self.assertTrue(short['cold_capacity_passed'])
            self.assertFalse(short['bounded_cold_regime'])
            self.assertFalse(short['full_worker_steady_state_proven'])
            for key, bad in [('reads', 1000), ('cache_bytes', 32 * 1024**2)]:
                changed = [r | {key: bad} if r['cycle'] >= WARMUP + CYCLES - 7 else r for r in rows]
                self.assertFalse(run(changed)['bounded_cold_regime'])
            for field, bad in [('rss_bytes', 4 * 1024**3), ('gets', 1000)]:
                changed = [r | {'retirement': r['retirement'] | {field: bad}}
                           if r['cycle'] >= WARMUP + CYCLES - 7 else r for r in rows]
                self.assertFalse(run(changed)['bounded_cold_regime'])
            changed = [rows[0] | {'ranges': rows[0]['ranges'] | dict(active_batches=1, pinned_bytes=100, pinned_windows=1)}, *rows[1:]]
            self.assertFalse(run(changed)['bounded_cold_regime'])
            changed = [rows[0] | {'ranges': rows[0]['ranges'] | dict(resident_charge=5 * 1024**2)}, *rows[1:]]
            self.assertFalse(run(changed)['bounded_cold_regime'])
            for bad in (rows[:-1], rows + [rows[0]], list(reversed(rows)),
                        [rows[0] | {'logical_elapsed_seconds': 0}, *rows[1:]],
                        [rows[0] | {'reads': True}, *rows[1:]],
                        [rows[0] | {'index_bytes': 0}, *rows[1:]],
                        [rows[0] | {'warmup': 1}, *rows[1:]]):
                with self.assertRaises(ValueError):
                    run(bad)
