"""Reject incomplete evidence and improvements falsely presented as a plateau."""
import json
import pathlib
import tempfile
import unittest
from fast_steady import SIZES, assess


class FastSteadyTest(unittest.TestCase):
    def test_growth_failure_is_distinct_from_control_improvement(self):
        rows = [dict(rows=size, cycle=cycle, coalesced=coalesced, changes=4096,
                     reads=20 if coalesced else 40, read_bytes=2000 if coalesced else 4000,
                     decoded_bytes=10000, virtual_io_us=20000, wall_us=1000,
                     filter_bytes=100, filter_entries=1, compactions=1,
                     compaction_reads=1, compaction_read_bytes=100, compaction_wall_us=1000)
                for size in SIZES for cycle in range(2) for coalesced in (False, True)]
        with tempfile.TemporaryDirectory() as directory:
            path = pathlib.Path(directory)/'work.log'
            path.write_text('\n'.join(json.dumps(row) for row in rows))
            result = assess(path)
            self.assertTrue(result['non_regression'])
            self.assertTrue(result['component_plateau'])
            self.assertFalse(result['full_worker_steady_state_proven'])
            growing = [row | {'reads':30} if row['rows']==SIZES[-1] and row['coalesced'] else row for row in rows]
            path.write_text('\n'.join(json.dumps(row) for row in growing))
            result = assess(path)
            self.assertTrue(result['non_regression'])
            self.assertFalse(result['component_plateau'])
            path.write_text('\n'.join(json.dumps(row | {'decoded_bytes':20000} if row['coalesced'] else row) for row in rows))
            self.assertFalse(assess(path)['non_regression'])
            for broken in (rows[:-1], rows+[rows[0]], [rows[0] | {'reads':True}, *rows[1:]], [rows[2] | {'cycle':True}, *rows[:2], *rows[3:]]):
                path.write_text('\n'.join(json.dumps(row) for row in broken))
                with self.assertRaises(ValueError):
                    assess(path)
