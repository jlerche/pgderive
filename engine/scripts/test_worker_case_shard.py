"""Sharding preserves the complete live history inventory."""
import os
import unittest
from unittest.mock import patch

from worker_case_shard import select_cases


class CaseShardTests(unittest.TestCase):
    def test_shards_partition_histories_once_and_reject_invalid_selection(self):
        cases = list('abcdefg')
        selected = []
        for index in range(3):
            with patch.dict(os.environ, {'PGDERIVE_TEST_CASE_SHARD': f'{index}/3'}):
                selected.extend(select_cases(iter(cases)))
        self.assertEqual(selected, list('adgbecf'))

        for invalid in ('3/3', '0/8'):
            with patch.dict(os.environ, {'PGDERIVE_TEST_CASE_SHARD': invalid}):
                with self.assertRaisesRegex(ValueError, 'Invalid case shard'):
                    select_cases(cases)
