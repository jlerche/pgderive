"""Matrix grammar, explicit NULL/threshold semantics and saved-history invariants."""
import collections
import copy
import json
import pathlib
import unittest
from application_model import state
from composition_matrix_model import FAMILIES, expected, specification
from composition_matrix_trace import generate, validate


class CompositionMatrixTests(unittest.TestCase):
    def test_checked_in_manifest_matches_executable_grammar(self):
        manifest = json.loads(pathlib.Path('queries/composition/matrix.json').read_text())
        self.assertEqual(manifest['accepted'],[specification(f,10000) for f in FAMILIES])
        self.assertEqual(manifest['seeds'],list(range(10000,10010)))
        self.assertEqual(manifest['committed_transactions'],30)

    def test_all_eighty_histories_have_exact_commits_and_recovery(self):
        for family in FAMILIES:
            for seed in range(10000,10010):
                trace = generate(family,seed)
                validate(trace)
                self.assertEqual(trace,generate(family,seed))
                self.assertEqual(sum(not s.get('restart') and not s.get('rollback') for s in trace['history']),30)
                self.assertTrue(any(s.get('rollback') for s in trace['history']))
                self.assertEqual(sum(bool(s.get('restart')) for s in trace['history']),1)
                self.assertTrue(any(s.get('operations')==[] for s in trace['history']))

    def test_membership_does_not_multiply_right_duplicates_or_match_null(self):
        source = state({'account':[dict(id=1,label=None,cohort=2),dict(id=2,label=None,cohort=None)],
                        'entry':[dict(id=1,account_id=2,amount=None),dict(id=2,account_id=2,amount=7),dict(id=3,account_id=None,amount=1)],'region':[]})
        self.assertEqual(expected(specification('membership-group',10000),source),collections.Counter({(2,1):1}))
        self.assertEqual(expected(specification('membership-group',10001),source),collections.Counter({(None,1):1}))
        source['entry'].clear()
        self.assertEqual(expected(specification('membership-group',10000),source),collections.Counter())
        self.assertEqual(expected(specification('membership-group',10001),source),collections.Counter({(2,1):1,(None,1):1}))

    def test_left_filter_does_not_manufacture_unmatched_rows(self):
        source = state({'account':[dict(id=1,label=None,cohort=2),dict(id=2,label=None,cohort=None)],
                        'entry':[dict(id=1,account_id=2,amount=-1)],'region':[]})
        self.assertEqual(expected(specification('left-group',10000),source),collections.Counter({(None,1,0,None):1}))

    def test_version_one_traces_remain_replayable(self):
        trace = generate('distinct',10000)
        trace['matrix'] = specification('distinct',10000,1)
        validate(trace)
        source = state(trace['initial'])
        self.assertNotEqual(expected(trace['matrix'],source),expected(specification('distinct',10000),source))

    def test_trace_validation_rejects_drift_before_connecting(self):
        trace = generate('three-join',10000)
        altered = copy.deepcopy(trace)
        altered['matrix']['sql'] += ' LIMIT 1'
        with self.assertRaises(AssertionError): validate(altered)
        altered = copy.deepcopy(trace)
        altered['initial']['region'][0]['id'] = '1'
        with self.assertRaises(AssertionError): validate(altered)


if __name__=='__main__': unittest.main()
