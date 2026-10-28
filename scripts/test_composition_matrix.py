"""Matrix grammar, explicit NULL/threshold semantics and saved-history invariants."""
import collections
import copy
import json
import pathlib
import tempfile
from unittest import mock
from application_portfolio_harness import replay
from verify_composition_matrix import case
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

    def test_obsolete_traces_rejected_before_fixture_or_output_creation(self):
        trace = generate('distinct',10000)
        obsolete = copy.deepcopy(trace)
        obsolete['matrix']['version'] = 1
        envelope = {**trace,'spec':{'recipe':'projection'}}
        with tempfile.TemporaryDirectory() as directory:
            output = pathlib.Path(directory)/'replay'
            factory = mock.Mock()
            for candidate in (obsolete,envelope):
                with self.assertRaises(AssertionError):
                    replay(output,[],candidate,fixture_factory=factory,validator=validate)
                factory.assert_not_called()
                self.assertFalse(output.exists())

    def test_mislabeled_evidence_rejected_without_rewriting(self):
        with tempfile.TemporaryDirectory() as directory:
            path = pathlib.Path(directory)
            (path/'replay.json').write_text(json.dumps(generate('distinct',10000)))
            result = path/'result.json'
            original = json.dumps({'query':{'recipe':'projection'}})
            result.write_text(original)
            with self.assertRaises(AssertionError): case(path)
            self.assertEqual(result.read_text(),original)
            self.assertEqual({file.name for file in path.iterdir()},{'replay.json','result.json'})

    def test_trace_validation_rejects_drift_before_connecting(self):
        trace = generate('three-join',10000)
        altered = copy.deepcopy(trace)
        altered['matrix']['sql'] += ' LIMIT 1'
        with self.assertRaises(AssertionError): validate(altered)
        altered = copy.deepcopy(trace)
        altered['initial']['region'][0]['id'] = '1'
        with self.assertRaises(AssertionError): validate(altered)


if __name__=='__main__': unittest.main()
