"""Independent outer-bag oracle regressions without a PostgreSQL dependency."""
import collections
from decimal import Decimal
import unittest

from outer_join_harness import CASES, MODES, TYPES, OuterFixture, merged, outer_pairs
from join_predicate_harness import eq
import json
import pathlib
COUNTS = json.loads((pathlib.Path(__file__).resolve().parents[1]/'tests/fixtures/semantics-manifest.json').read_text())['families']


def fixture(mode):
    result = OuterFixture.__new__(OuterFixture)
    result.mode, result.case = mode, CASES[mode]
    result.types = TYPES[result.case.domain]
    return result


def left(identifier, key, category='same', group=0):
    return dict(id=identifier,k=key,category=category,group_id=group,tk='one')


def right(identifier, key, price=5):
    return dict(id=identifier,k=key,price=price,tk='one')


class OuterJoinOracleTests(unittest.TestCase):
    def test_full_bag_retains_duplicates_and_null_keys_on_each_side(self):
        a = [left(1,1),left(2,1),left(3,None,'null'),left(4,2,'left')]
        b = [right(1,1),right(2,1),right(3,None,7),right(4,3,9)]
        self.assertEqual(fixture('full-on').memory(a,b),collections.Counter({
            (1,1,'same',5):4,(None,None,'null',None):1,
            (2,None,'left',None):1,(None,None,None,7):1,(None,3,None,9):1}))
        self.assertEqual(fixture('right-on').memory(a,b),collections.Counter({
            (1,1,'same',5):4,(None,None,None,7):1,(None,3,None,9):1}))

    def test_residual_matches_are_per_occurrence_and_where_runs_after_padding(self):
        a = [left(1,1,group=2),left(2,1,group=8)]
        b = [right(1,1),right(2,1,1)]
        self.assertEqual(fixture('full-residual').memory(a,b),collections.Counter({
            (1,1,'same',5):1,(1,None,'same',None):1,(None,1,None,1):1}))
        self.assertEqual(fixture('full-where').memory(
            [left(1,1),left(2,2)],[right(1,1,0),right(2,3,3)]),
            collections.Counter({(None,3,None,3):1}))

    def test_chained_using_null_extends_hidden_originals_for_third_only_row(self):
        a,b,c = [left(1,1)],[],[right(1,1,10),right(2,42,20),right(3,None,30)]
        self.assertEqual(fixture('chained-using').memory(a,b,c),collections.Counter({
            (1,1,None,1,'same',None,10):1,
            (42,None,None,42,None,None,20):1,
            (None,None,None,None,None,None,30):1}))

    def test_merged_numeric_prefers_native_var_and_keeps_selected_display_scale(self):
        a,b = left(1,1),right(1,Decimal('1.0000'))
        for kind,scale in [('INNER',-4),('LEFT',0),('RIGHT',-4),('FULL',0)]:
            self.assertEqual(merged(a,b,'k',kind,'mixed-numeric').as_tuple().exponent,scale)
        a = left(1,Decimal('1.20'))
        b = right(1,Decimal('1.2000'))
        for kind,scale in [('INNER',-4),('LEFT',-2),('RIGHT',-4),('FULL',-2)]:
            self.assertEqual(merged(a,b,'k',kind,'numeric-var').as_tuple().exponent,scale)

    def test_using_between_joined_subtrees_keeps_each_null_occurrence(self):
        self.assertEqual(fixture('subtrees-using').memory(
            [left(1,1),left(2,None,'null')],[right(1,1),right(2,None,None)]),
            collections.Counter({
                (1,1,1,1,1,'same',5,5,'same'):1,
                (None,None,None,None,None,'null',None,None,None):1,
                (None,None,None,None,None,None,None,None,'null'):1,
                (None,None,None,None,None,None,None,None,None):2}))

    def test_empty_inputs_and_nan_equality_preserve_sql_occurrences(self):
        a,b = [left(1,Decimal('NaN'))],[right(1,Decimal('NaN'))]
        matches = lambda a,b:eq(a['k'],b['k'])
        self.assertEqual(list(outer_pairs(a,b,matches,'FULL')),[(a[0],b[0])])
        self.assertEqual(list(outer_pairs([],b,matches,'FULL')),[(None,b[0])])
        self.assertEqual(list(outer_pairs(a,[],matches,'FULL')),[(a[0],None)])
        self.assertEqual(list(outer_pairs([],[],matches,'FULL')),[])

    def test_gate_names_match_the_case_matrix(self):
        self.assertEqual(COUNTS['outer_join'],len(MODES))
        self.assertEqual(len(MODES),len(set(MODES)))
        self.assertTrue(all(case.kind in ('INNER','LEFT','RIGHT','FULL') for case in CASES.values()))


if __name__ == '__main__':
    unittest.main()
