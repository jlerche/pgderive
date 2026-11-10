"""Frozen native oracles cannot silently go stale, lose families or skip checks."""
import collections
import copy
import hashlib
import json
import pathlib
import subprocess
import unittest
from check_semantics_report import verify
from export_semantics import MODULES, updates

ROOT=pathlib.Path(__file__).resolve().parents[1]
COUNTS={'sql_composition':28,'set_operation':44,'subquery':75,'numeric_arithmetic':19,
        'temporal_arithmetic':40,'join_predicate':29,'outer_join':45,'ordering':18,
        'ordering_domains':14,'unordered':17,'sliding_extrema':6,'prefix_window':6,
        'expression_composition':12,'sliding_window':5,'functions_compiler':1,
        'computed_join':6,'aggregate_inputs_compiler':6,'relational_compiler':2,
        'partition_compiler':7,'numeric_compiler':4,'circuit_numeric':9,'native_compiler':4,
        'navigation_compiler':5,'case_compiler':6,'lookup_compiler':5,'sql_compiler':4}


class SemanticReplayTests(unittest.TestCase):
    def test_native_corpus_covers_the_declared_families_and_current_recipes(self):
        folder=ROOT/'tests/fixtures'
        manifest=json.loads((folder/'semantics-manifest.json').read_text())
        packed=folder/'semantics.json.zst'
        self.assertEqual(manifest['fixture_sha256'],hashlib.sha256(packed.read_bytes()).hexdigest())
        matrix=json.loads(subprocess.check_output(['zstd','-q','-d','-c',str(packed)]))
        self.assertEqual(matrix['version'],1)
        self.assertEqual(manifest['families'],COUNTS)
        self.assertEqual(set(COUNTS),set(MODULES))
        self.assertEqual(manifest['cases'],[case['name'] for case in matrix['cases']])
        self.assertEqual(len(set(manifest['cases'])),sum(COUNTS.values()))
        for name, digest in manifest['source_sha256'].items():
            self.assertEqual(digest,hashlib.sha256((ROOT/name).read_bytes()).hexdigest(),
                             'Refresh native semantic fixtures after changing '+name)
        for case in matrix['cases']:
            self.assertTrue(case['relations'])
            self.assertTrue(case['output'])
            self.assertTrue(all(step['expected'] is not None for step in case['steps']))
            self.assertTrue(any(step['restart'] for step in case['steps']))
            for step in case['steps']:
                choices=step.get('representatives')
                if choices is not None:
                    self.assertIn(case['name'],{
                        'circuit_numeric--group-key','circuit_numeric--extrema-min',
                        'circuit_numeric--extrema-max','circuit_numeric--extrema-mixed'})
                    self.assertEqual(choices['column'],0)
                    for canonical,spellings in choices['choices']:
                        self.assertIn(canonical,spellings)


    def test_snapshot_delta_preserves_full_tuple_weights_and_cancelled_endpoints(self):
        old=('source',json.dumps({'id':'1','other':'old'}))
        new=('source',json.dumps({'id':'1','other':'new'}))
        before=collections.Counter({old:2}); after=collections.Counter({old:1,new:1})
        delta=after.copy();delta.subtract(before)
        self.assertEqual(updates(delta),[
            {'table':'source','row':{'id':'1','other':'new'},'weight':1},
            {'table':'source','row':{'id':'1','other':'old'},'weight':-1}])
        delta=before.copy();delta.subtract(before)
        self.assertEqual(updates(delta),[])

    def test_report_rejects_missing_duplicate_or_unchecked_histories(self):
        manifest={'cases':['a'],'histories':{'a':{'commits':2,'oracle_checks':3,'restarts':1}}}
        row={'case':'a','cold_reopen_equal':True,'oracle_checks':3,'logical_time':2,
             'lifecycle':{'restarts':1,'maintenance_quiescent':True}}
        self.assertEqual(verify([row],manifest),1)
        for report in ([],[row,row],[dict(row,oracle_checks=0)],[dict(row,cold_reopen_equal=False)]):
            with self.assertRaises(ValueError):verify(report,manifest)
        bad=copy.deepcopy(row);bad['lifecycle']['restarts']=0
        with self.assertRaises(ValueError):verify([bad],manifest)
