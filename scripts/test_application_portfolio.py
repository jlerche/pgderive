import copy
import unittest
from application_model import apply, expected, generated, query, scalar, state, statements
from application_reduce import reduce_history
from application_evidence import checked_phases
from application_trace import validate
from application_baseline import percentile


class PortfolioTests(unittest.TestCase):
    def test_seeded_queries_and_histories_are_reproducible(self):
        self.assertEqual(generated(17),generated(17))
        self.assertEqual(query('window',17),query('window',17))
        self.assertNotEqual(generated(17),generated(18))
        initial,history = generated(17)
        source = state(initial)
        before = copy.deepcopy(source)
        apply(source,history[2])
        self.assertEqual(source,before)
        self.assertTrue(statements('owned',history[2]).endswith('ROLLBACK'))
        self.assertEqual(scalar("a'b"),"'a''b'")
        for recipe in ['projection','group','join','left','derived','window']:
            self.assertTrue(all(value>0 for value in expected(query(recipe,17),source).values()))

    def test_independent_oracle_preserves_bag_identity_and_left_join_predicate(self):
        source = state({'account':[{'id':1,'label':'x','cohort':-1},{'id':2,'label':'x','cohort':1}],
                        'entry':[{'id':1,'account_id':1,'amount':2},{'id':2,'account_id':-1,'amount':2}]})
        self.assertEqual(expected(query('join',17),source),{('x',2):4})
        self.assertEqual(expected(query('left',17),source),{('x',2):1,('x',None):1})
        source['entry'][1]['amount']=-99
        self.assertEqual(expected(query('left',17),source),{('x',None):1})
        apply(source,{'rollback':False,'operations':[{'table':'entry','kind':'delete','id':99}]})
        self.assertEqual(len(source['entry']),2)

    def test_window_bounds_and_all_null_group(self):
        source = state({'account':[], 'entry':[{'id':1,'account_id':None,'amount':None},{'id':2,'account_id':None,'amount':2},{'id':3,'account_id':None,'amount':3}]})
        spec = {**query('window',17),'frame':[1,1]}
        self.assertEqual(expected(spec,source),{(1,None,2):1,(2,None,5):1,(3,None,5):1})
        self.assertEqual(expected({**spec,'frame':[-2,1]},source),{(1,None,None):1,(2,None,None):1,(3,None,None):1})
        source['entry']={1:{'id':1,'account_id':None,'amount':None}}
        self.assertEqual(expected(query('group',17),source),{(None,1,0,0,None):1})

    def test_reducer_reproduces_then_preserves_failure_and_reports_budget(self):
        result = reduce_history(list(range(10)),lambda values:3 in values and 7 in values)
        self.assertEqual(result['history'],[3,7])
        self.assertEqual(result['status'],'1-minimal')
        self.assertEqual(reduce_history([1],lambda _:False)['status'],'not-reproduced')
        self.assertEqual(reduce_history([1],lambda _:True)['history'],[])
        self.assertEqual(reduce_history(list(range(8)),lambda values:4 in values,budget=2)['status'],'budget-limited')
        with self.assertRaises(ValueError):
            reduce_history([],lambda _:True,budget=0)

    def test_phase_attribution_rejects_bad_or_overlapping_accounting(self):
        phases = {key:1 for key in ('source_decode','inputs','prepare_including_object_io','finalize','local_completion')}
        phases['publication']=3
        event = {'phases_us':phases,'publication_us':{'membership_layout_progress':1,'sink_dml':1,'commit':1}}
        self.assertEqual(checked_phases(event),phases)
        event['publication_us']['commit']=10
        with self.assertRaises(AssertionError):
            checked_phases(event)

    def test_replay_validation_precedes_any_sql_and_binds_generator_version(self):
        initial,history=generated(17)
        trace={'version':1,'seed':17,'spec':query('group',17),'initial':initial,'history':history}
        validate(trace)
        changed_seed=copy.deepcopy(trace); changed_seed['seed']=99
        validate(changed_seed)  # Persisted grammar, not current RNG output, defines replay.
        wrong=copy.deepcopy(trace); wrong['spec']['sql']='SELECT 1'
        with self.assertRaises(AssertionError): validate(wrong)
        wrong=copy.deepcopy(trace); wrong['initial']['entry'][0]['unknown']=1
        with self.assertRaises(AssertionError): validate(wrong)
        wrong=copy.deepcopy(trace); wrong['history'][0]['operations'][0]['row']['id']=1
        with self.assertRaises(AssertionError): validate(wrong)
        self.assertIsNone(percentile([],.95))
        self.assertEqual(percentile([1,2,3,4],.95),4)
