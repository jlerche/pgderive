"""Shared source traces retain endpoint semantics and oracle independence."""
import collections
import copy
import json
import pathlib
import unittest
from application_model import apply, state
from fast_replay import bag, delta, export, preloaded, text, traces


def snapshot(source):
    return collections.Counter((table, tuple(sorted(text(row).items())))
                               for table, rows in source.items() for row in rows.values())


class FastReplayTests(unittest.TestCase):
    def test_saved_traces_satisfy_live_replay_contract(self):
        from application_trace import validate
        for trace in traces(range(3)):
            validate(trace)
            self.assertTrue(trace['id'])

    def test_exported_default_fixture_is_reproducible(self):
        actual = json.loads((pathlib.Path(__file__).parents[1]/'tests/fixtures/fast-replay.json').read_text())
        self.assertEqual(actual, {'version': 1, 'limits': {}, 'cases': [export(t) for t in traces([1], transactions=8)]})

    def test_weighted_changes_match_independent_full_snapshot_difference(self):
        for trace in traces(range(3)):
            source = state(trace['initial'])
            for transaction in trace['history']:
                if transaction.get('restart'):
                    continue
                before = snapshot(source)
                separate = copy.deepcopy(source)
                apply(separate, transaction)
                expected = snapshot(separate)
                expected.subtract(before)
                actual = collections.Counter()
                for change in delta(source, transaction):
                    actual[change['table'], tuple(sorted(change['row'].items()))] += change['weight']
                self.assertEqual({k:v for k,v in actual.items() if v}, {k:v for k,v in expected.items() if v})
                self.assertEqual(source, separate)

    def test_transaction_endpoints_cancel_intermediate_values(self):
        source = {'entry': {1: {'id':1, 'account_id':None, 'amount':2}}, 'account':{}}
        transaction = {'rollback':False, 'operations':[
            {'table':'entry', 'kind':'upsert', 'row':{'id':1, 'account_id':None, 'amount':9}},
            {'table':'entry', 'kind':'upsert', 'row':{'id':1, 'account_id':None, 'amount':2}},
            {'table':'entry', 'kind':'upsert', 'row':{'id':2, 'account_id':1, 'amount':3}},
            {'table':'entry', 'kind':'delete', 'id':2}]}
        self.assertEqual(delta(source, transaction), [])

    def test_growth_limits_scale_output_and_total_scratch_guards(self):
        from fast_validation import growth_limits
        self.assertEqual(growth_limits(1000), {'output_entries':250000, 'output_bytes':67108864, 'scratch_bytes':1073741824})
        self.assertEqual(growth_limits(128000), {'output_entries':2048000, 'output_bytes':524288000, 'scratch_bytes':16777216000})

    def test_preload_excludes_setup_and_warmup_from_measurements(self):
        trace = preloaded(10, warmup=2, measured=3)
        case = export(trace, measured_from=3, final_only=True)
        self.assertEqual(sum(step['measure'] for step in case['steps']), 3)
        self.assertEqual(len(bag(trace['spec'], state(trace['initial']))), 10)
        self.assertTrue(case['steps'][1]['restart'])
        self.assertTrue(case['steps'][-1]['expected'])
        self.assertEqual(sum(step['expected'] is not None for step in case['steps']), 3)

    def test_rollback_does_not_become_an_engine_commit(self):
        trace = next(traces([1]))
        case = export(trace)
        committed = sum(not step.get('restart') and not step['rollback'] for step in trace['history'])
        self.assertEqual(sum(not step['restart'] for step in case['steps']), committed + 1)

    def test_trailing_rollback_keeps_final_oracle(self):
        trace = preloaded(4, warmup=1, measured=1)
        trace['history'].append({'rollback': True, 'operations': []})
        case = export(trace, measured_from=2, final_only=True)
        self.assertIsNotNone(case['steps'][-1]['expected'])


class CoverageTests(unittest.TestCase):
    def test_zero_selected_tests_is_not_success(self):
        import tempfile
        from fast_validation import validate_tests, VIRTUAL_TESTS
        with tempfile.TemporaryDirectory() as directory:
            path = pathlib.Path(directory)/'tests.log'
            path.write_text('running 0 tests\ntest result: ok. 0 passed; 0 failed\n')
            with self.assertRaises(RuntimeError):
                validate_tests(path, VIRTUAL_TESTS)
            path.write_text('\n'.join('test '+name+' ... ok' for name in VIRTUAL_TESTS))
            validate_tests(path, VIRTUAL_TESTS)

    def test_missing_or_incomplete_engine_report_is_not_success(self):
        import tempfile
        from fast_validation import validate_report
        cases = [{'name':'one', 'steps':[
            {'expected':[[['a'],1]], 'measure':False, 'restart':False, 'updates':[]},
            {'expected':[[['b'],1]], 'measure':True, 'restart':False, 'updates':[{},{}]}]}]
        valid = {'bootstrap_ns':100, 'bootstrap_rows':0, 'case':'one', 'prepare_commit_ns':[123], 'oracle_checks':2,
                 'logical_time':2, 'measured_weighted_updates':2, 'output_groups':1,
                 'measured_object_reads':1, 'measured_object_puts':0,
                 'object_puts':3, 'stored_objects':3, 'stored_bytes':128,
                 'memtable_bytes':100, 'measured_cache_misses':0, 'measured_decoded_bytes':100, 'measured_decoded_rows':1}
        with tempfile.TemporaryDirectory() as directory:
            path = pathlib.Path(directory)/'engine.json'
            with self.assertRaises(FileNotFoundError):
                validate_report(path, cases)
            path.write_text(json.dumps([valid]))
            validate_report(path, cases)
            for mutation in [{'bootstrap_ns':-1}, {'bootstrap_rows':-1}, {'prepare_commit_ns':[]}, {'prepare_commit_ns':[True]},
                             {'oracle_checks':0}, {'case':'wrong'}, {'logical_time':3},
                             {'measured_weighted_updates':0}, {'memtable_bytes':-1},
                             {'measured_cache_misses':-1}, {'measured_decoded_bytes':-1}, {'measured_decoded_rows':-1}, {'measured_object_reads':-1},
                             {'measured_object_puts':1}, {'object_puts':-1},
                             {'stored_objects':-1}, {'stored_bytes':-1}]:
                path.write_text(json.dumps([valid | mutation]))
                with self.subTest(mutation=mutation), self.assertRaises(RuntimeError):
                    validate_report(path, cases)
            path.write_text('[]')
            with self.assertRaises(RuntimeError):
                validate_report(path, cases)
