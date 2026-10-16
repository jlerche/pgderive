#!/usr/bin/env python3
"""Exercise live replay/reduction evidence with an explicit test-only sentinel."""
import json
import pathlib
import sys
import application_portfolio_harness as harness
from application_model import generated, query


original_expected = harness.expected


def injected_expected(spec, model):
    bag = original_expected(spec, model)
    if 99 in model['entry']:
        bag[('test-only-injected-oracle-fault',)] += 1
    return bag


def main():
    output = pathlib.Path(sys.argv[1])
    command = sys.argv[2:]
    if command[:1]==['--']:
        command = command[1:]
    initial,_ = generated(17)
    trace = {'version':1,'id':'reducer-self-test','seed':17,'spec':query('projection',17),
             'initial':initial,'history':[
                 {'rollback':False,'operations':[]},
                 {'rollback':False,'operations':[{'table':'entry','kind':'upsert','row':{'id':99,'account_id':1,'amount':7}}]},
                 {'rollback':False,'operations':[{'table':'entry','kind':'delete','id':2}]}]}
    harness.expected = injected_expected
    try:
        try:
            harness.run_case(output,command,trace,'injected')
            raise AssertionError('sentinel was not observed')
        except harness.DifferentialFailure as error:
            assert error.kind=='memory_oracle'
        failure = json.loads((output/'injected/failure.json').read_text())
        assert failure['reduction']['status']=='1-minimal',failure
        reduced = failure['reduction']['history']
        assert len(reduced)==1 and reduced[0]['operations'][0]['row']['id']==99,reduced
        assert (output/'injected/failure-fixture.sql').exists()
    finally:
        harness.expected = original_expected
    # The same SQL/source history succeeds without the explicitly injected oracle fault.
    replay = json.loads((output/'injected/minimized-replay.json').read_text())
    harness.run_case(output,command,replay,'without-sentinel')
    (output/'qualification.json').write_text(json.dumps({'test_only_injection':True,
        'actual_native_and_source_oracles':'passed; independent result oracle deliberately corrupted', 'reduction':'1-minimal transaction sequence',
        'original_and_minimized_replays':'retained','fixture_dump':'retained',
        'uninjected_replay':'passed; no actual engine defect asserted'})+'\n')


if __name__=='__main__':
    main()
