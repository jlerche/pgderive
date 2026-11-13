#!/usr/bin/env python3
"""Live proof that new matrix traces preserve, replay and minimize same-kind failures."""
import json
import pathlib
import sys
from application_portfolio_harness import DifferentialFailure, run_case
from composition_matrix_harness import MatrixFixture
from composition_matrix_trace import generate, validate


class InjectedFixture(MatrixFixture):
    def memory(self):
        result = super().memory()
        if 99 in self.model['entry']:
            result[('test-only-oracle-fault',None)] += 1
        return result


def main():
    output = pathlib.Path(sys.argv[1])
    command = sys.argv[2:]
    if command[:1]==['--']: command=command[1:]
    trace = generate('distinct',10000)
    trace['history'] = [
        {'rollback':False,'operations':[]},
        {'rollback':False,'operations':[{'table':'entry','kind':'upsert','row':dict(id=99,account_id=1,amount=7)}]},
        {'rollback':False,'operations':[{'table':'region','kind':'delete','id':2}]},
    ]
    try:
        run_case(output,command,trace,'injected',fixture_factory=InjectedFixture,validator=validate)
        raise AssertionError('sentinel not observed')
    except DifferentialFailure as error:
        assert error.kind=='memory_oracle'
    failure = json.loads((output/'injected/failure.json').read_text())
    assert failure['reduction']['status']=='1-minimal',failure
    reduced = failure['reduction']['history']
    assert len(reduced)==1 and reduced[0]['operations'][0]['row']['id']==99,reduced
    assert (output/'injected/failure-fixture.sql').exists()
    saved = json.loads((output/'injected/minimized-replay.json').read_text())
    run_case(output,command,saved,'without-sentinel',fixture_factory=MatrixFixture,validator=validate)
    (output/'qualification.json').write_text(json.dumps({'test_only_oracle_injection':True,
        'reduction':'1-minimal','fixture_dump':True,'saved_trace_uninjected_replay':'passed',
        'scope':'tests failure machinery; no actual engine defect asserted'})+'\n')


if __name__=='__main__': main()
