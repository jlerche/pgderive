"""Validate the persisted finite grammar before replay touches PostgreSQL."""
from application_model import VERSION, render

FIELDS = {'account':{'id','label','cohort'}, 'entry':{'id','account_id','amount'}}


def row_valid(fields, row):
    assert set(row)==fields
    assert type(row['id']) is int and 1<=row['id']<=100000
    for field,value in row.items():
        if field == 'label':
            assert value is None or isinstance(value,str) and len(value)<=64 and '\0' not in value
        else:
            assert value is None or type(value) is int and -2147483647<=value<=2147483647


def validate(trace):
    spec = trace['spec']
    assert type(spec['parameter']) is int and 1<=spec['parameter']<=3
    assert spec['frame'] in [[2,0],[1,1],[-2,1]]
    assert spec == render(spec['recipe'],spec['parameter'],spec['frame']), 'replay grammar/version differs'
    validate_source(trace, FIELDS)


def validate_source(trace, fields):
    assert trace['version']==VERSION and type(trace['seed']) is int
    assert set(trace['initial'])==set(fields)
    for table, rows in trace['initial'].items():
        assert len(rows)<=4096 and len({row['id'] for row in rows})==len(rows)
        for row in rows:
            row_valid(fields[table],row)
    assert any(row['id']==1 for row in trace['initial']['account']), 'pulse identity absent'
    assert len(trace['history'])<=264
    for step in trace['history']:
        if step.get('restart'):
            assert step=={'restart':True}
            continue
        assert set(step)=={'rollback','operations'} and type(step['rollback']) is bool
        assert len(step['operations'])<=16
        for operation in step['operations']:
            table = operation['table']
            assert table in fields
            if operation['kind']=='upsert':
                assert set(operation)=={'table','kind','row'}
                row_valid(fields[table],operation['row'])
                identifier=operation['row']['id']
            else:
                assert operation['kind']=='delete' and set(operation)=={'table','kind','id'}
                identifier=operation['id']
                assert type(identifier) is int and 1<=identifier<=100000
            assert table!='account' or identifier!=1, 'pulse identity is immutable'
