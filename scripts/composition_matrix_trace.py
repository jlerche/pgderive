"""Reproducible histories: exactly thirty committed boundaries plus rollback/restart."""
import copy
from application_model import generated
from application_trace import FIELDS, validate_source
from composition_matrix_model import specification


def generate(family, seed):
    initial,history = generated(seed,rows=24,accounts=8,transactions=64)
    initial['region'] = copy.deepcopy(initial['account'])
    committed = [step for step in history if not step['rollback']][:30]
    assert len(committed)==30
    # Force observable duplicates, sign/NULL transitions, and right membership thresholds.
    initial['account'][1:3] = [dict(id=i,label='same',cohort=2) for i in (2,3)]
    initial['entry'][1:3] = [dict(id=i,account_id=2,amount=None) for i in (2,3)]
    committed[3] = {'rollback':False,'operations':[
        {'table':'entry','kind':'delete','id':2},
        {'table':'entry','kind':'delete','id':3}]}
    committed[4] = {'rollback':False,'operations':[
        {'table':'entry','kind':'upsert','row':dict(id=2,account_id=None,amount=None)},
        {'table':'account','kind':'upsert','row':dict(id=2,label='same',cohort=None)}]}
    committed[5] = {'rollback':False,'operations':[
        {'table':'entry','kind':'upsert','row':dict(id=2,account_id=-2,amount=7)},
        {'table':'account','kind':'upsert','row':dict(id=2,label='same',cohort=2)},
        {'table':'region','kind':'delete','id':2}]}
    committed[6] = {'rollback':False,'operations':[
        {'table':'region','kind':'upsert','row':dict(id=2,label='same',cohort=2)}]}
    history = committed[:10]+[{'rollback':True,'operations':[
        {'table':'entry','kind':'delete','id':2}]}]+committed[10:15]+[{'restart':True}]+committed[15:]
    return {'version':1,'id':family,'seed':seed,
            'matrix':specification(family,seed),'initial':initial,'history':history}


def validate(trace):
    assert set(trace)=={'version','id','seed','matrix','initial','history'}
    assert trace['matrix']==specification(trace['id'],trace['seed'])
    validate_source(trace, {**FIELDS,'region':FIELDS['account']})
