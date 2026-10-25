"""Export existing application traces for unpaced replay through the compiled engine."""
import copy
import random
from application_model import VERSION, apply, expected, generated, query, state

LIFECYCLE = {'memtable_bytes': 64 * 1024**2, 'checkpoint_interval_ms': 5000,
             'maintenance_ticks': 16, 'source_rows_per_second': 1000}

RECIPES = ('projection', 'group', 'join', 'left', 'derived', 'window')


def text(row):
    return {key: None if value is None else str(value) for key, value in row.items()}


def delta(source, transaction):
    if transaction['rollback']:
        return [], 0
    changes = []
    affected = 0
    for operation in transaction['operations']:
        table = operation['table']
        key = operation['row']['id'] if operation['kind'] == 'upsert' else operation['id']
        old = source[table].get(key)
        apply(source, {'rollback': False, 'operations': [operation]})
        new = source[table].get(key)
        if old is not None or new is not None:
            affected += 1
        for row, weight in [(old, -1), (new, 1)]:
            if row is not None:
                changes.append({'table': table, 'row': text(row), 'weight': weight})
    return changes, affected


def bag(spec, source):
    return [[list(None if cell is None else str(cell) for cell in row), count]
            for row, count in sorted(expected(spec, source).items(), key=lambda item: repr(item[0]))]


def export(trace, *, measured_from=None, final_only=False):
    source = state(trace['initial'])
    spec = trace['spec']
    steps = [{'updates': [{'table': table, 'row': text(row), 'weight': 1}
                           for table, rows in source.items() for row in rows.values()],
              'expected': bag(spec, source), 'source_rows': 0, 'measure': False, 'restart': False}]
    for index, transaction in enumerate(trace['history']):
        if transaction.get('restart'):
            steps.append({'updates': [], 'expected': bag(spec, source),
                          'source_rows': 0, 'measure': False, 'restart': True})
            continue
        updates, affected = delta(source, transaction)
        if transaction['rollback']:
            continue
        check = not final_only or index == len(trace['history']) - 1
        steps.append({'updates': updates, 'expected': bag(spec, source) if check else None,
                      'source_rows': affected, 'measure': measured_from is not None and index >= measured_from,
                      'restart': False})
    # A trailing rollback must not suppress the final committed-state check.
    if steps[-1]['expected'] is None:
        steps[-1]['expected'] = bag(spec, source)
    return {'name': trace['spec']['recipe'] + '-' + str(trace['seed']),
            'sql': spec['sql'].format(schema='source'), 'steps': steps}


def traces(seeds, transactions=14):
    for seed in seeds:
        initial, history = generated(seed, transactions=transactions)
        history.insert(len(history) // 2, {'restart': True})
        for recipe in RECIPES:
            yield {'version': VERSION, 'id': 'fast-' + recipe, 'seed': seed, 'spec': query(recipe, seed),
                   'initial': initial, 'history': history}


def preloaded(rows, *, measured=32, warmup=8):
    initial, _ = generated(0, rows=rows, transactions=0)
    # Unique groups make state size explicit rather than waiting for slow growth.
    for row in initial['entry']:
        row['account_id'] = row['id']
        row['amount'] = row['id'] % 7
    history = [{'restart': True}]
    rng = random.Random(0)
    for index in range(warmup + measured):
        row = copy.deepcopy(rng.choice(initial['entry']))
        row['amount'] = 100 + index
        history.append({'rollback': False, 'operations': [{'table': 'entry', 'kind': 'upsert', 'row': row}]})
    return {'version': VERSION, 'seed': rows, 'spec': query('group', 0),
            'initial': initial, 'history': history}


def endurance(rows, epochs, *, batch_rows=1000):
    """Reuse the source model and full oracle, without resetting active engine state."""
    trace = preloaded(rows, warmup=0, measured=0)
    for row in trace['initial']['entry']:
        row['account_id'] = 1 + (row['id'] - 1) // 12
    spec = query('linear', 0)
    source = state(trace['initial'])
    steps = [{'updates':[{'table': table,'row':text(row),'weight':1}
                          for table, items in source.items() for row in items.values()],
              'expected':bag(spec,source),'source_rows':0,'measure':False,'restart':False}]
    rng = random.Random(42)
    next_id = rows + 1
    for epoch in range(epochs):
        operations = []
        for index in range(batch_rows):
            if index % 10 == 0:
                row = {'id':next_id,'account_id':1+(next_id-1)//12,'amount':rng.randrange(-2500,2501)}
                next_id += 1
                operations.append({'table':'entry','kind':'upsert','row':row})
                continue
            key = 1 + rng.randrange(next_id - 1)
            old = source['entry'].get(key)
            if old is None:
                continue
            if index % 13 == 0:
                operations.append({'table':'entry','kind':'delete','id':key})
            else:
                row = dict(old, amount=None if index % 17 == 0 else rng.randrange(-2500,2501))
                operations.append({'table':'entry','kind':'upsert','row':row})
        updates, affected = delta(source, {'rollback':False,'operations':operations})
        steps.append({'updates':updates,'expected':bag(spec,source) if epoch%16==15 else None,
                      'source_rows':affected,'measure':True,'restart':False})
    steps[-1]['expected'] = bag(spec,source)
    return {'name':f'linear-endurance-{rows}-{epochs}', 'sql':spec['sql'].format(schema='source'),
            'steps':steps}
