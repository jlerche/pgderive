"""Export existing application traces for unpaced replay through the compiled engine."""
import copy
import random
from application_model import VERSION, apply, expected, generated, query, state

RECIPES = ('projection', 'group', 'join', 'left', 'derived', 'window')


def text(row):
    return {key: None if value is None else str(value) for key, value in row.items()}


def delta(source, transaction):
    if transaction['rollback']:
        return []
    keys = {(op['table'], op['row']['id'] if op['kind'] == 'upsert' else op['id'])
            for op in transaction['operations']}
    before = {(table, key): source[table].get(key) for table, key in keys}
    apply(source, transaction)
    changes = []
    for table, key in sorted(keys):
        old, new = before[table, key], source[table].get(key)
        if old != new:
            for row, weight in [(old, -1), (new, 1)]:
                if row is not None:
                    changes.append({'table': table, 'row': text(row), 'weight': weight})
    return changes


def bag(spec, source):
    return [[list(None if cell is None else str(cell) for cell in row), count]
            for row, count in sorted(expected(spec, source).items(), key=lambda item: repr(item[0]))]


def export(trace, *, measured_from=None, final_only=False):
    source = state(trace['initial'])
    spec = trace['spec']
    steps = [{'updates': [{'table': table, 'row': text(row), 'weight': 1}
                           for table, rows in source.items() for row in rows.values()],
              'expected': bag(spec, source), 'measure': False, 'restart': False}]
    for index, transaction in enumerate(trace['history']):
        if transaction.get('restart'):
            steps.append({'updates': [], 'expected': bag(spec, source),
                          'measure': False, 'restart': True})
            continue
        updates = delta(source, transaction)
        if transaction['rollback']:
            continue
        check = not final_only or index == len(trace['history']) - 1
        steps.append({'updates': updates, 'expected': bag(spec, source) if check else None,
                      'measure': measured_from is not None and index >= measured_from,
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
