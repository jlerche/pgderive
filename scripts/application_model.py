"""Versioned finite query grammar and independent full-source bag oracle."""
import collections
import copy
import random

VERSION = 1


def scalar(value):
    if value is None:
        return 'NULL'
    if isinstance(value, str):
        return "'" + value.replace("'", "''") + "'"
    return str(value)


def query(recipe, seed):
    rng = random.Random(seed)
    parameter = rng.randint(1, 3)
    frame = rng.choice([(2, 0), (1, 1), (-2, 1)])
    return render(recipe, parameter, frame)


def render(recipe, parameter, frame):
    # Signed start denotes preceding; positive end denotes following.
    start, end = frame
    bounds = ('2 FOLLOWING AND 1 FOLLOWING' if start < 0 else
              f'{start} PRECEDING AND ' + ('CURRENT ROW' if end == 0 else f'{end} FOLLOWING'))
    source = '{schema}'
    templates = {
        'projection': f'SELECT reverse(label) AS label,COALESCE(abs(cohort),0)+{parameter} AS bucket FROM {source}.account WHERE id>={parameter}',
        'group': f'SELECT account_id AS group_key,COUNT(*) AS rows,COUNT(abs(amount)) AS present,COUNT(DISTINCT abs(amount)) AS unique_amounts,SUM(abs(amount)) AS total FROM {source}.entry WHERE amount IS NULL OR amount>={-parameter} GROUP BY account_id',
        'join': f'SELECT a.label AS label,b.amount AS amount FROM {source}.account a JOIN {source}.entry b ON abs(a.cohort)=abs(b.account_id) WHERE b.amount IS NULL OR b.amount>={-parameter}',
        'left': f'SELECT a.label AS label,b.amount AS amount FROM {source}.account a LEFT JOIN {source}.entry b ON a.id=b.account_id WHERE b.amount IS NULL OR b.amount>={-parameter}',
        'derived': f'SELECT d.account_id AS group_key,d.n AS rows FROM (SELECT account_id,COUNT(*) AS n FROM {source}.entry GROUP BY account_id) d WHERE d.n>={parameter}',
        'window': f'SELECT id,account_id,SUM(amount) OVER(PARTITION BY account_id ORDER BY id ROWS BETWEEN {bounds}) AS total FROM {source}.entry',
    }
    return {'version': VERSION, 'recipe': recipe, 'parameter': parameter,
            'frame': list(frame), 'sql': templates[recipe]}


def generated(seed, rows=24, accounts=8, transactions=14):
    rng = random.Random(seed)
    def key():
        return rng.choice([1, 1, 1, -1, 2, None])
    def label():
        return rng.choice(['same', 'same', 'é🙂', '', None, "a'b"])
    def amount():
        return rng.choice([None, -9, -2, 0, 2, 2, 7])
    initial = {
        'account': [{'id': i, 'label': label(), 'cohort': key()} for i in range(1, accounts+1)],
        'entry': [{'id': i, 'account_id': key(), 'amount': amount()} for i in range(1, rows+1)],
    }
    history = [
        {'rollback': False, 'operations': [
            {'table':'account','kind':'upsert','row':{'id':2,'label':'same','cohort':2}},
            {'table':'entry','kind':'upsert','row':{'id':2,'account_id':2,'amount':2}}]},
        {'rollback': False, 'operations': [
            {'table':'account','kind':'upsert','row':{'id':accounts+1,'label':None,'cohort':None}},
            {'table':'entry','kind':'upsert','row':{'id':rows+1,'account_id':None,'amount':None}}]},
        {'rollback': True, 'operations': [{'table':'entry','kind':'delete','id':2}]},
        {'rollback': False, 'operations': []},
    ]
    for _ in range(max(0, transactions-len(history))):
        operations = []
        for _ in range(rng.randint(1, 3)):
            table = rng.choice(['account','entry'])
            identifier = rng.randint(2, (accounts if table == 'account' else rows)+3)
            if rng.random() < .3:
                operations.append({'table':table,'kind':'delete','id':identifier})
            else:
                row = {'id':identifier, 'label':label(), 'cohort':key()} if table == 'account' else {'id':identifier,'account_id':key(),'amount':amount()}
                operations.append({'table':table,'kind':'upsert','row':row})
        history.append({'rollback':rng.random()<.1,'operations':operations})
    return initial, history[:transactions]


def state(initial):
    return {table: {row['id']: copy.deepcopy(row) for row in rows} for table, rows in initial.items()}


def apply(source, transaction):
    if transaction['rollback']:
        return
    for operation in transaction['operations']:
        table = source[operation['table']]
        if operation['kind'] == 'delete':
            table.pop(operation['id'], None)
        else:
            row = operation['row']
            table[row['id']] = copy.deepcopy(row)


def statements(schema, transaction):
    result = [f'UPDATE {schema}.account SET label=label WHERE id=1']
    for operation in transaction['operations']:
        table = schema+'.'+operation['table']
        if operation['kind'] == 'delete':
            result.append(f"DELETE FROM {table} WHERE id={operation['id']}")
        else:
            row = operation['row']
            columns = list(row)
            updates = ','.join(f'{column}=EXCLUDED.{column}' for column in columns if column != 'id')
            result.append(f"INSERT INTO {table}({','.join(columns)}) VALUES({','.join(scalar(row[column]) for column in columns)}) ON CONFLICT(id) DO UPDATE SET {updates}")
    return 'BEGIN;'+';'.join(result)+';'+('ROLLBACK' if transaction['rollback'] else 'COMMIT')


def expected(spec, source):
    recipe, parameter = spec['recipe'], spec['parameter']
    accounts, entries = list(source['account'].values()), list(source['entry'].values())
    if recipe == 'projection':
        return collections.Counter((row['label'][::-1] if row['label'] is not None else None,
                                    abs(row['cohort'] or 0)+parameter) for row in accounts if row['id']>=parameter)
    if recipe in ('group','derived','window'):
        return grouped(spec,entries)
    result = collections.Counter()
    for a in accounts:
        matches = [b for b in entries if b['account_id'] is not None and
                   (a['id'] == b['account_id'] if recipe == 'left' else
                    a['cohort'] is not None and abs(a['cohort']) == abs(b['account_id']))]
        if not matches and recipe == 'left':
            matches = [{'amount':None}]
        for b in matches:
            if b['amount'] is None or b['amount']>=-parameter:
                result[(a['label'],b['amount'])] += 1
    return result


def grouped(spec, entries):
    groups = collections.defaultdict(list)
    recipe, parameter = spec['recipe'],spec['parameter']
    for b in entries:
        if recipe != 'group' or b['amount'] is None or b['amount']>=-parameter:
            groups[b['account_id']].append(b)
    result = collections.Counter()
    for key, rows in groups.items():
        if recipe == 'group':
            prices = [abs(row['amount']) for row in rows if row['amount'] is not None]
            result[(key,len(rows),len(prices),len(set(prices)),sum(prices) if prices else None)] += 1
        elif recipe == 'derived':
            if len(rows)>=parameter:
                result[(key,len(rows))] += 1
        else:
            rows.sort(key=lambda row:row['id'])
            start,end = spec['frame']
            for index,row in enumerate(rows):
                lo = min(len(rows),max(0,index-start))
                hi = min(len(rows),index+end+1)
                prices = [item['amount'] for item in rows[min(lo,hi):hi] if item['amount'] is not None]
                result[(row['id'],key,sum(prices) if prices else None)] += 1
    return result
