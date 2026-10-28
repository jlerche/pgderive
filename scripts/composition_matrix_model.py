"""Finite eight-family grammar and independent bag evaluator; no engine IR reuse."""
import collections
from numeric_compiler_harness import average

FAMILIES = ('distinct','join-group','left-group','membership-group','regroup',
            'group-window','window-project','three-join')


def specification(family, seed):
    assert family in FAMILIES
    p = 1+seed%3
    s = '{schema}'
    a,b,r = s+'.account',s+'.entry',s+'.region'
    inner = f'SELECT account_id AS g,AVG(amount) AS v FROM {b} GROUP BY account_id'
    templates = {
        'distinct':f'SELECT DISTINCT d.label,d.bucket FROM (SELECT reverse(label) AS label,COALESCE(abs(cohort),0)+{p} AS bucket FROM {a}) d WHERE d.bucket>={p+1}',
        'join-group':f'SELECT a.cohort AS g,COUNT(*) AS n,SUM(abs(b.amount)) AS total,AVG(abs(b.amount)) AS mean FROM {a} a JOIN {b} b ON abs(a.cohort)=abs(b.account_id) GROUP BY a.cohort',
        'left-group':f'SELECT a.cohort AS g,COUNT(*) AS n,COUNT(b.amount) AS present,SUM(b.amount) AS total FROM {a} a LEFT JOIN {b} b ON a.cohort+{p}=b.account_id+{p} WHERE b.amount IS NULL OR b.amount>=0 GROUP BY a.cohort',
        'membership-group':f'SELECT a.cohort AS g,COUNT(*) AS n FROM {a} a WHERE {"NOT " if seed%2 else ""}EXISTS(SELECT 1 FROM {b} b WHERE a.cohort+{p}=b.account_id+{p}) GROUP BY a.cohort',
        'regroup':f'SELECT d.g,COUNT(*) AS n,MAX(d.v) AS v FROM ({inner}) d WHERE d.v>=0 GROUP BY d.g',
        'group-window':f'SELECT d.g,SUM(d.v) OVER(ORDER BY d.g ROWS BETWEEN 1 PRECEDING AND CURRENT ROW) AS v FROM ({inner}) d',
        'window-project':f'SELECT d.id,COALESCE(d.v,0::numeric)+{p} AS v FROM (SELECT id,SUM(amount) OVER(PARTITION BY account_id ORDER BY id ROWS BETWEEN 1 PRECEDING AND CURRENT ROW) AS v FROM {b}) d WHERE d.v>=0',
        'three-join':f'SELECT a.cohort AS g,COUNT(*) AS n,SUM(abs(b.amount)+{p}) AS total FROM {a} a JOIN {b} b ON abs(a.cohort)=abs(b.account_id) JOIN {r} r ON a.id=r.id GROUP BY a.cohort',
    }
    labels = {
        'distinct':['label','bucket'],'join-group':['g','n','total','mean'],
        'left-group':['g','n','present','total'],'membership-group':['g','n'],
        'regroup':['g','n','v'],'group-window':['g','v'],
        'window-project':['id','v'],'three-join':['g','n','total'],
    }
    return {'version':2,'family':family,'seed':seed,'parameter':p,
            'anti':bool(seed%2),'sql':templates[family],'labels':labels[family]}


def absolute(value):
    return None if value is None else abs(value)


def ordered(value):
    return value is None,value


def total(values):
    present = [value for value in values if value is not None]
    return sum(present) if present else None


def expected(spec, source):
    family,p = spec['family'],spec['parameter']
    accounts,entries = list(source['account'].values()),list(source['entry'].values())
    if family == 'distinct':
        return collections.Counter({(row['label'][::-1] if row['label'] is not None else None,
                                     abs(row['cohort'] or 0)+p):1 for row in accounts if abs(row['cohort'] or 0)>=1})
    if family in ('join-group','left-group','three-join','membership-group'):
        return joins(spec,accounts,entries,source.get('region',{}))
    groups = collections.defaultdict(list)
    for row in entries:
        groups[row['account_id']].append(row)
    result = collections.Counter()
    means = {key:average([row['amount'] for row in rows if row['amount'] is not None])
             for key,rows in groups.items()}
    if family == 'regroup':
        return collections.Counter({(key,1,value):1 for key,value in means.items()
                                    if value is not None and value>=0})
    if family == 'group-window':
        keys = sorted(means,key=ordered)
        for i,key in enumerate(keys):
            result[(key,total([means[k] for k in keys[max(0,i-1):i+1]]))] += 1
        return result
    for rows in groups.values():
        rows.sort(key=lambda row:row['id'])
        for i,row in enumerate(rows):
            value = total([other['amount'] for other in rows[max(0,i-1):i+1]])
            if value is not None and value>=0:
                result[(row['id'],value+p)] += 1
    return result


def joins(spec, accounts, entries, regions):
    family,p = spec['family'],spec['parameter']
    groups = collections.defaultdict(list)
    for account in accounts:
        key = account['cohort']
        if family in ('join-group','three-join'):
            matches = [row for row in entries if key is not None and row['account_id'] is not None
                       and abs(key)==abs(row['account_id'])]
        else:
            matches = [row for row in entries if key is not None and row['account_id'] is not None
                       and key==row['account_id']]
        if family == 'membership-group':
            if bool(matches) != spec['anti']:
                groups[key].append(None)
            continue
        if family == 'three-join' and account['id'] not in regions:
            continue
        if family == 'left-group' and not matches:
            matches = [{'amount':None}]
        for row in matches:
            value = row['amount']
            if family == 'left-group':
                if value is not None and value<0:
                    continue
            elif value is not None:
                value = abs(value)+(p if family=='three-join' else 0)
            groups[key].append(value)
    result = collections.Counter()
    for key,values in groups.items():
        present = [v for v in values if v is not None]
        if family == 'membership-group': row = (key,len(values))
        elif family == 'left-group': row = (key,len(values),len(present),total(values))
        elif family == 'join-group': row = (key,len(values),total(values),average(present))
        else: row = (key,len(values),total(values))
        result[row] += 1
    return result
