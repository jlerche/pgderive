#!/usr/bin/env python3
"""M6 frame domains and finalized relational scopes against two independent oracles."""
import collections
import json
from application_model import apply, generated, statements
from application_portfolio_harness import ApplicationFixture
from numeric_compiler_harness import average
from worker_harness import run_modes

MODES = ('window-rows','window-peers','window-empty','window-rank',
         'avg-filter','avg-project','avg-regroup','avg-window','avg-join',
         'outer','exists','anti')


def absolute(value):
    return abs(value) if value is not None else None


def sorting(value):
    return (value is None,value)


class CompositionFixture(ApplicationFixture):
    def __init__(self, output, mode):
        initial,history = generated(6021,rows=18,accounts=6,transactions=10)
        if mode == 'window-empty':
            for row in initial['entry']:
                row['amount'] = -9223372036854775808
        trace = {'version':1,'id':mode,'seed':6021,
                 'initial':initial,'history':history}
        self.initialize_sources(output,trace)
        self.mode = mode
        a,b = self.name+'.account',self.name+'.entry'
        window = 'PARTITION BY abs(account_id) ORDER BY id ROWS BETWEEN 1 PRECEDING AND 1 FOLLOWING'
        inner = f'SELECT account_id AS g,AVG(amount) AS v FROM {b} GROUP BY account_id'
        queries = {
            'window-rows':f'SELECT id,SUM(abs(amount)) FILTER(WHERE amount>0) OVER({window}) AS v,COUNT(abs(amount)) FILTER(WHERE amount>0) OVER({window}) AS n,AVG(abs(amount)) FILTER(WHERE amount>0) OVER({window}) AS mean,MIN(abs(amount)) FILTER(WHERE amount>0) OVER({window}) AS lo,MAX(abs(amount)) FILTER(WHERE amount>0) OVER({window}) AS hi,SUM(abs(amount)::numeric) FILTER(WHERE amount>0) OVER({window}) AS numeric_total FROM {b}',
            'window-peers':f'SELECT id,SUM(abs(amount)) FILTER(WHERE amount>0) OVER(PARTITION BY abs(account_id) ORDER BY abs(amount) GROUPS BETWEEN 1 PRECEDING AND CURRENT ROW) AS v FROM {b}',
            'window-empty':f'SELECT id,SUM(abs(amount)) OVER(ORDER BY id ROWS BETWEEN 2 FOLLOWING AND 1 FOLLOWING) AS v,COUNT(abs(amount)) OVER(ORDER BY id ROWS BETWEEN 2 FOLLOWING AND 1 FOLLOWING) AS n FROM {b}',
            'window-rank':f'SELECT id,RANK() OVER(PARTITION BY abs(account_id) ORDER BY abs(amount)) AS v FROM {b}',
            'avg-filter':f'SELECT d.g,d.v FROM ({inner}) d WHERE d.v>0',
            'avg-project':f'SELECT d.g,COALESCE(d.v,0::numeric)+1 AS v FROM ({inner}) d',
            'avg-regroup':f'SELECT d.g,MAX(d.v) AS v FROM ({inner}) d GROUP BY d.g',
            'avg-window':f'SELECT d.g,SUM(d.v) OVER(ORDER BY d.g ROWS BETWEEN 1 PRECEDING AND CURRENT ROW) AS v FROM ({inner}) d',
            'avg-join':f'SELECT a.label,d.v FROM {a} a JOIN ({inner}) d ON abs(a.cohort)=abs(d.g)',
            'outer':f'SELECT a.label,b.amount FROM {a} a LEFT JOIN {b} b ON abs(a.cohort)=abs(b.account_id) WHERE b.amount IS NULL OR b.amount>=0',
            'exists':f'SELECT a.label,a.cohort FROM {a} a WHERE EXISTS(SELECT 1 FROM {b} b WHERE abs(a.cohort)=abs(b.account_id))',
            'anti':f'SELECT a.label,a.cohort FROM {a} a WHERE NOT EXISTS(SELECT 1 FROM {b} b WHERE abs(a.cohort)=abs(b.account_id))',
        }
        self.query = queries[mode]
        self.labels = ['id','v'] if mode.startswith('window') else ['g','v'] if mode.startswith('avg') and mode!='avg-join' else ['label','v'] if mode=='avg-join' else ['label','amount'] if mode=='outer' else ['label','cohort']
        if mode=='window-rows': self.labels=['id','v','n','mean','lo','hi','numeric_total']
        if mode=='window-empty': self.labels=['id','v','n']
        (output/'case.json').write_text(json.dumps({'mode':mode,'sql':self.query,'initial':initial,'history':history},indent=2)+'\n')

    def memory(self):
        entries = list(self.model['entry'].values())
        accounts = list(self.model['account'].values())
        result = collections.Counter()
        if self.mode.startswith('window'):
            groups = collections.defaultdict(list)
            for row in entries:
                groups[absolute(row['account_id'])].append(row)
            for group in groups.values():
                if self.mode in ('window-peers','window-rank'):
                    group.sort(key=lambda row:sorting(absolute(row['amount'])))
                    keys = sorted({absolute(row['amount']) for row in group},key=sorting)
                else:
                    group.sort(key=lambda row:row['id'])
                for index,row in enumerate(group):
                    if self.mode=='window-empty':
                        value = None
                    elif self.mode=='window-rank':
                        value = 1+sum(sorting(absolute(other['amount']))<sorting(absolute(row['amount'])) for other in group)
                    else:
                        if self.mode=='window-peers':
                            position = keys.index(absolute(row['amount']))
                            frame = [other for other in group if absolute(other['amount']) in keys[max(0,position-1):position+1]]
                        else:
                            frame = group[max(0,index-1):index+2]
                        values = [abs(other['amount']) for other in frame if other['amount'] is not None and other['amount']>0]
                        value = sum(values) if values else None
                    if self.mode=='window-rows':
                        result[(row['id'],value,len(values),average(values),min(values) if values else None,max(values) if values else None,value)] += 1
                    elif self.mode=='window-empty':
                        result[(row['id'],value,0)] += 1
                    else:
                        result[(row['id'],value)] += 1
            return result
        groups = collections.defaultdict(list)
        for row in entries:
            groups[row['account_id']].append(row['amount'])
        averages = {key:average([v for v in values if v is not None]) for key,values in groups.items()}
        if self.mode.startswith('avg') and self.mode!='avg-join':
            ordered = sorted(averages,key=sorting)
            for index,key in enumerate(ordered):
                value = averages[key]
                if self.mode=='avg-filter' and (value is None or value<=0):
                    continue
                if self.mode=='avg-project':
                    value = (value or 0)+1
                if self.mode=='avg-window':
                    values = [averages[k] for k in ordered[max(0,index-1):index+1] if averages[k] is not None]
                    value = sum(values) if values else None
                result[(key,value)] += 1
            return result
        right = [{'account_id':key,'v':value} for key,value in averages.items()] if self.mode=='avg-join' else entries
        for a in accounts:
            matches = [b for b in right if a['cohort'] is not None and b['account_id'] is not None and abs(a['cohort'])==abs(b['account_id'])]
            if self.mode in ('exists','anti'):
                if bool(matches)==(self.mode=='exists'):
                    result[(a['label'],a['cohort'])] += 1
            elif self.mode=='avg-join':
                for b in matches: result[(a['label'],b['v'])] += 1
            else:
                for b in matches or [{'amount':None}]:
                    if b['amount'] is None or b['amount']>=0:
                        result[(a['label'],b['amount'])] += 1
        return result

    def run_trace(self, command):
        worker = self.start(command,'initial')
        ready = worker.event('ready')
        self.verify()
        committed = 0
        for index,transaction in enumerate(self.trace['history']):
            self.sql(statements(self.name,transaction))
            apply(self.model,transaction)
            if not transaction['rollback']:
                committed += 1
                worker.event('published',minimum_time=ready['time']+committed)
            self.verify()
            if index==4:
                worker.abort()
                worker = self.start(command,'cold')
                reopened = worker.event('ready')
                assert reopened['time']==ready['time']+committed and reopened['slot']==ready['slot']
                self.verify()
        worker.finish(graceful=True)


def qualify(output, command, mode, fixture_type=CompositionFixture):
    fixture = fixture_type(output,mode)
    with fixture.managed():
        fixture.run_trace(command)
        (output/'result.json').write_text(json.dumps({'mode':mode,'native_mv':'exact bag and output metadata',
            'independent_memory':'full-source frames/groups/nested loops','rollback':True,'cold_restart':True,
            'history':'saved case.json; simultaneous changes, NULLs, duplicates, retractions and key moves'})+'\n')


if __name__ == '__main__':
    run_modes(MODES, qualify)
