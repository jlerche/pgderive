#!/usr/bin/env python3
"""Durable prefix windows: exact native bags/metadata and independent prefix frames."""
import collections
import json
from application_model import apply, statements
from expression_composition_harness import CompositionFixture
from native_mv_oracle import binary_difference
from numeric_compiler_harness import average
from worker_harness import run_cases

class PrefixFixture(CompositionFixture):
    def __init__(self,output,native,peers):
        super().__init__(output,'window-rows')
        self.peers=peers
        self.sql(f'ALTER TABLE {self.name}.entry ALTER COLUMN amount TYPE {native}')
        if peers:
            self.sql(f'ALTER TABLE {self.name}.entry ADD COLUMN prefix_order {native} NOT NULL DEFAULT 0')
            self.sql(f'UPDATE {self.name}.entry SET prefix_order=id%3')
            for row in self.model['entry'].values(): row['prefix_order']=row['id']%3
            for row in self.trace['initial']['entry']: row['prefix_order']=row['id']%3
            for tick,transaction in enumerate(self.trace['history']):
                for operation in transaction['operations']:
                    if operation['table']=='entry' and operation['kind']=='upsert':
                        operation['row']['prefix_order']=(operation['row']['id']+tick)%3
            order='prefix_order'
        else:
            self.sql(f'ALTER TABLE {self.name}.entry ALTER COLUMN id TYPE {native}')
            order='id'
        window=f'OVER(PARTITION BY account_id ORDER BY {order} RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)'
        self.query='SELECT id,COUNT(*) '+window+' AS n,'+','.join(
            f'{fn}(abs(amount)) FILTER(WHERE amount>0) {window} AS {label}' for fn,label in [('COUNT','c'),('SUM','v'),('AVG','mean')])+f' FROM {self.name}.entry'
        self.labels=['id','n','c','v','mean']
        (output/'case.json').write_text(json.dumps({'sql':self.query,'initial':self.trace['initial'],'history':self.trace['history'],'order_type':native,'peers':peers},indent=2)+'\n')

    def memory(self):
        groups=collections.defaultdict(list)
        for row in self.model['entry'].values(): groups[row['account_id']].append(row)
        result=collections.Counter()
        for rows in groups.values():
            for row in rows:
                frame=[other for other in rows if (other['prefix_order']<=row['prefix_order'] if self.peers else other['id']<=row['id'])]
                values=[other['amount'] for other in frame if other['amount'] is not None and other['amount']>0]
                result[(row['id'],len(frame),len(values),sum(values) if values else None,average(values))]+=1
        return result

    def verify(self):
        result=super().verify()
        assert binary_difference(self)=='0','prefix window native binary representation differs'
        checkpoint=json.loads(self.sql(f'SELECT checkpoint FROM {self.name}.pgderive_queries'))
        arrangements={member['id'] for member in checkpoint['arrangements']}
        assert {'prefix_input','prefix_statistics'}<=arrangements,'durable prefix path was not selected'
        self.prefix_witness=sorted(arrangements)
        return result


def qualify(output,command,native,peers):
    fixture=PrefixFixture(output,native,peers)
    with fixture.managed():
        worker=fixture.start(command,'initial');ready=worker.event('ready');fixture.verify()
        committed=0
        for i,transaction in enumerate(fixture.trace['history']):
            fixture.sql(statements(fixture.name,transaction));apply(fixture.model,transaction)
            if not transaction['rollback']:
                committed+=1;worker.event('published',minimum_time=ready['time']+committed)
            fixture.verify()
            if i==4:
                worker.abort();worker=fixture.start(command,'cold');reopened=worker.event('ready')
                assert reopened['time']==ready['time']+committed and reopened['slot']==ready['slot']
                fixture.verify()
        worker.finish(graceful=True)
        (output/'result.json').write_text(json.dumps({'native':native,'peers':peers,'native_binary_and_metadata':True,'independent_prefix_frames':True,'cold_restart':True,'rollback':True,'prefix_statistics':True,'durable_arrangements':fixture.prefix_witness})+'\n')

if __name__ == '__main__':
    run_cases(((native + ('-peers' if peers else '-ordered'), native, peers)
               for native in ('smallint','integer','bigint') for peers in (False,True)), qualify)
