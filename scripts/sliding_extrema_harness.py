#!/usr/bin/env python3
"""Mixed integral extrema windows: native binary values, independent frames and restart."""
import collections
import json
from application_model import apply, statements
from expression_composition_harness import CompositionFixture, absolute, sorting
from native_mv_oracle import binary_difference
from numeric_compiler_harness import average
from worker_harness import run_cases


class ExtremaFixture(CompositionFixture):
    def __init__(self, output, peers, native):
        super().__init__(output,'window-rows')
        self.sql(f'ALTER TABLE {self.name}.entry ALTER COLUMN amount TYPE {native}')
        self.peers = peers
        frame = ('ORDER BY abs(amount) GROUPS BETWEEN 1 PRECEDING AND CURRENT ROW' if peers else
                 'ORDER BY id ROWS BETWEEN 3 PRECEDING AND 2 FOLLOWING')
        window = 'OVER(PARTITION BY abs(account_id) '+frame+')'
        terms = [('SUM','v'),('COUNT','n'),('AVG','mean'),('MIN','lo'),('MAX','hi')]
        self.query = 'SELECT id,'+','.join(f'{fn}(abs(amount)) FILTER(WHERE amount>0) {window} AS {label}' for fn,label in terms)+f' FROM {self.name}.entry'
        self.labels = ['id','v','n','mean','lo','hi']
        (output/'case.json').write_text(json.dumps({'sql':self.query,'initial':self.trace['initial'],
                                                  'history':self.trace['history']},indent=2)+'\n')

    def memory(self):
        groups = collections.defaultdict(list)
        for row in self.model['entry'].values(): groups[absolute(row['account_id'])].append(row)
        result = collections.Counter()
        for rows in groups.values():
            rows.sort(key=lambda row:sorting(absolute(row['amount'])) if self.peers else row['id'])
            keys = sorted({absolute(row['amount']) for row in rows},key=sorting)
            for i,row in enumerate(rows):
                if self.peers:
                    position = keys.index(absolute(row['amount']))
                    frame = [other for other in rows if absolute(other['amount']) in keys[max(0,position-1):position+1]]
                else: frame = rows[max(0,i-3):i+3]
                values = [abs(other['amount']) for other in frame if other['amount'] is not None and other['amount']>0]
                result[(row['id'],sum(values) if values else None,len(values),average(values),
                        min(values) if values else None,max(values) if values else None)] += 1
        return result

    def verify(self):
        result = super().verify()
        assert binary_difference(self)=='0','extrema native binary representation differs'
        return result


def qualify(output, command, peers, native):
    fixture = ExtremaFixture(output,peers,native)
    with fixture.managed():
        worker = fixture.start(command,'initial')
        ready = worker.event('ready')
        fixture.verify()
        committed = 0
        for i,transaction in enumerate(fixture.trace['history']):
            fixture.sql(statements(fixture.name,transaction))
            apply(fixture.model,transaction)
            if not transaction['rollback']:
                committed += 1
                worker.event('published',minimum_time=ready['time']+committed)
            fixture.verify()
            if i==4:
                worker.abort()
                worker = fixture.start(command,'cold')
                reopened = worker.event('ready')
                assert reopened['time']==ready['time']+committed and reopened['slot']==ready['slot']
                fixture.verify()
        worker.finish(graceful=True)
        (output/'result.json').write_text(json.dumps({'native':native,'peers':peers,'native_binary_and_metadata':True,
            'independent_frames':True,'cold_restart':True,'rollback':True,'computed_filtered_mixed_aggregates':True})+'\n')


if __name__ == '__main__':
    run_cases(((native + ('-groups' if peers else '-rows'), peers, native)
               for native in ('smallint','integer','bigint') for peers in (False,True)), qualify)
