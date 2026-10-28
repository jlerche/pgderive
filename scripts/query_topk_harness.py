#!/usr/bin/env python3
"""Owned PostgreSQL exact MV and raw occurrence oracle for retractable top-k."""
from worker_harness import run_modes, History
import collections
import json
from native_mv_oracle import expected
from sql_compiler_harness import SqlFixture
from query_ordered_state_harness import temporal_key

class TopKFixture(SqlFixture):
    def __init__(self,output,mode):
        super().__init__(output,False)
        self.mode=mode
        if mode=="integral-extremes":
            self.sql(f"UPDATE {self.name}.bid SET price=CASE WHEN id%5=0 THEN '9223372036854775807'::bigint WHEN id%7=0 THEN '-9223372036854775808'::bigint ELSE price END")
        self.temporal=mode in ("timestamp","timestamptz")
        if self.temporal:
            self.sql(f"ALTER TABLE {self.name}.bid ALTER COLUMN price TYPE {mode} USING CASE WHEN price IS NULL THEN NULL WHEN id%5=0 THEN '-infinity'::{mode} WHEN id%11=0 THEN 'infinity'::{mode} ELSE '2000-01-01'::{mode}+id*interval '1 microsecond' END")
        self.descending=mode!='ascending'
        self.nulls_first=mode=='nulls-first'
        self.limit=0 if mode=='empty' else 3
        self.condition={'equality':'q.rn=3','strict':'q.rn<4','reversed':'3>=q.rn'}.get(mode,f'q.rn<={self.limit}')
        direction='DESC' if self.descending else 'ASC'
        nulls='FIRST' if self.nulls_first else 'LAST'
        self.query=f'SELECT q.auction,q.price FROM (SELECT b.auction,b.price,b.id,ROW_NUMBER() OVER(PARTITION BY b.auction ORDER BY b.price {direction} NULLS {nulls},b.id) AS rn FROM {self.name}.bid b) q WHERE {self.condition}'
        self.labels=['auction','price']
    def value(self,value):
        if not self.temporal:return str(value)
        text="-infinity" if value<0 else f"2000-01-01 00:00:00.{value:06}"
        return f"'{text}'::{self.mode}"
    def changed_cutoff(self):
        return self.query.replace(f'WHERE {self.condition}', 'WHERE q.rn<=2')
    def changes(self):
        return [
            f'INSERT INTO {self.name}.bid VALUES(100,NULL,NULL),(101,1,{self.value(7)}),(102,1,{self.value(7)}),(103,1,{self.value(7)})',
            f'UPDATE {self.name}.bid SET price=NULL WHERE id=101; UPDATE {self.name}.bid SET price={self.value(-8)} WHERE id=2',
            f'DELETE FROM {self.name}.bid WHERE id=102; UPDATE {self.name}.bid SET auction=2 WHERE id=103',
            f'DELETE FROM {self.name}.bid WHERE auction=1',
            f'INSERT INTO {self.name}.bid VALUES(104,1,{self.value(-9)}),(105,1,NULL),(106,1,{self.value(-9)}),(107,1,{self.value(0)})',
        ]
    def verify(self):
        native=expected(self,self.labels)
        self.verify_sql(native)
        rows=json.loads(self.sql(f"SELECT COALESCE(json_agg(b),'[]') FROM {self.name}.bid b"))
        partitions=collections.defaultdict(list)
        for row in rows:partitions[row['auction']].append(row)
        oracle=collections.Counter()
        for key,rows in partitions.items():
            stable=sorted(rows,key=lambda row:row['id'])
            nulls=[row for row in stable if row['price'] is None]
            values=[row for row in stable if row['price'] is not None]
            values.sort(key=lambda row:temporal_key(row['price']) if self.temporal else row['price'],reverse=self.descending)
            ordered=nulls+values if self.nulls_first else values+nulls
            for position,row in enumerate(ordered,1):
                value=row['price']
                if self.temporal and value is not None:value=value.replace(' ','T')
                if position<=self.limit and (self.mode!='equality' or position==3):oracle[(key,value)]+=1
        self.verify_memory(dict(oracle))

def qualify(output,command,mode,fixture_type=TopKFixture):
    fixture=fixture_type(output,mode)
    with fixture.managed():
        changes=fixture.changes()
        fixture.run_history(command, History(
            changes=changes,
            final=f'DELETE FROM {fixture.name}.bid WHERE id=107',
            normalized=fixture.query,
            rejected=[('changed-cutoff', fixture.changed_cutoff())],
        ))
        (output/'result.json').write_text(json.dumps({'mode':mode,'native_mv':'identical SQL exact bag','memory':'independent sorted raw source occurrences','duplicates':'cutoff and projection bag','restart':True,'transactions':len(changes)+1})+'\n')

if __name__ == '__main__':
    run_modes(('descending','ascending','nulls-first','equality','strict','reversed','empty','timestamp','timestamptz','integral-extremes'), qualify)
