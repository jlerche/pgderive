#!/usr/bin/env python3
"""Exact numeric values, send bytes, types and error boundaries through real CDC."""
import collections
import json
import subprocess
from decimal import Decimal

from native_mv_oracle import expected, binary_difference
from numeric_arithmetic_oracle import number, clean, division, rounding, cast, average
from sql_compiler_harness import SqlFixture
from worker_harness import History, run_modes

MODES = ('division', 'mixed', 'integer-division', 'rounding', 'integer-casts', 'typmods', 'positive-scale',
         'average-group', 'average-global', 'average-window', 'average-filter',
         'average-computed', 'average-derived', 'average-having', 'average-union',
         'division-error', 'integer-error', 'typmod-error')


def finite(value):
    return value is not None and value.is_finite()


class NumericFixture(SqlFixture):
    def __init__(self, output, mode):
        super().__init__(output, False)
        self.mode = mode
        b = self.name+'.bid'
        self.sql(f'''ALTER TABLE {b} ALTER COLUMN price TYPE numeric;
            ALTER TABLE {b} ADD COLUMN divisor numeric,ADD COLUMN places integer;
            DELETE FROM {b}; INSERT INTO {b} VALUES
            (1,1,1.2000,3.00,2),(2,1,-2.500,0.0003,0),(3,2,NULL,0,NULL),
            (4,2,'NaN',0,-2),(5,2,'Infinity',-2,-1),(6,2,'-Infinity','Infinity',3),
            (7,NULL,0.0000100,10000,6),(8,NULL,999.995,1.00,-2),
            (9,3,32767.49,7,5),(10,3,-32768.49,100000,1),(11,3,1,'NaN',2)''')
        if mode == 'integer-casts':
            self.sql(f'''INSERT INTO {b} VALUES
                (904,4,2147483647.49,1,0),(905,4,-2147483648.49,1,0),
                (906,4,9223372036854775807.49,1,0),(907,4,-9223372036854775808.49,1,0)''')
        expressions = {
            'division': ['price/divisor'],
            'mixed': ['price/2', 'CASE WHEN divisor=0 THEN NULL ELSE 2::numeric/divisor END'],
            'integer-division': ['id/2','-id/2','id/(-2)','id::bigint/2'],
            'rounding': ['round(price)', 'round(price,places)', 'trunc(price)', 'trunc(price,places)',
                         'ceil(price)', 'ceiling(price)', 'floor(price)', 'abs(price)'],
            'integer-casts': [f"CASE WHEN price>='{lo}'::numeric AND price<='{hi}'::numeric THEN price::{kind} ELSE NULL END"
                              for kind,lo,hi in [('smallint','-32768.49','32767.49'),('integer','-2147483648.49','2147483647.49'),('bigint','-9223372036854775808.49','9223372036854775807.49')]],
            'typmods': [f"CASE WHEN price='Infinity'::numeric OR price='-Infinity'::numeric THEN NULL ELSE {expr} END"
                        for expr in ['price::numeric(8,2)', 'price::numeric(4,-2)', '(price::numeric(8,2))::numeric']],
            'positive-scale': ["CASE WHEN price='Infinity'::numeric OR price='-Infinity'::numeric THEN NULL ELSE (price/10000000)::numeric(3,5) END", 'id::numeric(6,2)'],
            'division-error': ['(CASE WHEN id<999 THEN NULL::numeric ELSE price END)/divisor'],
            'integer-error': ['(CASE WHEN id<999 THEN 0::numeric ELSE price END)::smallint'],
            'typmod-error': ['(CASE WHEN id<999 THEN 0::numeric ELSE price END)::numeric(3,1)'],
        }
        if mode in expressions:
            self.labels = ['id']+['v'+str(i) for i in range(len(expressions[mode]))]
            self.query = 'SELECT id,'+','.join(expr+' AS '+label for expr,label in zip(expressions[mode], self.labels[1:]))+f' FROM {b}'
        else:
            if mode == 'average-computed':
                self.sql(f'UPDATE {b} SET price=7 WHERE id=3')
            self.labels = ['g','n','mean']
            argument = 'price/divisor' if mode == 'average-computed' else 'price'
            guard = ' FILTER(WHERE divisor<>0)' if mode == 'average-computed' else " FILTER(WHERE price>'-Infinity'::numeric AND price<'Infinity'::numeric)" if mode == 'average-filter' else ''
            grouped = f'SELECT auction AS g,COUNT(*) AS n,AVG({argument}){guard} AS mean FROM {b} GROUP BY auction'
            self.query = grouped
            if mode == 'average-global':
                self.sql(f'DELETE FROM {b}')
                self.labels = ['n','mean']
                self.query = f'SELECT COUNT(*) AS n,AVG(price) AS mean FROM {b}'
            elif mode == 'average-window':
                self.labels = ['id','g','n','mean']
                frame = ' OVER(PARTITION BY auction ORDER BY id ROWS BETWEEN 1 PRECEDING AND 1 FOLLOWING)'
                self.query = f'SELECT id,auction AS g,COUNT(*){frame} AS n,AVG(price){frame} AS mean FROM {b}'
            elif mode == 'average-derived':
                self.labels = ['g','v0','v1']
                self.query = f'SELECT q.g,round(q.mean,3) AS v0,q.mean/2 AS v1 FROM ({grouped}) q'
            elif mode == 'average-having':
                self.query += ' HAVING AVG(price)>0'
            elif mode == 'average-union':
                self.query = f'WITH inputs AS (SELECT auction,price FROM {b} UNION ALL SELECT auction,price FROM {b}) SELECT auction AS g,COUNT(*) AS n,AVG(price) AS mean FROM inputs GROUP BY auction'

    def verify(self):
        self.verify_sql(expected(self, self.labels))
        assert binary_difference(self) == '0', 'numeric send representation/scale differs'
        attrs = "SELECT json_agg(json_build_array(attname,atttypid,atttypmod,attcollation) ORDER BY attnum) FROM pg_attribute WHERE attrelid='%s'::regclass AND attnum>0 AND NOT attisdropped"
        assert self.sql(attrs % (self.name+'.groups')) == self.sql(attrs % (self.name+'.pgderive_native_oracle')), 'output metadata differs'
        rows = json.loads(self.sql(f"SELECT COALESCE(json_agg(b),'[]') FROM {self.name}.bid b"), parse_float=Decimal)
        for row in rows:
            row['price'], row['divisor'] = number(row['price']), number(row['divisor'])
        self.verify_memory(dict(self.memory(rows)), parse_float=Decimal)

    def row_values(self, row):
        value, divisor, places = row['price'], row['divisor'], row['places']
        mode = self.mode
        if mode == 'division':
            return [division(value, divisor)]
        if mode == 'mixed':
            return [division(value, Decimal(2)), None if divisor == 0 else division(Decimal(2), divisor)]
        if mode == 'rounding':
            return [rounding(value),rounding(value,places),rounding(value,mode='trunc'),rounding(value,places,'trunc'),rounding(value,mode='ceil'),rounding(value,mode='ceil'),rounding(value,mode='floor'),None if value is None else value.copy_abs()]
        if mode == 'integer-division':
            quotient = row['id']//2
            return [quotient,-quotient,-quotient,quotient]
        if mode == 'integer-casts':
            return [int(rounding(value)) if finite(value) and Decimal(lo)<=value<=Decimal(hi) else None
                    for lo,hi in [('-32768.49','32767.49'),('-2147483648.49','2147483647.49'),('-9223372036854775808.49','9223372036854775807.49')]]
        if mode == 'typmods':
            return [cast(value,8,2),cast(value,4,-2),cast(value,8,2)] if value is None or not value.is_infinite() else [None]*3
        if mode == 'positive-scale':
            return [cast(division(value,Decimal(10000000)),3,5) if value is None or not value.is_infinite() else None,cast(Decimal(row['id']),6,2)]
        return [None if mode == 'division-error' else Decimal(0)]

    def memory(self, rows):
        if not self.mode.startswith('average-'):
            return collections.Counter((row['id'],*(clean(value) if isinstance(value,Decimal) else value for value in self.row_values(row))) for row in rows)
        groups = collections.defaultdict(list)
        for row in rows:
            groups[row['auction']].append(row)
        if self.mode == 'average-global':
            return collections.Counter({(len(rows),clean(average([r['price'] for r in rows]))):1})
        result = collections.Counter()
        for key, group in groups.items():
            group.sort(key=lambda row:row['id'])
            if self.mode == 'average-union':
                group *= 2
            for index in range(len(group) if self.mode == 'average-window' else 1):
                frame = group[max(0,index-1):index+2] if self.mode == 'average-window' else group
                values = [row['price'] for row in frame]
                if self.mode == 'average-filter':
                    values = [v for v in values if finite(v)]
                elif self.mode == 'average-computed':
                    values = [division(row['price'],row['divisor']) for row in frame if row['divisor'] is not None and row['divisor']!=0]
                mean = average(values)
                if self.mode == 'average-having' and not (mean is not None and (mean.is_nan() or mean>0)):
                    continue
                if self.mode == 'average-derived':
                    value = (key,clean(rounding(mean,3)),clean(division(mean,Decimal(2))))
                else:
                    value = (key,len(frame),clean(mean))
                    if self.mode == 'average-window':
                        value = (group[index]['id'],*value)
                result[value] += 1
        return result


def changes(fixture):
    b = fixture.name+'.bid'
    statements = [
            f"INSERT INTO {b} VALUES(900,1,1.230000000000000000000000000000,7,4),(901,1,-1.23,10000,-3)",
            f'DELETE FROM {b} WHERE id IN(4,11)',
            f'DELETE FROM {b} WHERE id=6',
            f'DELETE FROM {b} WHERE id=5',
            f'DELETE FROM {b} WHERE id=900; UPDATE {b} SET price=1.200000 WHERE id=1',
            f'UPDATE {b} SET price=1.20 WHERE id=1; DELETE FROM {b} WHERE id=1',
            f'DELETE FROM {b}',
            f"INSERT INTO {b} VALUES(902,1,-1.25,3,2),(903,1,1.2500,3,2)",
        ]
    # Some deletion steps intentionally target absent rows in the empty-bootstrap
    # case. A real change on the other captured source still supplies one tick.
    return [statement+f'; UPDATE {fixture.name}.auction SET group_id=COALESCE(group_id,0)+1 WHERE id=1'
            for statement in statements]


def qualify(output, command, mode):
    fixture = NumericFixture(output, mode)
    b = fixture.name+'.bid'
    with fixture.managed():
        fixture.run_history(command, History(changes=changes(fixture),
            final=f'UPDATE {b} SET price=2.50000 WHERE id=903',
            normalized=fixture.query.replace('SELECT','select'),rejected=[]))
        if mode.endswith('-error'):
            before = fixture.sql(f"SELECT to_jsonb(p)-'fence' FROM {fixture.name}.pgderive_progress p")
            sink = fixture.sql(f"SELECT COALESCE(jsonb_agg(jsonb_build_array(tuple,weight) ORDER BY tuple),'[]') FROM {fixture.bag_relation()}")
            worker = fixture.start(command,'expected-error'); worker.event('ready')
            value, divisor = ('1','0') if mode == 'division-error' else ('40000','1') if mode == 'integer-error' else ('99.95','1')
            fixture.sql(f'INSERT INTO {b} VALUES(999,77,{value},{divisor},0)')
            worker.process.wait(timeout=60)
            assert worker.process.returncode != 0, 'numeric runtime error was accepted'
            worker.abort()
            native = subprocess.run(fixture.pg+[fixture.query],env=fixture.environment,capture_output=True,text=True)
            assert native.returncode != 0, 'native PostgreSQL did not reject the same input'
            (output/'native-error.txt').write_text(native.stderr)
            diagnostic = 'division by zero' if mode == 'division-error' else 'overflow'
            assert diagnostic in (output/'expected-error/stderr.log').read_text().lower()
            assert any(term in native.stderr.lower() for term in ('division by zero','out of range','overflow'))
            assert fixture.sql(f"SELECT to_jsonb(p)-'fence' FROM {fixture.name}.pgderive_progress p") == before
            assert fixture.sql(f"SELECT COALESCE(jsonb_agg(jsonb_build_array(tuple,weight) ORDER BY tuple),'[]') FROM {fixture.bag_relation()}") == sink
            assert fixture.acknowledgement_ahead().strip() == '0'
        (output/'result.json').write_text(json.dumps({'mode':mode,'original_sql_mv':True,
            'exact_send_bytes_and_metadata':True,'decimal_memory_oracle':True,
            'scale_special_retractions':True,'cold_restart':True,
            'error_boundary':mode.endswith('-error')})+'\n')


if __name__ == '__main__':
    run_modes(MODES,qualify)
