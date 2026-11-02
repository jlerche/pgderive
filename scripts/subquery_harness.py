#!/usr/bin/env python3
"""Scalar aggregates and null-aware membership through native SQL and real CDC."""
import collections
import json
import subprocess
from decimal import Decimal

from native_mv_oracle import expected, binary_difference
from numeric_arithmetic_oracle import number, average, clean
from sql_compiler_harness import SqlFixture
from worker_harness import blocked_diagnostic, History, run_modes

SCALAR = ('count','sum','avg','min','max','distinct','filter','having-zero','having',
          'limit-zero','computed','where','double','error','shadow','unaliased','self','order-alias','ordered-default','ordered-coalesce')
CORRELATED = ('count','sum','avg','min','max','distinct','filter','having-zero','having',
              'side-filter','computed','where','limit-zero','self','temporal-max')
MODES = tuple('scalar-'+m for m in SCALAR)+tuple('correlated-'+m for m in CORRELATED)+(
    'lists','lists-finite','lists-null','lists-numeric','lists-text','lists-probe-null','lists-mixed-width','lists-mixed-numeric',
    'in','in-filtered','in-null','in-literal','in-aggregate','in-sum','in-empty',
    'in-cte','in-set','in-order-alias','in-group','in-self','in-uuid','in-temporal','in-numeric','in-text','in-boolean','in-shadow','in-unaliased','in-probe-literal','in-probe-null','in-aggregate-having','in-aggregate-limit-zero',
    'in-correlated','in-correlated-filtered','in-correlated-where','in-correlated-or',
    'in-correlated-case','in-correlated-aggregate','in-correlated-aggregate-having','in-correlated-composite','in-correlated-computed')


def equal(left, right):
    if isinstance(left,Decimal) and left.is_nan():
        return isinstance(right,Decimal) and right.is_nan()
    if isinstance(right,Decimal) and right.is_nan():
        return False
    return left == right


def member(value, values):
    if not values:
        return False
    if value is None:
        return None
    if any(other is not None and equal(value,other) for other in values):
        return True
    return None if None in values else False


def negate(value):
    return None if value is None else not value


def sum_values(values):
    values = [v for v in values if v is not None]
    return sum(values) if values else None


class SubqueryFixture(SqlFixture):
    def __init__(self, output, mode):
        super().__init__(output,False)
        self.mode = mode
        a, b = self.name+'.auction', self.name+'.bid'
        self.sql(f'''ALTER TABLE {a} ALTER COLUMN category TYPE text COLLATE "C";
            ALTER TABLE {a} ADD COLUMN probe bigint,ADD COLUMN n numeric,ADD COLUMN at timestamp;
            ALTER TABLE {b} ADD COLUMN label text COLLATE "C",ADD COLUMN flag boolean,ADD COLUMN n numeric,ADD COLUMN token uuid,ADD COLUMN at timestamp;
            DELETE FROM {a}; DELETE FROM {b};
            INSERT INTO {a}(id,category,group_id,probe,enabled) VALUES
            (1,'same',1,10,true),(2,'same',1,10,true),(3,'empty',3,NULL,false),
            (4,NULL,NULL,NULL,NULL),(5,'nullprobe',2,NULL,false),(6,'hit',2,20,false),
            (7,'miss',2,99,true),(8,'empty2',4,99,false);
            INSERT INTO {b}(id,auction,price,label,flag) VALUES
            (1,1,10,'same',true),(2,1,10,'same',true),(3,1,NULL,NULL,NULL),
            (4,2,20,'hit',false),(5,2,NULL,NULL,NULL),(6,NULL,99,'miss',true),(7,NULL,NULL,NULL,NULL);
            UPDATE {a} SET n=probe; UPDATE {b} SET n=price;
            UPDATE {a} SET n='NaN' WHERE id=8; UPDATE {b} SET n='NaN' WHERE id=6;
            UPDATE {a} SET n=10.0000 WHERE id=1; UPDATE {b} SET n=10.0 WHERE id=1''')
        if mode == 'in-correlated-computed':
            self.sql(f'UPDATE {a} SET group_id=-1 WHERE id=1; UPDATE {b} SET auction=-1 WHERE id=1')
        self.sql(f"UPDATE {a} SET token=CASE WHEN probe=10 THEN '00000000-0000-0000-0000-000000000001'::uuid WHEN probe=20 THEN '00000000-0000-0000-0000-000000000002'::uuid ELSE NULL END,at=TIMESTAMP '2000-01-01'+probe*INTERVAL '1 second'; UPDATE {b} SET token=CASE WHEN price=10 THEN '00000000-0000-0000-0000-000000000001'::uuid WHEN price=20 THEN '00000000-0000-0000-0000-000000000002'::uuid ELSE NULL END,at=TIMESTAMP '2000-01-01'+price*INTERVAL '1 second'")
        if mode in ('in-temporal','correlated-temporal-max'):
            self.sql(f"UPDATE {a} SET at='infinity' WHERE id=6; UPDATE {b} SET at='infinity' WHERE id=4")
        self.labels = ['c','v']
        if mode.startswith(('scalar-','correlated-')):
            correlated = mode.startswith('correlated-')
            kind = mode.split('-',1)[1]
            aggregate = {'count':'COUNT(*)','sum':'SUM(b.price)','avg':'AVG(b.price)',
                         'min':'MIN(b.price)','max':'MAX(b.price)','distinct':'COUNT(DISTINCT b.price)',
                         'filter':'SUM(b.price) FILTER(WHERE b.price>10)',
                         'error':'SUM(10/b.price)','unaliased':'SUM(b.price)',
                         'self':'COUNT(*)' if correlated else 'MAX(b.price)', 'temporal-max':'MAX(b.at)', 'order-alias':'SUM(b.price)', 'ordered-default':'SUM(b.price)', 'ordered-coalesce':'SUM(b.price)'}.get(kind,'SUM(b.price)' if kind in ('computed','shadow') else 'AVG(b.price)' if kind == 'where' else 'COUNT(*)')
            predicates = ['b.auction=a.group_id'] if correlated else []
            if kind == 'side-filter':
                predicates.append('b.price>0')
            if kind == 'shadow':
                predicates.append('b.auction=1')
            sub = f'SELECT {aggregate} FROM {b} b'+(' WHERE '+' AND '.join(predicates) if predicates else '')
            if kind == 'order-alias':
                sub = sub.replace('SELECT SUM(b.price)','SELECT SUM(b.price) AS x')+' ORDER BY x LIMIT 1'
            if kind == 'self':
                sub = sub.replace(f'FROM {b} b',f'FROM {a} b').replace('b.price','b.probe').replace('b.auction','b.group_id')
            if kind in ('having-zero','having'):
                sub += ' HAVING COUNT(*)'+('=0' if kind == 'having-zero' else '>2')
            if kind == 'limit-zero':
                sub += ' LIMIT 0'
            if kind == 'shadow':
                sub = sub.replace('b.','a.').replace('bid b','bid a')
            value = f'({sub})'
            if kind == 'computed':
                value = f'COALESCE({value},0)+a.probe'
            elif kind == 'double':
                value += '+'+value
            self.query = f'SELECT a.category AS c,{value} AS v FROM {a} a'
            if kind == 'unaliased':
                self.query = self.query.removesuffix(f' AS v FROM {a} a')+f' FROM {a} a'
                self.labels = ['c','sum']
            if kind in ('ordered-default','ordered-coalesce'):
                label = 'sum' if kind == 'ordered-default' else 'coalesce'
                value = f'({sub})' if kind == 'ordered-default' else f'COALESCE(({sub}),0)'
                self.query = f'SELECT {value} FROM {a} a ORDER BY {label} LIMIT 2'
                self.labels = [label]
            if kind == 'where':
                self.labels = ['c']
                self.query = f'SELECT a.category AS c FROM {a} a WHERE a.probe>{value}'
        else:
            probe = 'a.n' if mode == 'in-numeric' else 'a.category' if mode in ('in-text','lists-text') else 'a.enabled' if mode == 'in-boolean' else 'a.probe'
            if mode in ('in-uuid','in-temporal'):
                probe = 'a.token' if mode == 'in-uuid' else 'a.at'
            selected = 'b.n' if mode == 'in-numeric' else 'b.label' if mode == 'in-text' else 'b.flag' if mode == 'in-boolean' else 'b.price'
            if mode in ('in-uuid','in-temporal'):
                selected = 'b.token' if mode == 'in-uuid' else 'b.at'
            rhs = f'SELECT {selected} FROM {b} b'
            if mode.startswith('in-correlated'):
                rhs += ' WHERE b.auction=a.group_id'
            if mode == 'in-correlated-composite':
                rhs += ' AND b.flag=a.enabled'
            elif mode == 'in-correlated-computed':
                rhs = rhs.replace('b.auction=a.group_id','abs(b.auction)=abs(a.group_id)')
            if mode in ('in-filtered','in-correlated-filtered','in-cte'):
                rhs += (' AND' if ' WHERE ' in rhs else ' WHERE')+' b.price IS NOT NULL'
            if mode in ('in-aggregate','in-correlated-aggregate','in-sum','in-aggregate-having','in-aggregate-limit-zero','in-correlated-aggregate-having'):
                rhs = rhs.replace('SELECT b.price', 'SELECT '+('SUM' if mode == 'in-sum' else 'MAX')+'(b.price)')
            if mode in ('in-aggregate-having','in-correlated-aggregate-having'):
                rhs += ' HAVING COUNT(*)>0'
            elif mode == 'in-aggregate-limit-zero':
                rhs += ' LIMIT 0'
            if mode == 'in-empty':
                rhs += ' WHERE false'
            elif mode == 'in-null':
                rhs = 'SELECT NULL::bigint'
            elif mode == 'in-literal':
                rhs = "SELECT '10'::bigint"
            elif mode == 'in-cte':
                rhs = f'WITH q AS ({rhs}) SELECT price FROM q'
            elif mode == 'in-set':
                rhs = f'SELECT price FROM {b} WHERE id%2=0 UNION SELECT price FROM {b} WHERE id%2=1'
            elif mode == 'in-order-alias':
                rhs = f'SELECT price AS x FROM {b} ORDER BY x DESC NULLS LAST LIMIT 1'
            elif mode == 'in-group':
                rhs = f'SELECT price AS x FROM {b} GROUP BY price HAVING COUNT(*)>=2'
            elif mode == 'in-self':
                rhs = f'SELECT z.probe FROM {a} z'
            elif mode == 'in-shadow':
                rhs = f'SELECT a.price FROM {b} a WHERE a.auction=1'
            if mode == 'in-probe-literal':
                probe = "'10'"
            elif mode in ('in-probe-null','lists-probe-null'):
                probe = 'NULL'
            if mode.startswith('lists'):
                rhs = {'lists':'10,20,NULL','lists-finite':'10,20','lists-null':'NULL',
                       'lists-numeric':'10::numeric,20::numeric,NULL',
                       'lists-text':"'same','hit',NULL",'lists-probe-null':'10,20',
                       'lists-mixed-width':"32768::integer,'32768',10",
                       'lists-mixed-numeric':"1::numeric,'1.2','10.0',NULL"}[mode]
                if mode == 'lists-mixed-width':
                    probe = 'a.probe::smallint'
                if mode == 'lists-numeric':
                    probe = 'a.probe::numeric'
            m, n = f'{probe} IN ({rhs})', f'{probe} NOT IN ({rhs})'
            self.labels = ['c','m','n']
            self.query = f'SELECT a.category AS c,{m} AS m,{n} AS n FROM {a} a'
            if mode == 'in-unaliased':
                self.labels = ['c','?column?']
                self.query = f'SELECT a.category AS c,{m} FROM {a} a'
            if mode in ('in-correlated-where','in-correlated-or'):
                self.labels = ['c']
                self.query = f'SELECT a.category AS c FROM {a} a WHERE '+n+(' OR a.enabled' if mode.endswith('-or') else '')
            elif mode == 'in-correlated-case':
                self.labels = ['c','v']
                self.query = f'SELECT q.c,CASE WHEN q.m THEN 1 WHEN q.n THEN 0 ELSE -1 END AS v FROM ({self.query}) q'

    def scalar(self, row, right):
        kind = self.mode.split('-',1)[1]
        if self.mode.startswith('correlated-'):
            right = [r for r in right if row['group_id'] is not None and row['group_id']==r['auction']]
        if kind == 'temporal-max':
            return max((r['at'] for r in right if r['at'] is not None),default=None)
        if kind == 'side-filter':
            right = [r for r in right if r['price'] is not None and r['price']>0]
        elif kind == 'shadow':
            right = [r for r in right if r['auction']==1]
        values = [r['price'] for r in right]
        present = [v for v in values if v is not None]
        if kind == 'limit-zero' or (kind == 'having-zero' and right) or (kind == 'having' and len(right)<=2):
            return None
        if kind in ('count','having-zero','having','double','side-filter'):
            return len(right)*(2 if kind == 'double' else 1)
        if kind == 'distinct':
            return len(set(present))
        if kind in ('sum','shadow','computed','unaliased','order-alias','ordered-default','ordered-coalesce'):
            value = sum_values(values)
            if kind == 'computed':
                return None if row['probe'] is None else (value or 0)+row['probe']
            return (value or 0) if kind == 'ordered-coalesce' else value
        if kind in ('avg','where'):
            return average([Decimal(v) for v in present])
        if kind == 'self':
            return len(right) if self.mode.startswith('correlated-') else max(present,default=None)
        if kind == 'min':
            return min(present,default=None)
        if kind == 'max':
            return max(present,default=None)
        if kind == 'filter':
            return sum_values([v for v in present if v>10])
        if kind == 'error':
            return sum_values([abs(10)//abs(v)*(1 if v>0 else -1) for v in present])
        raise AssertionError(kind)

    def membership(self, row, right):
        mode = self.mode
        probe = row['category'] if mode == 'lists-text' or mode == 'in-text' else row['enabled'] if mode == 'in-boolean' else number(row['n']) if mode == 'in-numeric' else row['probe']
        if mode in ('in-uuid','in-temporal'):
            probe = row['token' if mode == 'in-uuid' else 'at']
        if mode == 'in-probe-literal':
            probe = 10
        elif mode in ('in-probe-null','lists-probe-null'):
            probe = None
        if mode.startswith('lists'):
            values = ['same','hit',None] if mode == 'lists-text' else [None] if mode == 'lists-null' else [10,20] if mode in ('lists-finite','lists-probe-null') else [10,20,None]
            if mode == 'lists-mixed-width':
                values = [32768,32768,10]
            elif mode == 'lists-mixed-numeric':
                values = [Decimal(1),Decimal('1.2'),Decimal(10),None]
        else:
            if mode.startswith('in-correlated'):
                right = [r for r in right if row['group_id'] is not None and r['auction'] is not None and (abs(row['group_id'])==abs(r['auction']) if mode == 'in-correlated-computed' else row['group_id']==r['auction'])]
                if mode == 'in-correlated-composite':
                    right = [r for r in right if row['enabled'] is not None and r['flag'] is not None and row['enabled']==r['flag']]
            if mode in ('in-filtered','in-correlated-filtered','in-cte'):
                right = [r for r in right if r['price'] is not None]
            if mode == 'in-shadow':
                right = [r for r in right if r['auction']==1]
            values = [number(r['n']) if mode == 'in-numeric' else r['label'] if mode == 'in-text' else r['flag'] if mode == 'in-boolean' else r['token'] if mode == 'in-uuid' else r['at'] if mode == 'in-temporal' else r['price'] for r in right]
            if mode in ('in-aggregate','in-correlated-aggregate','in-sum','in-aggregate-having','in-aggregate-limit-zero','in-correlated-aggregate-having'):
                values = [] if mode == 'in-aggregate-limit-zero' or mode in ('in-aggregate-having','in-correlated-aggregate-having') and not right else [sum_values(values) if mode == 'in-sum' else max((v for v in values if v is not None),default=None)]
            elif mode == 'in-order-alias':
                values = sorted(values,key=lambda v:(v is None,-(v or 0)))[:1]
            elif mode == 'in-group':
                values = [v for v,n in collections.Counter(values).items() if n>=2]
            elif mode == 'in-empty':
                values = []
            elif mode == 'in-null':
                values = [None]
            elif mode == 'in-literal':
                values = [10]
        return member(probe,values)

    def verify(self):
        self.verify_sql(expected(self,self.labels))
        assert binary_difference(self) == '0', 'subquery send representation differs'
        attrs = "SELECT json_agg(json_build_array(attname,atttypid,atttypmod,attcollation) ORDER BY attnum) FROM pg_attribute WHERE attrelid='%s'::regclass AND attnum>0 AND NOT attisdropped"
        assert self.sql(attrs%(self.name+'.groups')) == self.sql(attrs%(self.name+'.pgderive_native_oracle')), 'subquery metadata differs'
        load = lambda table: json.loads(self.sql(f"SELECT COALESCE(json_agg(t),'[]') FROM {self.name}.{table} t"),parse_float=Decimal)
        left, right = load('auction'), load('bid')
        result = collections.Counter()
        for row in left:
            if self.mode.startswith(('scalar-','correlated-')):
                source = [{'price':r['probe'],'auction':r['group_id']} for r in left] if self.mode.endswith('-self') else right
                value = self.scalar(row,source)
                if self.mode.endswith('-where'):
                    if row['probe'] is not None and value is not None and row['probe']>value:
                        result[(row['category'],)] += 1
                elif self.mode in ('scalar-ordered-default','scalar-ordered-coalesce'):
                    result[(clean(value) if isinstance(value,Decimal) else value,)] += 1
                else:
                    result[(row['category'],clean(value) if isinstance(value,Decimal) else value)] += 1
            else:
                source = [{'price':r['probe']} for r in left] if self.mode == 'in-self' else right
                m = self.membership(row,source)
                n = negate(m)
                if self.mode in ('in-correlated-where','in-correlated-or'):
                    if n is True or self.mode.endswith('-or') and row['enabled'] is True:
                        result[(row['category'],)] += 1
                elif self.mode == 'in-unaliased':
                    result[(row['category'],m)] += 1
                elif self.mode == 'in-correlated-case':
                    result[(row['category'],1 if m is True else 0 if n is True else -1)] += 1
                else:
                    result[(row['category'],m,n)] += 1
        if self.mode in ('scalar-ordered-default','scalar-ordered-coalesce'):
            result = collections.Counter({key:min(weight,2) for key,weight in result.items()})
        self.verify_memory(dict(result),parse_float=Decimal)


def changes(f):
    a, b = f.name+'.auction', f.name+'.bid'
    statements = [
        f'DELETE FROM {b} WHERE id IN(3,5,7)',
        f'DELETE FROM {b} WHERE id=1; UPDATE {a} SET group_id=2,probe=20 WHERE id=2',
        f'UPDATE {b} SET auction=2 WHERE id=2; UPDATE {a} SET probe=NULL WHERE id=1',
        f'INSERT INTO {b}(id,auction,price) VALUES(100,1,NULL),(101,2,NULL); UPDATE {a} SET probe=10 WHERE id=1',
        f'DELETE FROM {b}',
        f'DELETE FROM {a}; INSERT INTO {b}(id,auction,price,label,flag) VALUES(200,2,5,\'hit\',true)',
        f"INSERT INTO {a}(id,category,group_id,probe,enabled) VALUES(100,'same',2,5,true),(101,'same',2,NULL,NULL),(102,'empty',1,NULL,false),(103,NULL,NULL,NULL,false); INSERT INTO {b}(id,auction,price) VALUES(201,2,NULL),(202,1,NULL),(203,NULL,99)",
    ]
    return [sql+f'; UPDATE {a} SET n=probe; UPDATE {b} SET n=price' for sql in statements]


def qualify(output, command, mode):
    f = SubqueryFixture(output,mode)
    a, b = f.name+'.auction', f.name+'.bid'
    with f.managed():
        if mode == MODES[0]:
            negatives = {
                'multirow-scalar':f'SELECT (SELECT price FROM {b}) AS v FROM {a}',
                'grouped-scalar':f'SELECT (SELECT SUM(price) FROM {b} GROUP BY auction) AS v FROM {a}',
                'untyped-null-rhs':f'SELECT a.probe IN(SELECT NULL) FROM {a} a',
                'untyped-string-rhs':f"SELECT a.probe IN(SELECT '10') FROM {a} a",
                'invalid-scalar-ordinal':f'SELECT (SELECT SUM(price) FROM {b} ORDER BY 2) FROM {a}',
                'ungrouped-scalar-target':f'SELECT (SELECT b.auction+COUNT(*) FROM {b} b WHERE b.auction=a.group_id) FROM {a} a',
                'ungrouped-scalar-having':f'SELECT (SELECT COUNT(*) FROM {b} b WHERE b.auction=a.group_id HAVING b.auction=1) FROM {a} a',
                'constant-boolean-guard':f'SELECT a.id FROM {a} a WHERE true OR a.probe>(SELECT SUM(10/b.price) FROM {b} b)',
                'strict-null-guard':f'SELECT NULL::numeric+(SELECT SUM(10/b.price) FROM {b} b) FROM {a}',
                'filter-guard':f'SELECT SUM((SELECT SUM(10/b.price) FROM {b} b)) FILTER(WHERE false) FROM {a}',
                'row-in':f'SELECT (a.id,a.probe) IN(SELECT id,price FROM {b}) FROM {a} a',
                'outer-aggregate-arg':f'SELECT (SELECT SUM(b.price+a.probe) FROM {b} b WHERE b.auction=a.group_id) FROM {a} a',
                'guarded-subquery':f'SELECT CASE WHEN a.enabled THEN 0::numeric ELSE (SELECT SUM(10/b.price) FROM {b} b) END FROM {a} a WHERE a.enabled',
            }
            guarded = ';'.join(negatives[key] for key in ('guarded-subquery','filter-guard','strict-null-guard','constant-boolean-guard'))
            f.sql(f'BEGIN; INSERT INTO {b}(id,auction,price) VALUES(999,2,0); '+guarded+'; ROLLBACK')
            for key in ('untyped-null-rhs','untyped-string-rhs','ungrouped-scalar-target','ungrouped-scalar-having','invalid-scalar-ordinal'):
                native = subprocess.run(f.pg+['EXPLAIN '+negatives[key]],env=f.environment,capture_output=True,text=True)
                (output/(key+'-native-error.txt')).write_text(native.stderr)
                assert native.returncode != 0, 'native SQL unexpectedly admitted '+key
            (output/'native-lazy-guard.sql').write_text(negatives['guarded-subquery']+';\n')
            for label,sql in negatives.items():
                f.rejected(command,label,sql)
            assert f.sql(f"SELECT to_regclass('{f.name}.pgderive_worker_registration') IS NULL").strip() == 't'
        f.run_history(command,History(changes=changes(f),
            final=f'UPDATE {a} SET probe=99,n=99 WHERE id=101; DELETE FROM {b} WHERE id=201',
            normalized=f.query.replace('SELECT','select'),rejected=[]))
        if mode == 'scalar-error':
            before = f.sql(f"SELECT to_jsonb(p)-'fence' FROM {f.name}.pgderive_progress p")
            sink = f.sql(f"SELECT COALESCE(jsonb_agg(jsonb_build_array(tuple,weight) ORDER BY tuple),'[]') FROM {f.bag_relation()}")
            worker = f.start(command,'expected-error');worker.event('ready')
            f.sql(f'INSERT INTO {b}(id,auction,price) VALUES(999,2,0)')
            assert 'division by zero' in blocked_diagnostic(worker)
            native = subprocess.run(f.pg+[f.query],env=f.environment,capture_output=True,text=True)
            (output/'native-error.txt').write_text(native.stderr)
            assert native.returncode != 0 and 'division by zero' in native.stderr.lower()
            assert f.sql(f"SELECT to_jsonb(p)-'fence' FROM {f.name}.pgderive_progress p") == before
            assert f.sql(f"SELECT COALESCE(jsonb_agg(jsonb_build_array(tuple,weight) ORDER BY tuple),'[]') FROM {f.bag_relation()}") == sink
            assert f.acknowledgement_ahead().strip() == '0'
        (output/'result.json').write_text(json.dumps({'mode':mode,'native_sql_send_metadata':True,
            'independent_raw_source_oracle':True,'null_poison_and_empty_rhs':True,
            'both_sources_change':True,'duplicate_outer_occurrences':True,'cold_restart':True,
            'error_boundary':mode=='scalar-error'})+'\n')


if __name__ == '__main__':
    run_modes(MODES,qualify)
