#!/usr/bin/env python3
"""Join domains and ON match transitions against original SQL and raw-source bags."""
import collections
import json
import subprocess
from decimal import Decimal

from native_mv_oracle import expected, binary_difference
from numeric_arithmetic_oracle import number, clean
from sql_compiler_harness import SqlFixture
from worker_harness import History, run_modes

MODES = ('same-side-text', 'mixed-equality-left', 'constant-equality-left', 'numeric-cast-left', 'numeric', 'numeric-typmods', 'numeric-computed', 'numeric-mixed', 'text', 'text-computed',
         'text-varchar', 'derived-text', 'composite', 'residual-inner', 'residual-left',
         'left-side', 'right-side', 'mixed-expression', 'keyless', 'keyless-left',
         'or-left', 'constant-left', 'null-left', 'true-left', 'text-order-left',
         'nested-left', 'residual-error', 'aggregate', 'topk')


def eq(a, b):
    if a is None or b is None:
        return False
    if isinstance(a, Decimal) and isinstance(b, Decimal) and (a.is_nan() or b.is_nan()):
        return a.is_nan() and b.is_nan()
    return a == b


def greater(a, b):
    return a is not None and b is not None and a > b


class JoinFixture(SqlFixture):
    def __init__(self, output, mode):
        super().__init__(output, False)
        self.mode = mode
        a, b = self.name+'.auction', self.name+'.bid'
        self.sql(f'''ALTER TABLE {a} ADD COLUMN nk numeric,ADD COLUMN tk text COLLATE "C";
            ALTER TABLE {b} ADD COLUMN nk numeric,ADD COLUMN tk text COLLATE "C",ADD COLUMN vk varchar(20) COLLATE "C";
            DELETE FROM {a}; DELETE FROM {b};
            INSERT INTO {a}(id,category,enabled,group_id,nk,tk) VALUES
            (1,'same',true,2,1.20,'a-b'),(2,'same',false,8,1.2000,'a-b'),
            (3,'null',NULL,NULL,NULL,NULL),(4,'unicode',true,-1,-2.50,'é'),
            (5,'special',true,0,'NaN',''),(6,'special',false,1,'Infinity','z');
            INSERT INTO {b}(id,auction,price,nk,tk,vk) VALUES
            (1,1,5,1.200,'a-b','a-b'),(2,2,5,1.2,'a-b','a-b'),
            (3,NULL,NULL,NULL,NULL,NULL),(4,4,-2,-2.5000,'é','é'),
            (5,5,0,'NaN','',''),(6,6,2,'Infinity','z','z'),(7,1,-1,9,'ab','ab')''')
        if mode == 'numeric-typmods':
            self.sql(f'UPDATE {a} SET nk=6 WHERE id=6; UPDATE {b} SET nk=6 WHERE id=6; ALTER TABLE {a} ALTER COLUMN nk TYPE numeric(8,2); ALTER TABLE {b} ALTER COLUMN nk TYPE numeric(9,4)')
        if mode == 'mixed-equality-left':
            self.sql(f'UPDATE {b} SET auction=3 WHERE id=1; UPDATE {b} SET auction=4 WHERE id=2')
        if mode == 'same-side-text':
            self.sql(f'UPDATE {a} SET category=NULL WHERE id=3')
        conditions = {
            'same-side-text': 'a.category=a.category',
            'mixed-equality-left': 'a.id+b.id=b.auction',
            'constant-equality-left': '1=b.auction',
            'numeric-cast-left': 'a.id::numeric=b.auction::numeric',
            'numeric': 'a.nk=b.nk',
            'numeric-typmods': 'a.nk=b.nk',
            'numeric-computed': 'abs(a.nk)=abs(b.nk)',
            'numeric-mixed': 'a.group_id=b.nk',
            'text': 'a.tk=b.tk',
            'text-computed': "replace(a.tk,'-','')=replace(b.tk,'-','')",
            'text-varchar': 'a.tk=b.vk',
            'derived-text': 'a.tk=b.tk',
            'residual-error': 'a.nk=b.nk AND 10/(b.price-999)>0',
            'composite': 'a.nk=b.nk AND a.tk=b.tk',
            'residual-inner': 'a.nk=b.nk AND (b.price>a.group_id OR b.price IS NULL)',
            'residual-left': 'a.nk=b.nk AND (b.price>a.group_id OR b.price IS NULL)',
            'left-side': 'a.nk=b.nk AND a.enabled',
            'right-side': 'a.nk=b.nk AND b.price>0',
            'mixed-expression': 'a.nk=b.nk AND a.id+b.auction>b.price',
            'keyless': 'a.id<b.auction',
            'keyless-left': 'a.id<b.auction',
            'or-left': 'a.nk=b.nk OR a.tk=b.tk',
            'constant-left': 'false',
            'null-left': 'NULL',
            'true-left': 'true',
            'text-order-left': 'a.nk=b.nk AND a.tk<b.tk',
            'nested-left': 'a.nk=b.nk AND b.price>a.group_id',
            'aggregate': 'a.nk=b.nk AND b.price>a.group_id',
            'topk': 'a.nk=b.nk AND b.price>a.group_id',
        }
        outer = mode in ('mixed-equality-left','constant-equality-left','numeric-cast-left','residual-left','left-side','right-side','keyless-left','or-left','constant-left','null-left','true-left','text-order-left','nested-left','residual-error')
        source = f'{a} a {"LEFT JOIN" if outer else "JOIN"} {b} b ON {conditions[mode]}'
        if mode == 'derived-text':
            source = f"(SELECT id,category,nk,replace(tk,'-','') AS tk FROM {a}) a JOIN (SELECT price,replace(tk,'-','') AS tk FROM {b}) b ON a.tk=b.tk"
        self.labels = ['c','p','n']
        self.query = f'SELECT a.category AS c,b.price AS p,a.nk AS n FROM {source}'
        if mode == 'nested-left':
            self.labels = ['c','p','r']
            self.query = f'SELECT a.category AS c,b.price AS p,r.price AS r FROM {source} LEFT JOIN {b} r ON b.tk=r.tk AND r.price>b.price'
        elif mode == 'aggregate':
            self.labels = ['c','n','total']
            self.query = f'SELECT a.category AS c,COUNT(*) AS n,SUM(b.price) AS total FROM {source} GROUP BY a.category'
        elif mode == 'topk':
            self.labels = ['aid','bid','p']
            self.query = f'SELECT a.id AS aid,b.id AS bid,b.price AS p FROM {source} ORDER BY b.price DESC,a.id,b.id LIMIT 3'

    def matches(self, a, b):
        mode = self.mode
        numeric = eq(a['nk'], b['nk'])
        if mode == 'same-side-text':
            return a['category'] is not None
        if mode == 'mixed-equality-left':
            return eq(a['id']+b['id'],b['auction'])
        if mode == 'constant-equality-left':
            return eq(1,b['auction'])
        if mode == 'numeric-cast-left':
            return eq(a['id'],b['auction'])
        if mode == 'numeric-computed':
            return eq(a['nk'].copy_abs() if a['nk'] is not None else None,
                      b['nk'].copy_abs() if b['nk'] is not None else None)
        if mode == 'residual-error':
            return numeric and b['price'] is not None and b['price']>999 and b['price']<=1009
        if mode == 'numeric-mixed':
            return eq(a['group_id'], b['nk'])
        if mode in ('text','text-varchar','text-computed','derived-text'):
            left, right = a['tk'], b['vk'] if mode == 'text-varchar' else b['tk']
            if mode in ('text-computed','derived-text'):
                left, right = (v.replace('-','') if v is not None else None for v in (left,right))
            return eq(left, right)
        if mode == 'composite':
            return numeric and eq(a['tk'], b['tk'])
        if mode in ('residual-inner','residual-left'):
            return numeric and (greater(b['price'], a['group_id']) or b['price'] is None)
        if mode == 'left-side':
            return numeric and a['enabled'] is True
        if mode == 'right-side':
            return numeric and greater(b['price'], 0)
        if mode == 'mixed-expression':
            return numeric and b['auction'] is not None and greater(a['id']+b['auction'], b['price'])
        if mode in ('keyless','keyless-left'):
            return greater(b['auction'], a['id'])
        if mode == 'or-left':
            return numeric or eq(a['tk'], b['tk'])
        if mode in ('constant-left','null-left'):
            return False
        if mode == 'true-left':
            return True
        if mode == 'text-order-left':
            return numeric and a['tk'] is not None and b['tk'] is not None and a['tk'] < b['tk']
        if mode in ('nested-left','aggregate','topk'):
            return numeric and greater(b['price'], a['group_id'])
        return numeric

    def verify(self):
        self.verify_sql(expected(self, self.labels))
        assert binary_difference(self) == '0', 'join changed observable value representation'
        attrs = "SELECT json_agg(json_build_array(attname,atttypid,atttypmod,attcollation) ORDER BY attnum) FROM pg_attribute WHERE attrelid='%s'::regclass AND attnum>0 AND NOT attisdropped"
        assert self.sql(attrs % (self.name+'.groups')) == self.sql(attrs % (self.name+'.pgderive_native_oracle')), 'join output metadata differs'
        load = lambda table: json.loads(self.sql(f"SELECT COALESCE(json_agg(t),'[]') FROM {self.name}.{table} t"), parse_float=Decimal)
        left, right = load('auction'), load('bid')
        for row in left+right:
            row['nk'] = number(row['nk'])
        result, groups, ranked = collections.Counter(), collections.defaultdict(list), []
        outer = self.mode in ('mixed-equality-left','constant-equality-left','numeric-cast-left','residual-left','left-side','right-side','keyless-left','or-left','constant-left','null-left','true-left','text-order-left','nested-left','residual-error')
        for a in left:
            matches = [b for b in right if self.matches(a,b)]
            for b in matches or ([None] if outer else []):
                if self.mode == 'nested-left':
                    others = [r for r in right if b is not None and eq(b['tk'],r['tk']) and greater(r['price'],b['price'])]
                    for r in others or [None]:
                        result[(a['category'],b['price'] if b else None,r['price'] if r else None)] += 1
                elif self.mode == 'aggregate':
                    groups[a['category']].append(b['price'])
                elif self.mode == 'topk':
                    ranked.append((a['id'],b['id'],b['price']))
                else:
                    result[(a['category'],b['price'] if b else None,clean(a['nk']))] += 1
        for key, prices in groups.items():
            finite = [p for p in prices if p is not None]
            result[(key,len(prices),sum(finite) if finite else None)] += 1
        if self.mode == 'topk':
            result.update(sorted(ranked,key=lambda r:(r[2] is not None,-(r[2] or 0),r[0],r[1]))[:3])
        self.verify_memory(dict(result),parse_float=Decimal)


def qualify(output, command, mode):
    fixture = JoinFixture(output, mode)
    a, b = fixture.name+'.auction', fixture.name+'.bid'
    with fixture.managed():
        if mode == 'numeric':
            fixture.sql(f'ALTER TABLE {b} ADD COLUMN conflict text COLLATE "POSIX"')
            for label, predicate in [('unsupported-function','sqrt(a.nk)=b.nk'),
                                     ('unsupported-collation','a.tk COLLATE "C"=b.tk'),
                                     ('conflicting-collation','a.tk=b.conflict')]:
                fixture.rejected(command,label,f'SELECT a.id FROM {a} a JOIN {b} b ON {predicate}')
            fixture.rejected(command,'derived-conflicting-collation',
                f"SELECT a.id FROM (SELECT CASE WHEN price>0 THEN tk ELSE conflict END AS k FROM {b}) q JOIN {a} a ON replace(q.k,'x','y')=a.tk")
            assert fixture.sql(f"SELECT to_regclass('{fixture.name}.pgderive_worker_registration') IS NULL").strip() == 't'
        changes = [
            f'UPDATE {a} SET group_id=0 WHERE id=2; UPDATE {b} SET price=7 WHERE id=1',
            f'UPDATE {b} SET price=-9 WHERE id IN(1,2); UPDATE {a} SET enabled=true WHERE id=2',
            f'UPDATE {a} SET nk=1.200000,tk=\'ab\' WHERE id=4; INSERT INTO {b}(id,auction,price,nk,tk,vk) VALUES(100,4,10,1.20000,\'ab\',\'ab\')',
            f'DELETE FROM {b} WHERE id IN(1,2,100); UPDATE {a} SET group_id=20 WHERE id=1',
            f'UPDATE {a} SET nk=NULL,tk=NULL WHERE id=2; UPDATE {b} SET nk=1.20,tk=\'a-b\',vk=\'a-b\',price=25 WHERE id=3',
            f'DELETE FROM {a} WHERE id=5; DELETE FROM {b} WHERE id=5',
            f'DELETE FROM {b}',
            f"INSERT INTO {b}(id,auction,price,nk,tk,vk) VALUES(101,1,30,1.2,'a-b','a-b'),(102,1,30,1.200,'a-b','a-b'); UPDATE {a} SET group_id=0 WHERE id=1",
        ]
        fixture.run_history(command,History(changes=changes,
            final=f'UPDATE {a} SET group_id=40 WHERE id=1; DELETE FROM {b} WHERE id=101',
            normalized=fixture.query.replace('SELECT','select'),rejected=[]))
        if mode == 'residual-error':
            before = fixture.sql(f"SELECT to_jsonb(p)-'fence' FROM {fixture.name}.pgderive_progress p")
            sink = fixture.sql(f"SELECT COALESCE(jsonb_agg(jsonb_build_array(tuple,weight) ORDER BY tuple),'[]') FROM {fixture.bag_relation()}")
            worker = fixture.start(command,'expected-error'); worker.event('ready')
            fixture.sql(f"INSERT INTO {b}(id,auction,price,nk,tk,vk) VALUES(999,1,999,1.20,'a-b','a-b')")
            worker.process.wait(timeout=60)
            assert worker.process.returncode != 0, 'residual runtime error was accepted'
            worker.abort()
            assert 'division by zero' in (output/'expected-error/stderr.log').read_text().lower()
            native = subprocess.run(fixture.pg+[fixture.query],env=fixture.environment,capture_output=True,text=True)
            (output/'native-error.txt').write_text(native.stderr)
            assert native.returncode != 0 and 'division by zero' in native.stderr.lower()
            assert fixture.sql(f"SELECT to_jsonb(p)-'fence' FROM {fixture.name}.pgderive_progress p") == before
            assert fixture.sql(f"SELECT COALESCE(jsonb_agg(jsonb_build_array(tuple,weight) ORDER BY tuple),'[]') FROM {fixture.bag_relation()}") == sink
            assert fixture.acknowledgement_ahead().strip() == '0'
        (output/'result.json').write_text(json.dumps({'mode':mode,'native_mv':True,
            'send_bytes_and_metadata':True,'independent_nested_loop':True,
            'match_transitions':'per-left full tuple, last match removal, NULLs and duplicate matches',
            'simultaneous_inputs':True,'cold_restart':True})+'\n')


if __name__ == '__main__':
    run_modes(MODES,qualify)
