#!/usr/bin/env python3
"""SQL set bags against native send values, metadata and independent counters."""
import collections
import json
from decimal import Decimal

from native_mv_oracle import expected, binary_difference
from numeric_arithmetic_oracle import number, clean, cast
from sql_compiler_harness import SqlFixture
from worker_harness import History, run_modes

OPS = {'union':'UNION','intersect':'INTERSECT','except':'EXCEPT',
       'intersect-all':'INTERSECT ALL','except-all':'EXCEPT ALL'}
MODES = tuple(domain+'-'+op for domain in ('tuple','numeric','widen','text') for op in OPS)+(
    'text-default','text-icu','varchar','boolean','uuid','uuid-literal','temporal','numeric-mixed','numeric-null','numeric-roundscale','numeric-group',
    'explicit-union','explicit-intersect','explicit-except','self-except','self-intersect',
    'precedence','association','branch-limit','global-limit','cte-group','window','join','constant-empty')


def operation(left, right, op):
    result = collections.Counter()
    for key in left.keys() | right.keys():
        a, b = left[key], right[key]
        count = {'union':int(a+b>0),'intersect':int(a>0 and b>0),
                 'except':int(a>0 and b==0),'intersect-all':min(a,b),
                 'except-all':max(a-b,0)}[op]
        if count:
            result[key] = count
    return result


def timestamp(value, plain=False):
    if value is None or value in ('infinity','-infinity'):
        return value
    return value+('+00:00' if plain else '')


class SetFixture(SqlFixture):
    def __init__(self, output, mode):
        super().__init__(output, False)
        self.mode = mode
        self.domain, self.op = next(((domain,mode[len(domain)+1:]) for domain in ('tuple','numeric','widen','text') if mode.startswith(domain+'-') and mode[len(domain)+1:] in OPS),('widen','intersect'))
        a, b = self.name+'.auction', self.name+'.bid'
        self.sql(f'''ALTER TABLE {a} ALTER COLUMN category TYPE text COLLATE "C";
            ALTER TABLE {a} ADD COLUMN n numeric(12,2),ADD COLUMN v varchar(16) COLLATE "C",ADD COLUMN plain timestamp;
            ALTER TABLE {b} ADD COLUMN label text COLLATE "C",ADD COLUMN n numeric(10,2),
                ADD COLUMN v varchar(24) COLLATE "C",ADD COLUMN enabled boolean,ADD COLUMN token uuid,ADD COLUMN zoned timestamptz;
            DELETE FROM {a}; DELETE FROM {b};
            INSERT INTO {a}(id,category,group_id,enabled) VALUES
                (1,'alpha',1,true),(2,'alpha',1,true),(3,NULL,NULL,NULL),
                (4,'β',2,false),(5,'left',3,true),(6,'',0,false),(8,'nan',8,NULL);
            INSERT INTO {b}(id,auction,price,label,enabled) VALUES
                (1,1,10,'alpha',true),(2,1,10,'alpha',true),(3,1,10,'alpha',true),
                (4,NULL,NULL,NULL,NULL),(5,2,20,'β',false),(6,4,40,'right',false),(7,NULL,NULL,NULL,NULL),(8,8,80,'nan',NULL);
            UPDATE {a} SET n=group_id*10.01,v=category,token=CASE WHEN enabled THEN '00000000-0000-0000-0000-000000000001'::uuid ELSE NULL END,plain=TIMESTAMP '2000-01-01'+group_id*INTERVAL '1 second';
            UPDATE {b} SET n=auction*10.01,v=label,token=CASE WHEN enabled THEN '00000000-0000-0000-0000-000000000001'::uuid ELSE NULL END,zoned=TIMESTAMPTZ '2000-01-01 00:00:00+00'+auction*INTERVAL '1 second';
            UPDATE {a} SET n='NaN',plain='infinity' WHERE id=8;
            UPDATE {b} SET n='NaN',zoned='infinity' WHERE id=8''')
        if mode in ('text-default','text-icu'):
            collation = 'default' if mode == 'text-default' else 'und-x-icu'
            self.sql(f'ALTER TABLE {a} ALTER COLUMN category TYPE text COLLATE pg_catalog."{collation}"; ALTER TABLE {b} ALTER COLUMN label TYPE text COLLATE pg_catalog."{collation}"')
            if mode == 'text-icu':
                self.sql(f"INSERT INTO {a}(id,category,group_id) VALUES(9,'é',9),(10,'é',9); INSERT INTO {b}(id,auction,label) VALUES(9,9,'é')")
            self.domain, self.op = 'text','intersect-all'
        if mode == 'numeric-group':
            self.sql(f'UPDATE {a} SET n=group_id*10 WHERE id<>8; UPDATE {b} SET n=auction*10 WHERE id<>8')
        self.labels = ['g','label'] if self.domain == 'tuple' else ['g']
        select = {'tuple':('group_id AS g,category AS label','auction AS ignored,label'),
                  'numeric':('n AS g','n AS ignored'),'widen':('group_id::smallint AS g','auction::bigint AS ignored'),
                  'text':('category AS g','label AS ignored')}
        first, second = select[self.domain]
        left, right = f'SELECT {first} FROM {a}', f'SELECT {second} FROM {b}'
        self.query = left+' '+OPS[self.op]+' '+right
        if mode in ('varchar','boolean','uuid','uuid-literal','temporal','numeric-mixed','numeric-null','numeric-roundscale','numeric-group'):
            self.labels = ['g']
            first, second, self.op = {
                'varchar':('v','v','intersect'), 'boolean':('enabled','enabled','except-all'),
                'uuid':('token','token','union'),
                'uuid-literal':("'00000000-0000-0000-0000-000000000001'",'token','intersect'),'temporal':('plain','zoned','intersect-all'),
                'numeric-mixed':('group_id::numeric(12,0)','auction','except-all'),
                'numeric-null':('n','NULL','intersect'),
                'numeric-roundscale':('n::numeric(8,-1)','n::numeric(10,0)','except-all'),
                'numeric-group':('n','n','intersect-all'),
            }[mode]
            right = 'SELECT NULL' if mode == 'numeric-null' else f'SELECT {second} FROM {b}'
            self.query = f'SELECT {first} AS g FROM {a} '+('GROUP BY n HAVING COUNT(*)>0 ' if mode == 'numeric-group' else '')+OPS[self.op]+' '+right
        elif mode.startswith('explicit-'):
            self.op = mode.removeprefix('explicit-')
            self.query = left+' '+OPS[self.op]+' DISTINCT '+right
        elif mode.startswith('self-'):
            self.op = 'except' if mode == 'self-except' else 'intersect-all'
            self.query = left+' '+OPS[self.op]+' '+left
        elif mode == 'precedence':
            self.query = left+' UNION '+right+' INTERSECT SELECT 1 AS g'
        elif mode == 'association':
            self.query = left+' EXCEPT '+right+' EXCEPT SELECT 1 AS g'
        elif mode == 'branch-limit':
            self.query = f'({left} ORDER BY 1 ASC NULLS LAST LIMIT 3 OFFSET 1) INTERSECT ALL ({right} ORDER BY 1 DESC NULLS FIRST LIMIT 4)'
        elif mode == 'global-limit':
            self.query = left+' UNION '+right+' ORDER BY 1 DESC NULLS LAST LIMIT 3 OFFSET 1'
        elif mode == 'cte-group':
            self.labels = ['g','n']
            self.query = f'WITH s AS ({left} EXCEPT ALL {right}) SELECT g,COUNT(*) AS n FROM s GROUP BY g HAVING COUNT(*)>=1'
        elif mode == 'window':
            self.labels = ['g','n']
            self.query = f'SELECT s.g,COUNT(*) OVER(PARTITION BY s.g) AS n FROM ({left} INTERSECT ALL {right}) s'
        elif mode == 'join':
            self.labels = ['g','label']
            self.query = f'SELECT s.g,a.category AS label FROM ({left} EXCEPT ALL {right}) s JOIN {a} a ON s.g=a.group_id'
        elif mode == 'constant-empty':
            self.query = f'(SELECT NULL::integer AS g INTERSECT SELECT NULL::integer) EXCEPT ALL SELECT auction FROM {b} WHERE false'

    def inputs(self, left, right):
        mode, domain = self.mode, self.domain
        def value(row, side):
            key = row['group_id' if side == 0 else 'auction']
            label = row['category' if side == 0 else 'label']
            if mode == 'uuid-literal' and side == 0:
                return ('00000000-0000-0000-0000-000000000001',)
            if mode == 'uuid-literal':
                return (row['token'],)
            if mode in ('varchar','boolean','uuid'):
                return (row[{'varchar':'v','boolean':'enabled','uuid':'token'}[mode]],)
            if mode == 'temporal':
                return (timestamp(row['plain' if side == 0 else 'zoned'],side==0),)
            if mode in ('numeric-null','numeric-roundscale','numeric-group') or domain == 'numeric':
                n = number(row['n'])
                if mode == 'numeric-roundscale':
                    n = cast(n,8,-1) if side == 0 else cast(n,10,0)
                return (clean(n),)
            return (key,label) if domain == 'tuple' else (label,) if domain == 'text' else (key,)
        a = collections.Counter(value(row,0) for row in left)
        if mode == 'numeric-group':
            a = collections.Counter({key:1 for key in a})
        b = collections.Counter(value(row,1) for row in right)
        return a, collections.Counter({(None,):1}) if mode == 'numeric-null' else b

    def verify(self):
        self.verify_sql(expected(self,self.labels))
        assert binary_difference(self) == '0', 'set-operation send representation differs'
        attrs = "SELECT json_agg(json_build_array(attname,atttypid,atttypmod,attcollation) ORDER BY attnum) FROM pg_attribute WHERE attrelid='%s'::regclass AND attnum>0 AND NOT attisdropped"
        assert self.sql(attrs % (self.name+'.groups')) == self.sql(attrs % (self.name+'.pgderive_native_oracle')), 'set-operation metadata differs'
        load = lambda table: json.loads(self.sql(f"SELECT COALESCE(json_agg(t),'[]') FROM {self.name}.{table} t"),parse_float=Decimal)
        left, right = load('auction'), load('bid')
        a, b = self.inputs(left,right)
        mode = self.mode
        if mode == 'precedence':
            result = operation(a,operation(b,collections.Counter({(1,):1}),'intersect'),'union')
        elif mode == 'association':
            result = operation(operation(a,b,'except'),collections.Counter({(1,):1}),'except')
        elif mode in ('branch-limit','global-limit'):
            def expanded(bag,descending=False):
                return sorted((key[0] for key,n in bag.items() for _ in range(n)),key=lambda v:(v is not None,-(v or 0)) if descending else (v is None,v or 0))
            if mode == 'branch-limit':
                result = operation(collections.Counter((v,) for v in expanded(a)[1:4]),collections.Counter((v,) for v in expanded(b,True)[:4]),'intersect-all')
            else:
                values = operation(a,b,'union')
                ordered = sorted(values,key=lambda key:(key[0] is None,-(key[0] or 0)))[1:4]
                result = collections.Counter({key:1 for key in ordered})
        elif mode == 'constant-empty':
            result = collections.Counter({(None,):1})
        else:
            result = operation(a,a if mode.startswith('self-') else b,self.op if mode not in ('cte-group','window','join') else 'intersect-all' if mode == 'window' else 'except-all')
            if mode in ('cte-group','window'):
                result = collections.Counter({(key[0],n):n if mode == 'window' else 1 for key,n in result.items()})
            elif mode == 'join':
                joined = collections.Counter()
                for (key,), weight in result.items():
                    for row in left:
                        if key is not None and key == row['group_id']:
                            joined[(key,row['category'])] += weight
                result = joined
        self.verify_memory(dict(result),parse_float=Decimal)


def changes(fixture):
    a, b = fixture.name+'.auction', fixture.name+'.bid'
    return [
        f"INSERT INTO {a}(id,category,group_id,n,v,enabled) VALUES(100,'alpha',1,10.01,'alpha',true); INSERT INTO {b}(id,auction,price,label,n,v,enabled) VALUES(100,1,10,'alpha',10.01,'alpha',true),(101,1,10,'alpha',10.01,'alpha',true)",
        f"UPDATE {a} SET group_id=2,category='β',n=20.02,v='β',enabled=false WHERE id=2; UPDATE {b} SET auction=3,label='left',n=30.03,v='left' WHERE id=1",
        f'DELETE FROM {a} WHERE id=1; DELETE FROM {b} WHERE id=2',
        f"DELETE FROM {b}; UPDATE {a} SET category='moved',v='moved' WHERE id=100",
        f"DELETE FROM {a}; INSERT INTO {b}(id,auction,price,label,n,v) VALUES(199,1,10,'alpha',10.01,'alpha')",
        f'DELETE FROM {a}; DELETE FROM {b}',
        f"INSERT INTO {a}(id,category,group_id,n,v) VALUES(200,'alpha',1,10.01,'alpha'),(201,'alpha',1,10.01,'alpha'); INSERT INTO {b}(id,auction,price,label,n,v) VALUES(200,2,20,'β',20.02,'β'),(201,2,20,'β',20.02,'β')",
    ]


def qualify(output, command, mode):
    fixture = SetFixture(output,mode)
    a, b = fixture.name+'.auction', fixture.name+'.bid'
    with fixture.managed():
        if mode == MODES[0]:
            fixture.sql(f'ALTER TABLE {b} ADD COLUMN conflict text COLLATE "POSIX"')
            for label,query in {
                'width':f'SELECT id FROM {a} UNION SELECT id,price FROM {b}',
                'type':f'SELECT enabled FROM {a} INTERSECT SELECT price FROM {b}',
                'collation':f'SELECT category FROM {a} INTERSECT SELECT conflict FROM {b}',
                'numeric-scale':f'SELECT n::numeric FROM {a} EXCEPT SELECT n FROM {b}',
                'numeric-mixed-scale':f'SELECT n::numeric(12,3) FROM {a} INTERSECT SELECT n FROM {b}',
                'unknown-numeric-literal':f"SELECT n FROM {a} UNION SELECT '10.01'",
                'set-sort-expression':f'SELECT group_id AS g FROM {a} UNION SELECT auction FROM {b} ORDER BY g+1',
            }.items():
                fixture.rejected(command,label,query)
            assert fixture.sql(f"SELECT to_regclass('{fixture.name}.pgderive_worker_registration') IS NULL").strip() == 't'
        fixture.run_history(command,History(changes=changes(fixture),
            final=f"UPDATE {a} SET group_id=2,category='β',n=20.02,v='β' WHERE id=200; UPDATE {b} SET auction=1,label='alpha',n=10.01,v='alpha' WHERE id=200",
            normalized=fixture.query.replace('SELECT','select'),rejected=[]))
        (output/'result.json').write_text(json.dumps({'mode':mode,'original_sql_native_mv':True,
            'send_bytes_and_metadata':True,'independent_counter_oracle':True,
            'duplicates_nulls_nans_empty_states':True,'simultaneous_sources':True,'cold_restart':True})+'\n')


if __name__ == '__main__':
    run_modes(MODES,qualify)
