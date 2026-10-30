#!/usr/bin/env python3
"""RIGHT/FULL and USING joins against native metadata/send and raw-source bags."""
import collections
from dataclasses import dataclass
from datetime import datetime, timedelta
from decimal import Decimal
import json
import uuid

from join_predicate_harness import eq, greater
from native_mv_oracle import expected, binary_difference
from numeric_arithmetic_oracle import clean, number
from sql_compiler_harness import SqlFixture
from worker_harness import History, run_modes


@dataclass(frozen=True)
class Case:
    kind: str = 'FULL'
    using: bool = False
    domain: str = 'int'
    variant: str = ''


CASES = {
    'right-on': Case('RIGHT'), 'full-on': Case(),
    'right-residual': Case('RIGHT', variant='residual'),
    'full-residual': Case(variant='residual'),
    'right-keyless': Case('RIGHT', variant='keyless'),
    'full-true': Case(variant='true'), 'full-false': Case(variant='false'),
    'full-null': Case(variant='null'),
    'right-where': Case('RIGHT', variant='where'),
    'full-where': Case(variant='where'),
    'right-derived': Case('RIGHT', variant='derived'),
    'full-derived': Case(variant='derived'),
    'inner-using': Case('INNER', True), 'left-using': Case('LEFT', True),
    'right-using': Case('RIGHT', True), 'full-using': Case(using=True),
    'full-using-composite': Case(using=True, variant='composite'),
    'full-using-mixed-int': Case(using=True, domain='mixed-int'),
    'inner-using-mixed-numeric': Case('INNER', True, 'mixed-numeric'),
    'left-using-mixed-numeric': Case('LEFT', True, 'mixed-numeric'),
    'right-using-mixed-numeric': Case('RIGHT', True, 'mixed-numeric'),
    'full-using-mixed-numeric': Case(using=True, domain='mixed-numeric'),
    'inner-using-numeric-int': Case('INNER', True, 'numeric-int'),
    'inner-using-numeric-var': Case('INNER', True, 'numeric-var'),
    'full-using-numeric': Case(using=True, domain='numeric'),
    'full-using-typmod-same': Case(using=True, domain='typmod-same'),
    'full-using-typmod-different': Case(using=True, domain='typmod-different'),
    'full-using-text-varchar': Case(using=True, domain='text-varchar'),
    'full-using-varchar-text': Case(using=True, domain='varchar-text'),
    'inner-using-collation': Case('INNER', True, 'collation'),
    'left-using-collation': Case('LEFT', True, 'collation'),
    'right-using-collation': Case('RIGHT', True, 'collation'),
    'full-using-collation': Case(using=True, domain='collation'),
    'inner-using-reverse-collation': Case('INNER', True, 'reverse-collation'),
    'full-using-reverse-collation': Case(using=True, domain='reverse-collation'),
    'full-using-bool': Case(using=True, domain='bool'),
    'full-using-uuid': Case(using=True, domain='uuid'),
    'full-using-timestamp': Case(using=True, domain='timestamp'),
    'full-using-instant': Case(using=True, domain='instant'),
    'chained-using': Case(using=True, variant='chained'),
    'subtrees-using': Case(using=True, variant='subtrees'),
    'full-using-group': Case(using=True, variant='group'),
    'full-using-topk': Case(using=True, variant='topk'),
}
MODES = tuple(CASES)
TYPES = {
    'int': ('integer', 'integer'), 'mixed-int': ('smallint', 'bigint'),
    'mixed-numeric': ('bigint', 'numeric'), 'numeric-int': ('numeric(8,2)', 'integer'),
    'numeric-var': ('numeric(8,2)', 'numeric'), 'numeric': ('numeric', 'numeric'),
    'typmod-same': ('numeric(8,2)', 'numeric(8,2)'),
    'typmod-different': ('numeric(8,2)', 'numeric(9,4)'),
    'text-varchar': ('text COLLATE "C"', 'varchar(20) COLLATE "C"'),
    'varchar-text': ('varchar(20) COLLATE "C"', 'text COLLATE "C"'),
    'collation': ('text', 'text COLLATE "C"'),
    'reverse-collation': ('text COLLATE "C"', 'text'),
    'bool': ('boolean', 'boolean'), 'uuid': ('uuid', 'uuid'),
    'timestamp': ('timestamp', 'timestamp'), 'instant': ('timestamptz', 'timestamptz'),
}


def outer_pairs(left, right, matches, kind):
    """Enumerate occurrences before projection, preserving duplicates on both sides."""
    seen = set()
    for a in left:
        passing = [(index, b) for index, b in enumerate(right) if matches(a, b)]
        for index, b in passing:
            seen.add(index)
            yield a, b
        if not passing and kind in ('LEFT', 'FULL'):
            yield a, None
    if kind in ('RIGHT', 'FULL'):
        for index, b in enumerate(right):
            if index not in seen:
                yield None, b


def field(row, name):
    return None if row is None else row[name]


def merged(a, b, name, kind, domain):
    # INNER prefers an already common-typed Var; otherwise it keeps the left
    # coercion. LEFT/RIGHT select their own operand; FULL coalesces left first.
    right = kind == 'RIGHT' or (kind == 'INNER' and domain in ('mixed-numeric', 'numeric-var'))
    value = field(b if right else a, name)
    if kind == 'FULL' and value is None:
        value = field(b, name)
    return number(value) if name == 'k' and domain in (
        'mixed-numeric', 'numeric-int', 'numeric-var', 'numeric',
        'typmod-same', 'typmod-different') else value


class OuterFixture(SqlFixture):
    def __init__(self, output, mode):
        super().__init__(output, False)
        self.mode, self.case = mode, CASES[mode]
        self.types = TYPES[self.case.domain]
        a, b = self.name+'.auction', self.name+'.bid'
        self.sql(f'''ALTER TABLE {a} ADD COLUMN k {self.types[0]},ADD COLUMN tk text COLLATE "C";
            ALTER TABLE {b} ADD COLUMN k {self.types[1]},ADD COLUMN tk text COLLATE "C";
            DELETE FROM {a}; DELETE FROM {b}''')
        left = [(1, 'same', 2, 1, 'one'), (2, 'same', 8, 1, 'one'),
                (3, 'null', None, None, None), (4, 'left', -1, -2, 'left'),
                (5, 'special', 0, 5, ''), (6, 'left-only', 1, 6, 'tail'),
                (7, 'infinite', 1, 8, 'special'), (8, 'left-only', 1, -9, 'tail')]
        right = [(1, 1, 5, 1, 'one'), (2, 2, 5, 1, 'one'),
                 (3, None, None, None, None), (4, 4, -2, -2, 'different'),
                 (5, 5, 0, 5, ''), (6, 6, 2, 7, 'tail'),
                 (7, 1, -1, 9, 'other'), (8, 7, 3, 8, 'special'),
                 (9, 8, 4, -8, 'tail')]
        for identifier, category, group, key, text in left:
            self.sql(f'INSERT INTO {a}(id,category,group_id,k,tk) VALUES'
                     f'({identifier},{literal(category)},{literal(group)},{self.key(key,0)},{literal(text)})')
        for identifier, auction, price, key, text in right:
            self.sql(f'INSERT INTO {b}(id,auction,price,k,tk) VALUES'
                     f'({identifier},{literal(auction)},{literal(price)},{self.key(key,1)},{literal(text)})')
        if self.case.variant == 'residual':
            self.sql(f'UPDATE {b} SET price=1 WHERE id=2')
        self.labels, self.query = self.statement()

    def key(self, value, side):
        if value is None:
            return 'NULL'
        kind, domain = self.types[side], self.case.domain
        if kind.startswith('numeric'):
            if domain not in ('mixed-numeric', 'numeric-int') and value == 5:
                return "'NaN'"
            if domain == 'numeric' and value in (8, -8):
                return "'Infinity'" if value == 8 else "'-Infinity'"
            if domain not in ('mixed-numeric', 'numeric-int') and value in (1, -2):
                return str(Decimal('1.20' if value == 1 else '-2.50'))+'00'*side
            return literal(str(value)+('.0000' if side else '.00'))
        if kind.startswith(('text', 'varchar')):
            return literal('é' if value == -2 else str(value))
        if kind == 'boolean':
            return 'true' if value % 2 else 'false'
        if kind == 'uuid':
            return literal(str(uuid.UUID(int=value+100)))
        if kind in ('timestamp', 'timestamptz'):
            value = (datetime(2000, 1, 1)+timedelta(seconds=value)).isoformat(sep=' ')
            return literal(value+('+00' if kind == 'timestamptz' else ''))
        return str(value)

    def statement(self):
        case, a, b = self.case, self.name+'.auction', self.name+'.bid'
        if case.variant == 'derived':
            a = f'(SELECT id,k,tk,category,group_id FROM {a})'
            b = f'(SELECT id,k,tk,price FROM {b})'
        condition = {'residual': 'a.k=b.k AND b.price>a.group_id',
                     'keyless': 'a.group_id<b.price', 'true': 'true',
                     'false': 'false', 'null': 'NULL'}.get(case.variant, 'a.k=b.k')
        clause = ('USING(k,tk)' if case.variant == 'composite' else 'USING(k)') if case.using else 'ON '+condition
        source = f'{a} a {case.kind} JOIN {b} b {clause}'
        labels, expressions = ['ak','bk','c','p'], ['a.k','b.k','a.category','b.price']
        if case.using:
            labels, expressions = ['k']+labels, ['k']+expressions
        if case.variant == 'composite':
            labels, expressions = ['k','tk','ak','bk','at','bt','c','p'], ['k','tk','a.k','b.k','a.tk','b.tk','a.category','b.price']
        if case.variant == 'chained':
            third = self.name+'.third'
            self.sql(f'''CREATE TABLE {third}(id integer PRIMARY KEY,k integer,price bigint);
                ALTER TABLE {third} REPLICA IDENTITY FULL;
                INSERT INTO {third} VALUES(1,1,50),(2,42,-1),(3,NULL,7);
                ALTER PUBLICATION {self.name} ADD TABLE {third}''')
            source += f' FULL JOIN {third} c USING(k)'
            labels, expressions = ['k','ak','bk','ck','c','p','q'], ['k','a.k','b.k','c.k','a.category','b.price','c.price']
        if case.variant == 'subtrees':
            source = f'({source}) FULL JOIN ({b} c FULL JOIN {a} d USING(k)) USING(k)'
            labels, expressions = ['k','ak','bk','ck','dk','c','p','q','d'], ['k','a.k','b.k','c.k','d.k','a.category','b.price','c.price','d.category']
        if case.variant == 'group':
            labels, expressions = ['k','n','total'], ['k','COUNT(*)','SUM(b.price)']
        if case.variant == 'topk':
            labels, expressions = ['k','aid','bid','p'], ['k','a.id','b.id','b.price']
        query = 'SELECT '+','.join(f'{value} AS {label}' for value,label in zip(expressions,labels))+f' FROM {source}'
        if case.variant == 'where':
            query += ' WHERE '+('a.group_id>0' if case.kind == 'RIGHT' else 'b.price>0')
        if case.variant == 'group':
            query += ' GROUP BY k'
        if case.variant == 'topk':
            query += ' ORDER BY b.price DESC,a.id,b.id LIMIT 4'
        return labels, query

    def matches(self, a, b):
        variant = self.case.variant
        if variant in ('false', 'null'):
            return False
        if variant == 'true':
            return True
        if variant == 'keyless':
            return greater(b['price'], a['group_id'])
        return eq(a['k'],b['k']) and (variant != 'residual' or greater(b['price'],a['group_id'])) and (variant != 'composite' or eq(a['tk'],b['tk']))

    def memory(self, left, right, third=()):
        case = self.case
        pairs = list(outer_pairs(left,right,self.matches,case.kind))
        if case.variant == 'chained':
            joined = [(a,b,merged(a,b,'k',case.kind,case.domain)) for a,b in pairs]
            result = collections.Counter()
            for prior,c in outer_pairs(joined,third,lambda row,c:eq(row[2],c['k']),'FULL'):
                a,b,key = prior if prior is not None else (None,None,None)
                result[(key if key is not None else field(c,'k'),field(a,'k'),field(b,'k'),field(c,'k'),field(a,'category'),field(b,'price'),field(c,'price'))] += 1
            return result
        if case.variant == 'subtrees':
            first = [(a,b,merged(a,b,'k','FULL',case.domain)) for a,b in pairs]
            second = [(c,d,merged(c,d,'k','FULL',case.domain))
                      for c,d in outer_pairs(right,left,lambda c,d:eq(c['k'],d['k']),'FULL')]
            result = collections.Counter()
            for prior,other in outer_pairs(first,second,lambda a,b:eq(a[2],b[2]),'FULL'):
                a,b,key = prior if prior is not None else (None,None,None)
                c,d,other_key = other if other is not None else (None,None,None)
                result[(key if key is not None else other_key,field(a,'k'),field(b,'k'),field(c,'k'),field(d,'k'),field(a,'category'),field(b,'price'),field(c,'price'),field(d,'category'))] += 1
            return result
        result, grouped, ranked = collections.Counter(), collections.defaultdict(list), []
        for a,b in pairs:
            key = merged(a,b,'k',case.kind,case.domain)
            if case.variant == 'where' and not (greater(field(a,'group_id'),0) if case.kind == 'RIGHT' else greater(field(b,'price'),0)):
                continue
            if case.variant == 'group':
                grouped[key].append(field(b,'price'))
                continue
            if case.variant == 'topk':
                ranked.append((key,field(a,'id'),field(b,'id'),field(b,'price')))
                continue
            values = [field(a,'k'),field(b,'k'),field(a,'category'),field(b,'price')]
            if case.using:
                values.insert(0,key)
            if case.variant == 'composite':
                values = [key,merged(a,b,'tk',case.kind,case.domain),field(a,'k'),field(b,'k'),field(a,'tk'),field(b,'tk'),field(a,'category'),field(b,'price')]
            result[tuple(clean(value) if isinstance(value,Decimal) else value for value in values)] += 1
        for key,values in grouped.items():
            present = [value for value in values if value is not None]
            result[(key,len(values),sum(present) if present else None)] += 1
        if case.variant == 'topk':
            result.update(sorted(ranked,key=lambda row:(row[3] is not None,-(row[3] or 0),row[1] is None,row[1] or 0,row[2] is None,row[2] or 0))[:4])
        return result

    def verify(self):
        self.verify_sql(expected(self,self.labels))
        assert binary_difference(self) == '0', 'outer join changed native send representation'
        attrs = "SELECT json_agg(json_build_array(attname,atttypid,atttypmod,attcollation) ORDER BY attnum) FROM pg_attribute WHERE attrelid='%s'::regclass AND attnum>0 AND NOT attisdropped"
        assert self.sql(attrs % (self.name+'.groups')) == self.sql(attrs % (self.name+'.pgderive_native_oracle')), 'outer join output metadata differs'
        def load(table, side=None):
            rows = json.loads(self.sql(f"SELECT COALESCE(json_agg(t),'[]') FROM {self.name}.{table} t"),parse_float=Decimal)
            if side is not None and self.types[side].startswith('numeric'):
                for row in rows:
                    row['k'] = number(row['k'])
            return rows
        third = load('third') if self.case.variant == 'chained' else []
        self.verify_memory(dict(self.memory(load('auction',0),load('bid',1),third)),parse_float=Decimal)


def literal(value):
    if value is None:
        return 'NULL'
    return "'"+value.replace("'","''")+"'" if isinstance(value,str) else str(value)


def changes(fixture):
    a,b = fixture.name+'.auction',fixture.name+'.bid'
    key = fixture.key
    statements = [
        f"UPDATE {a} SET k={key(9,0)},category='moved' WHERE id=4; UPDATE {b} SET k={key(9,1)},price=11 WHERE id=7; INSERT INTO {a}(id,category,group_id,k,tk) VALUES(100,'new',3,{key(10,0)},'new'); INSERT INTO {b}(id,auction,price,k,tk) VALUES(100,100,10,{key(10,1)},'new')",
        f'UPDATE {a} SET group_id=20 WHERE id=1; UPDATE {b} SET price=-9 WHERE id=1',
        f'DELETE FROM {b} WHERE id IN(1,2,100); UPDATE {a} SET group_id=-4 WHERE id=2',
        f"UPDATE {a} SET k=NULL,tk=NULL WHERE id=1; UPDATE {b} SET k={key(9,1)},tk='other',price=17 WHERE id=3",
        f"DELETE FROM {b}; UPDATE {a} SET category='empty-right' WHERE id=1",
        f'DELETE FROM {a}',
        f"INSERT INTO {b}(id,auction,price,k,tk) VALUES(101,101,30,{key(11,1)},'one'),(102,101,30,{key(11,1)},'one'),(103,NULL,NULL,NULL,NULL)",
        f"INSERT INTO {a}(id,category,group_id,k,tk) VALUES(101,'restored',0,{key(11,0)},'one'),(102,'restored',40,{key(11,0)},'one')",
        f"UPDATE {a} SET k={key(12,0)},category='shift',group_id=50 WHERE id=101; UPDATE {b} SET k={key(12,1)} WHERE id IN(101,102)",
    ]
    if fixture.case.variant == 'chained':
        statements[0] += f'; UPDATE {fixture.name}.third SET price=60 WHERE id=1; INSERT INTO {fixture.name}.third VALUES(100,10,77)'
        statements[-1] += f'; UPDATE {fixture.name}.third SET k=12 WHERE id=1'
    return statements


def qualify(output, command, mode):
    fixture = OuterFixture(output,mode)
    a,b = fixture.name+'.auction',fixture.name+'.bid'
    with fixture.managed():
        if mode == 'full-on':
            rejected = {
                'natural': f'SELECT a.id FROM {a} a NATURAL FULL JOIN {b} b',
                'inequality-full': f'SELECT a.k,b.k AS bk FROM {a} a FULL JOIN {b} b ON a.k<b.k',
                'missing-using': f'SELECT a.id FROM {a} a FULL JOIN {b} b USING(missing)',
                'duplicate-using': f'SELECT a.id FROM {a} a FULL JOIN {b} b USING(k,k)',
                'ambiguous-using': f'SELECT b.id FROM {a} a CROSS JOIN {a} c FULL JOIN {b} b USING(k)',
                'ambiguous-on-output': f'SELECT k FROM {a} a FULL JOIN {b} b ON a.k=b.k',
                'join-alias': f'SELECT j.k FROM ({a} a FULL JOIN {b} b USING(k)) j',
                'using-alias': f'SELECT j.k FROM {a} a FULL JOIN {b} b USING(k) AS j',
                'lateral': f'SELECT a.id FROM {a} a RIGHT JOIN LATERAL (SELECT id,k FROM {b}) b ON a.k=b.k',
            }
            for label,query in rejected.items():
                fixture.rejected(command,label,query)
            assert fixture.sql(f"SELECT to_regclass('{fixture.name}.pgderive_worker_registration') IS NULL").strip() == 't'
            assert fixture.sql(f"SELECT count(*) FROM pg_replication_slots WHERE slot_name LIKE '{fixture.name}%'").strip() == '0'
        final_key = 42 if fixture.case.variant == 'chained' else 12
        fixture.run_history(command,History(changes=changes(fixture),
            final=f"INSERT INTO {a}(id,category,group_id,k,tk) VALUES(104,'restored',-1,{fixture.key(final_key,0)},'one'); DELETE FROM {b} WHERE id=101",
            normalized=fixture.query.replace('SELECT','select'),rejected=[]))
        (output/'result.json').write_text(json.dumps({'mode':mode,'original_sql_mv':True,
            'native_send_and_metadata':True,'independent_nested_loop':True,
            'simultaneous_inputs':True,'duplicates_nulls_empty_sides':True,
            'using_merged_and_qualified':fixture.case.using,'cold_restart':True})+'\n')


if __name__ == '__main__':
    run_modes(MODES,qualify)
