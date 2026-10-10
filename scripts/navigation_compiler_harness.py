#!/usr/bin/env python3
"""PostgreSQL native LAG/LEAD with signed offsets and independent occurrence bags."""
import collections
from decimal import Decimal
import json
import pathlib
import sys
from native_compiler_harness import NativeFixture, instant
from ranking_compiler_harness import qualify


class NavigationFixture(NativeFixture):
    oracle_name = 'independent native ordered occurrence navigation'

    def __init__(self, output, mode):
        super().__init__(output, 'projection')
        self.mode = mode
        self.sql(f'''ALTER TABLE {self.name}.bid ADD COLUMN step integer, ADD COLUMN label text,
            ADD COLUMN code varchar(20),ADD COLUMN enabled boolean,ADD COLUMN token uuid;
            UPDATE {self.name}.bid SET step=CASE WHEN id%5=4 THEN NULL ELSE (id%5)::integer-2 END,
            label=CASE WHEN id%7=0 THEN NULL ELSE 'value-'||id END,
            code=CASE WHEN id%4=0 THEN NULL ELSE 'code-'||id END,
            enabled=CASE WHEN id%3=0 THEN NULL ELSE id%2=0 END,
            token=CASE WHEN id%4=0 THEN NULL ELSE '00000000-0000-0000-0000-000000000001'::uuid END;''')
        if mode in ('integral', 'filtered'):
            self.sql(f'ALTER TABLE {self.name}.bid ALTER COLUMN price TYPE smallint')
        self.order = 'event_time' if mode == 'temporal' else 'id'
        ordering = 'b.event_time DESC NULLS FIRST,b.id' if self.order == 'event_time' else 'b.id'
        window = f' OVER(PARTITION BY b.auction ORDER BY {ordering})'
        # (function, field, offset, default): columns are represented by strings,
        # literals by Python values, allowing an oracle independent of AST/IR.
        specs = {
            'integral': [('lag','price',1,None),('lead','price',1,None),
                ('lag','price','step','price'),('lead','price','step',-99),
                ('lag','price',None,99),('lead','price',-2147483648,'price'),
                ('lag','price',0,-99),('lag','price',1,9223372036854775807)],
            'temporal': [('lag','event_at',1,None),('lead','event_time','step','event_time'),
                ('lag','event_time',0,None)],
            'numeric': [('lag','amount',1,0),('lead','amount','step','price')],
            'opaque': [('lag','label',1,('literal','fallback')),('lead','code','step','label'),
                ('lag','enabled',1,True),('lead','token',1,'token')],
            'filtered': [('lag','price',1,None)],
        }
        self.specs = specs[mode]
        functions = []
        for index, (function, field, offset, default) in enumerate(self.specs):
            if mode in ('temporal', 'filtered') and index == 0:
                expression = f'{function}(b.{field})'
            else:
                offset_sql = f'b.{offset}' if isinstance(offset,str) else ('NULL' if offset is None else str(offset))
                if isinstance(default, tuple):
                    default_sql = "'"+default[1]+"'"
                elif isinstance(default, str):
                    default_sql = f'b.{default}'
                else:
                    default_sql = 'NULL' if default is None else ('true' if default is True else str(default))
                expression = f'{function}(b.{field},{offset_sql},{default_sql})'
            frame = ' ROWS BETWEEN CURRENT ROW AND CURRENT ROW' if index == 1 else ''
            functions.append(expression + window.replace(')',frame+')') + f' AS f{index}')
        self.query = f"SELECT b.id,{','.join(functions)} FROM {self.name}.bid b"
        self.labels = ['id'] + [f'f{index}' for index in range(len(self.specs))]
        if mode == 'filtered':
            inner = self.query.replace(' FROM ', f',row_number(){window} AS r FROM ') + ' WHERE b.id<>1'
            self.query = f'SELECT q.id,q.f0 FROM({inner}) q WHERE q.r<=2'

    def changed_query(self):
        if self.mode == 'filtered':
            return self.query.replace('q.r<=2', 'q.r<=1')
        if self.mode == 'temporal':
            return self.query.replace('lag(b.event_at)', 'lag(b.event_at,2)')
        if self.mode == 'numeric':
            return self.query.replace('lag(b.amount,1,0)', 'lag(b.amount,2,0)')
        if self.mode == 'opaque':
            return self.query.replace("lag(b.label,1,'fallback')", "lag(b.label,2,'fallback')")
        return self.query.replace('lag(b.price,1,NULL)', 'lag(b.price,2,NULL)')

    def verify(self):
        prefix = "SET DateStyle='ISO,MDY'; SET TimeZone='UTC'; "
        values = ','.join(self.labels)
        expected = f'SELECT jsonb_build_array(NULL,jsonb_build_array({values})) tuple,count(*)::bigint weight FROM ({self.query}) q GROUP BY 1'
        difference = self.sql(prefix + f'''WITH expected AS ({expected}), actual AS
            (SELECT tuple,weight FROM {self.name}.groups), difference AS
            ((SELECT * FROM expected EXCEPT ALL SELECT * FROM actual) UNION ALL
            (SELECT * FROM actual EXCEPT ALL SELECT * FROM expected)) SELECT count(*) FROM difference''')
        assert difference.strip() == '0', 'navigation differs from PostgreSQL'
        source = json.loads(self.sql(prefix + f"SELECT COALESCE(json_agg(b),'[]') FROM {self.name}.bid b"), parse_float=Decimal)
        # Temporal ordering uses native text rather than Python's bounded datetime.
        temporal = {row['id']:row['time'] for row in json.loads(self.sql(prefix + f"SELECT COALESCE(json_agg(q),'[]') FROM(SELECT id,event_time::text AS time FROM {self.name}.bid) q"))}
        groups = collections.defaultdict(list)
        for row in source:
            if self.mode != 'filtered' or row['id'] != 1:
                groups[row['auction']].append(row)
        memory = collections.Counter()
        for group in groups.values():
            group.sort(key=lambda row: row['id'])
            if self.order == 'event_time':
                group.sort(key=lambda row: instant(temporal[row['id']]), reverse=True)
            for index, row in enumerate(group):
                if self.mode == 'filtered' and index>=2:
                    continue
                outputs = []
                for function, field, offset, default in self.specs:
                    distance = row[offset] if isinstance(offset, str) else offset
                    if distance is None:
                        value = None
                    else:
                        target = index + distance if function == 'lead' else index - distance
                        if 0<=target<len(group):
                            value = group[target][field]
                        elif isinstance(default, str):
                            value = row[default]
                        elif isinstance(default, tuple):
                            value = default[1]
                        else:
                            value = default
                    outputs.append(value)
                memory[(row['id'],)+tuple(outputs)] += 1
        actual = json.loads(self.sql(f"SELECT COALESCE(json_agg(g),'[]') FROM {self.name}.groups g"), parse_float=Decimal)
        assert {tuple(row['tuple'][1]): row['weight'] for row in actual} == dict(memory)
        assert self.sql(f'''SELECT count(*) FROM pg_replication_slots s JOIN
            {self.name}.pgderive_worker_registration r ON r.slot_name=s.slot_name JOIN
            {self.name}.pgderive_progress p ON true WHERE s.confirmed_flush_lsn>p.end_lsn''').strip() == '0'


if __name__ == '__main__':
    output = pathlib.Path(sys.argv[1])
    command = sys.argv[2:]
    if command and command[0] == '--':
        command = command[1:]
    for mode in ('integral','temporal','numeric','opaque','filtered'):
        directory = output/mode
        directory.mkdir()
        qualify(directory, command, mode, fixture_class=NavigationFixture)
