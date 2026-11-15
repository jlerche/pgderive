#!/usr/bin/env python3
"""Verify grouping sets against PostgreSQL and a direct weighted count."""
from collections import Counter
import json
import subprocess

from native_mv_oracle import binary_difference, expected
from sql_compiler_harness import SqlFixture
from worker_harness import History, run_modes


class GroupingFixture(SqlFixture):
    labels = ['k', 'g', 'grouping', 'n', 'total']

    def __init__(self, output, mode):
        super().__init__(output, False)
        self.source = self.name + '.bid'
        self.sql(f'DELETE FROM {self.source}')
        self.sets = {
            'sets': [(0,), (), (0,), (1,)],
            'rollup': [(0, 1), (0,), ()],
            'cube': [(0, 1), (0,), (1,), ()],
            'distinct': [(0,), ()],
            'product': [(0, 1), (0,), (0,), (1,), (), ()],
            'metadata': [(0,), ()],
            'constants': [(0, 1), (0,), ()],
            'primary': [(0, 1), (0,)],
            'ordinary-primary': [(0,)],
            'joined-primary': [(0,)],
        }[mode]
        clause = {
            'sets': 'GROUPING SETS ((auction), (), (auction), (price))',
            'rollup': 'ROLLUP (auction, price)',
            'cube': 'CUBE (auction, price)',
            'distinct': 'DISTINCT GROUPING SETS ((auction), (1), ())',
            'product': 'GROUPING SETS (auction, ()), GROUPING SETS (price, (), ())',
            'metadata': 'ROLLUP (tag)',
            'constants': 'ROLLUP (k, g)',
            'primary': 'id, ROLLUP(auction)',
            'ordinary-primary': '1',
            'joined-primary': 'b.id',
        }[mode]
        self.query = f'''SELECT auction AS k, price AS g, GROUPING(auction,price), COUNT(*) AS n,
            SUM(price) AS total FROM {self.source} GROUP BY {clause}'''
        if mode == 'distinct':
            self.labels = ['k', 'grouping', 'n']
            self.query = f'''SELECT auction AS k, GROUPING(auction), COUNT(*) AS n
                FROM {self.source} GROUP BY {clause}'''
        if mode == 'metadata':
            self.sql(f"""ALTER TABLE {self.source}
                ADD COLUMN tag varchar(3) COLLATE "C" DEFAULT 'abc',
                DROP CONSTRAINT bid_pkey, ADD PRIMARY KEY (id) DEFERRABLE""")
            self.labels = ['k', 'grouping', 'n']
            self.query = f'SELECT tag AS k, GROUPING(tag), COUNT(*) AS n FROM {self.source} GROUP BY {clause}'
        if mode == 'constants':
            self.labels = ['k', 'g', 'grouping', 'n', 'total']
            self.query = f'''SELECT 5+0 AS k, 5+0 AS g, GROUPING(5+0), COUNT(*) AS n,
                (5+0)+1 AS total FROM {self.source}
                GROUP BY {clause} HAVING (5+0) IS NOT NULL'''
        if mode == 'primary':
            self.sql(f'''ALTER TABLE {self.source} DROP CONSTRAINT bid_pkey,
                ADD PRIMARY KEY (id) INCLUDE (price)''')
            self.labels = ['k', 'g', 'grouping', 'n', 'total']
            self.query = f'''SELECT id AS k, auction AS g, GROUPING(id,auction) AS grouping,
                COUNT(*) AS n, price AS total FROM {self.source}
                GROUP BY {clause} HAVING price IS NULL OR price > 0'''
        if mode == 'ordinary-primary':
            self.labels = ['k', 'n', 'total']
            self.query = f'''SELECT id::integer AS k, COUNT(*) AS n, price AS total
                FROM {self.source} GROUP BY {clause}'''
        if mode == 'joined-primary':
            self.labels = ['k', 'n', 'total']
            self.query = f'''SELECT b.id AS k, COUNT(*) AS n, b.price AS total
                FROM {self.source} a LEFT JOIN {self.source} b ON a.id=b.id AND b.price>0
                GROUP BY {clause}'''
        self.mode = mode

    def verify(self):
        self.verify_sql(expected(self, self.labels))
        assert binary_difference(self) == '0'
        if self.mode == 'metadata':
            columns = self.sql(f"""SELECT atttypid,atttypmod,attcollation
                FROM pg_attribute
                WHERE attrelid IN ('{self.name}.groups'::regclass,
                                    '{self.name}.pgderive_native_oracle'::regclass)
                    AND attname='k'""").splitlines()
            assert len(columns) == 2 and columns[0] == columns[1], columns
        rows = json.loads(self.sql(f"SELECT COALESCE(json_agg(b),'[]') FROM {self.source} b"))
        bag = Counter()
        for keys in self.sets:
            groups = {(): []} if not keys else {}
            for row in rows:
                if self.mode == 'primary' and row['price'] is not None and row['price'] <= 0:
                    continue
                values = (row['tag'] if self.mode == 'metadata' else row['auction'], row['price'])
                if self.mode == 'constants':
                    values = (5, 5)
                if self.mode in ('primary', 'ordinary-primary', 'joined-primary'):
                    values = (row['id'], row['auction'])
                price = row['price']
                if self.mode == 'joined-primary' and (price is None or price <= 0):
                    values = (None, None)
                    price = None
                groups.setdefault(tuple(values[index] for index in keys), []).append(price)
            mask = sum(1 << (1-index) for index in range(2) if index not in keys)
            for values, prices in groups.items():
                projected = dict(zip(keys, values))
                if self.mode in ('distinct', 'metadata'):
                    bag[(projected.get(0), int(not keys), len(prices))] += 1
                elif self.mode in ('ordinary-primary', 'joined-primary'):
                    bag[(projected[0], len(prices), prices[0])] += 1
                elif self.mode == 'constants':
                    bag[(projected.get(0), projected.get(1), int(0 not in keys), len(prices), 6)] += 1
                else:
                    present = [value for value in prices if value is not None]
                    bag[(projected.get(0), projected.get(1), mask, len(prices),
                         sum(present) if present else None)] += 1
        self.verify_memory(bag)


def qualify(output, command, mode):
    fixture = GroupingFixture(output, mode)
    source = fixture.source
    with fixture.managed():
        if mode == 'metadata':
            query = f'SELECT price, COUNT(*) FROM {source} GROUP BY id, ROLLUP(auction)'
            native = subprocess.run(fixture.pg + [query], env=fixture.environment,
                                    capture_output=True, text=True, check=False)
            assert native.returncode and 'must appear in the GROUP BY' in native.stderr, native.stderr
            fixture.rejected(command, 'deferrable-dependency', query)
        fixture.run_history(command, History(
            changes=[
                f'INSERT INTO {source}(id,auction,price) VALUES (100,1,5),(101,NULL,NULL),(102,NULL,7),(103,1,5)',
                f'UPDATE {source} SET auction=2,price=-1 WHERE id=100',
                f'DELETE FROM {source}',
            ],
            final=f'INSERT INTO {source}(id,auction,price) VALUES(104,NULL,NULL)',
            normalized=fixture.query.replace('GROUP BY', 'group by'),
            rejected=[],
        ))
        (output/'result.json').write_text(json.dumps({
            'mode': mode, 'native_binary_and_bag': True, 'independent_group_counts': True,
            'cold_restart': True,
        })+'\n')


if __name__ == '__main__':
    run_modes(('sets', 'rollup', 'cube', 'distinct', 'product', 'metadata', 'constants', 'primary', 'ordinary-primary', 'joined-primary'), qualify)
