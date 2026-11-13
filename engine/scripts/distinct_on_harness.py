#!/usr/bin/env python3
"""Native DISTINCT ON winners with independent per-key selection and CDC."""
from collections import Counter
from decimal import Decimal
import json

from native_mv_oracle import binary_difference, expected
from sql_compiler_harness import SqlFixture
from worker_harness import History, run_modes


class DistinctOnFixture(SqlFixture):
    def __init__(self, output, mode):
        super().__init__(output, False)
        self.mode = mode
        b = self.name + '.bid'
        self.sql(f"""DELETE FROM {b}; INSERT INTO {b} VALUES
            (1,1,9),(2,1,9),(3,1,5),(4,2,9),(5,2,4),(6,NULL,7),(7,NULL,NULL)""")
        if mode == 'numeric':
            self.sql(f"""ALTER TABLE {b} ALTER COLUMN auction TYPE numeric USING
                CASE id WHEN 1 THEN 1.0 WHEN 2 THEN 1.00 ELSE auction::numeric END""")
        if mode == 'permuted':
            self.query = f'SELECT DISTINCT ON (auction,price) id FROM {b} ORDER BY price,auction,id'
            self.labels = ['id']
        elif mode in ('partial', 'ties'):
            self.query = f'SELECT DISTINCT ON (auction,price) price FROM {b} ORDER BY auction NULLS FIRST'
            if mode == 'ties':
                self.query += ' FETCH FIRST ROW WITH TIES'
            self.labels = ['price']
        elif mode == 'unordered':
            self.query = f'SELECT DISTINCT ON (auction) id FROM {b}'
            self.labels = ['id']
        else:
            inner = (f'SELECT DISTINCT ON (auction) price FROM {b} '
                     'ORDER BY auction NULLS FIRST,price DESC NULLS LAST,id')
            if mode == 'bounded':
                self.query = inner + ' OFFSET 1 ROW FETCH NEXT 1 ROW ONLY'
            elif mode == 'outer':
                self.query = f'SELECT q.price FROM ({inner}) q WHERE q.price<8'
            else:
                self.query = inner
            self.labels = ['price']

    def verify(self):
        oracle = self.query
        if self.mode == 'unordered':
            # Select the same legal arbitrary winner as the logical-tuple order.
            oracle += ' ORDER BY auction,id'
        self.verify_sql(expected(self, self.labels, query_sql=oracle))
        assert binary_difference(self) == '0'
        b = self.name + '.bid'
        rows = json.loads(self.sql(f"SELECT COALESCE(json_agg(b),'[]') FROM {b} b"),
                          parse_float=Decimal)
        if self.mode in ('permuted', 'partial', 'ties', 'unordered'):
            groups = {}
            for row in sorted(rows, key=lambda row: row['id']):
                key = row['auction'] if self.mode == 'unordered' else (row['auction'], row['price'])
                groups.setdefault(key, row)
            label = self.labels[0]
            winners = groups.values()
            if self.mode == 'ties' and groups:
                first = min(groups.values(), key=lambda row: (row['auction'] is not None, row['auction']))
                winners = [row for row in winners if row['auction'] == first['auction']]
            self.verify_memory(Counter((row[label],) for row in winners))
            return
        rows.sort(key=lambda row: (row['auction'] is not None, row['auction'],
                                   row['price'] is None, -(row['price'] or 0), row['id']))
        winners = {}
        for row in rows:
            winners.setdefault(row['auction'], row['price'])
        values = list(winners.values())
        if self.mode == 'bounded':
            values = values[1:2]
        elif self.mode == 'outer':
            values = [value for value in values if value is not None and value < 8]
        self.verify_memory(Counter((value,) for value in values))


def qualify(output, command, mode):
    fixture = DistinctOnFixture(output, mode)
    b = fixture.name + '.bid'
    with fixture.managed():
        fixture.rejected(command, 'bad-prefix',
                         f'SELECT DISTINCT ON (auction,price) id FROM {b} ORDER BY auction,id')
        fixture.run_history(command, History(
            changes=[
                f'DELETE FROM {b} WHERE id=1; INSERT INTO {b} VALUES (100,NULL,8)',
                f'UPDATE {b} SET auction=2,price=12 WHERE id=2',
                f'DELETE FROM {b} WHERE auction=2',
                f'UPDATE {b} SET price=NULL WHERE auction=1; DELETE FROM {b} WHERE id=100',
            ],
            final=f'INSERT INTO {b} VALUES (101,2,7),(102,2,7)',
            normalized=fixture.query.replace('DISTINCT ON', 'distinct on'),
            rejected=[],
        ))
        (output/'result.json').write_text(json.dumps({
            'mode': mode, 'native_bag_and_binary': True, 'independent_winners': True,
            'cold_restart': True,
        })+'\n')


if __name__ == '__main__':
    run_modes(('winner', 'numeric', 'permuted', 'partial', 'ties', 'unordered', 'bounded', 'outer'), qualify)
