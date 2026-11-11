#!/usr/bin/env python3
"""Compare independent window stages with PostgreSQL across CDC and cold restart."""
from collections import Counter
from decimal import Decimal
import json

from native_mv_oracle import binary_difference, expected
from numeric_compiler_harness import average
from sql_compiler_harness import SqlFixture
from worker_harness import History, run_modes


class MultipleWindowFixture(SqlFixture):
    def __init__(self, output, mode):
        super().__init__(output, False)
        self.mode = mode
        b = self.name + '.bid'
        self.sql(f'''ALTER TABLE {b} ALTER COLUMN price TYPE int;
            UPDATE {b} SET price=CASE WHEN id%4=0 THEN NULL ELSE id%3 END''')
        if mode == 'grouped':
            self.labels = ['g', 'n', 'r', 'all_groups']
            self.query = f'''SELECT b.auction AS g,count(*) AS n,
                rank() OVER (ORDER BY count(*) DESC) AS r,count(*) OVER () AS all_groups
                FROM {b} b WHERE b.id<>1 GROUP BY b.auction HAVING count(*)>1'''
        else:
            self.labels = ['id', 'running', 'mean', 'moving', 'r', 'previous', 'peer_last']
            self.query = f'''SELECT b.id,count(*) OVER prefix AS running,
                avg(b.price) OVER prefix AS mean,
                sum(b.price) OVER (prefix ROWS BETWEEN 1 PRECEDING AND 1 FOLLOWING) AS moving,
                rank() OVER (PARTITION BY b.price ORDER BY b.auction DESC NULLS LAST) AS r,
                lag(b.price) OVER (PARTITION BY b.auction ORDER BY b.id DESC) AS previous,
                last_value(b.price) OVER (ORDER BY b.price GROUPS CURRENT ROW) AS peer_last
                FROM {b} b WHERE b.id<>1
                WINDOW base AS (PARTITION BY b.auction),prefix AS (base ORDER BY b.id)'''

    def verify(self):
        self.verify_sql(expected(self, self.labels))
        assert binary_difference(self) == '0'
        source = json.loads(self.sql(f"""SELECT COALESCE(json_agg(b),'[]')
            FROM {self.name}.bid b WHERE b.id<>1"""))
        memory = Counter()
        counts = Counter(row['auction'] for row in source)
        if self.mode == 'grouped':
            groups = {key: count for key, count in counts.items() if count > 1}
            for key, count in groups.items():
                memory[(key, count, 1 + sum(other > count for other in groups.values()), len(groups))] += 1
        else:
            for row in source:
                peers = sorted((item for item in source if item['auction'] == row['auction']),
                               key=lambda item: item['id'])
                i = peers.index(row)
                prices = [item['price'] for item in peers[:i+1] if item['price'] is not None]
                frame = [item['price'] for item in peers[max(0, i-1):i+2] if item['price'] is not None]
                before = sum(item['price'] == row['price'] and item['auction'] is not None
                             and (row['auction'] is None or item['auction'] > row['auction'])
                             for item in source)
                memory[(row['id'], i+1, average(prices), sum(frame) if frame else None,
                        before+1, peers[i+1]['price'] if i+1 < len(peers) else None,
                        row['price'])] += 1
        self.verify_memory(memory, parse_float=Decimal)


def qualify(output, command, mode):
    fixture = MultipleWindowFixture(output, mode)
    b = fixture.name + '.bid'
    with fixture.managed():
        fixture.run_history(command, History(
            changes=[
                f'INSERT INTO {b} VALUES (100,NULL,NULL),(101,1,2),(102,1,2)',
                f'UPDATE {b} SET auction=1,price=7 WHERE id IN (100,101)',
                f'UPDATE {b} SET price=NULL WHERE auction=1',
                f'DELETE FROM {b} WHERE auction=1; UPDATE {b} SET auction=NULL WHERE id=3',
            ],
            final=f'INSERT INTO {b} VALUES (103,2,1),(104,2,1); DELETE FROM {b} WHERE id=2',
            normalized=fixture.query.replace('b.', '"B".').replace('bid b', 'bid AS "B"'),
            rejected=[('changed-window', fixture.query.replace('OVER ()', 'OVER (ORDER BY count(*))')
                       if mode == 'grouped' else fixture.query.replace('1 FOLLOWING', '2 FOLLOWING'))],
        ))
        (output/'result.json').write_text(json.dumps({
            'mode': mode, 'native_bag_and_binary': True, 'independent_oracle': True,
            'cold_restart': True,
        })+'\n')


if __name__ == '__main__':
    run_modes(('mixed', 'grouped'), qualify)
