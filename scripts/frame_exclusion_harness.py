#!/usr/bin/env python3
"""Verify frame exclusion against native PostgreSQL through CDC and restart."""
from collections import Counter, defaultdict
import json

from native_mv_oracle import binary_difference, expected
from sql_compiler_harness import SqlFixture
from worker_harness import History, harness_arguments


class ExclusionFixture(SqlFixture):
    labels = ['id', 'empty_count', 'empty_sum', 'empty_value', 'peer_count', 'peer_nth',
              'group_count', 'distance_count', 'temporal_count', 'following_count', 'plain_count']

    def __init__(self, output):
        super().__init__(output, False)
        table = self.name + '.bid'
        self.sql(f"""ALTER TABLE {table} ADD COLUMN observed timestamp;
            UPDATE {table} SET price=CASE WHEN id%4=0 THEN NULL ELSE id%3 END,
                observed=timestamp '2024-01-01'+(id%5)*interval '1 day';""")
        self.query = f"""SELECT id,count(*) OVER single AS empty_count,
            sum(price) OVER single AS empty_sum,first_value(price) OVER single AS empty_value,
            count(*) OVER peers AS peer_count,nth_value(price,1) OVER peers AS peer_nth,
            count(*) OVER (PARTITION BY auction ORDER BY price GROUPS BETWEEN 1 PRECEDING AND
                CURRENT ROW EXCLUDE CURRENT ROW) AS group_count,
            count(*) OVER (PARTITION BY auction ORDER BY price RANGE BETWEEN 1 PRECEDING AND
                1 FOLLOWING EXCLUDE CURRENT ROW) AS distance_count,
            count(*) OVER (PARTITION BY auction ORDER BY observed RANGE BETWEEN interval '1 day'
                PRECEDING AND CURRENT ROW EXCLUDE CURRENT ROW) AS temporal_count,
            count(*) OVER (PARTITION BY auction ORDER BY id ROWS BETWEEN 1 FOLLOWING AND
                1 FOLLOWING EXCLUDE CURRENT ROW) AS following_count,
            count(*) OVER (PARTITION BY auction ORDER BY price RANGE CURRENT ROW EXCLUDE NO OTHERS) AS plain_count
            FROM {table}
            WINDOW single AS (ORDER BY id ROWS CURRENT ROW EXCLUDE CURRENT ROW),
                   peers AS (PARTITION BY auction ORDER BY price RANGE CURRENT ROW EXCLUDE CURRENT ROW)"""

    def verify(self):
        self.verify_sql(expected(self, self.labels))
        assert binary_difference(self) == '0'
        source = json.loads(self.sql(f"SELECT COALESCE(json_agg(b),'[]') FROM {self.name}.bid b"))
        groups = defaultdict(list)
        for row in source:
            groups[row['auction']].append(row)
        memory = Counter()
        for group in groups.values():
            peer_keys = sorted({row['price'] for row in group}, key=lambda price: (price is None, price))
            for row in group:
                price, observed = row['price'], row['observed']
                others = [item for item in group if item['id'] != row['id']]
                same = sum(item['price'] == price for item in others)
                index = peer_keys.index(price)
                group_keys = peer_keys[max(index-1, 0):index+1]
                group_count = sum(item['price'] in group_keys for item in others)
                distance = sum(item['price'] == price or
                               (price is not None and item['price'] is not None and abs(item['price']-price) <= 1)
                               for item in others)
                temporal = sum(item['observed'] == observed or
                               (observed is not None and item['observed'] is not None
                                and 0 <= int(observed[8:10])-int(item['observed'][8:10]) <= 1)
                               for item in others)
                following = int(any(item['id'] > row['id'] for item in others))
                memory[(row['id'], 0, None, None, same, price if same else None,
                        group_count, distance, temporal, following, same+1)] += 1
        self.verify_memory(memory)


def qualify(output, command):
    fixture = ExclusionFixture(output)
    table = fixture.name + '.bid'
    with fixture.managed():
        fixture.run_history(command, History(
            changes=[
                f"INSERT INTO {table} VALUES (100,1,NULL,NULL),(101,1,2,'2024-01-03'),(102,1,2,'2024-01-03')",
                f"UPDATE {table} SET auction=2,price=4,observed='2024-01-02' WHERE id IN (1,100,101)",
                f'UPDATE {table} SET price=NULL,observed=NULL WHERE id IN (3,4,102)',
                f'DELETE FROM {table} WHERE id IN (2,5,100)',
            ],
            final=f"UPDATE {table} SET auction=1,price=-3 WHERE id IN (1,101); DELETE FROM {table} WHERE id=4",
            normalized=fixture.query.replace(' EXCLUDE NO OTHERS', ''),
            rejected=[('changed-exclusion', fixture.query.replace(' EXCLUDE CURRENT ROW', '', 1))],
        ))
        (output/'result.json').write_text(json.dumps({
            'native_bag_and_binary': True, 'independent_oracle': True, 'cold_restart': True,
        })+'\n')


if __name__ == '__main__':
    qualify(*harness_arguments())
