#!/usr/bin/env python3
"""Global SQL ordering: exact native MV bags plus independent source sorting."""
import collections
import json
from native_mv_oracle import expected
from query_ordered_state_harness import temporal_key
from query_topk_harness import TopKFixture, qualify as qualify_topk
from worker_harness import run_modes


class OrderingFixture(TopKFixture):
    def __init__(self, output, mode):
        super().__init__(output, mode)
        direction = 'ASC' if mode == 'ascending' else 'DESC'
        nulls = 'FIRST' if self.nulls_first else 'LAST'
        self.query = (f'SELECT b.auction,b.price FROM {self.name}.bid b '
                      f'ORDER BY price {direction} NULLS {nulls},b.id LIMIT {self.limit}')
        if mode == 'computed':
            self.query = self.query.replace('b.auction,b.price', 'b.auction,abs(b.price) AS price')
        elif mode == 'grouped':
            self.query = (f'SELECT auction,count(*) AS price FROM {self.name}.bid '
                          'GROUP BY auction ORDER BY price DESC,auction NULLS LAST LIMIT 3')
        elif mode == 'distinct':
            self.query = (f'SELECT DISTINCT auction,price FROM {self.name}.bid '
                          'ORDER BY 2 DESC NULLS LAST,1 NULLS LAST LIMIT 3')
        elif mode == 'derived':
            inner = self.query.replace('b.auction,b.price', 'b.auction,b.price,b.id').replace('LIMIT 3', 'LIMIT 5')
            self.query = f'SELECT q.auction,q.price FROM ({inner}) q ORDER BY q.id LIMIT 3'
        elif mode == 'unbounded':
            self.query = self.query.replace(' LIMIT 3', '')
        elif mode == 'join':
            self.query = (f'SELECT a.id AS auction,b.price FROM {self.name}.auction a '
                          f'JOIN {self.name}.bid b ON a.id=b.auction '
                          'ORDER BY b.price DESC NULLS LAST,a.id,b.id LIMIT 3')
        elif mode == 'window':
            self.query = (f'SELECT b.auction,COUNT(*) OVER(PARTITION BY b.auction ORDER BY b.id '
                          f'ROWS BETWEEN 1 PRECEDING AND CURRENT ROW) AS price FROM {self.name}.bid b '
                          'ORDER BY price DESC,b.auction NULLS LAST,b.id LIMIT 3')
        elif mode.startswith('empty-'):
            self.sql(f"UPDATE {self.name}.bid SET price='-9223372036854775808'::bigint WHERE id=1")
            self.limit = 0
            self.query = (f'SELECT b.auction,abs(b.price) AS price FROM {self.name}.bid b '
                          'ORDER BY b.id LIMIT 0')
            if mode == 'empty-derived':
                self.query = f'SELECT q.auction,q.price FROM ({self.query}) q ORDER BY q.auction'
            elif mode == 'empty-join':
                self.query = (f'SELECT a.id AS auction,abs(b.price) AS price FROM {self.name}.auction a '
                              f'JOIN {self.name}.bid b ON a.id=b.auction ORDER BY a.id,b.id LIMIT 0')
            elif mode == 'empty-window':
                self.query = (f'SELECT b.auction,COUNT(abs(b.price)) OVER(ORDER BY b.id '
                              f'ROWS UNBOUNDED PRECEDING) AS price FROM {self.name}.bid b ORDER BY b.id LIMIT 0')

    def changes(self):
        changes = super().changes()
        if self.mode == 'join':
            changes[0] += (f'; UPDATE {self.name}.auction SET id=50 WHERE id=8; '
                           f'UPDATE {self.name}.bid SET auction=50,price=999 WHERE id=101; '
                           f'INSERT INTO {self.name}.bid VALUES(108,3,998),(109,2,998),(110,1,998)')
            changes[1] += (f'; UPDATE {self.name}.auction SET id=8 WHERE id=50; '
                           f'UPDATE {self.name}.bid SET auction=8 WHERE id=101')
        return changes

    def changed_cutoff(self):
        if self.limit == 0:
            return self.query.replace('LIMIT 0', 'LIMIT 2')
        if self.mode == 'unbounded':
            return self.query + ' LIMIT 2'
        before, _, _ = self.query.rpartition('LIMIT ')
        return before + 'LIMIT 2'

    def verify(self):
        self.verify_sql(expected(self, self.labels))
        rows = json.loads(self.sql(f"SELECT COALESCE(json_agg(b),'[]') FROM {self.name}.bid b"))
        if self.mode == 'join':
            left = json.loads(self.sql(f'SELECT json_agg(a) FROM {self.name}.auction a'))
            rows = [dict(row) for a in left for row in rows if a['id'] == row['auction']]
        elif self.mode == 'window':
            groups = collections.defaultdict(list)
            for row in rows:
                groups[row['auction']].append(row)
            for group in groups.values():
                for index, row in enumerate(sorted(group, key=lambda row: row['id'])):
                    row['price'] = min(index + 1, 2)
        if self.mode == 'grouped':
            counts = collections.Counter(row['auction'] for row in rows)
            rows = [{'auction': key, 'price': count, 'id': 0} for key, count in counts.items()]
        elif self.mode == 'computed':
            for row in rows:
                if row['price'] is not None:
                    row['price'] = abs(row['price'])
        elif self.mode == 'distinct':
            rows = [dict(auction=key, price=value, id=0) for key, value in
                    set((row['auction'], row['price']) for row in rows)]
        if self.mode in ('grouped', 'distinct', 'window', 'join'):
            rows.sort(key=lambda row: (row['auction'] is None, row['auction'], row['id']))
        else:
            rows.sort(key=lambda row: row['id'])
        nulls = [row for row in rows if row['price'] is None]
        values = [row for row in rows if row['price'] is not None]
        values.sort(key=lambda row: temporal_key(row['price']) if self.temporal else row['price'],
                    reverse=self.descending)
        ordered = nulls + values if self.nulls_first else values + nulls
        if self.mode == 'derived':
            ordered = sorted(ordered[:5], key=lambda row: row['id'])
        if self.mode != 'unbounded':
            ordered = ordered[:self.limit]
        oracle = collections.Counter()
        for row in ordered:
            value = row['price']
            if self.temporal and value is not None:
                value = value.replace(' ', 'T')
            oracle[(row['auction'], value)] += 1
        self.verify_memory(dict(oracle))


def qualify(output, command, mode):
    qualify_topk(output, command, mode, fixture_type=OrderingFixture)


if __name__ == '__main__':
    run_modes(('descending', 'ascending', 'nulls-first', 'empty', 'unbounded',
               'computed', 'grouped', 'distinct', 'derived', 'timestamp',
               'timestamptz', 'integral-extremes', 'join', 'window', 'empty-overflow',
               'empty-derived', 'empty-join', 'empty-window'), qualify)
