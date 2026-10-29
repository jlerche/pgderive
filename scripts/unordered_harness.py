#!/usr/bin/env python3
"""Unordered counts qualified against a legal logical-tuple refinement."""
import collections
from native_mv_oracle import expected, binary_difference
from ordering_domains_harness import TEXT
from ordering_harness import OrderingFixture
from query_topk_harness import qualify as qualify_topk
from worker_harness import run_modes


COUNTS = {
    'limit': (3, 0, 'LIMIT 3'),
    'offset': (None, 2, 'OFFSET 2'),
    'limit-offset': (3, 2, 'LIMIT 3 OFFSET 2'),
    'offset-zero': (None, 0, 'OFFSET 0'),
    'zero': (0, 0, 'LIMIT 0'),
    'all': (None, 0, 'LIMIT ALL'),
    'all-offset': (None, 2, 'LIMIT ALL OFFSET 2'),
    'null-limit': (None, 0, 'LIMIT NULL'),
    'null-offset': (None, 0, 'OFFSET NULL'),
    'all-ordered': (None, 2, 'LIMIT ALL OFFSET 2'),
    'all-hidden': (None, 2, 'LIMIT ALL OFFSET 2'),
    'derived': (3, 1, 'LIMIT 3 OFFSET 1'),
    'join': (3, 1, 'LIMIT 3 OFFSET 1'),
    'window': (3, 1, 'LIMIT 3 OFFSET 1'),
    'text-default': (3, 1, 'LIMIT 3 OFFSET 1'),
    'zero-join': (0, 0, 'LIMIT 0'),
    'zero-window': (0, 0, 'LIMIT 0'),
}


class UnorderedFixture(OrderingFixture):
    def __init__(self, output, mode):
        base = {'join': 'join', 'window': 'window', 'zero': 'empty-overflow',
                'zero-join': 'empty-join', 'zero-window': 'empty-window'}.get(mode, 'descending')
        super().__init__(output, base)
        self.selection = mode
        self.text = mode == 'text-default'
        if self.text:
            cases = ' '.join(f"WHEN {index} THEN '{value}'" for index, value in enumerate(TEXT))
            self.sql(f'ALTER TABLE {self.name}.bid ALTER COLUMN price TYPE text USING CASE id%8 {cases} END')
        elif base == 'descending':
            self.sql(f'UPDATE {self.name}.bid SET price=7')
        body = self.query.rsplit(' ORDER BY ', 1)[0]
        self.limit, self.offset, tail = COUNTS[mode]
        keys = 'auction ASC NULLS FIRST,price ASC NULLS FIRST'
        if self.text:
            keys = 'auction ASC NULLS FIRST,price COLLATE "C" ASC NULLS FIRST'
        if mode == 'all-ordered':
            self.query = f'{body} ORDER BY price DESC NULLS LAST {tail}'
            self.oracle_query = f'{body} ORDER BY price DESC NULLS LAST,auction ASC NULLS FIRST {tail}'
        elif mode == 'all-hidden':
            self.query = f'{body} ORDER BY b.id DESC {tail}'
            self.oracle_query = self.query
        elif mode == 'derived':
            self.query = f'SELECT q.auction,q.price FROM ({body} LIMIT 5) q {tail}'
            inner = f'{body} ORDER BY {keys} LIMIT 5'
            self.oracle_query = f'SELECT q.auction,q.price FROM ({inner}) q ORDER BY {keys} {tail}'
        else:
            self.query = f'{body} {tail}'
            self.oracle_query = f'{body} ORDER BY {keys} {tail}'

    def value(self, value):
        if self.text:
            return "'" + TEXT[value % len(TEXT)] + "'"
        return super().value(value)

    def changed_cutoff(self):
        # A finite valid change for every original form, including unbounded ALL.
        body = f'SELECT b.auction,b.price FROM {self.name}.bid b'
        return body + ' LIMIT 2 OFFSET 1'

    def verify(self):
        self.verify_sql(expected(self, self.labels, query_sql=self.oracle_query))
        assert binary_difference(self) == '0', self.query
        rows = self.candidates()
        def nullable(value):
            if isinstance(value, str): value = value.encode('utf8')
            return (value is not None, value)
        rows.sort(key=lambda row: (nullable(row['auction']), nullable(row['price'])))
        if self.selection == 'derived':
            rows = rows[:5]
        elif self.selection == 'all-ordered':
            values = [row for row in rows if row['price'] is not None]
            nulls = [row for row in rows if row['price'] is None]
            rows = sorted(values, key=lambda row: row['price'], reverse=True) + nulls
        elif self.selection == 'all-hidden':
            rows.sort(key=lambda row: row['id'], reverse=True)
        rows = rows[self.offset:]
        if self.limit is not None:
            rows = rows[:self.limit]
        self.verify_memory(dict(collections.Counter((row['auction'], row['price']) for row in rows)))


def qualify(output, command, mode):
    qualify_topk(output, command, mode, fixture_type=UnorderedFixture)


if __name__ == '__main__':
    run_modes(tuple(COUNTS), qualify)
