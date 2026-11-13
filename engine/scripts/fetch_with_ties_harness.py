#!/usr/bin/env python3
"""Exact native cutoff peers and independent occurrence oracles through CDC."""
from collections import Counter
from decimal import Decimal
import json

from native_mv_oracle import binary_difference, expected
from query_topk_harness import TopKFixture, qualify as qualify_topk
from worker_harness import run_modes


class TiesFixture(TopKFixture):
    def __init__(self, output, mode):
        super().__init__(output, mode)
        self.offset = 1 if mode == 'offset' else 0
        self.limit = 0 if mode == 'zero' else 1
        self.nulls_first = mode == 'nulls'
        self.labels = ['price'] if self.offset else ['auction', 'price']
        if mode == 'hidden':
            self.labels = ['auction']
        source = self.name + '.bid'
        self.sql(f'''DELETE FROM {source};
            INSERT INTO {source} VALUES
                (1,2,7),(2,2,7),(3,3,7),(4,2,4),(5,NULL,NULL),(6,2,NULL)''')
        if mode == 'numeric':
            self.sql(f'''ALTER TABLE {source} ALTER COLUMN price TYPE numeric USING
                CASE id WHEN 1 THEN 7.0 WHEN 2 THEN 7.00 ELSE price::numeric END''')
        nulls = 'FIRST' if self.nulls_first else 'LAST'
        order = f'price DESC NULLS {nulls}'
        if mode == 'multi':
            order += ',auction ASC NULLS FIRST'
        self.query = (f'SELECT {",".join(self.labels)} FROM {source} ORDER BY {order} '
                      f'OFFSET {self.offset} ROWS FETCH NEXT {self.limit} ROWS WITH TIES')

    def changed_cutoff(self):
        if self.limit == 0:
            return self.query.replace('NEXT 0', 'NEXT 1')
        return self.query.replace('WITH TIES', 'ONLY')

    def changes(self):
        source = self.name + '.bid'
        return [
            f'INSERT INTO {source} VALUES (101,2,7),(102,3,7),(107,NULL,NULL)',
            f'UPDATE {source} SET price=8 WHERE id=101',
            f'DELETE FROM {source} WHERE id=101',
            f'DELETE FROM {source} WHERE price=7',
            f'UPDATE {source} SET price=8 WHERE id=107',
        ]

    def key(self, row):
        price = row['price']
        # DESC on non-NULL prices. Use a separate discriminator for NULL placement.
        key = (price is not None if self.nulls_first else price is None,
               -price if price is not None else 0)
        auction = row['auction']
        return key + ((auction is not None, auction) if self.mode == 'multi' else ())

    def verify(self):
        self.verify_sql(expected(self, self.labels))
        assert binary_difference(self) == '0'
        source = self.name + '.bid'
        rows = json.loads(self.sql(f"SELECT COALESCE(json_agg(b),'[]') FROM {source} b"),
                          parse_float=Decimal)
        rows.sort(key=self.key)
        remaining = rows[self.offset:]
        prefix = remaining[:self.limit]
        cutoff = self.key(prefix[-1]) if prefix else None
        selected = [row for row in remaining if cutoff is not None and self.key(row) <= cutoff]
        self.verify_memory(Counter(tuple(row[label] for label in self.labels) for row in selected),
                           parse_float=Decimal)


def qualify(output, command, mode):
    qualify_topk(output, command, mode, fixture_type=TiesFixture)


if __name__ == '__main__':
    run_modes(('peers', 'nulls', 'offset', 'zero', 'multi', 'numeric', 'hidden'), qualify)
