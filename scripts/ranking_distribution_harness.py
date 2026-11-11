#!/usr/bin/env python3
"""Native PostgreSQL ranking distributions through CDC and cold restart."""
from collections import Counter, defaultdict
import json

from native_mv_oracle import binary_difference, expected
from ranking_compiler_harness import qualify
from sql_compiler_harness import SqlFixture
from worker_harness import run_modes


class DistributionFixture(SqlFixture):
    oracle_name = 'independent peer positions and bucket sizes'

    def __init__(self, output, mode):
        super().__init__(output, False)
        self.mode = mode
        self.sql(f'''ALTER TABLE {self.name}.bid ADD COLUMN buckets smallint;
            UPDATE {self.name}.bid SET price=CASE WHEN id%4=0 THEN NULL ELSE id%3 END,
                buckets=CASE WHEN id%16<>1 THEN 2+id%3 END''')
        order = {
            'peers': 'ORDER BY b.price DESC NULLS FIRST GROUPS BETWEEN 2 FOLLOWING AND 1 FOLLOWING',
            'unordered': '',
            'tiles': 'ORDER BY b.id ROWS BETWEEN 1 FOLLOWING AND 1 FOLLOWING',
        }[mode]
        functions = 'percent_rank() OVER w AS pr,cume_dist() OVER w AS cd'
        self.labels = ['id', 'pr', 'cd']
        if mode == 'tiles':
            functions += (",ntile(b.buckets) OVER w AS dynamic,ntile('3') OVER w AS three,"
                          "ntile(2147483647) OVER w AS many,ntile(NULL) OVER w AS missing")
            self.labels.extend(['dynamic', 'three', 'many', 'missing'])
        self.query = (f'SELECT b.id,{functions} FROM {self.name}.bid b '
                      f'WINDOW w AS (PARTITION BY b.auction {order})')

    def changed_query(self):
        return self.query.replace('PARTITION BY b.auction', 'PARTITION BY b.price')

    def verify(self):
        self.verify_sql(expected(self, self.labels))
        assert binary_difference(self) == '0'
        metadata = json.loads(self.sql(f"""SELECT json_agg(atttypid::bigint ORDER BY attnum)
            FROM pg_attribute WHERE attrelid='{self.name}.groups'::regclass
            AND attnum>0 AND NOT attisdropped"""))
        assert metadata == [23, 701, 701] + ([23] * 4 if self.mode == 'tiles' else []), metadata

        source = json.loads(self.sql(f"SELECT COALESCE(json_agg(b),'[]') FROM {self.name}.bid b"))
        partitions = defaultdict(list)
        for row in source:
            partitions[row['auction']].append(row)
        memory = Counter()
        for partition in partitions.values():
            key = {
                'peers': lambda row: (row['price'] is not None, -(row['price'] or 0)),
                'unordered': lambda row: (),
                'tiles': lambda row: row['id'],
            }[self.mode]
            partition.sort(key=key)
            keys = [key(row) for row in partition]
            size = len(partition)
            first = next((i for i, row in enumerate(partition) if row['buckets'] is not None), size)
            dynamic = [None] * first
            if first < size:
                dynamic.extend(bucket_labels(size, partition[first]['buckets']))
            three = bucket_labels(size, 3)
            for index, row in enumerate(partition):
                before = sum(other < keys[index] for other in keys)
                through = sum(other <= keys[index] for other in keys)
                values = (row['id'], before / max(1, size-1), through / size)
                if self.mode == 'tiles':
                    values += (dynamic[index], three[index], index + 1, None)
                memory[values] += 1
        self.verify_memory(memory)


def bucket_labels(size, buckets):
    width, extra = divmod(size, min(size, buckets))
    return [bucket for bucket in range(1, min(size, buckets)+1)
            for _ in range(width + (bucket <= extra))]


if __name__ == '__main__':
    run_modes(('peers', 'unordered', 'tiles'), qualify, fixture_class=DistributionFixture)
