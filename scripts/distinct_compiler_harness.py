#!/usr/bin/env python3
"""PostgreSQL DISTINCT aggregates with retractions, FILTER and composition."""
import collections
from decimal import Decimal
import json

from native_mv_oracle import binary_difference, expected
from numeric_compiler_harness import average
from partition_compiler_harness import PartitionFixture, qualify
from worker_harness import History, run_cases


class DistinctFixture(PartitionFixture):
    def __init__(self, output, window, mode):
        super().__init__(output, False)
        self.mode = mode
        typename = {'int2': 'smallint', 'int4': 'integer', 'int8': 'bigint',
                    'nested': 'bigint', 'global': 'integer', 'mixed': 'integer'}[mode]
        source = f'{self.name}.bid'
        self.sql(f'ALTER TABLE {source} ALTER COLUMN price TYPE {typename}; '
                 f'UPDATE {source} SET price=-2 WHERE id%3=0; '
                 f'UPDATE {source} SET price=NULL WHERE id%5=0')
        if mode in ('int8', 'nested'):
            self.sql(f'UPDATE {source} SET price=9223372036854775807 WHERE id=1; '
                     f'UPDATE {source} SET price=-9223372036854775808 WHERE id=2')
        fields = ('COUNT(DISTINCT b.price) AS unique_count,'
                  'COUNT(DISTINCT b.auction) AS present_key,'
                  'COUNT(DISTINCT b.price) FILTER (WHERE b.price<0) AS negative,COUNT(*) AS n,'
                  'SUM(DISTINCT b.price) AS total,AVG(DISTINCT b.price) AS mean,'
                  'SUM(DISTINCT (10 / b.price)::bigint) FILTER (WHERE b.price<>0) AS safe_total')
        self.labels = ['unique_count', 'present_key', 'negative', 'n', 'total', 'mean', 'safe_total']
        if mode == 'mixed':
            fields += (',MIN(DISTINCT b.price) AS lo,MAX(DISTINCT b.price) AS hi,'
                       'bool_and(DISTINCT b.price<0) AS all_negative,'
                       'bool_or(DISTINCT b.price<0) AS any_negative,'
                       'every(DISTINCT b.price<0) FILTER (WHERE b.price<0) AS every_negative')
            self.labels += ['lo', 'hi', 'all_negative', 'any_negative', 'every_negative']
        if mode == 'global':
            self.query = f'SELECT {fields} FROM {source} b'
            return
        self.query = f'SELECT b.auction AS auction,{fields} FROM {source} b GROUP BY b.auction'
        self.labels.insert(0, 'auction')
        if mode == 'nested':
            self.query = (f'SELECT {",".join(f"q.{label}" for label in self.labels)},'
                          f'MAX(q.unique_count) OVER() AS maximum FROM ({self.query}) q')
            self.labels.append('maximum')

    def aggregate(self, rows):
        values = {row['price'] for row in rows if row['price'] is not None}
        keys = {row['auction'] for row in rows if row['auction'] is not None}
        negative = sum(value < 0 for value in values)
        quotients = {(-1 if value < 0 else 1) * (10 // abs(value)) for value in values if value}
        result = (len(values), len(keys), negative, len(rows),
                  sum(values) if values else None, average(values),
                  sum(quotients) if quotients else None)
        if self.mode == 'mixed':
            result += (min(values) if values else None, max(values) if values else None,
                       negative == len(values) if values else None,
                       negative > 0 if values else None, True if negative else None)
        return result

    def verify(self):
        self.verify_sql(expected(self, self.labels))
        assert binary_difference(self) == '0'
        attrs = ("SELECT json_agg(json_build_array(attname,atttypid,atttypmod,attcollation) ORDER BY attnum)"
                 " FROM pg_attribute WHERE attrelid='%s'::regclass AND attnum>0 AND NOT attisdropped")
        assert self.sql(attrs % (self.name+'.groups')) == self.sql(attrs % (self.name+'.pgderive_native_oracle'))
        rows = json.loads(self.sql(f"SELECT COALESCE(json_agg(b),'[]') FROM {self.name}.bid b"))
        if self.mode == 'global':
            self.verify_memory({self.aggregate(rows): 1}, parse_float=Decimal)
            return
        groups = collections.defaultdict(list)
        for row in rows:
            groups[row['auction']].append(row)
        aggregates = {key: self.aggregate(group) for key, group in groups.items()}
        maximum = max((values[0] for values in aggregates.values()), default=None)
        memory = {(key, *values, *((maximum,) if self.mode == 'nested' else ())): 1
                  for key, values in aggregates.items()}
        self.verify_memory(memory, parse_float=Decimal)


def qualify_distinct(output, command, window, mode):
    if mode not in ('global', 'mixed'):
        qualify(output, command, window, mode, fixture_class=DistinctFixture)
        return
    fixture = DistinctFixture(output, window, mode)
    source = fixture.name+'.bid'
    with fixture.managed():
        fixture.run_history(command, History(
            changes=[
                f'DELETE FROM {source}',
                f'INSERT INTO {source} VALUES (100,1,0),(101,1,3),(102,1,3),(103,1,NULL),(104,NULL,NULL)',
                f'DELETE FROM {source} WHERE id=101',
                f'UPDATE {source} SET price=NULL WHERE id=102',
                f'DELETE FROM {source} WHERE id=100',
                f'DELETE FROM {source}',
            ],
            final=f'INSERT INTO {source} VALUES (105,NULL,-2),(106,NULL,-2),(107,NULL,3)',
            normalized=fixture.query.replace('SUM(', 'pg_catalog.sum('),
            rejected=[],
        ))
        (output/'result.json').write_text(json.dumps({
            'mode': mode, 'native_send_and_metadata': True, 'independent_bag': True,
            'threshold_crossings': True, 'empty': True, 'cold_restart': True,
        })+'\n')


if __name__ == '__main__':
    run_cases(((mode, False, mode) for mode in ('int2', 'int4', 'int8', 'nested', 'global', 'mixed')),
              qualify_distinct)
