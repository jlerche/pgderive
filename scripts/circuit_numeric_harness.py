#!/usr/bin/env python3
"""Exact PostgreSQL numeric circuit versus identical SQL MV and decimal row oracle."""
from worker_harness import run_cases
import collections
import decimal
import json
import os
from partition_compiler_harness import PartitionFixture, qualify
from native_mv_oracle import expected, binary_difference


class NumericFixture(PartitionFixture):
    def __init__(self, output, window, mode=None):
        super().__init__(output, False)
        self.mode = mode
        self.computed = mode in ('computed', 'sum-computed')
        self.sql(f'''ALTER TABLE {self.name}.bid ALTER COLUMN price TYPE numeric;
            UPDATE {self.name}.bid SET price=price/100.0;
            UPDATE {self.name}.bid SET price='NaN' WHERE id=1;
            UPDATE {self.name}.bid SET price='Infinity' WHERE id=2;
            UPDATE {self.name}.bid SET price='-Infinity' WHERE id=3;
            UPDATE {self.name}.bid SET price=1.2000 WHERE id=4;
            UPDATE {self.name}.bid SET price=1.20 WHERE id=5''')
        if mode in ('sum', 'sum-computed'):
            self.query = self.query.replace('MIN(b.price) AS lo,MAX(b.price) AS hi,', '')
            self.labels = ['auction', 'n', 'present', 'total', 'negative']
        if mode == 'group-key':
            self.query = f'SELECT b.price AS price,COUNT(*) AS n,COUNT(*) FILTER(WHERE b.price<0) AS negative FROM {self.name}.bid b GROUP BY b.price'
            self.labels = ['price', 'n', 'negative']
        if mode == 'join-key':
            self.query = f'SELECT b.id AS left_id,r.id AS right_id FROM {self.name}.bid b JOIN {self.name}.bid r ON b.price=r.price WHERE b.price<0 OR b.price>=0 OR b.price IS NULL'
            self.labels = ['left_id', 'right_id']
        if self.computed:
            self.query = self.query.replace('b.', '"B".').replace(
                f'FROM {self.name}.bid b',
                f'FROM (SELECT b.auction AS auction,b.price * 1.20 + 0.30 - 0.10 AS price FROM {self.name}.bid b WHERE b.price<0 OR b.price>=0 OR b.price IS NULL) "B"')

    def run_history(self, command, history):
        # Exercise inverse transitions, including display-only changes and masked
        # finite sums, through the same CDC/restart/oracle harness as other queries.
        history.changes = [
            f"INSERT INTO {self.name}.bid VALUES(900,900,1.2000),(901,900,2.30),(902,900,'Infinity'),(903,900,'-Infinity'),(904,900,'NaN')",
            f"DELETE FROM {self.name}.bid WHERE id=904",
            f"DELETE FROM {self.name}.bid WHERE id=903",
            f"DELETE FROM {self.name}.bid WHERE id=900",
            f"DELETE FROM {self.name}.bid WHERE id=902",
            f"UPDATE {self.name}.bid SET price=2.30000 WHERE id=901",
            f"UPDATE {self.name}.bid SET price=2.30 WHERE id=901",
            f"DELETE FROM {self.name}.bid WHERE id=901",
        ] + history.changes
        super().run_history(command, history)

    def verify(self):
        native = expected(self, self.labels)
        self.verify_sql(native)
        if self.mode in ('sum', 'sum-computed'):
            assert binary_difference(self) == '0', 'numeric SUM scale/send representation differs'

        source = json.loads(self.sql(f"SELECT COALESCE(json_agg(b),'[]') FROM {self.name}.bid b"), parse_float=decimal.Decimal)
        if self.mode == 'join-key':
            expected_memory = self.join_memory(source)
        elif self.mode == 'group-key':
            expected_memory = self.group_key_memory(source)
        else:
            expected_memory = self.aggregate_memory(source)
        self.verify_memory(expected_memory, parse_float=decimal.Decimal)

    @staticmethod
    def key(value):
        if value is None:
            return None
        numeric = decimal.Decimal(value)
        return str(numeric) if not numeric.is_finite() else numeric

    @classmethod
    def join_memory(cls, source):
        # Deliberately independent nested loops: NULL never matches; NaN does.
        output = collections.Counter()
        for left in source:
            for right in source:
                left_key = cls.key(left['price'])
                right_key = cls.key(right['price'])
                if left_key is not None and right_key is not None and left_key == right_key:
                    output[(left['id'], right['id'])] += 1
        return dict(output)

    @classmethod
    def group_key_memory(cls, source):
        groups = collections.defaultdict(list)
        for row in source:
            groups[cls.key(row['price'])].append(row)
        output = {}
        for key, rows in groups.items():
            negative = int(key == '-Infinity' or isinstance(key, decimal.Decimal) and key < 0)
            output[(key, len(rows), len(rows) * negative)] = 1
        return output

    def aggregate_memory(self, source):
        groups = collections.defaultdict(list)
        for row in source:
            groups[row['auction']].append(row)
        expected_memory = {}
        for key, rows in groups.items():
            values = [decimal.Decimal(row['price']) for row in rows if row['price'] is not None]
            if self.computed:
                with decimal.localcontext() as context:
                    context.prec = 10000
                    context.traps[decimal.InvalidOperation] = False
                    values = [value * decimal.Decimal('1.20') + decimal.Decimal('0.30') - decimal.Decimal('0.10') for value in values]
            def order(value):
                return (3, 0) if value.is_nan() else (0, 0) if value == decimal.Decimal('-Infinity') else (2, 0) if value == decimal.Decimal('Infinity') else (1, value)
            ordered = sorted(values, key=order)
            with decimal.localcontext() as context:
                context.prec = 10000
                context.traps[decimal.InvalidOperation] = False
                total = sum(values, decimal.Decimal(0)) if values else None
            negative = sum(order(value) < (1, decimal.Decimal(0)) for value in values)
            result = [key, len(rows), len(values), total, ordered[0] if values else None, ordered[-1] if values else None, negative]
            if self.mode in ('sum', 'sum-computed'):
                result = [key, len(rows), len(values), total, negative]
            expected_memory[tuple(str(value) if isinstance(value, decimal.Decimal) and not value.is_finite() else value for value in result)] = 1
        return expected_memory

if __name__ == '__main__':
    modes = ('sum', 'sum-computed') if os.environ.get('PGDERIVE_CHECK_PROFILE') == 'core' else ('native', 'computed', 'sum', 'sum-computed', 'group-key', 'join-key')
    run_cases(((mode, False, mode) for mode in modes),
              qualify, fixture_class=NumericFixture)
