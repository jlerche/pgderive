#!/usr/bin/env python3
"""Qualified native ordering, occurrence offsets, and deterministic allowed ties."""
import collections
from decimal import Decimal
import json
import uuid
from native_mv_oracle import expected, binary_difference
from query_topk_harness import TopKFixture, qualify as qualify_topk
from worker_harness import run_modes


TEXT = ('', 'a', 'a ', 'aa', 'é', '🙂', '\x01', 'A')


class DomainFixture(TopKFixture):
    def __init__(self, output, mode):
        super().__init__(output, mode)
        self.domain = mode.split('-')[0]
        self.descending = mode.endswith('desc') or self.domain in ('ties', 'offset', 'zero')
        self.offset = 1000000 if mode == 'zero-offset' else 3
        self.limit = None if mode == 'offset-only' else (0 if mode == 'zero-offset' else 3)
        using = None
        native = None
        if self.domain == 'bool':
            native, using = 'boolean', 'CASE WHEN price IS NULL THEN NULL ELSE id%2=0 END'
        elif self.domain == 'uuid':
            native = 'uuid'
            using = "CASE WHEN price IS NULL THEN NULL WHEN id%7=0 THEN 'ffffffff-ffff-ffff-ffff-ffffffffffff'::uuid ELSE lpad(to_hex(id%4),32,'0')::uuid END"
        elif self.domain == 'numeric':
            native = 'numeric'
            using = ("CASE WHEN price IS NULL THEN NULL WHEN id%11=0 THEN 'NaN'::numeric "
                     "WHEN id%7=0 THEN 'Infinity'::numeric WHEN id%13=0 THEN '-Infinity'::numeric "
                     "WHEN id%17=0 THEN '1e4090'::numeric WHEN id%19=0 THEN '1e-4090'::numeric "
                     "WHEN id%3=0 THEN (id%4-2)::numeric(4,1) ELSE (id%4-2)::numeric(5,2) END")
        elif self.domain in ('text', 'varchar'):
            native = 'varchar(24)' if self.domain == 'varchar' else 'text'
            if mode != 'text-explicit':
                native += ' COLLATE "POSIX"' if self.domain == 'varchar' else ' COLLATE "C"'
            cases = ' '.join(f"WHEN {index} THEN '{value}'" for index, value in enumerate(TEXT))
            using = f'CASE WHEN price IS NULL THEN NULL ELSE CASE id%8 {cases} END END'
        else:
            self.sql(f'UPDATE {self.name}.bid SET price=7')
        if native is not None:
            self.sql(f'ALTER TABLE {self.name}.bid ALTER COLUMN price TYPE {native} USING {using}')
        direction = 'DESC' if self.descending else 'ASC'
        key = 'b.price COLLATE pg_catalog."C"' if mode == 'text-explicit' else 'b.price'
        order = f'{key} {direction} NULLS LAST'
        prefix = f'SELECT b.auction,b.price FROM {self.name}.bid b ORDER BY '
        tail = ('' if self.limit is None else f' LIMIT {self.limit}') + f' OFFSET {self.offset}'
        self.query = prefix + order + tail
        # PostgreSQL may choose any rows within a tied key. Qualify one legal
        # ordering matching Feldera's complete logical-tuple secondary comparison.
        secondary = ',b.auction ASC NULLS FIRST'
        if self.domain == 'numeric':
            secondary += ',b.price::text COLLATE "C" ASC'
        self.oracle_query = prefix + order + secondary + tail

    def value(self, value):
        if self.domain == 'bool':
            return 'true' if value%2 else 'false'
        if self.domain == 'uuid':
            return f"'{value % (1 << 128):032x}'::uuid"
        if self.domain == 'numeric':
            return f"'{value}.00'::numeric"
        if self.domain in ('text', 'varchar'):
            return "'" + TEXT[value%len(TEXT)] + "'"
        return str(value)

    def changed_cutoff(self):
        if self.limit is None:
            return self.query.replace('OFFSET 3', 'OFFSET 4')
        if self.limit == 0:
            return self.query.replace('LIMIT 0', 'LIMIT 2').replace('OFFSET 1000000', 'OFFSET 0')
        return self.query.replace('LIMIT 3', 'LIMIT 2')

    def key(self, value):
        if self.domain == 'uuid':
            return uuid.UUID(value).bytes
        if self.domain == 'numeric':
            number = Decimal(str(value))
            if number.is_nan(): return (3, 0)
            if number.is_infinite(): return (0 if number.is_signed() else 2, 0)
            return (1, number)
        if self.domain in ('text', 'varchar'):
            return value.encode('utf8')
        return value

    def verify(self):
        self.verify_sql(expected(self, self.labels, query_sql=self.oracle_query))
        assert binary_difference(self) == '0', self.query
        rows = json.loads(self.sql(f"SELECT COALESCE(json_agg(b),'[]') FROM {self.name}.bid b"), parse_float=Decimal)
        rows.sort(key=lambda row: (row['auction'] is not None, row['auction'], str(row['price']).encode('utf8')))
        values = [row for row in rows if row['price'] is not None]
        nulls = [row for row in rows if row['price'] is None]
        values.sort(key=lambda row: self.key(row['price']), reverse=self.descending)
        selected = (values + nulls)[self.offset:]
        if self.limit is not None:
            selected = selected[:self.limit]
        memory = collections.Counter((row['auction'], row['price']) for row in selected)
        self.verify_memory(dict(memory), parse_float=Decimal)


def qualify(output, command, mode):
    qualify_topk(output, command, mode, fixture_type=DomainFixture)


if __name__ == '__main__':
    run_modes(('bool', 'bool-desc', 'uuid', 'uuid-desc', 'numeric', 'numeric-desc',
               'text-c', 'text-desc', 'varchar-posix', 'varchar-desc', 'text-explicit', 'ties-offset',
               'offset-only', 'zero-offset'), qualify)
