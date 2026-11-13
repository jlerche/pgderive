#!/usr/bin/env python3
"""Check calendar RANGE windows against PostgreSQL and an independent frame oracle."""
from bisect import bisect_left, bisect_right
from calendar import monthrange
from collections import Counter, defaultdict
from datetime import datetime, timedelta
import json

from native_mv_oracle import binary_difference, expected, sql
from sql_compiler_harness import SqlFixture
from worker_harness import History, run_modes


class TemporalRangeFixture(SqlFixture):
    frames = [
        (False, False, None, (1, 0, 0), "UNBOUNDED PRECEDING AND '1 month' FOLLOWING"),
        (True, True, (-1, 0, 0), (0, 2, 0), "interval '1 month' PRECEDING AND '2 days' FOLLOWING"),
        (False, True, (0, 0, 1), (0, 0, 2), "'1 microsecond' FOLLOWING AND '2 microseconds' FOLLOWING"),
        (False, False, (0, 0, 0), 'infinity', "CURRENT ROW AND interval 'infinity' FOLLOWING"),
        (True, False, '-infinity', (0, 0, 0), "interval 'infinity' PRECEDING AND CURRENT ROW"),
        (False, False, (0, 30, 0), (0, 30, 0), "'30 days' FOLLOWING AND '30 days' FOLLOWING"),
        (False, False, (1, 0, 0), (1, 0, 0), "'1 month' FOLLOWING AND '1 month' FOLLOWING"),
    ]

    def __init__(self, output, mode):
        super().__init__(output, False)
        self.mode = mode
        table = self.name + '.bid'
        self.sql(f"""DELETE FROM {table};
            ALTER TABLE {table} ADD COLUMN observed {mode};
            INSERT INTO {table}(id,auction,observed) VALUES
              (1,1,'2024-01-30 23:00:00'),(2,1,'2024-01-31 00:00:00'),
              (3,1,'2024-02-29 00:00:00'),(4,1,'2024-02-29 22:00:00'),
              (5,1,'2024-02-29 22:00:00'),(6,1,'2024-02-29 23:00:00'),
              (7,1,'2024-02-29 23:00:00.000001'),(8,1,NULL),(9,1,NULL),
              (10,1,'infinity'),(11,1,'-infinity'),
              (12,2,'2024-03-10 07:00:00'),(13,2,'2024-03-11 07:00:00');""")
        if mode == 'timestamptz':
            self.sql(f"UPDATE {table} SET observed='2024-02-29 17:00:00-05' WHERE id=5")
        fields = ['b.id']
        self.labels = ['id']
        for index, (descending, nulls_first, _, _, frame) in enumerate(self.frames):
            direction = ('DESC' if descending else 'ASC') + (' NULLS FIRST' if nulls_first else ' NULLS LAST')
            window = f'OVER (PARTITION BY b.auction ORDER BY b.observed {direction} RANGE BETWEEN {frame})'
            for label, function in [('n', 'count(*)'), ('lo', 'min(b.id)'), ('hi', 'max(b.id)')]:
                alias = f'{label}_{index}'
                fields.append(f'{function} {window} AS {alias}')
                self.labels.append(alias)
        self.query = f"SELECT {','.join(fields)} FROM {table} b"

    def verify(self):
        self.verify_sql(expected(self, self.labels))
        assert binary_difference(self) == '0'
        rows = json.loads(sql(self, f"""SET TIME ZONE 'UTC';
            SELECT COALESCE(json_agg(b),'[]') FROM {self.name}.bid b"""))
        groups = defaultdict(list)
        outputs = {}
        for row in rows:
            groups[row['auction']].append(row)
            outputs[row['id']] = [row['id']]
        for group in groups.values():
            for descending, nulls_first, start, end, _ in self.frames:
                def key(value):
                    if value is None:
                        return (0 if nulls_first else 4, 0)
                    if value in ('infinity', '-infinity'):
                        positive = (value == 'infinity') != descending
                        return (3 if positive else 1, 0)
                    value = datetime.fromisoformat(value)
                    delta = value.replace(tzinfo=None) - datetime(2000, 1, 1)
                    micros = (delta.days * 86400 + delta.seconds) * 1000000 + delta.microseconds
                    return (2, -micros if descending else micros)

                def boundary(value, offset, tail):
                    if offset is None:
                        return (5 if tail else -1, 0)
                    if value is None:
                        return key(value)
                    if isinstance(offset, str):
                        actual = ('infinity' if offset == '-infinity' else '-infinity') if descending else offset
                        if value in ('infinity', '-infinity') and value != actual:
                            # All non-NULL comparisons succeed for this indeterminate sum.
                            return ((3, 0) if tail else (1, 0))
                        return key(actual)
                    if value in ('infinity', '-infinity'):
                        return key(value)
                    months, days, micros = (component * (-1 if descending else 1) for component in offset)
                    date = datetime.fromisoformat(value)
                    year, month = divmod(date.year * 12 + date.month - 1 + months, 12)
                    clipped = date.replace(year=year, month=month + 1,
                                           day=min(date.day, monthrange(year, month + 1)[1]))
                    return key((clipped + timedelta(days=days, microseconds=micros)).isoformat())

                ordered = sorted(group, key=lambda row: key(row['observed']))
                keys = [key(row['observed']) for row in ordered]
                head = tail = 0
                for row in ordered:
                    value = row['observed']
                    head = max(head, bisect_left(keys, boundary(value, start, False)))
                    tail = max(tail, bisect_right(keys, boundary(value, end, True)))
                    ids = [item['id'] for item in ordered[head:tail]]
                    outputs[row['id']].extend((len(ids), min(ids, default=None), max(ids, default=None)))
        self.verify_memory(Counter(tuple(values) for values in outputs.values()))


def qualify(output, command, mode):
    fixture = TemporalRangeFixture(output, mode)
    table = fixture.name + '.bid'
    with fixture.managed():
        fixture.run_history(command, History(
            changes=[
                f"INSERT INTO {table}(id,auction,observed) VALUES (14,1,'2024-02-29 00:00:00'),(15,2,NULL)",
                f"UPDATE {table} SET auction=2,observed='2024-03-10 08:00:00' WHERE id IN (4,5)",
                f"UPDATE {table} SET observed=NULL WHERE id IN (10,11)",
                f"DELETE FROM {table} WHERE id IN (2,8,14)",
            ],
            final=f"UPDATE {table} SET auction=1,observed='2024-02-29 22:00:00.000001' WHERE id=13",
            normalized=fixture.query.replace('b.', '"B".').replace('bid b', 'bid AS "B"'),
            rejected=[('changed-offset', fixture.query.replace("'30 days'", "'1 month'", 1))],
        ))
        (output/'result.json').write_text(json.dumps({
            'mode': mode, 'native_bag_and_binary': True, 'independent_oracle': True,
            'cold_restart': True,
        })+'\n')


if __name__ == '__main__':
    run_modes(('timestamp', 'timestamptz'), qualify)
