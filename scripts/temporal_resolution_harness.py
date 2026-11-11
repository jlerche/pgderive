#!/usr/bin/env python3
"""Native temporal common types and interval UNION ALL through CDC."""
from collections import Counter
import json

from boolean_resolution_harness import OPERATIONS, ResolutionFixture
from worker_harness import History, run_modes


DATE = '2024-02-29'
LOCAL = DATE + 'T00:00:00'
INSTANT = LOCAL + '+00:00'


class TemporalResolutionFixture(ResolutionFixture):
    def __init__(self, output, mode):
        super().__init__(output, False)
        self.mode = mode
        self.pg[1:1] = ['-q', '-c', "SET TimeZone='UTC'; SET IntervalStyle='iso_8601'"]
        a = self.name + '.auction'
        b = self.name + '.bid'
        self.sql(f"""ALTER TABLE {a} ADD COLUMN d date, ADD COLUMN ts timestamp,
                  ADD COLUMN iv interval;
                  UPDATE {a} SET d=CASE WHEN id%3<>0 THEN '{DATE}'::date END,
                      ts=CASE WHEN id%3<>0 THEN '{LOCAL}'::timestamp END,
                      iv=CASE WHEN id%2=0 THEN 'P1M'::interval ELSE 'P30D'::interval END""")
        if mode == 'scalar':
            self.query = f"""SELECT id,
                CASE WHEN enabled THEN '{DATE}' COLLATE "C" ELSE d END AS day,
                CASE enabled WHEN 'yes' THEN '{DATE}' ELSE ts END AS local,
                COALESCE(NULL::date, NULL::timestamptz, '{DATE} 02:00:00+02') AS instant,
                CASE WHEN enabled THEN d ELSE NULL::timestamptz END AS widened,
                COALESCE(iv, '30 days', NULL) AS span
                FROM {a}"""
            self.labels = ['id', 'day', 'local', 'instant', 'widened', 'span']
        elif mode == 'interval':
            self.query = (f"SELECT iv AS span FROM {a} UNION ALL SELECT '1 mon' FROM {b} WHERE price>0 "
                          f"UNION ALL SELECT NULL FROM {b} WHERE price IS NULL")
            self.labels = ['span']
        else:
            self.query = (f"SELECT d AS day,d AS local,ts AS instant FROM {a} {OPERATIONS[mode]} "
                          f"(SELECT NULL::date,NULL::timestamp,NULL::timestamptz WHERE false UNION ALL "
                          f"SELECT '{DATE}','{LOCAL}','{DATE} 02:00:00+02' FROM {b} WHERE price>0 UNION ALL "
                          f"SELECT NULL,NULL,NULL FROM {b} WHERE price IS NULL)")
            self.labels = ['day', 'local', 'instant']

    def verify(self):
        self.verify_native()
        a = self.name + '.auction'
        b = self.name + '.bid'
        rows = json.loads(self.sql(f"SELECT COALESCE(json_agg(a),'[]') FROM {a} a"))
        if self.mode == 'scalar':
            values = Counter()
            for row in rows:
                enabled = row['enabled'] is True
                date, stamp, span = row['d'], row['ts'], row['iv']
                widened = date + 'T00:00:00+00:00' if enabled and date else None
                values[(row['id'], DATE if enabled else date, LOCAL if enabled else stamp,
                        INSTANT, widened, span or 'P30D')] += 1
            self.verify_memory(values)
            return
        prices = json.loads(self.sql(f"SELECT COALESCE(json_agg(price),'[]') FROM {b}"))
        if self.mode == 'interval':
            left = Counter((row['iv'],) for row in rows)
            right = Counter(('P1M' if price is not None else None,) for price in prices
                            if price is None or price > 0)
            self.verify_memory(left + right)
            return
        left = Counter((row['d'], row['d'] + 'T00:00:00' if row['d'] else None,
                        row['ts'] + '+00:00' if row['ts'] else None) for row in rows)
        right = Counter((DATE, LOCAL, INSTANT) if price is not None else (None, None, None)
                        for price in prices if price is None or price > 0)
        self.verify_set(left, right)


def qualify(output, command, mode):
    fixture = TemporalResolutionFixture(output, mode)
    a = fixture.name + '.auction'
    b = fixture.name + '.bid'
    with fixture.managed():
        if mode == 'scalar':
            fixture.rejected(command, 'invalid-unselected-literal',
                             f"SELECT COALESCE('{DATE}'::date,'2024-02-30') FROM {a}")
        fixture.run_history(command, History(
            changes=[
                f'UPDATE {a} SET d=NULL,ts=NULL,iv=NULL WHERE id=2; '
                f'INSERT INTO {b} VALUES (100,1,NULL)',
                f"UPDATE {a} SET d='{DATE}',ts='{LOCAL}',enabled=true WHERE id=3; "
                f'UPDATE {b} SET price=9 WHERE id=100',
                f"UPDATE {a} SET iv='720 hours' WHERE id=3; DELETE FROM {b} WHERE price IS NULL",
            ],
            final=f'UPDATE {a} SET d=NULL,iv=NULL WHERE id=3; UPDATE {b} SET price=NULL WHERE id=100',
            normalized=fixture.query,
            rejected=[],
        ))
        (output/'result.json').write_text(json.dumps({
            'mode': mode, 'native_binary_and_metadata': True, 'independent_bag': True,
            'cold_restart': True,
        })+'\n')


if __name__ == '__main__':
    run_modes(('scalar', 'interval', *OPERATIONS), qualify)
