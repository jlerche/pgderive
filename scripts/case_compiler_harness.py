#!/usr/bin/env python3
"""Lazy native CASE/gap flags and running totals, without implicit session expiry."""
import collections
import datetime
import json
import pathlib
import sys
from native_compiler_harness import NativeFixture, encoded, instant, qualify


class CaseFixture(NativeFixture):
    evidence = {'case':'lazy integral arms','gap':'same-type PostgreSQL timestamp subtraction','qualification':'flags and running totals; session boundaries follow separately'}

    def __init__(self, output, mode):
        super().__init__(output,'projection')
        self.mode = mode
        self.temporal = 'event_time' if mode.endswith('instant') else 'event_at'
        self.running = mode.startswith('running')
        self.nullable = mode == 'nullable'
        self.descending = mode == 'descending'
        self.sql(f"SET TimeZone='UTC'; UPDATE {self.name}.bid SET event_at=TIMESTAMP '2000-01-01 00:00:00'+INTERVAL '1 second'*(id%31),event_time=TIMESTAMPTZ '2000-01-01 00:00:00+00'+INTERVAL '1 second'*(id%31); UPDATE {self.name}.bid SET event_at=NULL,event_time=NULL WHERE id%7=0; UPDATE {self.name}.bid SET event_at='-infinity',event_time='-infinity',auction=1 WHERE id IN(1,4); UPDATE {self.name}.bid SET event_at='infinity',event_time='infinity',auction=1 WHERE id IN(2,3)")
        direction = ' DESC' if self.descending else ''
        order = f'{self.temporal}{direction} NULLS FIRST,id'
        inner = f'SELECT b.id,b.auction,b.{self.temporal} AS time,lag(b.{self.temporal}) OVER(PARTITION BY b.auction ORDER BY b.{self.temporal}{direction} NULLS FIRST,b.id) AS previous FROM {self.name}.bid b'
        if self.nullable:
            expression = "CASE WHEN q.previous IS NULL THEN q.auction WHEN q.time=q.previous THEN 2147483648 WHEN q.time-q.previous>=INTERVAL '10 seconds' THEN 1 END"
        else:
            expression = "CASE WHEN q.previous IS NULL THEN 1 WHEN q.time=q.previous THEN 0 WHEN q.time-q.previous>=INTERVAL '10 seconds' THEN 1 ELSE 0 END"
        flags = f'SELECT q.id AS id,q.auction AS auction,q.time AS time,{expression} AS flag FROM({inner}) q'
        self.query = flags
        self.labels = ['id','auction','time','flag']
        if self.running:
            self.query = f'SELECT z.id AS id,z.auction AS auction,z.time AS time,SUM(z.flag) OVER(PARTITION BY z.auction ORDER BY z.time NULLS FIRST,z.id ROWS UNBOUNDED PRECEDING) AS session FROM({flags}) z'
            self.labels[-1] = 'session'

    def verify(self):
        prefix = "SET DateStyle='ISO,MDY'; SET TimeZone='UTC'; "
        values = ','.join(self.labels)
        expected = f'SELECT jsonb_build_array(NULL,jsonb_build_array({values})) tuple,count(*)::bigint weight FROM ({self.query}) q GROUP BY 1'
        difference = self.sql(prefix+f'''WITH expected AS ({expected}),actual AS
            (SELECT tuple,weight FROM {self.name}.groups),difference AS
            ((SELECT * FROM expected EXCEPT ALL SELECT * FROM actual) UNION ALL
            (SELECT * FROM actual EXCEPT ALL SELECT * FROM expected)) SELECT count(*) FROM difference''')
        assert difference.strip() == '0','CASE/gap output differs from PostgreSQL'
        source = json.loads(self.sql(prefix+f"SELECT COALESCE(json_agg(q),'[]') FROM(SELECT id,auction,{self.temporal}::text AS time FROM {self.name}.bid) q"))
        groups = collections.defaultdict(list)
        for row in source:
            groups[row['auction']].append(row)
        memory = collections.Counter()
        for group in groups.values():
            group.sort(key=lambda row:row['id'])
            known = [row for row in group if row['time'] is not None]
            known.sort(key=lambda row:instant(row['time']),reverse=self.descending)
            group = [row for row in group if row['time'] is None]+known
            previous = None
            total = 0
            for row in group:
                time = row['time']
                if previous is None:
                    flag = row['auction'] if self.nullable else 1
                elif time == previous:
                    flag = 2147483648 if self.nullable else 0
                elif time is None:
                    flag = None if self.nullable else 0
                elif time in ('infinity','-infinity') or previous in ('infinity','-infinity'):
                    flag = 1 if instant(time)>instant(previous) else (None if self.nullable else 0)
                else:
                    gap = datetime.datetime.fromisoformat(time)-datetime.datetime.fromisoformat(previous)
                    micros = (gap.days*86400+gap.seconds)*1_000_000+gap.microseconds
                    flag = 1 if micros>=10_000_000 else (None if self.nullable else 0)
                previous = time
                if self.running:
                    total += flag
                memory[(row['id'],row['auction'],encoded(time,self.temporal=='event_time'),total if self.running else flag)] += 1
        actual = json.loads(self.sql(f"SELECT COALESCE(json_agg(g),'[]') FROM {self.name}.groups g"))
        assert {tuple(row['tuple'][1]):row['weight'] for row in actual} == dict(memory),'CASE/gap output differs from memory oracle'
        assert self.sql(f'''SELECT count(*) FROM pg_replication_slots s JOIN
            {self.name}.pgderive_worker_registration r ON r.slot_name=s.slot_name JOIN
            {self.name}.pgderive_progress p ON true WHERE s.confirmed_flush_lsn>p.end_lsn''').strip() == '0'


if __name__ == '__main__':
    output = pathlib.Path(sys.argv[1])
    command = sys.argv[2:]
    if command and command[0]=='--':
        command = command[1:]
    for mode in ('flags-local','flags-instant','running-local','running-instant','nullable','descending'):
        directory = output/mode
        directory.mkdir()
        qualify(directory,command,mode,fixture_class=CaseFixture)
