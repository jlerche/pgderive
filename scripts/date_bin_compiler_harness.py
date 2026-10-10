#!/usr/bin/env python3
"""Native date_bin map/group semantics against PostgreSQL and datetime recomputation."""
import collections
import datetime as dt
from decimal import Decimal
import json
import pathlib
import sys
from native_compiler_harness import NativeFixture, qualify
from numeric_compiler_harness import average


class BinFixture(NativeFixture):
    evidence = {"timestamp_range":"1970 through 2024, infinities, NULL and pre-origin microseconds", "numeric":"integer SUM and PostgreSQL exact AVG"}
    def __init__(self, output, mode):
        super().__init__(output,'grouped')
        self.mode = mode
        self.timezone = mode != 'local'
        self.projection = mode == 'projection'
        self.sql(f"ALTER TABLE {self.name}.bid ALTER COLUMN price TYPE integer; UPDATE {self.name}.bid SET event_at='1970-01-01 00:00:00',event_time='1970-01-01 00:00:00+00' WHERE id IN(1,2,5)")
        column = 'event_time' if self.timezone else 'event_at'
        origin = "TIMESTAMPTZ '2000-01-01 00:00:00+00'" if self.timezone else "TIMESTAMP '2000-01-01 00:00:00'"
        self.bin = f"date_bin(INTERVAL '10 seconds',b.{column},{origin})"
        if self.projection:
            self.query = f'SELECT {self.bin} AS bucket,b.auction AS auction FROM {self.name}.bid b'
            self.labels = ['bucket','auction']
        else:
            self.query = f'SELECT {self.bin} AS bucket,b.auction AS auction,COUNT(*) AS n,SUM(b.price) AS total,AVG(b.price) AS mean FROM {self.name}.bid b GROUP BY b.auction,{self.bin}'
            self.labels = ['bucket','auction','n','total','mean']

    def bucket(self, value):
        if value is None or value in ('infinity','-infinity'):
            return value
        source = dt.datetime.fromisoformat(value)
        origin = dt.datetime(2000,1,1,tzinfo=dt.timezone.utc if self.timezone else None)
        bucket = origin + ((source-origin)//dt.timedelta(seconds=10))*dt.timedelta(seconds=10)
        return bucket.isoformat()

    def verify(self):
        prefix = "SET DateStyle='ISO,MDY'; SET TimeZone='UTC'; "
        query = f"SELECT jsonb_build_array(NULL,jsonb_build_array({','.join(self.labels)})) tuple,count(*)::bigint weight FROM ({self.query}) q GROUP BY 1"
        difference = self.sql(prefix + f'''WITH expected AS ({query}),actual AS
            (SELECT tuple,weight FROM {self.name}.groups),difference AS
            ((SELECT * FROM expected EXCEPT ALL SELECT * FROM actual) UNION ALL
            (SELECT * FROM actual EXCEPT ALL SELECT * FROM expected)) SELECT count(*) FROM difference''')
        assert difference.strip()=='0','date_bin output differs from PostgreSQL'
        source = json.loads(self.sql(prefix + f"SELECT json_agg(x) FROM (SELECT auction,price,event_at::text local,event_time::text instant FROM {self.name}.bid) x"))
        expected = collections.Counter()
        groups = collections.defaultdict(list)
        for row in source:
            key = (self.bucket(row['instant' if self.timezone else 'local']),row['auction'])
            groups[key].append(row)
        for key, group in groups.items():
            if self.projection:
                expected[key] += len(group)
            else:
                prices = [row['price'] for row in group if row['price'] is not None]
                expected[key + (len(group),sum(prices) if prices else None,average(prices))] += 1
        actual = json.loads(self.sql(f"SELECT COALESCE(json_agg(g),'[]') FROM {self.name}.groups g"),parse_float=Decimal)
        assert {tuple(row['tuple'][1]):row['weight'] for row in actual}==dict(expected)
        assert self.sql(f'''SELECT count(*) FROM pg_replication_slots s JOIN {self.name}.pgderive_worker_registration r ON r.slot_name=s.slot_name JOIN {self.name}.pgderive_progress p ON true WHERE s.confirmed_flush_lsn>p.end_lsn''').strip()=='0'


if __name__=='__main__':
    output = pathlib.Path(sys.argv[1])
    command = sys.argv[2:]
    if command and command[0]=='--':
        command = command[1:]
    for mode in ('local','instant','projection'):
        directory = output/mode
        directory.mkdir()
        qualify(directory,command,mode,fixture_class=BinFixture)
