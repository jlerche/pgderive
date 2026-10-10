#!/usr/bin/env python3
"""Explicit series/date-bin hopping against PostgreSQL and datetime bag recomputation."""
import collections
import datetime as dt
from decimal import Decimal
import json
import pathlib
import sys
from date_bin_compiler_harness import BinFixture
from native_compiler_harness import qualify
from numeric_compiler_harness import average


class HoppingFixture(BinFixture):
    evidence = {"expansion":"explicit int4 generate_series with fixed-duration offsets", "timestamp_range":"1970 through 2024, NULLs, infinities, pre-origin and DST instants", "numeric":"integer SUM and exact AVG"}
    def __init__(self, output, mode):
        super().__init__(output,mode)
        self.projection = mode in ('projection','series')
        self.series_only = mode == 'series'
        self.first_ordinal,self.last_ordinal = (4,0) if mode == 'empty' else (0,4)
        self.bin = self.bin.replace('10 seconds','2 seconds') + " - w.n * INTERVAL '2 seconds'"
        source = f'{self.name}.bid b CROSS JOIN pg_catalog.generate_series({self.first_ordinal},{self.last_ordinal}) AS w(n)'
        if mode == 'rows':
            frame = ' OVER (PARTITION BY b.auction ORDER BY b.id,w.n ROWS BETWEEN 1 PRECEDING AND 1 FOLLOWING)'
            self.query = f'SELECT b.id AS id,b.auction AS auction,w.n AS ordinal,COUNT(*){frame} AS n,SUM(b.price){frame} AS total,AVG(b.price){frame} AS mean FROM {source}'
            self.labels = ['id','auction','ordinal','n','total','mean']
        elif self.series_only:
            self.query = f'SELECT w.n AS ordinal,b.auction AS auction FROM {source}'
            self.labels = ['ordinal','auction']
        elif self.projection:
            self.query = f'SELECT {self.bin} AS bucket,b.auction AS auction FROM {source}'
            self.labels = ['bucket','auction']
        else:
            self.query = f'SELECT {self.bin} AS bucket,b.auction AS auction,COUNT(*) AS n,SUM(b.price) AS total,AVG(b.price) AS mean FROM {source} GROUP BY b.auction,{self.bin}'
            self.labels = ['bucket','auction','n','total','mean']

    def bucket(self,value,ordinal):
        if value is None or value in ('infinity','-infinity'):
            return value
        source = dt.datetime.fromisoformat(value)
        origin = dt.datetime(2000,1,1,tzinfo=dt.timezone.utc if self.timezone else None)
        step = dt.timedelta(seconds=2)
        return (origin + ((source-origin)//step)*step - ordinal*step).isoformat()

    def verify(self):
        prefix = "SET DateStyle='ISO,MDY'; SET TimeZone='UTC'; "
        query = f"SELECT jsonb_build_array(NULL,jsonb_build_array({','.join(self.labels)})) tuple,count(*)::bigint weight FROM ({self.query}) q GROUP BY 1"
        difference = self.sql(prefix + f'''WITH expected AS ({query}),actual AS
            (SELECT tuple,weight FROM {self.name}.groups),difference AS
            ((SELECT * FROM expected EXCEPT ALL SELECT * FROM actual) UNION ALL
            (SELECT * FROM actual EXCEPT ALL SELECT * FROM expected)) SELECT count(*) FROM difference''')
        assert difference.strip() == '0','hopping output differs from PostgreSQL'
        source = json.loads(self.sql(prefix + f"SELECT json_agg(x) FROM (SELECT id,auction,price,event_at::text local,event_time::text instant FROM {self.name}.bid) x"))
        expected = self.expected(source)
        actual = json.loads(self.sql(f"SELECT COALESCE(json_agg(g),'[]') FROM {self.name}.groups g"),parse_float=Decimal)
        assert {tuple(row['tuple'][1]):row['weight'] for row in actual} == dict(expected)
        assert self.sql(f'''SELECT count(*) FROM pg_replication_slots s JOIN {self.name}.pgderive_worker_registration r ON r.slot_name=s.slot_name JOIN {self.name}.pgderive_progress p ON true WHERE s.confirmed_flush_lsn>p.end_lsn''').strip() == '0'

    def expected(self,source):
        groups = collections.defaultdict(list)
        expected = collections.Counter()
        if self.mode == 'rows':
            for row in source:
                for ordinal in range(self.first_ordinal,self.last_ordinal+1):
                    groups[row['auction']].append({**row,'ordinal':ordinal})
            for key,group in groups.items():
                group.sort(key=lambda row:(row['id'],row['ordinal']))
                for index,row in enumerate(group):
                    frame = group[max(0,index-1):index+2]
                    prices = [item['price'] for item in frame if item['price'] is not None]
                    expected[(row['id'],key,row['ordinal'],len(frame),sum(prices) if prices else None,average(prices))] += 1
            return expected
        groups = collections.defaultdict(list)
        for row in source:
            for ordinal in range(self.first_ordinal,self.last_ordinal+1):
                bucket = ordinal if self.series_only else self.bucket(row['instant' if self.timezone else 'local'],ordinal)
                groups[(bucket,row['auction'])].append(row)
        for key,group in groups.items():
            if self.projection:
                expected[key] += len(group)
            else:
                prices = [row['price'] for row in group if row['price'] is not None]
                expected[key+(len(group),sum(prices) if prices else None,average(prices))] += 1
        return expected


if __name__ == '__main__':
    output = pathlib.Path(sys.argv[1])
    command = sys.argv[2:]
    if command and command[0]=='--':
        command = command[1:]
    for mode in ('local','instant','projection','series','empty','rows'):
        directory = output/mode
        directory.mkdir()
        qualify(directory,command,mode,fixture_class=HoppingFixture)
