#!/usr/bin/env python3
"""Explicit series/date-bin hopping against PostgreSQL and datetime bag recomputation."""
from worker_harness import History, run_modes
import collections
import datetime as dt
from decimal import Decimal
import json
from date_bin_compiler_harness import BinFixture
from native_compiler_harness import qualify as qualify_hopping
from native_mv_oracle import binary_difference, expected
from sql_compiler_harness import SqlFixture
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
        self.verify_sql(query, context=prefix)
        source = json.loads(self.sql(prefix + f"SELECT json_agg(x) FROM (SELECT id,auction,price,event_at::text local,event_time::text instant FROM {self.name}.bid) x"))
        expected = self.expected(source)
        self.verify_memory(dict(expected), parse_float=Decimal)

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


SERIES = {
    'ascending-step': ('(-3)::smallint,4,2', (-3,-1,1,3)),
    'descending-bigint': ('9223372036854775807,9223372036854775802,-2', (
        9223372036854775807,9223372036854775805,9223372036854775803)),
    'empty-null': ('NULL::bigint,4,0', ()),
    'empty-direction': ('1,4,-2', ()),
}
MODES = ('local', 'instant', 'projection', 'series', 'empty', 'rows', *SERIES)


class SeriesFixture(SqlFixture):
    labels = ['auction', 'value', 'ordinal']

    def __init__(self, output, arguments, values):
        super().__init__(output, False)
        self.values = values
        self.query = (f'SELECT b.auction,w.n AS value,w.ordinality AS ordinal FROM {self.name}.bid b '
                      f'CROSS JOIN pg_catalog.generate_series({arguments}) WITH ORDINALITY AS w(n)')

    def verify(self):
        self.verify_sql(expected(self, self.labels))
        assert binary_difference(self) == '0'
        names = "attname,atttypid,atttypmod,attcollation"
        layout = (f'SELECT {names} FROM pg_attribute '
                  "WHERE attrelid='{}'::regclass AND attnum>0 AND NOT attisdropped ORDER BY attnum")
        assert self.sql(layout.format(self.name+'.groups')) == self.sql(
            layout.format(self.name+'.pgderive_native_oracle'))
        source = json.loads(self.sql(f"SELECT COALESCE(json_agg(auction),'[]') FROM {self.name}.bid"))
        memory = collections.Counter((auction, value, ordinal)
                                     for auction in source
                                     for ordinal, value in enumerate(self.values, 1))
        self.verify_memory(dict(memory))
        assert self.acknowledgement_ahead().strip() == '0'


def qualify(output, command, mode):
    if mode not in SERIES:
        qualify_hopping(output, command, mode, fixture_class=HoppingFixture)
        return
    fixture = SeriesFixture(output, *SERIES[mode])
    table = fixture.name+'.bid'
    with fixture.managed():
        fixture.run_history(command, History(
            changes=[f'INSERT INTO {table} VALUES (100,1,2),(101,1,2)',
                     f'UPDATE {table} SET auction=NULL WHERE id=100'],
            final=f'DELETE FROM {table} WHERE id IN (100,101)',
            normalized=fixture.query.replace('AS w(n)', 'AS w(n, ordinality)'),
            rejected=[('without-ordinality', fixture.query.replace('WITH ORDINALITY ', ''))],
            verify_reopen=True,
        ))
        (output/'result.json').write_text(json.dumps({
            'native_binary_and_metadata': True, 'independent_bag': True, 'cold_restart': True,
        })+'\n')


if __name__ == '__main__':
    run_modes(MODES, qualify)
