#!/usr/bin/env python3
"""q14 native terminal DAG: exact MV, independent Decimal/Unicode oracle, CDC/restart."""
from worker_harness import harness_arguments, History
import collections
import decimal
import json
import pathlib
from sql_compiler_harness import SqlFixture
from native_mv_oracle import expected as native_expected
from nexmark_temporal_harness import encode

decimal.getcontext().prec = 100
SQL = pathlib.Path(__file__).resolve().parents[1]/'queries/nexmark/q14.sql'
LABELS = ['auction','bidder','price','bidTimeType','date_time','extra','c_counts']

class Q14Fixture(SqlFixture):
    def __init__(self,output,mode):
        super().__init__(output,False)
        self.pg.insert(1,'-q')
        self.instant = mode!='local'
        temporal = 'timestamptz' if self.instant else 'timestamp'
        self.sql(f'''ALTER TABLE {self.name}.bid ADD COLUMN bidder bigint,
          ADD COLUMN date_time {temporal},ADD COLUMN extra text;
          UPDATE {self.name}.bid SET bidder=9,date_time='2000-01-01 12:00:00',extra='é🙂ccc';
          INSERT INTO {self.name}.bid(id,auction,price,bidder,date_time,extra) VALUES
          (300,1,2000000,9,'2000-01-01 08:00:00','ccc🙂'),
          (301,1,2000000,9,'2000-01-01 08:00:00','ccc🙂'),
          (302,2,2000000,NULL,NULL,NULL),
          (303,2,2000000,9,'2000-01-01 07:00:00',''),
          (304,2,2000000,9,'2000-01-01 19:00:00','éçC'),
          (305,2,2000000,9,'2000-01-01 20:00:00','c'),
          (306,2,2000000,9,'infinity','cc'),(307,2,2000000,9,'-infinity',NULL),
          (308,NULL,NULL,9,NULL,'cc'),(309,2,1101321,9,NULL,'boundary'),
          (310,2,1101322,9,NULL,'boundary'),(311,2,55066079,9,NULL,'boundary'),
          (312,2,55066080,9,NULL,'boundary'),
          (313,2,9223372036854775807,9,NULL,'maximum'),
          (314,2,-9223372036854775808,9,NULL,'minimum')''')
        self.query = SQL.read_text().replace('nexmark.',self.name+'.').strip().removesuffix(';')
        if not self.instant:
            self.query = self.query.replace("date_time AT TIME ZONE 'UTC'",'date_time')

    def verify(self):
        context = "SET DateStyle='ISO,MDY'; SET TimeZone='UTC'; "
        expected = native_expected(self,LABELS)
        difference = self.bag_difference(expected, context=context)
        assert difference.strip()=='0','q14 differs from exact native MV'
        source = json.loads(self.sql(context+f"SELECT COALESCE(json_agg(b),'[]') FROM {self.name}.bid b"))
        memory = collections.Counter()
        for row in source:
            price = None if row['price'] is None else decimal.Decimal(row['price'])*decimal.Decimal('0.908')
            if price is None or not decimal.Decimal(1000000)<price<decimal.Decimal(50000000):
                continue
            timestamp = row['date_time']
            hour = None if timestamp is None or timestamp in ('infinity','-infinity') else int(timestamp[11:13])
            label = 'dayTime' if hour is not None and 8<=hour<=18 else 'nightTime' if hour is not None and (hour<=6 or hour>=20) else 'otherTime'
            count = None if row['extra'] is None else row['extra'].count('c')
            memory[(row['auction'],row['bidder'],price,label,encode(timestamp,self.instant),row['extra'],count)] += 1
        actual = json.loads(self.sql(f"SELECT COALESCE(json_agg(g),'[]') FROM {self.name}.groups g"),parse_float=decimal.Decimal)
        assert {tuple(row['tuple'][1]):row['weight'] for row in actual}==dict(memory),'q14 differs from independent Decimal/Unicode/hour oracle'
        assert self.acknowledgement_ahead().strip()=='0'


def qualify(output,command,mode):
    fixture = Q14Fixture(output,mode)
    name = fixture.name
    with fixture.managed():
        for label,query in [
            ('clock',fixture.query.replace('length(extra)','length(now()::text)',1)),
            ('user-function',fixture.query.replace('replace(extra','public.replace(extra')),
            ('unsupported-division',fixture.query.replace('0.908 * price','2 / price')),
            ('timezone',fixture.query.replace("'UTC'","'America/New_York'") if fixture.instant else fixture.query.replace('extract(hour','extract(julian')),
            ('downstream',f'SELECT x.auction FROM ({fixture.query}) x WHERE x.price>1000000'),
        ]:
            fixture.rejected(command,label,query)
        assert fixture.sql(f"SELECT to_regclass('{name}.pgderive_worker_registration') IS NULL").strip()=='t'
        assert fixture.sql(f"SELECT count(*) FROM pg_replication_slots WHERE slot_name LIKE '{name}%' ").strip()=='0'
        changes = [
            f"INSERT INTO {name}.bid(id,auction,price,bidder,date_time,extra) VALUES(400,1,2000000,9,'2000-01-01 06:59:59','c🙂c'); UPDATE {name}.bid SET price=60000000 WHERE id=300",
            f"UPDATE {name}.bid SET price=2000000,date_time='2024-03-10 01:30:00-08',extra='çcc' WHERE id IN(301,308)",
            f"DELETE FROM {name}.bid WHERE id=301; UPDATE {name}.bid SET price=NULL WHERE id=400",
            f"UPDATE {name}.bid SET price=2000000,date_time='2024-11-03 01:30:00-07',extra=NULL WHERE id=400; UPDATE {name}.bid SET id=501 WHERE id=308",
            f"UPDATE {name}.auction SET group_id=NULL WHERE id=2",
        ]
        def initial():
            assert fixture.sql(f'SELECT max(weight)>1 FROM {name}.groups').strip()=='t'
            fixture.sql(f'BEGIN; DELETE FROM {name}.bid; ROLLBACK')
        fixture.run_history(command, History(
            changes=changes,
            final=f'DELETE FROM {name}.bid',
            normalized=fixture.query.replace('pg_catalog.', '').replace('SELECT auction,bidder', 'SELECT b.auction,b.bidder').replace(f'FROM {name}.bid', f'FROM {name}.bid b'),
            rejected=[('changed-filter', fixture.query.replace('50000000', '40000000')),('changed-output', fixture.query.replace("'dayTime'", "'day'"))],
        ), initial=initial)
        (output/'result.json').write_text(json.dumps({'query':'q14','mode':mode,'postgres':'exact native materialized-view weighted bag','memory':'independent Decimal Unicode and hour classification','restart':'cold same slot','terminal':'typed tuple-local DAG with native CASE and IS TRUE'})+'\n')

if __name__=='__main__':
    output, command = harness_arguments()
    for mode in ('local','instant'):
        directory = output/mode
        directory.mkdir()
        qualify(directory,command,mode)
