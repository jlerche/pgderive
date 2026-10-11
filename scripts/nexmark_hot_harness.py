#!/usr/bin/env python3
"""Exact native MV and independent bag oracles for hopping hot auctions/max bids."""
import collections
import datetime
import json
import pathlib
import sys
from sql_compiler_harness import SqlFixture
from native_mv_oracle import expected as native_expected
from nexmark_temporal_harness import bucket, encode, INFINITIES

QUERIES = pathlib.Path(__file__).resolve().parents[1]/'queries/nexmark'


class HotFixture(SqlFixture):
    def __init__(self,output,query,instant):
        super().__init__(output,False)
        self.pg.insert(1,'-q')
        self.kind,self.instant = query,instant
        temporal = 'timestamptz' if instant else 'timestamp'
        self.sql(f'''ALTER TABLE {self.name}.bid ADD COLUMN bidder bigint,ADD COLUMN date_time {temporal},ADD COLUMN extra text;
          UPDATE {self.name}.bid SET bidder=id%3,date_time='2000-01-01'::{temporal}+INTERVAL '1 second'*(id%31),extra='é🙂';
          INSERT INTO {self.name}.bid(id,auction,price,bidder,date_time,extra) VALUES
          (300,900,100,900,'2000-01-01','tie'),(301,900,100,900,'2000-01-01','tie'),
          (302,901,100,901,'2000-01-01 00:00:09.999999','x'),
          (303,901,200,901,'2000-01-01 00:00:10','boundary'),
          (304,902,NULL,902,'2000-01-01','null'),
          (305,902,9223372036854775807,902,'2000-01-01 00:00:10','max'),
          (306,NULL,-9223372036854775808,NULL,NULL,NULL),
          (307,NULL,0,NULL,'1999-12-31 23:59:59.999999',NULL),
          (308,NULL,NULL,NULL,'2000-01-01',NULL),
          (309,903,10,903,'-infinity','infinity'),(310,903,10,903,'-infinity','infinity'),
          (311,904,NULL,904,'infinity','null-infinity'),
          (320,910,NULL,910,'2000-01-02','all-null'),(321,910,NULL,910,'2000-01-02','all-null')''')
        self.query = (QUERIES/(query+'.sql')).read_text().replace('nexmark.bid',f'{self.name}.bid').strip().removesuffix(';')
        if not instant:
            self.query = self.query.replace('TIMESTAMPTZ','TIMESTAMP').replace('2000-01-01 00:00:00+00','2000-01-01 00:00:00')
        self.labels = ['auction','num'] if query=='q5' else ['auction','price','bidder','date_time','extra']

    def verify(self):
        prefix = "SET DateStyle='ISO,MDY'; SET TimeZone='UTC'; "
        expected = native_expected(self,self.labels)
        difference = self.sql(prefix+f'''WITH expected AS ({expected}),actual AS
          (SELECT tuple,weight FROM {self.name}.groups),difference AS
          ((SELECT * FROM expected EXCEPT ALL SELECT * FROM actual) UNION ALL
           (SELECT * FROM actual EXCEPT ALL SELECT * FROM expected)) SELECT count(*) FROM difference''')
        assert difference.strip()=='0',self.kind+' differs from exact native PostgreSQL MV'
        source = json.loads(self.sql(prefix+f"SELECT COALESCE(json_agg(b),'[]') FROM {self.name}.bid b"))
        memory = collections.Counter()
        if self.kind=='q5':
            windows = collections.defaultdict(collections.Counter)
            for row in source:
                if row['date_time'] is None:
                    continue
                start = bucket(row['date_time'],2)
                for ordinal in range(5):
                    window = start if start in INFINITIES else start-datetime.timedelta(seconds=2*ordinal)
                    windows[window][row['auction']] += 1
            for auctions in windows.values():
                maximum = max(auctions.values())
                for auction,count in auctions.items():
                    if count==maximum:
                        memory[(auction,count)] += 1
        else:
            windows = collections.defaultdict(list)
            for row in source:
                if row['date_time'] is not None:
                    windows[bucket(row['date_time'],10)].append(row)
            for rows in windows.values():
                prices = [row['price'] for row in rows if row['price'] is not None]
                if not prices:
                    continue
                maximum = max(prices)
                for row in rows:
                    if row['price']==maximum:
                        memory[(row['auction'],row['price'],row['bidder'],encode(row['date_time'],self.instant),row['extra'])] += 1
        actual = json.loads(self.sql(f"SELECT COALESCE(json_agg(g),'[]') FROM {self.name}.groups g"))
        assert {tuple(row['tuple'][1]):row['weight'] for row in actual}==dict(memory),self.kind+' differs from independent bucket/max bag oracle'
        assert self.sql(f'''SELECT count(*) FROM pg_replication_slots s JOIN {self.name}.pgderive_worker_registration r ON r.slot_name=s.slot_name CROSS JOIN {self.name}.pgderive_progress p WHERE s.confirmed_flush_lsn>p.end_lsn''').strip()=='0'


def qualify(output,command,query,instant):
    fixture = HotFixture(output,query,instant)
    try:
        fixture.rejected(command,'unsupported',fixture.query+' ORDER BY '+fixture.labels[0])
        assert fixture.sql(f"SELECT to_regclass('{fixture.name}.pgderive_worker_registration') IS NULL").strip()=='t'
        assert fixture.sql(f"SELECT count(*) FROM pg_replication_slots WHERE slot_name LIKE '{fixture.name}%' ").strip()=='0'
        worker = fixture.start(command,'first')
        ready = worker.event('ready')
        fixture.verify()
        changes = [
            ('ties',f"INSERT INTO {fixture.name}.bid(id,auction,price,bidder,date_time,extra) VALUES(312,900,100,900,'2000-01-01','tie'),(313,901,100,901,'2000-01-01','x'); UPDATE {fixture.name}.auction SET enabled=NULL WHERE id=1"),
            ('winner-retraction',f"DELETE FROM {fixture.name}.bid WHERE id=305; UPDATE {fixture.name}.bid SET price=NULL WHERE id=303"),
            ('bucket-key-moves',f"UPDATE {fixture.name}.bid SET id=330,auction=901,date_time='2000-01-01 00:00:10',price=300 WHERE id=300; UPDATE {fixture.name}.bid SET auction=NULL,date_time=NULL WHERE id=302"),
            ('late',f"INSERT INTO {fixture.name}.bid(id,auction,price,bidder,date_time,extra) VALUES(314,900,1000,NULL,'1999-12-31 23:59:59.999999',NULL); DELETE FROM {fixture.name}.bid WHERE id=313; UPDATE {fixture.name}.auction SET group_id=NULL WHERE id=2"),
            ('all-null',f"UPDATE {fixture.name}.bid SET price=NULL WHERE id IN(301,312,330); UPDATE {fixture.name}.bid SET date_time='2000-01-01',price=-1 WHERE id=306"),
            ('DST',f"SET DateStyle='German,DMY'; SET TimeZone='America/New_York'; UPDATE {fixture.name}.bid SET date_time='03.11.2024 01:30:09.999999-04',price=7 WHERE id=320; UPDATE {fixture.name}.bid SET date_time='03.11.2024 01:30:10-05',price=7 WHERE id=321"),
            ('infinity-retraction',f"DELETE FROM {fixture.name}.bid WHERE id=309; UPDATE {fixture.name}.bid SET date_time='infinity',auction=NULL,price=11 WHERE id=310"),
            ('empty-input',f"UPDATE {fixture.name}.auction SET group_id=3 WHERE id=2"),
        ]
        fixture.sql(f'BEGIN; DELETE FROM {fixture.name}.bid; ROLLBACK')
        for index,(_,change) in enumerate(changes,1):
            fixture.sql('BEGIN;'+change+';COMMIT')
            worker.event('published',minimum_time=ready['time']+index)
            fixture.verify()
        worker.abort()
        prior = fixture.sql(f'SELECT row_to_json(p) FROM {fixture.name}.pgderive_progress p')
        changed = fixture.query.replace('0,4','0,3') if query=='q5' else fixture.query.replace('10 seconds','11 seconds')
        fixture.rejected(command,'changed-semantics',changed)
        assert fixture.sql(f'SELECT row_to_json(p) FROM {fixture.name}.pgderive_progress p')==prior
        fixture.query = fixture.query.replace('b.','"B".').replace('bid b','bid AS "B"')
        resumed = fixture.start(command,'resumed',maximum=1)
        reopened = resumed.event('ready')
        assert reopened['slot']==ready['slot'] and reopened['time']==ready['time']+len(changes)
        fixture.verify()
        fixture.sql(f'DELETE FROM {fixture.name}.bid')
        resumed.event('published')
        resumed.finish()
        fixture.verify()
        (output/'result.json').write_text(json.dumps({'query':query,'time':'timestamptz' if instant else 'timestamp','postgres':'exact native MV weighted bag','memory':'independent integer buckets / '+('five memberships / maximum count ties' if query=='q5' else 'single membership / maximum price ties'),'restart':'cold same slot','identity':'changed stride/membership rejected','nulls':'time excluded, auction retained, max ignores NULL price','infinities':'PostgreSQL date_bin preserves infinity; '+('fixed offsets preserve infinity and hopping memberships consolidate fivefold' if query=='q5' else 'one bucket per infinite timestamp')})+'\n')
        fixture.cleanup()
    except BaseException:
        fixture.preserve()
        raise

if __name__=='__main__':
    output = pathlib.Path(sys.argv[1])
    command = sys.argv[2:]
    if command and command[0]=='--':
        command = command[1:]
    for query in ('q5','q7'):
        for instant in (False,True):
            directory = output/(query+('-instant' if instant else '-local'))
            directory.mkdir()
            qualify(directory,command,query,instant)
