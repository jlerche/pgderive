#!/usr/bin/env python3
"""Native calendar terminal maps and text-grouped daily statistics, exact MV bags."""
from worker_harness import harness_arguments
import collections
import json
import pathlib
from native_compiler_harness import NativeFixture, encoded, instant
from nexmark_q15_harness import counts, day_start
from native_mv_oracle import expected as native_expected

QUERIES = pathlib.Path(__file__).resolve().parents[1]/'queries/nexmark'
COUNTS = ['total_bids','rank1_bids','rank2_bids','rank3_bids','total_bidders','rank1_bidders','rank2_bidders','rank3_bidders','total_auctions','rank1_auctions','rank2_auctions','rank3_auctions']


def format_time(time,pattern):
    if time is None or time in ('infinity','-infinity'):
        return None
    date,clock = time.split(' ',1)
    return date if pattern=='YYYY-MM-DD' else clock[:5]


class CalendarFixture(NativeFixture):
    def __init__(self,output,query,mode):
        super().__init__(output,'projection')
        self.kind = query
        self.instant = mode in ('instant','icu')
        self.pattern = 'HH24:MI'
        temporal = 'timestamptz' if self.instant else 'timestamp'
        channel = 'varchar(128) COLLATE pg_catalog."C"' if mode=='c' else 'text COLLATE pg_catalog."und-x-icu"' if mode=='icu' else 'text'
        source = 'event_time' if self.instant else 'event_at'
        self.sql(f'''ALTER TABLE {self.name}.bid ADD COLUMN date_time {temporal},ADD COLUMN bidder bigint,ADD COLUMN extra text,ADD COLUMN channel {channel};
          UPDATE {self.name}.bid SET date_time={source},bidder=CASE WHEN id%4=0 THEN NULL ELSE id%5 END,extra='é🙂',channel=CASE id%6 WHEN 0 THEN NULL WHEN 1 THEN 'Web' WHEN 2 THEN 'web' WHEN 3 THEN 'web ' WHEN 4 THEN 'é' ELSE 'é' END;
          INSERT INTO {self.name}.bid(id,auction,price,date_time,bidder,extra,channel) VALUES
          (300,900,9999,'2000-01-01 23:59:59.999999+00',500,'same','web'),
          (301,900,9999,'2000-01-01 23:59:59.999999+00',500,'same','web'),
          (302,901,10000,'2000-01-01 23:58:59.999999+00',501,NULL,'web'),
          (303,901,999999,'2000-01-02 00:00:00+00',501,'x','web'),
          (304,NULL,1000000,NULL,NULL,NULL,NULL),
          (305,900,NULL,'2000-01-02 00:00:00+00',NULL,'x','web'),
          (306,NULL,NULL,NULL,NULL,NULL,NULL)''')
        self.query = (QUERIES/(query+'.sql')).read_text().replace('nexmark.bid',f'{self.name}.bid').strip().removesuffix(';')
        if not self.instant:
            self.query = self.query.replace(" AT TIME ZONE 'UTC'",'').replace('TIMESTAMPTZ','TIMESTAMP').replace('2000-01-01 00:00:00+00','2000-01-01 00:00:00')
        self.labels = ['auction','bidder','price','date_time','extra','dt','hm'] if query=='q10' else ['channel','day_start','minute',*COUNTS]

    def verify(self):
        prefix = "SET DateStyle='ISO,MDY'; SET TimeZone='UTC'; "
        expected = native_expected(self,self.labels)
        difference = self.bag_difference(expected, context=prefix)
        assert difference.strip()=='0',self.kind+' differs from native PostgreSQL MV'
        source = json.loads(self.sql(prefix+f"SELECT COALESCE(json_agg(q),'[]') FROM(SELECT id,auction,price,bidder,extra,channel,date_time::text AS time FROM {self.name}.bid) q"))
        memory = collections.Counter()
        if self.kind=='q10':
            for row in source:
                output = [row['auction'],row['bidder'],row['price'],encoded(row['time'],self.instant),row['extra'],format_time(row['time'],'YYYY-MM-DD'),format_time(row['time'],self.pattern)]
                memory[tuple(output)] += 1
        else:
            groups = collections.defaultdict(list)
            for row in source:
                day = day_start(row['time'])
                if not self.instant and day is not None:
                    day = day.replace('+00','')
                groups[(row['channel'],day)].append(row)
            for (channel,day),rows in groups.items():
                times = [row['time'] for row in rows if row['time'] is not None]
                latest = max(times,key=instant) if times else None
                ranks = [[],[],[]]
                for row in rows:
                    price = row['price']
                    if price is not None:
                        ranks[0 if price<10000 else 1 if price<1000000 else 2].append(row)
                all_counts = [counts(rows),*[counts(rank) for rank in ranks]]
                output = [channel,encoded(day,self.instant),format_time(latest,self.pattern)]
                for field in range(3):
                    output.extend(value[field] for value in all_counts)
                memory[tuple(output)] += 1
        actual = json.loads(self.sql(f"SELECT COALESCE(json_agg(g),'[]') FROM {self.bag_relation()} g"))
        assert {tuple(row['tuple'][1]):row['weight'] for row in actual}==dict(memory),self.kind+' differs from independent calendar/weighted statistics oracle'
        assert self.acknowledgement_ahead().strip()=='0'


def qualify(output,command,query,mode):
    fixture = CalendarFixture(output,query,mode)
    with fixture.managed():
        fixture.rejected(command,'unsafe-format',fixture.query.replace('HH24:MI','Month'))
        if fixture.instant:
            invalid = fixture.query.replace(" AT TIME ZONE 'UTC'", " AT TIME ZONE 'America/New_York'")
            fixture.rejected(command,'unsafe-zone',invalid)
        assert fixture.sql(f"SELECT to_regclass('{fixture.name}.pgderive_worker_registration') IS NULL").strip()=='t'
        assert fixture.sql(f"SELECT count(*) FROM pg_replication_slots WHERE slot_name LIKE '{fixture.name}%' ").strip()=='0'
        worker = fixture.start(command,'first')
        ready = worker.event('ready')
        fixture.verify()
        if query=='q10':
            assert int(fixture.sql(f'SELECT max(weight) FROM {fixture.bag_relation()}'))>1
        changes = [
          f"INSERT INTO {fixture.name}.bid(id,auction,price,date_time,bidder,extra,channel) VALUES(400,900,1000000,'2000-01-01 23:59:59.999999+00',500,'same','web'); UPDATE {fixture.name}.auction SET group_id=NULL WHERE id=1",
          f"DELETE FROM {fixture.name}.bid WHERE id IN(300,301,400); UPDATE {fixture.name}.bid SET price=999999,bidder=NULL,extra=NULL WHERE id=302",
          f"UPDATE {fixture.name}.bid SET channel='Web',date_time='2000-01-02 00:00:59.999999+00',auction=NULL WHERE id=302; UPDATE {fixture.name}.auction SET group_id=3 WHERE id=1",
          f"UPDATE {fixture.name}.bid SET date_time=NULL,channel=NULL,price=NULL WHERE id=303; UPDATE {fixture.name}.bid SET channel='web ' WHERE id=305",
          f"UPDATE {fixture.name}.bid SET id=450,channel='web',date_time='2000-01-01 23:58:00+00',bidder=500 WHERE id=302",
          f"SET DateStyle='German,DMY'; SET TimeZone='America/New_York'; UPDATE {fixture.name}.bid SET date_time='03.11.2024 01:30:59.999999-04',channel='DST' WHERE id=304; UPDATE {fixture.name}.bid SET date_time='03.11.2024 01:30:00-05',channel='DST' WHERE id=306",
          f"UPDATE {fixture.name}.bid SET channel='é' WHERE id=306; UPDATE {fixture.name}.bid SET channel='é' WHERE id=304; DELETE FROM {fixture.name}.bid WHERE id=450",
          f"UPDATE {fixture.name}.auction SET group_id=NULL WHERE id=2",
        ]
        fixture.sql(f'BEGIN; DELETE FROM {fixture.name}.bid; ROLLBACK')
        for index,change in enumerate(changes,1):
            fixture.sql('BEGIN;'+change+';COMMIT')
            worker.event('published',minimum_time=ready['time']+index)
            fixture.verify()
        worker.abort()
        prior = fixture.sql(f'SELECT row_to_json(p) FROM {fixture.name}.pgderive_progress p')
        fixture.rejected(command,'changed-format',fixture.query.replace('HH24:MI','YYYY-MM-DD'))
        if query=='q16':
            fixture.rejected(command,'changed-threshold',fixture.query.replace('10000','10001'))
        assert fixture.sql(f'SELECT row_to_json(p) FROM {fixture.name}.pgderive_progress p')==prior
        if fixture.instant:
            column = 'date_time' if query=='q10' else 'q.latest'
            fixture.query = fixture.query.replace(column+" AT TIME ZONE 'UTC'",f"pg_catalog.timezone('UTC',{column})")
        else:
            fixture.query = fixture.query.replace('pg_catalog.to_char','to_char')
        resumed = fixture.start(command,'resumed',maximum=1)
        reopened = resumed.event('ready')
        assert reopened['slot']==ready['slot'] and reopened['time']==ready['time']+len(changes)
        fixture.verify()
        fixture.sql(f'DELETE FROM {fixture.name}.bid')
        resumed.event('published')
        resumed.finish()
        fixture.verify()
        (output/'result.json').write_text(json.dumps({'query':query,'mode':mode,'postgres':'native MV exact weighted bag','memory':'direct calendar fields and raw grouped value sets','restart':'cold same slot','format':'BC/max-year/infinities/NULL, exact minute fields','changes':'latest retractions text/day moves simultaneous sources rollback empty ticks','identity':'changed pattern/threshold rejected; timezone spelling normalizes'})+'\n')

if __name__=='__main__':
    output, command = harness_arguments()
    for query,modes in [('q10',('local','instant')),('q16',('local','instant','c','icu'))]:
        for mode in modes:
            directory = output/(query+'-'+mode)
            directory.mkdir()
            qualify(directory,command,query,mode)
