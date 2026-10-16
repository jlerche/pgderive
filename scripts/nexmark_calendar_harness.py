from engine_rejection import qualify_unavailable
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
    qualify_unavailable(fixture,command)


if __name__=='__main__':
    output, command = harness_arguments()
    for query,modes in [('q10',('local','instant')),('q16',('local','instant','c','icu'))]:
        for mode in modes:
            directory = output/(query+'-'+mode)
            directory.mkdir()
            qualify(directory,command,query,mode)
