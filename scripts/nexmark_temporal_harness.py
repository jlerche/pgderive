#!/usr/bin/env python3
"""Named sessions, recorded processing buckets and exact daily statistics."""
from worker_harness import History, run_cases
from contextlib import contextmanager
import collections
import datetime
from decimal import Decimal
import json
import pathlib
from numeric_compiler_harness import average
from sql_compiler_harness import SqlFixture
from native_mv_oracle import expected as native_expected

QUERIES = pathlib.Path(__file__).resolve().parents[1]/'queries/nexmark'
EPOCH = datetime.datetime(2000,1,1)
INFINITIES = ('-infinity','infinity')


def date(value):
    result = datetime.datetime.fromisoformat(value)
    return result.astimezone(datetime.timezone.utc).replace(tzinfo=None) if result.tzinfo else result


def encode(value,instant):
    if value is None or value in INFINITIES:
        return value
    result = value if isinstance(value,datetime.datetime) else date(value)
    return result.isoformat(timespec='microseconds').rstrip('0').rstrip('.')+('+00:00' if instant else '')


def bucket(value,seconds):
    if value is None or value in INFINITIES:
        return value
    delta = date(value)-EPOCH
    micros = (delta.days*86400+delta.seconds)*1000000+delta.microseconds
    return EPOCH+datetime.timedelta(microseconds=micros//(seconds*1000000)*(seconds*1000000))


def ordered(value):
    if value=='-infinity':
        return (0,EPOCH)
    if value=='infinity':
        return (2,EPOCH)
    return (1,date(value))


def connected(left,right):
    if left==right:
        return True
    return left not in INFINITIES and right not in INFINITIES and date(right)-date(left)<datetime.timedelta(seconds=10)


class TemporalFixture(SqlFixture):
    def __init__(self,output,query,instant):
        super().__init__(output,False)
        self.pg.insert(1,'-q')
        self.kind,self.instant = query,instant
        temporal = 'timestamptz' if instant else 'timestamp'
        self.sql(f'''ALTER TABLE {self.name}.bid ADD COLUMN bidder bigint,ADD COLUMN date_time {temporal},ADD COLUMN processing_time {temporal};
          UPDATE {self.name}.bid SET bidder=id%3,date_time='2000-01-01 00:00:00'::{temporal}+INTERVAL '1 second'*(id%31),processing_time='2000-01-02 00:00:00'::{temporal}+INTERVAL '1 second'*(id%31);
          INSERT INTO {self.name}.bid(id,auction,price,bidder,date_time,processing_time) VALUES
            (300,900,9999,900,'2000-01-01','2000-01-01 23:59:59.999999'),
            (301,900,10000,900,'2000-01-01 00:00:20','2000-01-02'),
            (302,901,999999,901,'2000-01-01 00:00:00.000001','1999-12-31 23:59:59.999999'),
            (303,901,1000000,901,'2000-01-01 00:00:10','2000-01-01'),
            (304,902,NULL,902,'2000-01-01','2000-01-01 00:00:09.999999'),
            (305,902,9223372036854775807,902,'2000-01-01 00:00:10','2000-01-01 00:00:10'),
            (306,NULL,-9223372036854775808,NULL,NULL,NULL),
            (307,NULL,0,NULL,'2000-01-01','2000-01-01'),
            (308,NULL,NULL,NULL,'2000-01-01','2000-01-01'),
            (309,903,9223372036854775807,903,'-infinity','-infinity'),
            (310,903,9223372036854775807,903,'-infinity','-infinity'),
            (311,903,NULL,903,'infinity','infinity'),
            (320,910,NULL,910,'2000-01-01','2000-01-01'),
            (321,910,NULL,910,'2000-01-01','2000-01-01')''')
        self.query = (QUERIES/(query+'.sql')).read_text().replace('nexmark.bid',f'{self.name}.bid').strip().removesuffix(';')
        if not instant:
            self.query = self.query.replace('TIMESTAMPTZ','TIMESTAMP').replace("2000-01-01 00:00:00+00","2000-01-01 00:00:00")
        self.labels = {'q11':['bidder','starttime','endtime','bid_count'],
                       'q12':['bidder','bid_count','starttime','endtime'],
                       'q17':['auction','day_start','total_bids','rank1_bids','rank2_bids','rank3_bids','min_price','max_price','avg_price','sum_price']}[query]

    def verify(self):
        prefix = "SET DateStyle='ISO,MDY'; SET TimeZone='UTC'; "
        expected = native_expected(self,self.labels)
        self.verify_sql(expected, context=prefix)
        source = json.loads(self.sql(prefix+f"SELECT COALESCE(json_agg(b),'[]') FROM {self.name}.bid b"))
        groups = collections.defaultdict(list)
        for row in source:
            if self.kind=='q11':
                if row['date_time'] is not None:
                    groups[row['bidder']].append(row['date_time'])
            elif self.kind=='q12':
                groups[(row['bidder'],bucket(row['processing_time'],10))].append(row)
            else:
                groups[(row['auction'],bucket(row['date_time'],86400))].append(row)
        memory = collections.Counter()
        for key,rows in groups.items():
            if self.kind=='q11':
                rows.sort(key=ordered)
                components = []
                for value in rows:
                    if components and connected(components[-1][-1],value):
                        components[-1].append(value)
                    else:
                        components.append([value])
                for component in components:
                    last = component[-1]
                    end = last if last in INFINITIES else date(last)+datetime.timedelta(seconds=10)
                    memory[(key,encode(component[0],self.instant),encode(end,self.instant),len(component))] += 1
            elif self.kind=='q12':
                bidder,start = key
                end = start if start is None or start in INFINITIES else start+datetime.timedelta(seconds=10)
                memory[(bidder,len(rows),encode(start,self.instant),encode(end,self.instant))] += 1
            else:
                auction,day = key
                prices = [row['price'] for row in rows if row['price'] is not None]
                bands = [sum(p<10000 for p in prices),sum(10000<=p<1000000 for p in prices),sum(p>=1000000 for p in prices)]
                memory[(auction,encode(day,self.instant),len(rows),*bands,min(prices) if prices else None,max(prices) if prices else None,average(prices),sum(prices) if prices else None)] += 1
        self.verify_memory(dict(memory), parse_float=Decimal)

    def bag(self):
        return self.sql(f'SELECT jsonb_agg(jsonb_build_array(tuple,weight) ORDER BY tuple) FROM {self.bag_relation()}')


def qualify(output,command,query,instant):
    fixture = TemporalFixture(output,query,instant)
    with fixture.managed():
        fixture.rejected(command,'unsupported',fixture.query+' ORDER BY '+fixture.labels[0])
        assert fixture.sql(f"SELECT to_regclass('{fixture.name}.pgderive_worker_registration') IS NULL").strip()=='t'
        assert fixture.sql(f"SELECT count(*) FROM pg_replication_slots WHERE slot_name LIKE '{fixture.name}%' ").strip()=='0'
        changes = [
            ('bridge',f"INSERT INTO {fixture.name}.bid(id,auction,price,bidder,date_time,processing_time) VALUES(312,900,9999,900,'2000-01-01 00:00:09','2000-01-01 00:00:09'),(313,900,10000,900,'2000-01-01 00:00:11','2000-01-01 00:00:10'); UPDATE {fixture.name}.auction SET enabled=NULL WHERE id=1"),
            ('split',f"DELETE FROM {fixture.name}.bid WHERE id=312; UPDATE {fixture.name}.bid SET date_time='2000-01-01 00:00:09.999999',price=NULL WHERE id=305; UPDATE {fixture.name}.bid SET price=1 WHERE id=320"),
            ('merge-move',f"INSERT INTO {fixture.name}.bid(id,auction,price,bidder,date_time,processing_time) VALUES(312,900,1000000,900,'2000-01-01 00:00:09','2000-01-01 00:00:09'); UPDATE {fixture.name}.bid SET bidder=901,auction=901,date_time='2000-01-02',processing_time=NULL WHERE id=301; UPDATE {fixture.name}.bid SET price=NULL WHERE id=320"),
            ('retract',f"DELETE FROM {fixture.name}.bid WHERE id=313; UPDATE {fixture.name}.bid SET date_time=NULL,processing_time='2000-01-02',price=-1 WHERE id=300; UPDATE {fixture.name}.auction SET enabled=true WHERE id=1"),
            ('event-only',f"UPDATE {fixture.name}.bid SET date_time='2024-01-01' WHERE id=302; UPDATE {fixture.name}.auction SET group_id=NULL WHERE id=2"),
            ('DST',f"SET DateStyle='German,DMY'; SET TimeZone='America/New_York'; UPDATE {fixture.name}.bid SET date_time='03.11.2024 01:30:00.123456-04',processing_time='03.11.2024 01:30:09.999999-04' WHERE id=303; UPDATE {fixture.name}.bid SET date_time='03.11.2024 01:30:00.123456-05',processing_time='03.11.2024 01:30:10-05' WHERE id=304"),
            ('delete-partition',f"DELETE FROM {fixture.name}.bid WHERE bidder IN(900,901); DELETE FROM {fixture.name}.bid WHERE id=309; UPDATE {fixture.name}.bid SET auction=NULL,bidder=NULL,price=NULL WHERE id=310"),
            ('empty-input',f"UPDATE {fixture.name}.auction SET group_id=3 WHERE id=2"),
        ]
        @contextmanager
        def observe(index):
            before = fixture.bag() if query=='q12' and changes[index-1][0]=='event-only' else None
            yield
            if before is not None:
                assert fixture.bag()==before,'q12 must not use changed event time as processing time'
        changed = fixture.query.replace('10 seconds','11 seconds') if query in ('q11','q12') else fixture.query.replace('10000','10001')
        fixture.run_history(command, History([change for _,change in changes],
            f'DELETE FROM {fixture.name}.bid',
            fixture.query.replace('b.','"B".').replace('bid b','bid AS "B"'),
            [('changed-semantics',changed)]),
            initial=lambda: fixture.sql(f'BEGIN; DELETE FROM {fixture.name}.bid; ROLLBACK'), step=observe)
        (output/'result.json').write_text(json.dumps({'query':query,'time':'timestamptz' if instant else 'timestamp','postgres':'exact weighted bag','memory':'independent adjacency components / integer buckets / exact price statistics','restart':'cold same slot','session':'gap >=10 starts new component; late bridges merge and deletion splits','processing':'recorded source column, event-only change does not affect q12','statistics':'exact integral extrema/sum and PG-scale Decimal average','nulls':'NULL bidder/auction, q11 excludes NULL time, q12/q17 keep NULL bucket','infinities':'retained with guarded equal-time sessions','identity':'changed gap/stride/threshold rejected'})+'\n')

if __name__ == '__main__':
    run_cases(((query + ('-instant' if instant else '-local'), query, instant)
               for query in ('q11','q12','q17') for instant in (False,True)), qualify)
