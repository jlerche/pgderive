#!/usr/bin/env python3
"""Named q15 UTC-day portfolio qualification against PG and source-value sets."""
import collections
import json
import pathlib
import sys
from native_compiler_harness import NativeFixture, encoded

SQL = pathlib.Path(__file__).resolve().parents[1]/'queries/nexmark/q15.sql'


def day_start(time):
    if time is None or time in ('infinity','-infinity'):
        return time
    date = time.split(' ',1)[0]
    bc = ' BC' if time.endswith(' BC') else ''
    return date+' 00:00:00+00'+bc


def counts(rows):
    return [len(rows),len({row['bidder'] for row in rows if row['bidder'] is not None}),len({row['auction'] for row in rows if row['auction'] is not None})]


class Q15Fixture(NativeFixture):
    def __init__(self,output):
        super().__init__(output,'projection')
        self.sql(f"ALTER TABLE {self.name}.bid RENAME COLUMN event_time TO date_time; ALTER TABLE {self.name}.bid ADD COLUMN bidder bigint; UPDATE {self.name}.bid SET bidder=id%5; UPDATE {self.name}.bid SET bidder=NULL WHERE id%4=0")
        self.sql(f"INSERT INTO {self.name}.bid(id,auction,price,date_time,bidder) VALUES(200,900,9999,'2000-01-01 23:59:59.999999+00',500),(201,900,10000,'2000-01-01 23:59:59.999999+00',500),(202,901,999999,'2000-01-01 23:59:59.999999+00',501),(203,901,1000000,'2000-01-02 00:00:00+00',501),(204,NULL,NULL,NULL,NULL),(205,900,-1,'2000-01-02 00:00:00+00',500)")
        self.query = SQL.read_text().replace('nexmark.bid',f'{self.name}.bid').strip().removesuffix(';')
        self.labels = ['day_start','total_bids','rank1_bids','rank2_bids','rank3_bids','total_bidders','rank1_bidders','rank2_bidders','rank3_bidders','total_auctions','rank1_auctions','rank2_auctions','rank3_auctions']

    def verify(self):
        prefix = "SET DateStyle='ISO,MDY'; SET TimeZone='UTC'; "
        expected = f"SELECT jsonb_build_array(NULL,jsonb_build_array({','.join(self.labels)})) tuple,count(*)::bigint weight FROM ({self.query}) q GROUP BY 1"
        difference = self.sql(prefix+f'''WITH expected AS ({expected}),actual AS
            (SELECT tuple,weight FROM {self.name}.groups),difference AS
            ((SELECT * FROM expected EXCEPT ALL SELECT * FROM actual) UNION ALL
            (SELECT * FROM actual EXCEPT ALL SELECT * FROM expected)) SELECT count(*) FROM difference''')
        assert difference.strip() == '0','q15 output differs from PostgreSQL'
        source = json.loads(self.sql(prefix+f"SELECT COALESCE(json_agg(q),'[]') FROM(SELECT id,auction,price,bidder,date_time::text AS time FROM {self.name}.bid) q"))
        groups = collections.defaultdict(list)
        for row in source:
            groups[day_start(row['time'])].append(row)
        memory = collections.Counter()
        for day,rows in groups.items():
            ranks = [[],[],[]]
            for row in rows:
                price = row['price']
                if price is not None:
                    ranks[0 if price<10000 else 1 if price<1000000 else 2].append(row)
            all_counts = [counts(rows),*[counts(rank) for rank in ranks]]
            output = [encoded(day,True)]
            for field in range(3):
                output.extend(value[field] for value in all_counts)
            memory[tuple(output)] += 1
        actual = json.loads(self.sql(f"SELECT COALESCE(json_agg(g),'[]') FROM {self.name}.groups g"))
        assert {tuple(row['tuple'][1]):row['weight'] for row in actual} == dict(memory),'q15 output differs from independent UTC-day/value-presence oracle'
        assert self.sql(f'''SELECT count(*) FROM pg_replication_slots s JOIN {self.name}.pgderive_worker_registration r ON r.slot_name=s.slot_name JOIN {self.name}.pgderive_progress p ON true WHERE s.confirmed_flush_lsn>p.end_lsn''').strip() == '0'


def qualify(output,command):
    fixture = Q15Fixture(output)
    try:
        fixture.rejected(command,'unsupported',fixture.query+' ORDER BY day_start')
        assert fixture.sql(f"SELECT to_regclass('{fixture.name}.pgderive_worker_registration') IS NULL").strip() == 't'
        assert fixture.sql(f"SELECT count(*) FROM pg_replication_slots WHERE slot_name LIKE '{fixture.name}%'").strip() == '0'
        worker = fixture.start(command,'first')
        ready = worker.event('ready')
        fixture.verify()
        changes = [
            f"INSERT INTO {fixture.name}.bid(id,auction,price,date_time,bidder) VALUES(206,900,9999,'2000-01-01 23:59:59.999999+00',500),(207,NULL,1000000,NULL,NULL)",
            f"DELETE FROM {fixture.name}.bid WHERE id=200; UPDATE {fixture.name}.bid SET price=1000000,bidder=502,auction=NULL WHERE id=201; UPDATE {fixture.name}.auction SET group_id=NULL WHERE id=1",
            f"UPDATE {fixture.name}.bid SET date_time='2000-01-02 00:00:00+00',bidder=501 WHERE id=206; DELETE FROM {fixture.name}.bid WHERE id=202",
            f"UPDATE {fixture.name}.bid SET date_time='2024-03-10 01:59:59.999999-08',price=10000 WHERE id=1; UPDATE {fixture.name}.bid SET date_time='2024-03-10 03:00:00-07',price=999999 WHERE id=2",
            f"UPDATE {fixture.name}.bid SET date_time=NULL,price=NULL WHERE id IN(203,205); DELETE FROM {fixture.name}.bid WHERE id IN(201,206)",
            f"UPDATE {fixture.name}.auction SET group_id=3 WHERE id=2",
        ]
        fixture.sql(f"BEGIN; DELETE FROM {fixture.name}.bid; ROLLBACK")
        for index,change in enumerate(changes,1):
            fixture.sql("SET DateStyle='German,DMY'; SET TimeZone='America/New_York'; BEGIN;"+change+';COMMIT')
            worker.event('published',minimum_time=ready['time']+index)
            fixture.verify()
        worker.abort()
        prior = fixture.sql(f'SELECT row_to_json(p) FROM {fixture.name}.pgderive_progress p')
        fixture.rejected(command,'changed-threshold',fixture.query.replace('10000','10001'))
        fixture.rejected(command,'changed-distinct',fixture.query.replace('COUNT(DISTINCT','COUNT('))
        assert fixture.sql(f'SELECT row_to_json(p) FROM {fixture.name}.pgderive_progress p') == prior
        fixture.query = fixture.query.replace('b.','"B".').replace('bid b','bid AS "B"')
        resumed = fixture.start(command,'resumed',maximum=1)
        reopened = resumed.event('ready')
        assert reopened['slot']==ready['slot'] and reopened['time']==ready['time']+len(changes)
        fixture.verify()
        fixture.sql(f"DELETE FROM {fixture.name}.bid")
        resumed.event('published')
        resumed.finish()
        fixture.verify()
        (output/'result.json').write_text(json.dumps({'query':'q15','day':'UTC midnight timestamptz','sql_oracle':'exact bag','memory_oracle':'UTC-day price bands and value-presence sets','restart':'cold','infinities':'included','null_day':'included','thresholds':[10000,1000000]})+'\n')
        fixture.cleanup()
    except BaseException:
        fixture.preserve()
        raise


if __name__ == '__main__':
    output = pathlib.Path(sys.argv[1])
    command = sys.argv[2:]
    if command and command[0]=='--':
        command = command[1:]
    qualify(output,command)
