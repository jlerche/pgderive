#!/usr/bin/env python3
"""Named latest/top-ten bids: independent ordered occurrence bag, PG and restart."""
from worker_harness import harness_arguments, History
import collections
import datetime
import json
import pathlib
from sql_compiler_harness import SqlFixture
from native_mv_oracle import expected as native_expected

QUERIES = pathlib.Path(__file__).resolve().parents[1]/'queries/nexmark'
PAYLOAD = ['auction','bidder','price','channel','url','date_time','extra']


def descending_time(value):
    if value is None:
        return (0,0)
    if value=='infinity':
        return (1,0)
    if value=='-infinity':
        return (3,0)
    date = datetime.datetime.fromisoformat(value)
    if date.tzinfo is not None:
        date = date.astimezone(datetime.timezone.utc).replace(tzinfo=None)
    offset = date-datetime.datetime(2000,1,1)
    micros = (offset.days*86400+offset.seconds)*1000000+offset.microseconds
    return (2,-micros)


class OrderedFixture(SqlFixture):
    def __init__(self,output,query,instant):
        super().__init__(output,False)
        self.pg.insert(1,'-q')
        self.kind,self.instant = query,instant
        temporal = 'timestamptz' if instant else 'timestamp'
        self.sql(f'''ALTER TABLE {self.name}.bid ADD COLUMN bidder bigint,ADD COLUMN channel text,
          ADD COLUMN url text,ADD COLUMN date_time {temporal},ADD COLUMN extra text;
          UPDATE {self.name}.bid SET bidder=id%3,channel='é🙂',url='https://界/é/🙂',date_time='2000-01-01 00:00:05',extra='existing';
          INSERT INTO {self.name}.bid(id,auction,price,bidder,channel,url,date_time,extra)
          SELECT i,1,10,5,'same','',CASE WHEN i%3=0 THEN '2000-01-01 00:00:10'::{temporal} ELSE '2000-01-01 00:00:05'::{temporal} END,'same'
          FROM generate_series(300,339) i;
          INSERT INTO {self.name}.bid(id,auction,price,bidder,channel,url,date_time,extra) VALUES
          (340,1,NULL,5,NULL,NULL,NULL,NULL),(341,1,9223372036854775807,5,'extreme','', 'infinity','max'),
          (342,1,-9223372036854775808,5,'extreme','', '-infinity','min'),
          (343,NULL,9,NULL,NULL,NULL,NULL,NULL),(344,NULL,9,NULL,NULL,NULL,NULL,NULL),
          (345,2,9,NULL,'tie','', '2000-01-01 00:00:10','first'),
          (346,2,9,NULL,'tie','', '2000-01-01 00:00:10','second'),
          (347,3,NULL,NULL,'null price','', 'infinity',''),
          (348,3,1,NULL,'finite price','',NULL,''),
          (349,4,0,1,NULL,NULL,'1999-12-31 23:59:59.999999',NULL)''')
        self.query = (QUERIES/(query+'.sql')).read_text().replace('nexmark.bid',f'{self.name}.bid').strip().removesuffix(';')
        self.labels = PAYLOAD+(['rank_number'] if query=='q19' else [])

    def verify(self):
        prefix = "SET DateStyle='ISO,MDY'; SET TimeZone='UTC'; "
        expected = native_expected(self,self.labels)
        difference = self.bag_difference(expected, context=prefix)
        assert difference.strip()=='0',self.kind+' differs from PostgreSQL'
        source = json.loads(self.sql(prefix+f"SELECT COALESCE(json_agg(b),'[]') FROM {self.name}.bid b"))
        groups = collections.defaultdict(list)
        for row in source:
            group = (row['bidder'],row['auction']) if self.kind=='q18' else (row['auction'],)
            groups[group].append(row)
        memory = collections.Counter()
        for rows in groups.values():
            if self.kind=='q18':
                rows.sort(key=lambda row:(descending_time(row['date_time']),row['id']))
                selected = rows[:1]
            else:
                rows.sort(key=lambda row:(row['price'] is not None,-(row['price'] or 0),descending_time(row['date_time']),row['id']))
                selected = rows[:10]
            for index,row in enumerate(selected,1):
                payload = tuple(row[label] for label in PAYLOAD)
                memory[payload+((index,) if self.kind=='q19' else ())] += 1
        actual = json.loads(self.sql(f"SELECT COALESCE(json_agg(g),'[]') FROM {self.bag_relation()} g"))
        assert {tuple(row['tuple'][1]):row['weight'] for row in actual}==dict(memory),self.kind+' differs from independent occurrence ordering'
        assert self.acknowledgement_ahead().strip()=='0'


def qualify(output,command,query,instant):
    fixture = OrderedFixture(output,query,instant)
    with fixture.managed():
        fixture.rejected(command,'missing-stable-key',fixture.query.replace(',b.id ASC',''))
        fixture.rejected(command,'unsupported-function',fixture.query.replace('row_number()','ntile(10)'))
        assert fixture.sql(f"SELECT to_regclass('{fixture.name}.pgderive_worker_registration') IS NULL").strip()=='t'
        assert fixture.sql(f"SELECT count(*) FROM pg_replication_slots WHERE slot_name LIKE '{fixture.name}%' ").strip()=='0'
        changes = [
            f"INSERT INTO {fixture.name}.bid(id,auction,price,bidder,channel,url,date_time,extra) VALUES(400,1,11,5,'new','', '2000-01-01 00:00:09','late arrival'),(401,1,10,5,'tie','', '2000-01-01 00:00:10','new tie'); UPDATE {fixture.name}.auction SET enabled=NULL WHERE id=1",
            f"DELETE FROM {fixture.name}.bid WHERE id IN(340,341); UPDATE {fixture.name}.bid SET price=12,date_time='2000-01-01 00:00:11' WHERE id=339; UPDATE {fixture.name}.auction SET enabled=true WHERE id=1",
            f"DELETE FROM {fixture.name}.bid WHERE id IN(300,303,306); UPDATE {fixture.name}.bid SET auction=2,bidder=NULL,price=9,date_time='2000-01-01 00:00:10' WHERE id=400",
            f"UPDATE {fixture.name}.bid SET id=299 WHERE id=346; UPDATE {fixture.name}.bid SET auction=NULL,bidder=NULL,date_time=NULL,price=NULL WHERE id=401",
            f"UPDATE {fixture.name}.bid SET auction=3,price=NULL,date_time='infinity',bidder=NULL WHERE id=348; DELETE FROM {fixture.name}.bid WHERE id=347",
            f"SET DateStyle='German,DMY'; SET TimeZone='America/New_York'; UPDATE {fixture.name}.bid SET date_time='03.11.2024 01:30:00.123456-04' WHERE id=400; INSERT INTO {fixture.name}.bid(id,auction,price,bidder,date_time,extra) VALUES(402,2,9,NULL,'03.11.2024 01:30:00.123456-05','DST peer')",
            f"DELETE FROM {fixture.name}.bid WHERE auction=1; UPDATE {fixture.name}.bid SET auction=1,bidder=5,price=1,date_time='-infinity' WHERE id=349",
            f"UPDATE {fixture.name}.auction SET group_id=NULL WHERE id=3",
        ]
        def initial():
            if query == 'q19':
                assert fixture.sql(f"SELECT sum(weight) FROM {fixture.bag_relation()} WHERE tuple->1->>0='1'").strip() == '10'
            fixture.sql(f'BEGIN; DELETE FROM {fixture.name}.bid; ROLLBACK')
        fixture.run_history(command, History(
            changes=changes,
            final=f'DELETE FROM {fixture.name}.bid',
            normalized=fixture.query.replace('b.', '"B".').replace('bid b', 'bid AS "B"'),
            rejected=[('changed-cutoff', fixture.query.replace('<=10', '<=9') if query == 'q19' else fixture.query.replace('<=1', '<=2'))] + [('changed-null-order', fixture.query.replace('NULLS FIRST', 'NULLS LAST'))],
        ), initial=initial)
        (output/'result.json').write_text(json.dumps({'query':query,'time':'timestamptz' if instant else 'timestamp','postgres':'exact occurrence-ranked bag','memory':'independent timestamp/integral tuple ordering','restart':'cold same slot','ties':'explicit stable bid PK, omitted from output','changes':'late inserts, deleting winners/backfill, PK and partition moves, NULLs, infinities, rollback, empty query-input tick','identity':'changed cutoff/null order rejected; quoted aliases normalize'})+'\n')

if __name__=='__main__':
    output, command = harness_arguments()
    for query in ('q18','q19'):
        for instant in (False,True):
            directory = output/(query+('-instant' if instant else '-local'))
            directory.mkdir()
            qualify(directory,command,query,instant)
