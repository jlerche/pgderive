#!/usr/bin/env python3
"""Named q4/q9 PG eligibility and winner histories with source recomputation."""
from worker_harness import harness_arguments, History
import collections
import datetime
from decimal import Decimal
import json
import pathlib
from numeric_compiler_harness import average
from sql_compiler_harness import SqlFixture
from native_mv_oracle import expected as native_expected

ROOT = pathlib.Path(__file__).resolve().parents[1]
Q9_LABELS = ['auction_id','item_name','description','initial_bid','reserve','date_time','expires','seller','category','auction_extra','auction','bidder','price','bid_date_time','bid_extra']


def time_key(value):
    if value in ('-infinity','infinity'):
        return (-1 if value=='-infinity' else 1,datetime.datetime.min)
    time = datetime.datetime.fromisoformat(value)
    if time.tzinfo is not None:
        time = time.astimezone(datetime.timezone.utc).replace(tzinfo=None)
    return (0,time)


class AuctionFixture(SqlFixture):
    def __init__(self,output,query,instant):
        super().__init__(output,False)
        self.pg.insert(1,'-q')
        self.kind,self.instant = query,instant
        native = 'timestamptz' if instant else 'timestamp'
        self.sql(f'''ALTER TABLE {self.name}.auction ALTER COLUMN category TYPE integer USING id%3;
            ALTER TABLE {self.name}.auction ADD COLUMN date_time {native},ADD COLUMN expires {native},
            ADD COLUMN item_name text,ADD COLUMN description text,ADD COLUMN initial_bid bigint,
            ADD COLUMN reserve bigint,ADD COLUMN seller bigint,ADD COLUMN extra text;
            ALTER TABLE {self.name}.bid ADD COLUMN date_time {native},ADD COLUMN bidder bigint,ADD COLUMN extra text;
            UPDATE {self.name}.auction SET date_time='2000-01-01 00:00:00',expires='2000-01-01 00:00:10',item_name='é🙂',description='',initial_bid=1,reserve=3,seller=id%4,extra='a/'||id;
            UPDATE {self.name}.bid SET date_time='2000-01-01 00:00:05',bidder=id%5,extra='b/'||id;
            UPDATE {self.name}.auction SET category=NULL WHERE id=1;
            INSERT INTO {self.name}.auction(id,category,date_time,expires,item_name,description,initial_bid,reserve,seller,extra) VALUES
                (200,1,'2000-01-01','2000-01-01 00:00:10','boundary',NULL,-9223372036854775808,9223372036854775807,NULL,NULL),
                (201,1,NULL,'2000-01-01',NULL,NULL,NULL,NULL,NULL,NULL),
                (202,2,'-infinity','infinity','infinite','all',0,0,0,''),
                (203,2,'2000-01-01',NULL,NULL,NULL,NULL,NULL,NULL,NULL),
                (204,3,'2000-01-01','2000-01-01 00:00:10','null prices',NULL,NULL,NULL,NULL,NULL);
            INSERT INTO {self.name}.bid(id,auction,price,date_time,bidder,extra) VALUES
                (200,200,9223372036854775807,'2000-01-01',1,'at start'),
                (201,200,9223372036854775806,'2000-01-01 00:00:10',2,'at expiry'),
                (202,200,9223372036854775807,'1999-12-31 23:59:59.999999',3,'too early'),
                (203,200,9223372036854775807,'2000-01-01 00:00:10.000001',4,'too late'),
                (204,200,NULL,NULL,NULL,'null time'),(205,NULL,9,'2000-01-01',5,'null key'),
                (206,201,9,'2000-01-01',5,'null opening'),(207,203,9,'2000-01-01',5,'null expiry'),
                (208,202,3,'-infinity',6,'negative infinity'),(209,202,4,'infinity',7,'infinity'),
                (210,204,NULL,'2000-01-01',8,'all null'),(211,204,NULL,'2000-01-01 00:00:10',9,'all null')''')
        self.query = (ROOT/f'queries/nexmark/{query}.sql').read_text().replace('nexmark.auction',f'{self.name}.auction').replace('nexmark.bid',f'{self.name}.bid').strip().removesuffix(';')
        self.labels = ['category','average_price'] if query=='q4' else Q9_LABELS

    def verify(self):
        prefix = "SET DateStyle='ISO,MDY'; SET TimeZone='UTC'; "
        expected = native_expected(self,self.labels)
        difference = self.bag_difference(expected, context=prefix)
        assert difference.strip()=='0',f'{self.kind} differs from PostgreSQL'
        auctions = json.loads(self.sql(prefix+f"SELECT COALESCE(json_agg(a),'[]') FROM {self.name}.auction a"))
        bids = json.loads(self.sql(prefix+f"SELECT COALESCE(json_agg(b),'[]') FROM {self.name}.bid b"))
        groups = collections.defaultdict(list)
        for a in auctions:
            for b in bids:
                if b['auction'] is None or a['id']!=b['auction'] or any(v is None for v in (b['date_time'],a['date_time'],a['expires'])):
                    continue
                if time_key(a['date_time'])<=time_key(b['date_time'])<=time_key(a['expires']):
                    groups[a['id']].append((a,b))
        memory = collections.Counter()
        if self.kind=='q4':
            categories = collections.defaultdict(list)
            for rows in groups.values():
                prices = [b['price'] for a,b in rows if b['price'] is not None]
                categories[rows[0][0]['category']].append(max(prices) if prices else None)
            for key,prices in categories.items():
                memory[(key,average([p for p in prices if p is not None]))] += 1
        else:
            for rows in groups.values():
                a,b = min(rows,key=lambda pair:(pair[1]['price'] is not None,-(pair[1]['price'] or 0),time_key(pair[1]['date_time']),pair[1]['id'],pair[0]['id']))
                memory[(a['id'],a['item_name'],a['description'],a['initial_bid'],a['reserve'],a['date_time'],a['expires'],a['seller'],a['category'],a['extra'],b['auction'],b['bidder'],b['price'],b['date_time'],b['extra'])] += 1
        actual = json.loads(self.sql(f"SELECT COALESCE(json_agg(g),'[]') FROM {self.bag_relation()} g"),parse_float=Decimal)
        assert {tuple(row['tuple'][1]):row['weight'] for row in actual}==dict(memory),f'{self.kind} differs from independent nested-loop eligibility/winner/average oracle'
        assert self.acknowledgement_ahead().strip()=='0'


def qualify(output,command,query,instant):
    fixture = AuctionFixture(output,query,instant)
    with fixture.managed():
        fixture.rejected(command,'unsupported',fixture.query+' ORDER BY category')
        if query=='q9':
            fixture.rejected(command,'missing-bid-key',fixture.query.replace('j.bid_id,j.auction_id','j.auction_id'))
        assert fixture.sql(f"SELECT to_regclass('{fixture.name}.pgderive_worker_registration') IS NULL").strip()=='t'
        assert fixture.sql(f"SELECT count(*) FROM pg_replication_slots WHERE slot_name LIKE '{fixture.name}%' ").strip()=='0'
        changes = [
            f"INSERT INTO {fixture.name}.auction(id,category,date_time,expires,item_name,extra) VALUES(300,3,'2000-01-01 00:00:00+00','2000-01-01 00:00:10+00','new/new','auction'); INSERT INTO {fixture.name}.bid(id,auction,price,date_time,bidder,extra) VALUES(300,300,-11,'2000-01-01 00:00:00+00',1,'new/new'),(301,300,NULL,'2000-01-01 00:00:10+00',2,'NULL first')",
            f"INSERT INTO {fixture.name}.bid(id,auction,price,date_time,bidder,extra) VALUES(212,200,9223372036854775807,'2000-01-01',1,'tie'),(213,200,NULL,'2000-01-01 00:00:02',2,'NULL first'); UPDATE {fixture.name}.auction SET category=2,item_name='updated' WHERE id=200",
            f"DELETE FROM {fixture.name}.bid WHERE id=200; UPDATE {fixture.name}.auction SET expires='2000-01-01 00:00:02' WHERE id=200; UPDATE {fixture.name}.bid SET date_time='2000-01-01 00:00:02.000001' WHERE id=213",
            f"UPDATE {fixture.name}.auction SET date_time='1999-12-31 23:59:59.999999',category=NULL WHERE id=200; UPDATE {fixture.name}.bid SET auction=200,date_time='2000-01-01 00:00:02',price=-9223372036854775808 WHERE id=211",
            f"UPDATE {fixture.name}.auction SET date_time='2024-03-10 01:59:59-08',expires='2024-03-10 03:00:00-07' WHERE id=200; UPDATE {fixture.name}.bid SET date_time='2024-03-10 03:00:00-07',price=10 WHERE id=211",
            f"DELETE FROM {fixture.name}.auction WHERE id=202; DELETE FROM {fixture.name}.bid WHERE id IN(208,209); UPDATE {fixture.name}.bid SET auction=NULL,date_time=NULL WHERE id=212",
            f"UPDATE {fixture.name}.auction SET expires=NULL WHERE id=200; UPDATE {fixture.name}.bid SET price=NULL WHERE id=210",
        ]
        def initial():
            fixture.sql(f'BEGIN; DELETE FROM {fixture.name}.bid; ROLLBACK')
        fixture.run_history(command, History(
            changes=changes,
            final=f'BEGIN; DELETE FROM {fixture.name}.auction; DELETE FROM {fixture.name}.bid; COMMIT',
            normalized=fixture.query.replace('b.', '"B".').replace('bid b ', 'bid AS "B" '),
            rejected=[('changed-boundary', fixture.query.replace('b.date_time<=a.expires', 'b.date_time<a.expires'))] + ([] + [('changed-null-order', fixture.query.replace('DESC NULLS FIRST', 'DESC NULLS LAST'))] if query == 'q9' else []),
            session="SET DateStyle='German,DMY'; SET TimeZone='America/New_York'; ",
        ), initial=initial)
        (output/'result.json').write_text(json.dumps({'query':query,'native_time':'timestamptz' if instant else 'timestamp','eligibility':'inclusive both endpoints, no clock','postgres':'exact bag','memory':'independent eligibility/max/winner/Decimal average','nulls':'times/keys/prices/categories/payload','restart':'cold same slot','simultaneous':'both inputs'})+'\n')


if __name__=='__main__':
    output, command = harness_arguments()
    for query in ('q4','q9'):
        for instant in (False,True):
            directory = output/(query+('-instant' if instant else '-local'))
            directory.mkdir()
            qualify(directory,command,query,instant)
