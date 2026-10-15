#!/usr/bin/env python3
"""Exact terminal decimal q1: independent Decimal bag and PostgreSQL, CDC/restart."""
from worker_harness import harness_arguments, History
import collections
import decimal
import json
import pathlib
from sql_compiler_harness import SqlFixture
from native_mv_oracle import expected as native_expected

SQL = pathlib.Path(__file__).resolve().parents[1]/'queries/nexmark/q1.sql'
decimal.getcontext().prec = 100

class Q1Fixture(SqlFixture):
    def __init__(self,output,mode):
        super().__init__(output,False)
        self.mode = mode
        self.factor = '0.0' if mode=='zero' else '0.908'
        self.sql(f'UPDATE {self.name}.auction SET enabled=true WHERE id=1')
        native = {'int2':'smallint','int4':'integer','int8':'bigint','join':'bigint','zero':'bigint'}[mode]
        self.sql(f'ALTER TABLE {self.name}.bid ALTER COLUMN price TYPE {native}; ALTER TABLE {self.name}.bid ADD COLUMN bidder bigint,ADD COLUMN date_time timestamp,ADD COLUMN extra text; UPDATE {self.name}.bid SET bidder=5,date_time=\'2000-01-01 12:00:00.123456\',extra=\'é🙂\'')
        bound = {'int2':32767,'int4':2147483647,'int8':9223372036854775807,'join':9223372036854775807,'zero':9223372036854775807}[mode]
        self.sql(f"INSERT INTO {self.name}.bid(id,auction,price,bidder,date_time,extra) VALUES(300,1,{bound},5,NULL,'extreme'),(301,1,{-bound-1},5,NULL,'extreme'),(302,1,NULL,NULL,NULL,NULL),(303,1,0,5,NULL,'zero'),(304,1,0,5,NULL,'zero')")
        self.query = SQL.read_text().replace('nexmark.bid',f'{self.name}.bid').strip().removesuffix(';')
        if mode=='join':
            self.query = self.query.replace('SELECT auction,bidder,0.908 * price AS price,date_time,extra','SELECT b.auction,b.bidder,0.908 * b.price AS price,b.date_time,b.extra').replace(f'FROM {self.name}.bid',f'FROM {self.name}.auction a JOIN {self.name}.bid b ON a.id=b.auction WHERE a.enabled OR b.price IS NULL')
        if mode=='zero':
            self.query = self.query.replace('0.908',self.factor)
            self.sql(f"UPDATE {self.name}.bid SET extra='zero' WHERE id IN(300,301)")
        self.labels = ['auction','bidder','price','date_time','extra']

    def verify(self):
        expected = native_expected(self,self.labels)
        difference = self.bag_difference(expected)
        assert difference.strip()=='0','q1 differs from PostgreSQL'
        source = json.loads(self.sql(f"SELECT COALESCE(json_agg(b),'[]') FROM {self.name}.bid b"))
        auctions = {a['id']:a for a in json.loads(self.sql(f"SELECT COALESCE(json_agg(a),'[]') FROM {self.name}.auction a"))}
        memory = collections.Counter()
        for row in source:
            if self.mode=='join':
                a = auctions.get(row['auction'])
                if a is None or (a['enabled'] is not True and row['price'] is not None):
                    continue
            price = None if row['price'] is None else decimal.Decimal(row['price'])*decimal.Decimal(self.factor)
            memory[(row['auction'],row['bidder'],price,row['date_time'],row['extra'])] += 1
        actual = json.loads(self.sql(f"SELECT COALESCE(json_agg(g),'[]') FROM {self.name}.groups g"),parse_float=decimal.Decimal)
        assert {tuple(row['tuple'][1]):row['weight'] for row in actual}==dict(memory),'q1 differs from exact Decimal oracle'
        assert self.acknowledgement_ahead().strip()=='0'


def qualify(output,command,mode):
    fixture = Q1Fixture(output,mode)
    with fixture.managed():
        fixture.rejected(command,'unsupported-arithmetic',fixture.query.replace(fixture.factor+' *',fixture.factor+' +'))
        assert fixture.sql(f"SELECT to_regclass('{fixture.name}.pgderive_worker_registration') IS NULL").strip()=='t'
        assert fixture.sql(f"SELECT count(*) FROM pg_replication_slots WHERE slot_name LIKE '{fixture.name}%' ").strip()=='0'
        changes = [
            f"INSERT INTO {fixture.name}.auction(id,category,enabled) VALUES(400,'new',true); INSERT INTO {fixture.name}.bid(id,auction,price,bidder,date_time,extra) VALUES(400,400,9,5,NULL,'new'),(401,400,NULL,NULL,NULL,NULL)",
            f"UPDATE {fixture.name}.bid SET price=-9 WHERE id=300; UPDATE {fixture.name}.auction SET enabled=NULL WHERE id=400",
            f"DELETE FROM {fixture.name}.bid WHERE id=301; UPDATE {fixture.name}.bid SET price=NULL WHERE id=303; UPDATE {fixture.name}.auction SET enabled=true WHERE id=400",
            f"UPDATE {fixture.name}.bid SET price=0,extra=NULL WHERE id=400; DELETE FROM {fixture.name}.auction WHERE id=400",
            f"UPDATE {fixture.name}.auction SET group_id=NULL WHERE id=2",
        ]
        def initial():
            assert int(fixture.sql(f'SELECT max(weight) FROM {fixture.name}.groups'))>1
            fixture.sql(f'BEGIN; DELETE FROM {fixture.name}.bid; ROLLBACK')
        fixture.run_history(command, History(
            changes=changes,
            final=f'DELETE FROM {fixture.name}.bid',
            normalized=fixture.query.replace(fixture.factor, '0' + fixture.factor),
            rejected=[('changed-factor', fixture.query.replace(fixture.factor, '0.909')),('changed-scale', fixture.query.replace(fixture.factor, fixture.factor + '0'))],
        ), initial=initial)
        (output/'result.json').write_text(json.dumps({'query':'q1','mode':mode,'postgres':'exact weighted numeric bag','memory':'independent Decimal multiplication','restart':'cold same slot','factor_change':'rejected before progress change','simultaneous':'both source changes','terminal':'PG numeric_mul, retained raw integral bag'})+'\n')

if __name__=='__main__':
    output, command = harness_arguments()
    for mode in ('int2','int4','int8','join','zero'):
        directory = output/mode
        directory.mkdir()
        qualify(directory,command,mode)
