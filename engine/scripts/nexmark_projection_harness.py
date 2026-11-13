#!/usr/bin/env python3
"""Named q0/q2 exact weighted projection, remainder, CDC and cold restart."""
from worker_harness import History, run_cases
import collections
import json
import pathlib
from sql_compiler_harness import SqlFixture
from native_mv_oracle import expected as native_expected

QUERIES = pathlib.Path(__file__).resolve().parents[1]/'queries/nexmark'

class ProjectionFixture(SqlFixture):
    def __init__(self,output,query,mode):
        super().__init__(output,False)
        self.query_name = query
        self.mode = mode
        native = {'int2':'smallint','int4':'integer','int8':'bigint'}.get(mode,'bigint')
        temporal = 'timestamptz' if mode=='instant' else 'timestamp'
        self.sql(f"ALTER TABLE {self.name}.bid ALTER COLUMN auction TYPE {native}; ALTER TABLE {self.name}.bid ADD COLUMN bidder bigint,ADD COLUMN date_time {temporal},ADD COLUMN extra text; UPDATE {self.name}.bid SET bidder=5,date_time='2000-01-01 12:00:00.123456',extra='é🙂'")
        bound = {'int2':32767,'int4':2147483647,'int8':9223372036854775807}.get(mode,9223372036854775807)
        # Duplicate projected values have distinct source PKs; excluded PK changes
        # must never replace/collapse the source bag before projection.
        self.sql(f"INSERT INTO {self.name}.bid(id,auction,price,bidder,date_time,extra) VALUES(300,123,9,5,NULL,'same'),(301,123,9,5,NULL,'same'),(302,-123,NULL,NULL,'-infinity',NULL),(303,0,-9223372036854775808,5,'infinity','zero'),(304,NULL,9223372036854775807,NULL,NULL,NULL),(305,{bound},9,5,NULL,'max'),(306,{-bound-1},9,5,NULL,'min'),(307,124,9,5,NULL,'nonmultiple')")
        self.query = (QUERIES/(query+'.sql')).read_text().replace('nexmark.bid',f'{self.name}.bid').strip().removesuffix(';')
        self.labels = ['auction','bidder','price','date_time','extra'] if query=='q0' else ['auction','price']

    def verify(self):
        expected = native_expected(self,self.labels)
        self.verify_sql(expected)
        source = json.loads(self.sql(f"SELECT COALESCE(json_agg(b),'[]') FROM {self.name}.bid b"))
        memory = collections.Counter()
        for row in source:
            if self.query_name=='q2':
                auction = row['auction']
                # Divisibility independently derives qualification, without using
                # the compiler's remainder expression or PostgreSQL MOD.
                if auction is None or abs(auction)//123*123!=abs(auction):
                    continue
            memory[tuple(row[label] for label in self.labels)] += 1
        self.verify_memory(dict(memory))


def qualify(output,command,query,mode):
    fixture = ProjectionFixture(output,query,mode)
    with fixture.managed():
        invalid = fixture.query.replace('pg_catalog.mod(auction,123)','pg_catalog.mod(auction,0)') if query=='q2' else fixture.query.replace('SELECT auction,','SELECT auction/0,')
        fixture.rejected(command,'unsupported',invalid)
        assert fixture.sql(f"SELECT to_regclass('{fixture.name}.pgderive_worker_registration') IS NULL").strip()=='t'
        assert fixture.sql(f"SELECT count(*) FROM pg_replication_slots WHERE slot_name LIKE '{fixture.name}%' ").strip()=='0'
        changes = [
            f"INSERT INTO {fixture.name}.auction(id,category,enabled) VALUES(400,'new',true); INSERT INTO {fixture.name}.bid(id,auction,price,bidder,date_time,extra) VALUES(400,246,9,5,NULL,'new'),(401,-246,NULL,NULL,NULL,NULL)",
            f"UPDATE {fixture.name}.bid SET auction=124,price=NULL WHERE id=300; UPDATE {fixture.name}.auction SET enabled=NULL WHERE id=400",
            f"UPDATE {fixture.name}.bid SET auction=-123 WHERE id=307; DELETE FROM {fixture.name}.bid WHERE id=301; UPDATE {fixture.name}.auction SET enabled=true WHERE id=400",
            f"UPDATE {fixture.name}.bid SET auction=NULL,extra=NULL WHERE id=400; UPDATE {fixture.name}.bid SET id=450 WHERE id=401; DELETE FROM {fixture.name}.auction WHERE id=400",
            f"SET TIME ZONE 'America/New_York'; UPDATE {fixture.name}.bid SET date_time='2024-11-03 01:30:00.123456-04',bidder=NULL WHERE id=307; UPDATE {fixture.name}.auction SET group_id=NULL WHERE id=2",
            f"UPDATE {fixture.name}.auction SET group_id=3 WHERE id=2",
        ]
        changed = fixture.query.replace(',123)',',124)') if query=='q2' else fixture.query.replace('SELECT auction,','SELECT price AS auction,')
        if query=='q2':
            normalized = fixture.query.replace('pg_catalog.mod(auction,123)','auction % 123')
        else:
            normalized = fixture.query.replace('SELECT auction,bidder,price,date_time,extra', 'SELECT "B".auction,"B".bidder,"B".price,"B".date_time,"B".extra').replace(f'FROM {fixture.name}.bid',f'FROM {fixture.name}.bid AS "B"')
        def initial():
            assert int(fixture.sql(f'SELECT max(weight) FROM {fixture.bag_relation()}'))>1
            fixture.sql(f'BEGIN; DELETE FROM {fixture.name}.bid; ROLLBACK')
        fixture.run_history(command, History(changes, f'DELETE FROM {fixture.name}.bid', normalized,
            [('changed-query',changed)]), initial=initial)
        (output/'result.json').write_text(json.dumps({'query':query,'mode':mode,'postgres':'exact weighted bag','memory':'independent raw projection and integer divisibility','restart':'cold same slot','changes':'inserts updates deletes NULLs PK moves complete transactions and rollback','identity':'alias/remainder syntax normalize; changed query rejected','empty_tick':'other published relation changes advance progress'})+'\n')

if __name__ == '__main__':
    run_cases(((query+'-'+mode, query, mode)
               for query,modes in [('q0',('local','instant')),('q2',('int2','int4','int8'))]
               for mode in modes), qualify)
