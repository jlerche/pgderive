#!/usr/bin/env python3
"""Named q0/q2 exact weighted projection, remainder, CDC and cold restart."""
import collections
import json
import pathlib
import sys
from sql_compiler_harness import SqlFixture

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
        expected = f"SELECT jsonb_build_array(NULL,jsonb_build_array({','.join(self.labels)})) tuple,count(*)::bigint weight FROM({self.query}) q GROUP BY 1"
        difference = self.sql(f'''WITH expected AS ({expected}),actual AS
            (SELECT tuple,weight FROM {self.name}.groups),difference AS
            ((SELECT * FROM expected EXCEPT ALL SELECT * FROM actual) UNION ALL
             (SELECT * FROM actual EXCEPT ALL SELECT * FROM expected)) SELECT count(*) FROM difference''')
        assert difference.strip()=='0',self.query_name+' differs from PostgreSQL'
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
        actual = json.loads(self.sql(f"SELECT COALESCE(json_agg(g),'[]') FROM {self.name}.groups g"))
        assert {tuple(row['tuple'][1]):row['weight'] for row in actual}==dict(memory),self.query_name+' differs from independent raw bag oracle'
        assert self.sql(f'''SELECT count(*) FROM pg_replication_slots s JOIN {self.name}.pgderive_worker_registration r ON r.slot_name=s.slot_name CROSS JOIN {self.name}.pgderive_progress p WHERE s.confirmed_flush_lsn>p.end_lsn''').strip()=='0'


def qualify(output,command,query,mode):
    fixture = ProjectionFixture(output,query,mode)
    try:
        invalid = fixture.query.replace('pg_catalog.mod(auction,123)','pg_catalog.mod(auction,0)') if query=='q2' else fixture.query.replace('SELECT auction,','SELECT auction+1,')
        fixture.rejected(command,'unsupported',invalid)
        assert fixture.sql(f"SELECT to_regclass('{fixture.name}.pgderive_worker_registration') IS NULL").strip()=='t'
        assert fixture.sql(f"SELECT count(*) FROM pg_replication_slots WHERE slot_name LIKE '{fixture.name}%' ").strip()=='0'
        worker = fixture.start(command,'first')
        ready = worker.event('ready')
        fixture.verify()
        assert int(fixture.sql(f'SELECT max(weight) FROM {fixture.name}.groups'))>1
        changes = [
            f"INSERT INTO {fixture.name}.auction(id,category,enabled) VALUES(400,'new',true); INSERT INTO {fixture.name}.bid(id,auction,price,bidder,date_time,extra) VALUES(400,246,9,5,NULL,'new'),(401,-246,NULL,NULL,NULL,NULL)",
            f"UPDATE {fixture.name}.bid SET auction=124,price=NULL WHERE id=300; UPDATE {fixture.name}.auction SET enabled=NULL WHERE id=400",
            f"UPDATE {fixture.name}.bid SET auction=-123 WHERE id=307; DELETE FROM {fixture.name}.bid WHERE id=301; UPDATE {fixture.name}.auction SET enabled=true WHERE id=400",
            f"UPDATE {fixture.name}.bid SET auction=NULL,extra=NULL WHERE id=400; UPDATE {fixture.name}.bid SET id=450 WHERE id=401; DELETE FROM {fixture.name}.auction WHERE id=400",
            f"SET TIME ZONE 'America/New_York'; UPDATE {fixture.name}.bid SET date_time='2024-11-03 01:30:00.123456-04',bidder=NULL WHERE id=307; UPDATE {fixture.name}.auction SET group_id=NULL WHERE id=2",
            f"UPDATE {fixture.name}.auction SET group_id=3 WHERE id=2",
        ]
        fixture.sql(f'BEGIN; DELETE FROM {fixture.name}.bid; ROLLBACK')
        for index,change in enumerate(changes,1):
            fixture.sql('BEGIN;'+change+';COMMIT')
            worker.event('published',minimum_time=ready['time']+index)
            fixture.verify()
        worker.abort()
        prior = fixture.sql(f'SELECT row_to_json(p) FROM {fixture.name}.pgderive_progress p')
        changed = fixture.query.replace(',123)',',124)') if query=='q2' else fixture.query.replace('SELECT auction,','SELECT price AS auction,')
        fixture.rejected(command,'changed-query',changed)
        assert fixture.sql(f'SELECT row_to_json(p) FROM {fixture.name}.pgderive_progress p')==prior
        if query=='q2':
            fixture.query = fixture.query.replace('pg_catalog.mod(auction,123)','auction % 123')
        else:
            fixture.query = fixture.query.replace('SELECT auction,bidder,price,date_time,extra', 'SELECT "B".auction,"B".bidder,"B".price,"B".date_time,"B".extra').replace(f'FROM {fixture.name}.bid',f'FROM {fixture.name}.bid AS "B"')
        resumed = fixture.start(command,'resumed',maximum=1)
        reopened = resumed.event('ready')
        assert reopened['slot']==ready['slot'] and reopened['time']==ready['time']+len(changes)
        fixture.verify()
        fixture.sql(f'DELETE FROM {fixture.name}.bid')
        resumed.event('published')
        resumed.finish()
        fixture.verify()
        (output/'result.json').write_text(json.dumps({'query':query,'mode':mode,'postgres':'exact weighted bag','memory':'independent raw projection and integer divisibility','restart':'cold same slot','changes':'inserts updates deletes NULLs PK moves complete transactions and rollback','identity':'alias/remainder syntax normalize; changed query rejected','empty_tick':'other published relation changes advance progress'})+'\n')
        fixture.cleanup()
    except BaseException:
        fixture.preserve()
        raise

if __name__=='__main__':
    output = pathlib.Path(sys.argv[1])
    command = sys.argv[2:]
    if command and command[0]=='--':
        command = command[1:]
    for query,modes in [('q0',('local','instant')),('q2',('int2','int4','int8'))]:
        for mode in modes:
            directory = output/(query+'-'+mode)
            directory.mkdir()
            qualify(directory,command,query,mode)
