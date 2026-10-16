#!/usr/bin/env python3
"""Scoped grouped memberships with compound native identity/time join keys."""
from worker_harness import harness_arguments, History
import collections
import json
import pathlib
from sql_compiler_harness import SqlFixture
from native_mv_oracle import expected as native_expected
from nexmark_temporal_harness import bucket, encode

SQL = pathlib.Path(__file__).resolve().parents[1]/'queries/nexmark/q8.sql'
LABELS = ['id','name','starttime']

class Q8Fixture(SqlFixture):
    def __init__(self,output,mode):
        super().__init__(output,False)
        self.pg.insert(1,'-q')
        self.instant = mode!='local'
        temporal = 'timestamptz' if self.instant else 'timestamp'
        identifier = 'bigint' if mode=='icu' else 'integer'
        name = 'varchar(128) COLLATE pg_catalog."C"' if mode=='c' else 'text COLLATE pg_catalog."und-x-icu"' if mode=='icu' else 'text'
        self.sql(f'''ALTER TABLE {self.name}.auction ADD COLUMN seller {identifier},ADD COLUMN date_time {temporal};
          UPDATE {self.name}.auction SET seller=id,date_time='2000-01-01'::{temporal}+INTERVAL '1 second'*(id%31);
          CREATE TABLE {self.name}.person(id {identifier} PRIMARY KEY,name {name},date_time {temporal});
          ALTER TABLE {self.name}.person REPLICA IDENTITY FULL;
          INSERT INTO {self.name}.person VALUES
          (1,'Web','2000-01-01'),(2,'web','2000-01-01 00:00:09.999999'),
          (3,'web ','2000-01-01 00:00:10'),(4,'é','2000-01-01'),(5,'é','2000-01-01'),
          (6,NULL,NULL),(7,'past','1999-12-31 23:59:59.999999'),
          (8,'infinite','infinity'),(9,NULL,'-infinity');
          INSERT INTO {self.name}.auction(id,seller,date_time) VALUES
          (300,1,'2000-01-01'),(301,1,'2000-01-01'),(302,1,'2000-01-01 00:00:10'),
          (303,2,'2000-01-01 00:00:10'),(304,NULL,'2000-01-01'),(305,6,NULL),
          (306,7,'1999-12-31 23:59:59.999999'),(307,8,'infinity'),(308,9,'-infinity');
          ALTER PUBLICATION {self.name} ADD TABLE {self.name}.person''')
        self.query = SQL.read_text().replace('nexmark.',self.name+'.').strip().removesuffix(';')
        if not self.instant:
            self.query = self.query.replace('TIMESTAMPTZ','TIMESTAMP').replace('2000-01-01 00:00:00+00','2000-01-01 00:00:00')

    def verify(self):
        prefix = "SET DateStyle='ISO,MDY'; SET TimeZone='UTC'; "
        expected = native_expected(self,LABELS)
        difference = self.bag_difference(expected, context=prefix)
        assert difference.strip()=='0','q8 differs from exact native MV'
        people = json.loads(self.sql(prefix+f"SELECT COALESCE(json_agg(p),'[]') FROM {self.name}.person p"))
        auctions = json.loads(self.sql(prefix+f"SELECT COALESCE(json_agg(a),'[]') FROM {self.name}.auction a"))
        persons = {(p['id'],p['name'],encode(bucket(p['date_time'],10),self.instant)) for p in people}
        sellers = {(a['seller'],encode(bucket(a['date_time'],10),self.instant)) for a in auctions}
        memory = collections.Counter((identity,name,start) for identity,name,start in persons if start is not None and (identity,start) in sellers)
        actual = json.loads(self.sql(f"SELECT COALESCE(json_agg(g),'[]') FROM {self.bag_relation()} g"))
        assert {tuple(row['tuple'][1]):row['weight'] for row in actual}==dict(memory),'q8 differs from independent membership set/join oracle'
        assert self.acknowledgement_ahead().strip()=='0'


def qualify(output,command,mode):
    fixture = Q8Fixture(output,mode)
    with fixture.managed():
        for label,query in [
            ('or',fixture.query.replace('AND p.starttime=a.starttime','OR p.starttime=a.starttime')),
            ('non-equality',fixture.query.replace('p.starttime=a.starttime','p.starttime>=a.starttime')),
            ('ambiguous',fixture.query.replace('SELECT p.id,p.name,p.starttime','SELECT p.id,p.name,starttime')),
        ]:
            fixture.rejected(command,label,query)
        assert fixture.sql(f"SELECT to_regclass('{fixture.name}.pgderive_worker_registration') IS NULL").strip()=='t'
        assert fixture.sql(f"SELECT count(*) FROM pg_replication_slots WHERE slot_name LIKE '{fixture.name}%' ").strip()=='0'
        changes = [
            f"INSERT INTO {fixture.name}.person VALUES(400,'new','2000-01-01'); INSERT INTO {fixture.name}.auction(id,seller,date_time) VALUES(400,400,'2000-01-01'),(401,400,'2000-01-01')",
            f"DELETE FROM {fixture.name}.auction WHERE id=400; UPDATE {fixture.name}.person SET name=NULL WHERE id=400",
            f"DELETE FROM {fixture.name}.auction WHERE id=401; UPDATE {fixture.name}.person SET date_time='2000-01-01 00:00:10' WHERE id=400",
            f"UPDATE {fixture.name}.person SET id=450,name='moved' WHERE id=400; UPDATE {fixture.name}.auction SET id=330,seller=450,date_time='2000-01-01 00:00:10' WHERE id=300",
            f"INSERT INTO {fixture.name}.auction(id,seller,date_time) VALUES(402,450,'2000-01-01 00:00:19.999999'); DELETE FROM {fixture.name}.auction WHERE id=330; UPDATE {fixture.name}.person SET date_time='2000-01-01 00:00:20' WHERE id=3",
            f"SET DateStyle='German,DMY'; SET TimeZone='America/New_York'; UPDATE {fixture.name}.person SET date_time='03.11.2024 01:30:09.999999-04' WHERE id=4; UPDATE {fixture.name}.auction SET seller=4,date_time='03.11.2024 01:30:09.999999-04' WHERE id=4; UPDATE {fixture.name}.person SET date_time='03.11.2024 01:30:10-05' WHERE id=5; UPDATE {fixture.name}.auction SET seller=5,date_time='03.11.2024 01:30:10-05' WHERE id=5",
            f"UPDATE {fixture.name}.person SET date_time=NULL WHERE id=8; UPDATE {fixture.name}.auction SET date_time=NULL WHERE id=307; DELETE FROM {fixture.name}.person WHERE id=450; UPDATE {fixture.name}.auction SET seller=NULL WHERE id=402",
            f"UPDATE {fixture.name}.bid SET price=NULL WHERE id=1",
        ]
        def initial():
            fixture.sql(f'BEGIN; DELETE FROM {fixture.name}.person; DELETE FROM {fixture.name}.auction; ROLLBACK')
        fixture.run_history(command, History(
            changes=changes,
            final=f'DELETE FROM {fixture.name}.person; DELETE FROM {fixture.name}.auction',
            normalized=fixture.query.replace('p.', '"P".').replace('a.', '"A".').replace('person p', 'person AS "P"').replace('auction a', 'auction AS "A"').replace(') p', ') AS "P"').replace(') a', ') AS "A"').replace('"P".id="A".seller AND "P".starttime="A".starttime', '"A".starttime="P".starttime AND "A".seller="P".id'),
            rejected=[('changed-key', fixture.query.replace('AND p.starttime=a.starttime', '')),('changed-stride', fixture.query.replace('10 seconds', '11 seconds'))],
        ), initial=initial)
        (output/'result.json').write_text(json.dumps({'mode':mode,'postgres':'exact native MV bag','memory':'independent grouped memberships and compound identity/bucket equality','transactions':'complete simultaneous sources; duplicate partial/last deletion; PK/key/bucket moves; NULLs/infinities; rollback; empty bid-only input tick','restart':'cold same slot; aliases/operand orientation/conjunction order normalized','identity':'changed stride and compound join key rejected'})+'\n')

if __name__=='__main__':
    output, command = harness_arguments()
    for mode in ('local','instant','c','icu'):
        directory = output/mode
        directory.mkdir()
        qualify(directory,command,mode)
