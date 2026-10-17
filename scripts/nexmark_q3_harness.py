#!/usr/bin/env python3
"""Named local-item join: native deterministic text equality and complete deltas."""
from worker_harness import harness_arguments, History
import collections
import json
import pathlib
from sql_compiler_harness import SqlFixture
from native_mv_oracle import expected as native_expected

SQL = pathlib.Path(__file__).resolve().parents[1]/'queries/nexmark/q3.sql'
LABELS = ['name','city','state','id']

class Q3Fixture(SqlFixture):
    def __init__(self,output,mode):
        super().__init__(output,False)
        self.pg.insert(1,'-q')
        native = 'varchar(128)' if mode=='c' else 'text'
        collation = {'default':'','c':' COLLATE pg_catalog."C"','icu':' COLLATE pg_catalog."und-x-icu"'}[mode]
        self.sql(f'''ALTER TABLE {self.name}.auction ALTER COLUMN category TYPE integer USING CASE WHEN id IN(1,2) THEN 10 ELSE 11 END;
          ALTER TABLE {self.name}.auction ADD COLUMN seller integer;
          UPDATE {self.name}.auction SET seller=id;
          CREATE TABLE {self.name}.person(id integer PRIMARY KEY,name text,city text,state {native}{collation});
          ALTER TABLE {self.name}.person REPLICA IDENTITY FULL;
          INSERT INTO {self.name}.person VALUES(1,'é🙂','Portland','OR'),(2,NULL,'Boise','ID'),(3,'case','x','or'),(4,'space','x','OR '),(5,'unknown',NULL,NULL),(6,'canonical','x','é'),(7,'decomposed','x','é'),(8,'empty','x','');
          INSERT INTO {self.name}.auction(id,category,seller) VALUES(300,10,3),(301,10,4),(302,10,5),(303,10,6),(304,10,7),(305,10,8),(306,NULL,1),(307,10,NULL),(308,10,999);
          ALTER PUBLICATION {self.name} ADD TABLE {self.name}.person''')
        self.query = SQL.read_text().replace('nexmark.',self.name+'.').strip().removesuffix(';')

    def verify(self):
        expected = native_expected(self,LABELS)
        self.verify_sql(expected)
        people = json.loads(self.sql(f"SELECT COALESCE(json_agg(p),'[]') FROM {self.name}.person p"))
        auctions = json.loads(self.sql(f"SELECT COALESCE(json_agg(a),'[]') FROM {self.name}.auction a"))
        memory = collections.Counter()
        for a in auctions:
            for p in people:
                if a['seller'] is not None and a['seller']==p['id'] and a['category']==10 and p['state'] in ('OR','ID','CA'):
                    memory[(p['name'],p['city'],p['state'],a['id'])] += 1
        self.verify_memory(dict(memory))


def qualify(output,command,mode):
    fixture = Q3Fixture(output,mode)
    with fixture.managed():
        fixture.rejected(command,'text-order',fixture.query.replace("p.state='OR'","p.state<'OR'"))
        fixture.rejected(command,'column-collation',fixture.query.replace("p.state='OR'","p.state=p.city"))
        if mode=='icu':
            fixture.sql(f"CREATE COLLATION {fixture.name}.nondeterministic(provider=icu,locale='und-u-ks-level1',deterministic=false); ALTER TABLE {fixture.name}.person ALTER COLUMN state TYPE text COLLATE {fixture.name}.nondeterministic")
            fixture.rejected(command,'nondeterministic-source',fixture.query)
            fixture.sql(f'ALTER TABLE {fixture.name}.person ALTER COLUMN state TYPE text COLLATE pg_catalog."und-x-icu"')
        assert fixture.sql(f"SELECT to_regclass('{fixture.name}.pgderive_worker_registration') IS NULL").strip()=='t'
        assert fixture.sql(f"SELECT count(*) FROM pg_replication_slots WHERE slot_name LIKE '{fixture.name}%' ").strip()=='0'
        changes = [
          f"INSERT INTO {fixture.name}.person VALUES(400,'new','city','CA'); INSERT INTO {fixture.name}.auction(id,category,seller) VALUES(400,10,400),(401,10,400)",
          f"UPDATE {fixture.name}.person SET state='or',city=NULL WHERE id=400; UPDATE {fixture.name}.auction SET seller=1 WHERE id=401",
          f"UPDATE {fixture.name}.person SET id=450,state='ID' WHERE id=400; UPDATE {fixture.name}.auction SET seller=450 WHERE id=400",
          f"DELETE FROM {fixture.name}.person WHERE id=450; DELETE FROM {fixture.name}.auction WHERE id=401",
          f"INSERT INTO {fixture.name}.person VALUES(450,NULL,NULL,'OR'); UPDATE {fixture.name}.auction SET category=10 WHERE id=306; UPDATE {fixture.name}.person SET state=NULL WHERE id=1",
          f"UPDATE {fixture.name}.person SET state='CA' WHERE id=3; UPDATE {fixture.name}.person SET state='OR' WHERE id=4; UPDATE {fixture.name}.auction SET seller=NULL WHERE id=2",
          f"UPDATE {fixture.name}.person SET state='CA ' WHERE id=3; UPDATE {fixture.name}.person SET state='OR' WHERE id=1; UPDATE {fixture.name}.auction SET category=NULL WHERE id=400",
          f"UPDATE {fixture.name}.bid SET price=NULL WHERE id=1",
        ]
        def initial():
            fixture.sql(f'BEGIN; DELETE FROM {fixture.name}.person; DELETE FROM {fixture.name}.auction; ROLLBACK')
        fixture.run_history(command, History(
            changes=changes,
            final=f'BEGIN; DELETE FROM {fixture.name}.person; DELETE FROM {fixture.name}.auction; COMMIT',
            normalized=fixture.query.replace('p.', '"P".').replace('a.', '"A".').replace('person p', 'person AS "P"').replace('auction a', 'auction AS "A"'),
            rejected=[('changed-filter', fixture.query.replace("p.state='OR'", "p.state='WA'"))],
        ), initial=initial)
        (output/'result.json').write_text(json.dumps({'query':'q3','mode':mode,'postgres':'native MV exact bag','memory':'raw nested-loop local suggestions','restart':'cold same slot','changes':'simultaneous inserts updates deletes and key moves','text':'case/trailing spaces/NULL; deterministic source collation','identity':'changed literal rejected; aliases normalize'})+'\n')

if __name__=='__main__':
    output, command = harness_arguments()
    for mode in ('default','c','icu'):
        directory = output/mode
        directory.mkdir()
        qualify(directory,command,mode)
