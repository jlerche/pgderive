#!/usr/bin/env python3
"""Terminal PG URL components: absent vs empty, raw bag, CDC and cold replay."""
from worker_harness import harness_arguments
import collections
import json
import pathlib
import subprocess
import time
import sys
from sql_compiler_harness import SqlFixture
from native_mv_oracle import expected as native_expected

SQL = pathlib.Path(__file__).resolve().parents[1]/'queries/nexmark/q22.sql'


def literal(text):
    return 'NULL' if text is None else "E'"+text.replace('\\','\\\\').replace("'","''")+"'"


class Q22Fixture(SqlFixture):
    def __init__(self,output,mode):
        super().__init__(output,False)
        self.sql(f'UPDATE {self.name}.auction SET enabled=true WHERE id=1')
        self.mode = mode
        self.delimiter = "'\\🙂" if mode=='delimiter' else '/'
        native = 'varchar(256)' if mode=='varchar' else 'text'
        self.sql(f"ALTER TABLE {self.name}.bid ADD COLUMN bidder bigint,ADD COLUMN channel text,ADD COLUMN url {native}; UPDATE {self.name}.bid SET bidder=5,channel='é🙂',url='https://example/a/b/c'")
        samples = [None,'','https://example','https://example/','https://example/a/b/c','https://example//b/','https://example/a//c','/////','scheme://界/é/🙂/終',"https://example/o'ne/\\two/終",'https://other/a/b/c']
        for index,url in enumerate(samples,300):
            if mode=='delimiter' and url is not None:
                url = url.replace('/',self.delimiter)
            self.sql(f"INSERT INTO {self.name}.bid(id,auction,price,bidder,channel,url) VALUES({index},1,9,5,'é🙂',{literal(url)})")
        self.query = SQL.read_text().replace('nexmark.bid',f'{self.name}.bid').strip().removesuffix(';')
        if mode=='delimiter':
            self.query = self.query.replace("'/'",literal(self.delimiter))
        if mode=='join':
            self.query = self.query.replace(f'FROM {self.name}.bid b',f'FROM {self.name}.auction a JOIN {self.name}.bid b ON a.id=b.auction WHERE a.enabled OR b.price IS NULL')
        self.labels = ['auction','bidder','price','channel','dir1','dir2','dir3']

    def verify(self):
        expected = native_expected(self,self.labels)
        difference = self.bag_difference(expected)
        assert difference.strip()=='0','q22 differs from PostgreSQL'
        source = json.loads(self.sql(f"SELECT COALESCE(json_agg(b),'[]') FROM {self.name}.bid b"))
        auctions = {a['id']:a for a in json.loads(self.sql(f"SELECT COALESCE(json_agg(a),'[]') FROM {self.name}.auction a"))}
        memory = collections.Counter()
        for row in source:
            if self.mode=='join':
                a = auctions.get(row['auction'])
                if a is None or (a['enabled'] is not True and row['price'] is not None):
                    continue
            url = row['url']
            components = [] if url is None or url=='' else url.split(self.delimiter)
            dirs = [components[index] if index<len(components) else None for index in (3,4,5)]
            memory[tuple(row[label] for label in self.labels[:4])+tuple(dirs)] += 1
        actual = json.loads(self.sql(f"SELECT COALESCE(json_agg(g),'[]') FROM {self.bag_relation()} g"))
        assert {tuple(row['tuple'][1]):row['weight'] for row in actual}==dict(memory),'q22 differs from independent component splitting oracle'
        assert self.acknowledgement_ahead().strip()=='0'


def qualify(output,command,mode):
    fixture = Q22Fixture(output,mode)
    proxy = None
    proxy_log = None
    try:
        for label,query in [
            ('missing-collation',fixture.query.replace(' COLLATE pg_catalog."C"','')),
            ('bad-index',fixture.query.replace('[4]','[0]')),
            ('dynamic-index',fixture.query.replace('[4]','[b.id]')),
        ]:
            fixture.rejected(command,label,query)
        assert fixture.sql(f"SELECT to_regclass('{fixture.name}.pgderive_worker_registration') IS NULL").strip()=='t'
        assert fixture.sql(f"SELECT count(*) FROM pg_replication_slots WHERE slot_name LIKE '{fixture.name}%' ").strip()=='0'
        if mode=='text':
            proxy_log = (output/'sql-fault.jsonl').open('w')
            proxy = subprocess.Popen([sys.executable,'-u','scripts/pg_commit_proxy.py','--mode','after','--once','--arm-query','pgderive_progress SET commit_lsn'],stdout=proxy_log,stderr=proxy_log)
            deadline = time.monotonic()+10
            while time.monotonic()<deadline:
                lines = (output/'sql-fault.jsonl').read_text().splitlines()
                if lines:
                    fixture.environment['PGDERIVE__POSTGRES__PORT'] = str(json.loads(lines[0])['port'])
                    break
                time.sleep(.05)
            else:
                raise RuntimeError('q22 COMMIT fault proxy did not start')
        worker = fixture.start(command,'first')
        ready = worker.event('ready')
        fixture.verify()
        assert int(fixture.sql(f'SELECT max(weight) FROM {fixture.bag_relation()}'))>1
        url = literal(fixture.delimiter.join(['https:','','new','a','b','c']))
        changes = [
            f"INSERT INTO {fixture.name}.auction(id,category,enabled) VALUES(400,'new',true); INSERT INTO {fixture.name}.bid(id,auction,price,bidder,channel,url) VALUES(400,400,9,5,'é🙂',{url}),(401,400,NULL,NULL,NULL,NULL)",
            f"UPDATE {fixture.name}.bid SET url={literal(fixture.delimiter.join(['','','','','','']))},price=9 WHERE id=301; UPDATE {fixture.name}.auction SET enabled=NULL WHERE id=400",
            f"DELETE FROM {fixture.name}.bid WHERE id=304; UPDATE {fixture.name}.bid SET url=NULL WHERE id=310; UPDATE {fixture.name}.auction SET enabled=true WHERE id=400",
            f"UPDATE {fixture.name}.bid SET url='',channel=NULL,bidder=NULL WHERE id=400; DELETE FROM {fixture.name}.auction WHERE id=400",
            f"DELETE FROM {fixture.name}.bid WHERE id IN(300,301,302,303,305); UPDATE {fixture.name}.auction SET group_id=NULL WHERE id=2",
            f"UPDATE {fixture.name}.auction SET group_id=3 WHERE id=2",
        ]
        fixture.sql(f'BEGIN; DELETE FROM {fixture.name}.bid; ROLLBACK')
        for index,change in enumerate(changes,1):
            fixture.sql('BEGIN;'+change+';COMMIT')
            worker.event('ready' if mode=='text' and index==1 else 'published',minimum_time=ready['time']+index)
            fixture.verify()
        worker.abort()
        if mode=='text':
            faults = [json.loads(line) for line in (output/'sql-fault.jsonl').read_text().splitlines()[1:]]
            assert len(faults)==1 and faults[0]['server_committed'] is True,faults
        prior = fixture.sql(f'SELECT row_to_json(p) FROM {fixture.name}.pgderive_progress p')
        fixture.rejected(command,'changed-index',fixture.query.replace('[4]','[7]'))
        fixture.rejected(command,'changed-delimiter',fixture.query.replace(literal(fixture.delimiter) if mode=='delimiter' else "'/'", "'|'"))
        assert fixture.sql(f'SELECT row_to_json(p) FROM {fixture.name}.pgderive_progress p')==prior
        fixture.query = fixture.query.replace('b.','"B".').replace('bid b','bid AS "B"')
        resumed = fixture.start(command,'resumed',maximum=1)
        reopened = resumed.event('ready')
        assert reopened['slot']==ready['slot'] and reopened['time']==ready['time']+len(changes)
        fixture.verify()
        fixture.sql(f'DELETE FROM {fixture.name}.bid')
        resumed.event('published')
        resumed.finish()
        fixture.verify()
        (output/'result.json').write_text(json.dumps({'query':'q22','mode':mode,'postgres':'exact weighted bag','memory':'independent substring split and array positions','missing':'NULL','empty':'empty string preserved','null_source':'NULL','delimiter':'parameterized quote/backslash/Unicode' if mode=='delimiter' else '/','restart':'cold same slot','simultaneous':'both source changes','terminal':'PG publication with raw retained bag','lost_commit':'witnessed committed response loss' if mode=='text' else 'not injected'})+'\n')
        fixture.cleanup()
    except BaseException:
        fixture.preserve()
        raise
    finally:
        if proxy:
            proxy.terminate()
            proxy.wait(timeout=10)
        if proxy_log:
            proxy_log.close()


if __name__=='__main__':
    output, command = harness_arguments()
    for mode in ('text','varchar','delimiter','join'):
        directory = output/mode
        directory.mkdir()
        qualify(directory,command,mode)
