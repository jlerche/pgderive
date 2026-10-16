from engine_rejection import qualify_unavailable
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
    qualify_unavailable(fixture,command)


if __name__=='__main__':
    output, command = harness_arguments()
    for mode in ('text','varchar','delimiter','join'):
        directory = output/mode
        directory.mkdir()
        qualify(directory,command,mode)
