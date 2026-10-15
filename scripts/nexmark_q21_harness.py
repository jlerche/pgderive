#!/usr/bin/env python3
"""Explicit C text q21: native MV and independent ASCII/channel scanner bags."""
from worker_harness import harness_arguments
import collections
import json
import pathlib
from sql_compiler_harness import SqlFixture
from native_mv_oracle import expected as native_expected

SQL = pathlib.Path(__file__).resolve().parents[1]/'queries/nexmark/q21.sql'
LABELS = ['auction','bidder','price','channel','channel_id']
BRANDS = {'apple':'0','google':'1','facebook':'2','baidu':'3'}
ASCII_LOWER = str.maketrans('ABCDEFGHIJKLMNOPQRSTUVWXYZ','abcdefghijklmnopqrstuvwxyz')

def channel_id(channel,url):
    brand = None if channel is None else BRANDS.get(channel.translate(ASCII_LOWER))
    if brand is not None:
        return brand
    if url is None:
        return None
    # Independent delimiter scanner, not another regular-expression engine.
    for component in url.split('&'):
        if component.startswith('channel_id='):
            return component[len('channel_id='):]
    return None

class Q21Fixture(SqlFixture):
    def __init__(self,output,mode):
        super().__init__(output,False)
        self.pg.insert(1,'-q')
        collation = 'pg_catalog."default"'
        if mode=='c': collation = 'pg_catalog."C"'
        if mode=='icu':
            self.sql(f"CREATE COLLATION {self.name}.unicode(provider=icu,locale='und',deterministic=true)")
            collation = f'{self.name}.unicode'
        text = 'text' if mode=='default' else 'varchar(256)'
        self.sql(f'''ALTER TABLE {self.name}.bid ADD COLUMN bidder bigint,
          ADD COLUMN channel {text} COLLATE {collation},ADD COLUMN url {text} COLLATE {collation};
          UPDATE {self.name}.bid SET bidder=9,channel='Apple',url='ignored';
          INSERT INTO {self.name}.bid(id,auction,price,bidder,channel,url) VALUES
          (300,1,7,9,'APPLE','channel_id=anything'),(301,1,7,9,'APPLE','unrelated'),
          (302,2,NULL,NULL,'Google',NULL),(303,2,7,9,'facebook','channel_id=wrong'),
          (304,2,7,9,'BAIDU',''),(305,2,7,9,NULL,'channel_id=empty&other=x'),
          (306,2,7,9,NULL,'a=x&channel_id=&channel_id=later'),
          (307,2,7,9,'other','channel_id=first&channel_id=second'),
          (308,2,7,9,'other','prefixchannel_id=wrong&channel_id=é🙂'),
          (309,2,7,9,'AppleÉİ🙂',NULL),(310,2,7,9,NULL,NULL),
          (311,2,7,9,'other','?channel_id=wrong'),(312,2,7,9,'other','CHANNEL_ID=wrong'),
          (313,2,7,9,'other','channel_id=x=y'),(314,2,7,9,'other','&channel_id=one&'),
          (315,2,7,9,'other','channel_id=first&ignored'),
          (316,2,7,9,'other','channel_id=first&different'),
          (317,2,7,9,'other','channel_id=line\nnext'),
          (318,2,7,9,'APPLE ',NULL),(319,2,7,9,'apple',NULL)''')
        self.query = SQL.read_text().replace('nexmark.',self.name+'.').strip().removesuffix(';')

    def verify(self):
        expected = native_expected(self,LABELS)
        difference = self.bag_difference(expected)
        assert difference.strip()=='0','q21 differs from exact native MV'
        source = json.loads(self.sql(f"SELECT COALESCE(json_agg(b),'[]') FROM {self.name}.bid b"))
        memory = collections.Counter()
        for row in source:
            selected = channel_id(row['channel'],row['url'])
            if selected is not None:
                memory[(row['auction'],row['bidder'],row['price'],row['channel'],selected)] += 1
        actual = json.loads(self.sql(f"SELECT COALESCE(json_agg(g),'[]') FROM {self.name}.groups g"))
        assert {tuple(row['tuple'][1]):row['weight'] for row in actual}==dict(memory),'q21 differs from independent ASCII and delimiter scanner oracle'
        assert self.acknowledgement_ahead().strip()=='0'


def qualify(output,command,mode):
    fixture = Q21Fixture(output,mode)
    name = fixture.name
    with fixture.managed():
        for label,query in [
            ('implicit-collation',fixture.query.replace(' COLLATE pg_catalog."C"','')),
            ('other-collation',fixture.query.replace('pg_catalog."C"','pg_catalog."default"')),
            ('regex-flags',fixture.query.replace("'(&|^)channel_id=([^&]*)'","'(&|^)channel_id=([^&]*)','i'")),
            ('regex-srf',fixture.query.replace('regexp_match(','regexp_matches(')),
            ('index-zero',fixture.query.replace('[2]','[0]')),
            ('dynamic-pattern',fixture.query.replace("'(&|^)channel_id=([^&]*)'",'channel')),
            ('downstream',f'SELECT x.auction FROM ({fixture.query}) x WHERE x.channel_id IS NOT NULL'),
        ]:
            fixture.rejected(command,label,query)
        assert fixture.sql(f"SELECT to_regclass('{name}.pgderive_worker_registration') IS NULL").strip()=='t'
        assert fixture.sql(f"SELECT count(*) FROM pg_replication_slots WHERE slot_name LIKE '{name}%' ").strip()=='0'
        worker = fixture.start(command,'first')
        ready = worker.event('ready')
        fixture.verify()
        assert fixture.sql(f'SELECT max(weight)>1 FROM {name}.groups').strip()=='t'
        changes = [
            f"DELETE FROM {name}.bid WHERE id=300; UPDATE {name}.bid SET url='channel_id=changed' WHERE id=315",
            f"UPDATE {name}.bid SET channel='other',url='channel_id=' WHERE id=301; UPDATE {name}.bid SET channel='APPLE' WHERE id=310",
            f"UPDATE {name}.bid SET url=NULL WHERE id=306; UPDATE {name}.bid SET channel=NULL,url='a=x&channel_id=é🙂' WHERE id=304",
            f"UPDATE {name}.bid SET id=501,url='ignored' WHERE id=310; INSERT INTO {name}.bid(id,auction,price,bidder,channel,url) VALUES(400,2,7,9,'other','channel_id=first&new')",
            f"UPDATE {name}.auction SET group_id=NULL WHERE id=2",
        ]
        fixture.sql(f'BEGIN; DELETE FROM {name}.bid; ROLLBACK')
        for index,change in enumerate(changes,1):
            fixture.sql('BEGIN;'+change+';COMMIT')
            worker.event('published',minimum_time=ready['time']+index)
            fixture.verify()
        worker.abort()
        prior = fixture.sql(f'SELECT row_to_json(p) FROM {name}.pgderive_progress p')
        fixture.rejected(command,'changed-label',fixture.query.replace("THEN '0'","THEN '4'"))
        fixture.rejected(command,'changed-capture',fixture.query.replace('[2]','[1]'))
        assert fixture.sql(f'SELECT row_to_json(p) FROM {name}.pgderive_progress p')==prior
        fixture.query = fixture.query.replace('SELECT auction,bidder','SELECT b.auction,b.bidder').replace(f'FROM {name}.bid',f'FROM {name}.bid b')
        fixture.query = fixture.query.replace("lower(channel COLLATE pg_catalog.\"C\") IN ('apple','google','facebook','baidu')", "(lower(channel COLLATE pg_catalog.\"C\")='apple' OR lower(channel COLLATE pg_catalog.\"C\")='google' OR lower(channel COLLATE pg_catalog.\"C\")='facebook' OR lower(channel COLLATE pg_catalog.\"C\")='baidu')")
        resumed = fixture.start(command,'resumed',maximum=1)
        reopened = resumed.event('ready')
        assert reopened['slot']==ready['slot'] and reopened['time']==ready['time']+len(changes)
        fixture.verify()
        fixture.sql(f'DELETE FROM {name}.bid')
        resumed.event('published')
        resumed.finish()
        fixture.verify()
        (output/'result.json').write_text(json.dumps({'query':'q21','mode':mode,'postgres':'exact native materialized-view weighted bag','memory':'independent ASCII-C lower and delimiter scanner','restart':'cold same slot','hidden':'URL omitted from visible tuple; collisions consolidated','terminal':'attested C lower texteq regexp_match and array capture'})+'\n')

if __name__=='__main__':
    output, command = harness_arguments()
    for mode in ('default','c','icu'):
        directory = output/mode
        directory.mkdir()
        qualify(directory,command,mode)
