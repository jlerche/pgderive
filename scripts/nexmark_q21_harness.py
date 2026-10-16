from engine_rejection import qualify_unavailable
#!/usr/bin/env python3
"""Explicit C text q21: native MV and independent ASCII/channel scanner bags."""
from worker_harness import harness_arguments, History
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
        actual = json.loads(self.sql(f"SELECT COALESCE(json_agg(g),'[]') FROM {self.bag_relation()} g"))
        assert {tuple(row['tuple'][1]):row['weight'] for row in actual}==dict(memory),'q21 differs from independent ASCII and delimiter scanner oracle'
        assert self.acknowledgement_ahead().strip()=='0'


def qualify(output,command,mode):
    fixture = Q21Fixture(output,mode)
    qualify_unavailable(fixture,command)


if __name__=='__main__':
    output, command = harness_arguments()
    for mode in ('default','c','icu'):
        directory = output/mode
        directory.mkdir()
        qualify(directory,command,mode)
