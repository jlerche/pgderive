#!/usr/bin/env python3
"""Named full-payload category join against PostgreSQL and raw nested-loop bag."""
import collections
import json
import pathlib
import sys
from sql_compiler_harness import SqlFixture

SQL = pathlib.Path(__file__).resolve().parents[1]/'queries/nexmark/q20.sql'
LABELS = ['auction','bidder','price','channel','url','bid_date_time','bid_extra','item_name','description','initial_bid','reserve','auction_date_time','expires','seller','category','auction_extra']

class Q20Fixture(SqlFixture):
    def __init__(self,output,mode):
        super().__init__(output,False)
        self.pg.insert(1,'-q')
        self.mode = mode
        temporal = 'timestamptz' if mode=='instant' else 'timestamp'
        category = {'int2':'smallint','int4':'integer'}.get(mode,'bigint')
        text = 'varchar(256)' if mode=='int2' else 'text'
        self.sql(f'''ALTER TABLE {self.name}.auction ALTER COLUMN category TYPE {category} USING CASE WHEN id=1 THEN 10 WHEN id=2 THEN NULL ELSE 11 END;
          ALTER TABLE {self.name}.auction ADD COLUMN item_name {text},ADD COLUMN description text,
          ADD COLUMN initial_bid bigint,ADD COLUMN reserve bigint,ADD COLUMN date_time {temporal},
          ADD COLUMN expires {temporal},ADD COLUMN seller bigint,ADD COLUMN extra text;
          ALTER TABLE {self.name}.bid ADD COLUMN bidder bigint,ADD COLUMN channel {text},
          ADD COLUMN url text,ADD COLUMN date_time {temporal},ADD COLUMN extra text;
          UPDATE {self.name}.auction SET item_name='é🙂',description='',initial_bid=-9223372036854775808,reserve=9223372036854775807,date_time='2000-01-01',expires='infinity',seller=1,extra='auction';
          UPDATE {self.name}.bid SET bidder=5,channel='web',url='https://界/é/🙂',date_time='2000-01-01 12:00:00.123456',extra='bid';
          INSERT INTO {self.name}.auction(id,category,item_name,description,initial_bid,reserve,date_time,expires,seller,extra) VALUES
            (200,10,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL),(201,10,'infinite','',0,0,'-infinity','infinity',0,''),(202,11,'other','',0,0,NULL,NULL,NULL,NULL);
          INSERT INTO {self.name}.bid(id,auction,price,bidder,channel,url,date_time,extra) VALUES
            (300,1,9,5,'same','',NULL,'same'),(301,1,9,5,'same','',NULL,'same'),
            (302,200,NULL,NULL,NULL,NULL,NULL,NULL),(303,201,9223372036854775807,0,'','', 'infinity',''),
            (304,201,-9223372036854775808,0,NULL,NULL,'-infinity',NULL),
            (305,NULL,9,5,'null key',NULL,NULL,NULL),(306,999,9,5,'no match',NULL,NULL,NULL),
            (307,202,9,5,'excluded',NULL,NULL,NULL)''')
        self.query = SQL.read_text().replace('nexmark.bid',f'{self.name}.bid').replace('nexmark.auction',f'{self.name}.auction').strip().removesuffix(';')

    def verify(self):
        prefix = "SET DateStyle='ISO,MDY'; SET TimeZone='UTC'; "
        expected = f"SELECT jsonb_build_array(NULL,jsonb_build_array({','.join(LABELS)})) tuple,count(*)::bigint weight FROM({self.query}) q GROUP BY 1"
        difference = self.sql(prefix+f'''WITH expected AS ({expected}),actual AS
            (SELECT tuple,weight FROM {self.name}.groups),difference AS
            ((SELECT * FROM expected EXCEPT ALL SELECT * FROM actual) UNION ALL
            (SELECT * FROM actual EXCEPT ALL SELECT * FROM expected)) SELECT count(*) FROM difference''')
        assert difference.strip()=='0','q20 differs from PostgreSQL'
        auctions = json.loads(self.sql(prefix+f"SELECT COALESCE(json_agg(a),'[]') FROM {self.name}.auction a"))
        bids = json.loads(self.sql(prefix+f"SELECT COALESCE(json_agg(b),'[]') FROM {self.name}.bid b"))
        memory = collections.Counter()
        for b in bids:
            for a in auctions:
                if b['auction'] is None or b['auction']!=a['id'] or a['category']!=10:
                    continue
                memory[(b['auction'],b['bidder'],b['price'],b['channel'],b['url'],b['date_time'],b['extra'],a['item_name'],a['description'],a['initial_bid'],a['reserve'],a['date_time'],a['expires'],a['seller'],a['category'],a['extra'])] += 1
        actual = json.loads(self.sql(f"SELECT COALESCE(json_agg(g),'[]') FROM {self.name}.groups g"))
        assert {tuple(row['tuple'][1]):row['weight'] for row in actual}==dict(memory),'q20 differs from independent full-payload nested-loop oracle'
        assert self.sql(f'''SELECT count(*) FROM pg_replication_slots s JOIN {self.name}.pgderive_worker_registration r ON r.slot_name=s.slot_name CROSS JOIN {self.name}.pgderive_progress p WHERE s.confirmed_flush_lsn>p.end_lsn''').strip()=='0'


def qualify(output,command,mode):
    fixture = Q20Fixture(output,mode)
    try:
        fixture.rejected(command,'ambiguous-column',fixture.query.replace('b.date_time AS bid_date_time','date_time AS bid_date_time'))
        fixture.rejected(command,'unsupported-left-join',fixture.query.replace('INNER JOIN','LEFT JOIN'))
        assert fixture.sql(f"SELECT to_regclass('{fixture.name}.pgderive_worker_registration') IS NULL").strip()=='t'
        assert fixture.sql(f"SELECT count(*) FROM pg_replication_slots WHERE slot_name LIKE '{fixture.name}%' ").strip()=='0'
        worker = fixture.start(command,'first')
        ready = worker.event('ready')
        fixture.verify()
        assert int(fixture.sql(f'SELECT max(weight) FROM {fixture.name}.groups'))>1
        changes = [
            f"INSERT INTO {fixture.name}.auction(id,category,item_name,extra) VALUES(400,10,'new/new','auction'); INSERT INTO {fixture.name}.bid(id,auction,price,bidder,extra) VALUES(400,400,9,5,'new/new'),(401,400,NULL,NULL,NULL)",
            f"UPDATE {fixture.name}.auction SET category=NULL,item_name='updated' WHERE id=400; UPDATE {fixture.name}.bid SET price=-9,url='https://updated/é' WHERE id=400",
            f"UPDATE {fixture.name}.auction SET category=10,reserve=NULL WHERE id=400; UPDATE {fixture.name}.bid SET auction=400,channel=NULL WHERE id=300",
            f"UPDATE {fixture.name}.auction SET id=450,description='move' WHERE id=400; UPDATE {fixture.name}.bid SET auction=450 WHERE auction=400",
            f"DELETE FROM {fixture.name}.auction WHERE id=450; DELETE FROM {fixture.name}.bid WHERE id=401",
            f"INSERT INTO {fixture.name}.auction(id,category,item_name) VALUES(450,10,'restored'); UPDATE {fixture.name}.bid SET price=NULL,extra='changed' WHERE id=400; UPDATE {fixture.name}.auction SET category=10 WHERE id=202",
            f"SET DateStyle='German,DMY'; SET TimeZone='America/New_York'; UPDATE {fixture.name}.auction SET date_time='03.11.2024 01:30:00-04',expires='03.11.2024 01:30:00-05' WHERE id=450; UPDATE {fixture.name}.bid SET date_time='03.11.2024 01:30:00.123456-04' WHERE id=400",
            f"UPDATE {fixture.name}.auction SET group_id=NULL WHERE id=3",
        ]
        fixture.sql(f'BEGIN; DELETE FROM {fixture.name}.bid; DELETE FROM {fixture.name}.auction; ROLLBACK')
        for index,change in enumerate(changes,1):
            fixture.sql('BEGIN;'+change+';COMMIT')
            worker.event('published',minimum_time=ready['time']+index)
            fixture.verify()
        worker.abort()
        prior = fixture.sql(f'SELECT row_to_json(p) FROM {fixture.name}.pgderive_progress p')
        fixture.rejected(command,'changed-filter',fixture.query.replace('a.category=10','a.category=11'))
        fixture.rejected(command,'changed-projection',fixture.query.replace('b.url,','b.channel AS url,'))
        assert fixture.sql(f'SELECT row_to_json(p) FROM {fixture.name}.pgderive_progress p')==prior
        fixture.query = fixture.query.replace('b.','"B".').replace('a.','"A".').replace('bid b','bid AS "B"').replace('auction a','auction AS "A"')
        resumed = fixture.start(command,'resumed',maximum=1)
        reopened = resumed.event('ready')
        assert reopened['slot']==ready['slot'] and reopened['time']==ready['time']+len(changes)
        fixture.verify()
        fixture.sql(f'BEGIN; DELETE FROM {fixture.name}.bid; DELETE FROM {fixture.name}.auction; COMMIT')
        resumed.event('published')
        resumed.finish()
        fixture.verify()
        (output/'result.json').write_text(json.dumps({'query':'q20','mode':mode,'postgres':'exact full-payload weighted bag','memory':'independent raw nested-loop category join','restart':'cold same slot','changes':'both-input insert/update/delete and coordinated key moves','nulls':'join key/category/payload/temporal','identity':'changed filter/projection rejected; quoted aliases normalize','bag':'projected collisions retain distinct source occurrences'})+'\n')
        fixture.cleanup()
    except BaseException:
        fixture.preserve()
        raise

if __name__=='__main__':
    output = pathlib.Path(sys.argv[1])
    command = sys.argv[2:]
    if command and command[0]=='--':
        command = command[1:]
    for mode in ('local','instant','int2','int4'):
        directory = output/mode
        directory.mkdir()
        qualify(directory,command,mode)
