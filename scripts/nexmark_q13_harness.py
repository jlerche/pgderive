#!/usr/bin/env python3
"""Recorded processing-time predecessor with native signed modulo key lowering."""
from worker_harness import run_modes
import collections
import json
import pathlib
from sql_compiler_harness import SqlFixture
from native_mv_oracle import expected as native_expected
from nexmark_temporal_harness import encode, ordered

SQL = pathlib.Path(__file__).resolve().parents[1]/'queries/nexmark/q13.sql'
LABELS = ['auction','bidder','price','date_time','value']

class Q13Fixture(SqlFixture):
    def __init__(self,output,mode):
        super().__init__(output,False)
        self.pg.insert(1,'-q')
        self.instant = mode!='local'
        temporal = 'timestamptz' if self.instant else 'timestamp'
        source = 'smallint' if mode=='smallint' else 'bigint' if mode=='bigint' else 'integer'
        key = 'bigint' if mode=='bigint' else 'integer'
        value = 'varchar(128) COLLATE pg_catalog."C"' if mode=='bigint' else 'text'
        self.sql(f'''ALTER TABLE {self.name}.bid ALTER COLUMN auction TYPE {source},ADD COLUMN bidder bigint,
          ADD COLUMN date_time {temporal},ADD COLUMN processing_time {temporal};
          UPDATE {self.name}.bid SET bidder=id%3,date_time='2000-01-01'::{temporal}+INTERVAL '1 second'*(id%3),processing_time='2000-01-01 00:00:10';
          CREATE TABLE {self.name}.side_input(id integer PRIMARY KEY,key {key},date_time {temporal},value {value});
          ALTER TABLE {self.name}.side_input REPLICA IDENTITY FULL;
          INSERT INTO {self.name}.side_input VALUES
          (1,1,'2000-01-01 00:00:09','earlier'),(2,1,'2000-01-01 00:00:10',NULL),
          (3,1,'2000-01-01 00:00:10','tie'),(4,-1,'2000-01-01 00:00:10','negative'),
          (5,NULL,'2000-01-01 00:00:10','null key'),(6,2,NULL,'null time'),
          (7,3,'-infinity','past'),(8,3,'infinity','future');
          INSERT INTO {self.name}.bid(id,auction,price,bidder,date_time,processing_time) VALUES
          (300,10001,7,9,'2000-01-01','2000-01-01 00:00:10'),
          (301,10001,7,9,'2000-01-01','2000-01-01 00:00:10'),
          (302,-10001,NULL,9,NULL,'2000-01-01 00:00:10'),
          (303,NULL,0,NULL,'infinity','2000-01-01'),(304,10001,0,9,'-infinity',NULL),
          (305,3,0,9,'2000-01-01','infinity'),(306,3,0,9,'2000-01-01','-infinity');
          ALTER PUBLICATION {self.name} ADD TABLE {self.name}.side_input''')
        if mode=='bigint':
            self.sql(f'''INSERT INTO {self.name}.bid(id,auction,price,bidder,date_time,processing_time) VALUES
              (310,-9223372036854775808,-9223372036854775808,9,NULL,'2000-01-01'),
              (311,9223372036854775807,9223372036854775807,9,NULL,'2000-01-01');
              INSERT INTO {self.name}.side_input VALUES(10,-5808,'2000-01-01','minimum'),(11,5807,'2000-01-01','maximum')''')
        self.query = SQL.read_text().replace('nexmark.',self.name+'.').strip().removesuffix(';')

    def verify(self):
        prefix = "SET DateStyle='ISO,MDY'; SET TimeZone='UTC'; "
        expected = native_expected(self,LABELS)
        self.verify_sql(expected, context=prefix)
        bids = json.loads(self.sql(prefix+f"SELECT COALESCE(json_agg(b),'[]') FROM {self.name}.bid b"))
        sides = json.loads(self.sql(prefix+f"SELECT COALESCE(json_agg(s),'[]') FROM {self.name}.side_input s"))
        memory = collections.Counter()
        for bid in bids:
            key = None if bid['auction'] is None else abs(bid['auction'])%10000*(-1 if bid['auction']<0 else 1)
            candidates = [side for side in sides if key is not None and side['key']==key
              and bid['processing_time'] is not None and side['date_time'] is not None
              and ordered(side['date_time'])<=ordered(bid['processing_time'])]
            chosen = max(candidates,key=lambda side:(ordered(side['date_time']),side['id'])) if candidates else None
            memory[(bid['auction'],bid['bidder'],bid['price'],encode(bid['date_time'],self.instant),chosen['value'] if chosen else None)] += 1
        self.verify_memory(dict(memory))

    def values(self):
        return self.sql(f"SELECT COALESCE(jsonb_agg(x ORDER BY x.auction,x.value),'[]') FROM(SELECT tuple->1->0 auction,tuple->1->4 value,sum(weight)::text count FROM {self.bag_relation()} GROUP BY 1,2) x")


def qualify(output,command,mode):
    fixture = Q13Fixture(output,mode)
    name = fixture.name
    with fixture.managed():
        for label,query in [
            ('zero',fixture.query.replace('10000','0')),
            ('right-mod',fixture.query.replace('s.key=mod(b.auction,10000)','mod(s.key,10000)=b.auction')),
            ('wrong-type',fixture.query.replace('s.key=mod','s.value=mod')),
        ]:
            fixture.rejected(command,label,query)
        assert fixture.sql(f"SELECT to_regclass('{name}.pgderive_worker_registration') IS NULL").strip()=='t'
        assert fixture.sql(f"SELECT count(*) FROM pg_replication_slots WHERE slot_name LIKE '{name}%' ").strip()=='0'
        worker = fixture.start(command,'first')
        ready = worker.event('ready')
        fixture.verify()
        changes = [
            f"INSERT INTO {name}.side_input VALUES(20,1,'2000-01-01 00:00:08','simultaneous'); UPDATE {name}.bid SET processing_time='2000-01-01 00:00:08' WHERE id IN(300,301)",
            f"UPDATE {name}.bid SET date_time='2024-03-10 03:00:00-07' WHERE id IN(300,301)",
            f"UPDATE {name}.bid SET processing_time='2000-01-01 00:00:10' WHERE id=300; DELETE FROM {name}.side_input WHERE id=3",
            f"DELETE FROM {name}.side_input WHERE id=2; UPDATE {name}.bid SET auction=20001,bidder=NULL,price=NULL WHERE id=301",
            f"UPDATE {name}.side_input SET key=2,date_time='2000-01-01 00:00:10' WHERE id=1; UPDATE {name}.bid SET auction=10002,processing_time='2000-01-01 00:00:10' WHERE id=300",
            f"SET DateStyle='German,DMY'; SET TimeZone='America/New_York'; INSERT INTO {name}.side_input VALUES(21,2,'03.11.2024 01:30:09.999999-04','é'),(22,2,'03.11.2024 01:30:10-05','é'); UPDATE {name}.bid SET processing_time='03.11.2024 01:30:10-05' WHERE id=300",
            f"UPDATE {name}.side_input SET id=23,value=NULL WHERE id=22; UPDATE {name}.bid SET id=330 WHERE id=300",
            f"UPDATE {name}.side_input SET key=NULL,date_time=NULL WHERE id=23; UPDATE {name}.bid SET processing_time=NULL WHERE id=330",
            f"UPDATE {name}.auction SET category=NULL WHERE id=1",
        ]
        fixture.sql(f'BEGIN; DELETE FROM {name}.bid; DELETE FROM {name}.side_input; ROLLBACK')
        for index,change in enumerate(changes,1):
            values = fixture.values()
            fixture.sql('BEGIN;'+change+';COMMIT')
            worker.event('published',minimum_time=ready['time']+index)
            fixture.verify()
            if index==2:
                assert fixture.values()==values,'event-time-only update changed predecessor values'
        worker.abort()
        prior = fixture.sql(f'SELECT row_to_json(p) FROM {name}.pgderive_progress p')
        fixture.rejected(command,'changed-divisor',fixture.query.replace('10000','9999'))
        fixture.rejected(command,'changed-bound',fixture.query.replace('s.date_time<=b.processing_time','s.date_time<b.processing_time'))
        assert fixture.sql(f'SELECT row_to_json(p) FROM {name}.pgderive_progress p')==prior
        fixture.query = fixture.query.replace('s.key=mod(b.auction,10000) AND s.date_time<=b.processing_time','b.processing_time>=s.date_time AND b.auction%10000=s.key')
        fixture.query = fixture.query.replace('b.','"B".').replace('bid b','bid AS "B"')
        resumed = fixture.start(command,'resumed',maximum=1)
        reopened = resumed.event('ready')
        assert reopened['slot']==ready['slot'] and reopened['time']==ready['time']+len(changes)
        fixture.verify()
        fixture.sql(f'BEGIN; DELETE FROM {name}.bid; DELETE FROM {name}.side_input; COMMIT')
        resumed.event('published')
        resumed.finish()
        fixture.verify()
        (output/'result.json').write_text(json.dumps({'mode':mode,'postgres':'exact native MV bag','memory':'independent signed modulo and deterministic predecessor','transactions':'complete simultaneous sources; late/tied right updates; NULLs/infinities; modulo collisions; source/key/time/PK changes; rollback; empty input tick','processing':'recorded source field; event-only update preserves selected values','restart':'cold same slot; MOD/% and aliases normalize','identity':'changed divisor or bound rejected'})+'\n')

if __name__=='__main__':
    run_modes(('local', 'instant', 'smallint', 'bigint'), qualify)
