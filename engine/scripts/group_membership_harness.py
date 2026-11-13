#!/usr/bin/env python3
"""Native GROUP BY without aggregates: distinct membership, projected bags."""
from worker_harness import run_modes, History
import collections
import json
from sql_compiler_harness import SqlFixture
from native_mv_oracle import expected as native_expected
from nexmark_temporal_harness import bucket, encode


class MembershipFixture(SqlFixture):
    def __init__(self,output,mode):
        super().__init__(output,False)
        self.pg.insert(1,'-q')
        self.instant = mode!='local'
        temporal = 'timestamptz' if self.instant else 'timestamp'
        channel = 'varchar(128) COLLATE pg_catalog."C"' if mode=='c' else 'text COLLATE pg_catalog."und-x-icu"' if mode=='icu' else 'text'
        self.sql(f'''ALTER TABLE {self.name}.bid ADD COLUMN channel {channel},ADD COLUMN date_time {temporal};
          UPDATE {self.name}.bid SET channel=CASE id%6 WHEN 0 THEN NULL WHEN 1 THEN 'Web' WHEN 2 THEN 'web' WHEN 3 THEN 'web ' WHEN 4 THEN 'é' ELSE 'é' END,date_time='2000-01-01'::{temporal}+INTERVAL '1 second'*(id%31);
          INSERT INTO {self.name}.bid(id,auction,price,channel,date_time) VALUES
          (300,900,1,'same','2000-01-01'),(301,900,NULL,'same','2000-01-01'),
          (302,900,1,'same','2000-01-01 00:00:10'),(303,NULL,NULL,NULL,NULL),
          (304,NULL,1,NULL,NULL),(305,901,-1,'excluded','2000-01-01'),
          (306,902,1,'infinity','infinity'),(307,902,1,'infinity','-infinity')''')
        origin = "TIMESTAMPTZ '2000-01-01 00:00:00+00'" if self.instant else "TIMESTAMP '2000-01-01 00:00:00'"
        self.query = f'''SELECT b.auction,b.channel FROM {self.name}.bid b
          WHERE b.price>=0 OR b.price IS NULL
          GROUP BY b.auction,b.channel,date_bin(INTERVAL '10 seconds',b.date_time,{origin})'''
        self.labels = ['auction','channel']

    def verify(self):
        prefix = "SET DateStyle='ISO,MDY'; SET TimeZone='UTC'; "
        expected = native_expected(self,self.labels)
        self.verify_sql(expected, context=prefix)
        source = json.loads(self.sql(prefix+f"SELECT COALESCE(json_agg(b),'[]') FROM {self.name}.bid b"))
        memberships = {(row['auction'],row['channel'],encode(bucket(row['date_time'],10),self.instant)) for row in source if row['price'] is None or row['price']>=0}
        memory = collections.Counter((auction,channel) for auction,channel,_ in memberships)
        self.verify_memory(dict(memory))


def qualify(output,command,mode):
    fixture = MembershipFixture(output,mode)
    with fixture.managed():
        fixture.rejected(command,'ungrouped-column',fixture.query.replace('SELECT b.auction,b.channel','SELECT b.id,b.channel'))
        assert fixture.sql(f"SELECT to_regclass('{fixture.name}.pgderive_worker_registration') IS NULL").strip()=='t'
        assert fixture.sql(f"SELECT count(*) FROM pg_replication_slots WHERE slot_name LIKE '{fixture.name}%' ").strip()=='0'
        changes = [
            f"DELETE FROM {fixture.name}.bid WHERE id=300; UPDATE {fixture.name}.auction SET enabled=NULL WHERE id=1",
            f"DELETE FROM {fixture.name}.bid WHERE id=301; UPDATE {fixture.name}.bid SET price=-1 WHERE id=302",
            f"INSERT INTO {fixture.name}.bid(id,auction,price,channel,date_time) VALUES(308,900,1,'same','2000-01-01'),(309,900,1,'same','2000-01-01'); UPDATE {fixture.name}.bid SET price=1 WHERE id=305",
            f"UPDATE {fixture.name}.bid SET id=330,auction=NULL,channel=NULL,date_time=NULL WHERE id=308; UPDATE {fixture.name}.bid SET channel='Web',date_time='2000-01-01 00:00:09.999999' WHERE id=309",
            f"SET DateStyle='German,DMY'; SET TimeZone='America/New_York'; UPDATE {fixture.name}.bid SET date_time='03.11.2024 01:30:09.999999-04',channel='é' WHERE id=306; UPDATE {fixture.name}.bid SET date_time='03.11.2024 01:30:10-05',channel='é' WHERE id=307",
            f"UPDATE {fixture.name}.auction SET group_id=3 WHERE id=2",
        ]
        def initial():
            fixture.sql(f'BEGIN; DELETE FROM {fixture.name}.bid; ROLLBACK')
        fixture.run_history(command, History(
            changes=changes,
            final=f'DELETE FROM {fixture.name}.bid',
            normalized=fixture.query.replace('b.', '"B".').replace('bid b', 'bid AS "B"'),
            rejected=[('changed-key', fixture.query.replace('10 seconds', '11 seconds'))],
        ), initial=initial)
        (output/'result.json').write_text(json.dumps({'mode':mode,'postgres':'exact native MV bag','memory':'independent source membership set projected to a weighted bag','duplicates':'partial delete retains membership; last delete removes it; hidden bucket projection collisions retained','restart':'cold same slot','identity':'changed key rejected; alias normalized'})+'\n')

if __name__=='__main__':
    run_modes(('local', 'c', 'icu'), qualify)
