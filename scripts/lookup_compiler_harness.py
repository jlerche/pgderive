#!/usr/bin/env python3
"""PostgreSQL LATERAL LIMIT-one lookup against independent native source bags."""
import collections
from decimal import Decimal
import json
import pathlib
import re
import sys
from native_compiler_harness import NativeFixture, encoded, instant


class LookupFixture(NativeFixture):
    def __init__(self, output, mode):
        super().__init__(output, 'projection')
        self.mode = mode
        self.timezone = mode == 'instant'
        self.strict = mode == 'strict'
        self.filtered = mode == 'filtered'
        self.field = 'event_time' if self.timezone else 'event_at'
        self.sql(f'''ALTER TABLE {self.name}.auction ADD COLUMN lookup_key integer,
            ADD COLUMN event_at timestamp,ADD COLUMN event_time timestamptz;
            UPDATE {self.name}.auction a SET lookup_key=CASE WHEN a.id%4=0 THEN NULL ELSE 1 END,
                event_at=b.event_at,event_time=b.event_time FROM {self.name}.bid b WHERE b.id=a.id;
            UPDATE {self.name}.bid SET auction=1 WHERE id<=5;
            ALTER TABLE {self.name}.bid ADD COLUMN enabled boolean, ADD COLUMN token uuid;
            UPDATE {self.name}.bid SET enabled=CASE WHEN id%3=0 THEN NULL ELSE id%2=0 END,
                token=CASE WHEN id%4=0 THEN NULL ELSE 'a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11'::uuid END;''')
        equality = 'r.auction=l.lookup_key'
        if mode == 'composite':
            equality += ' AND r.enabled=l.enabled AND r.token=l.token'
        operator = '<' if self.strict else '<='
        self.query = f'''SELECT l.category AS category,p.price AS price,p.amount AS amount,p.chosen AS chosen
            FROM {self.name}.auction l LEFT JOIN LATERAL(
            SELECT r.price,r.amount,r.{self.field} AS chosen FROM {self.name}.bid r
            WHERE {equality} AND r.{self.field}{operator}l.{self.field}
            ORDER BY r.{self.field} DESC NULLS FIRST,r.id DESC LIMIT 1) p ON true'''
        if self.filtered:
            self.query += ' WHERE p.price IS NULL OR p.price>0'

    def verify(self):
        prefix = "SET DateStyle='ISO,MDY'; SET TimeZone='UTC'; "
        expected = f'SELECT jsonb_build_array(NULL,jsonb_build_array(category,price,amount,chosen)) tuple,count(*)::bigint weight FROM({self.query}) q GROUP BY 1'
        difference = self.sql(prefix+f'''WITH expected AS({expected}),actual AS
            (SELECT tuple,weight FROM {self.name}.groups),difference AS
            ((SELECT * FROM expected EXCEPT ALL SELECT * FROM actual) UNION ALL
            (SELECT * FROM actual EXCEPT ALL SELECT * FROM expected)) SELECT count(*) FROM difference''')
        assert difference.strip() == '0', 'lookup differs from PostgreSQL'
        left = json.loads(self.sql(prefix+f"SELECT COALESCE(json_agg(x),'[]') FROM(SELECT id,category,lookup_key,enabled,token,{self.field}::text AS time FROM {self.name}.auction) x"))
        right = json.loads(self.sql(prefix+f"SELECT COALESCE(json_agg(x),'[]') FROM(SELECT id,auction,price,enabled,token,{self.field}::text AS time,amount::text AS amount FROM {self.name}.bid) x"))
        memory = collections.Counter()
        for probe in left:
            candidates = []
            for candidate in right:
                if probe['lookup_key'] is None or probe['lookup_key'] != candidate['auction']:
                    continue
                if self.mode == 'composite' and any(probe[key] is None or candidate[key] is None or probe[key] != candidate[key] for key in ('enabled','token')):
                    continue
                if probe['time'] is None or candidate['time'] is None:
                    continue
                earlier = instant(candidate['time']) < instant(probe['time'])
                equal = instant(candidate['time']) == instant(probe['time'])
                if earlier or (not self.strict and equal):
                    candidates.append(candidate)
            selected = max(candidates,key=lambda row:(instant(row['time']),row['id'])) if candidates else None
            price = selected['price'] if selected else None
            if self.filtered and price is not None and price<=0:
                continue
            amount = selected['amount'] if selected else None
            if amount is not None and amount not in ('NaN','Infinity','-Infinity'):
                amount = Decimal(amount)
            memory[(probe['category'],price,amount,encoded(selected['time'],self.timezone) if selected else None)] += 1
        actual = json.loads(self.sql(f"SELECT COALESCE(json_agg(g),'[]') FROM {self.name}.groups g"),parse_float=Decimal)
        assert {tuple(row['tuple'][1]):row['weight'] for row in actual} == dict(memory), 'lookup differs from independent memory oracle'
        assert self.sql(f'''SELECT count(*) FROM pg_replication_slots s JOIN
            {self.name}.pgderive_worker_registration r ON r.slot_name=s.slot_name JOIN
            {self.name}.pgderive_progress p ON true WHERE s.confirmed_flush_lsn>p.end_lsn''').strip() == '0'


def quoted_left(query):
    return re.sub(r'\bl\.', '"L".', query).replace('auction l ', 'auction AS "L" ')


def qualify(output, command, mode):
    fixture = LookupFixture(output,mode)
    name = fixture.name
    try:
        fixture.rejected(command,'limit',fixture.query.replace('LIMIT 1','LIMIT 2'))
        fixture.rejected(command,'ties',fixture.query.replace(',r.id DESC',''))
        fixture.rejected(command,'hidden',fixture.query.replace('p.price','r.price'))
        assert fixture.sql(f"SELECT to_regclass('{name}.pgderive_worker_registration') IS NULL").strip() == 't'
        worker = fixture.start(command,'first')
        ready = worker.event('ready')
        fixture.verify()
        changes = [
            f"INSERT INTO {name}.bid(id,auction,price,event_at,event_time,amount) VALUES(100,1,NULL,'1999-12-31 23:59:59.999999','1999-12-31 15:59:59.999999-08','NaN'); UPDATE {name}.auction SET event_at='2000-01-01',event_time='2000-01-01+00' WHERE id=2",
            f"UPDATE {name}.bid SET event_at='1999-12-31 23:59:59.999999',event_time='1999-12-31 15:59:59.999999-08',price=-3 WHERE id=3",
            f"DELETE FROM {name}.bid WHERE id=1; UPDATE {name}.auction SET lookup_key=1 WHERE id=4",
            f"INSERT INTO {name}.bid(id,auction,price,event_at,event_time,amount) VALUES(101,2,9,'0001-02-29 00:00:00 BC','0001-02-29 00:00:00+00 BC',1.2300); UPDATE {name}.auction SET lookup_key=2,event_at='0001-02-29 00:00:00 BC',event_time='0001-02-29 00:00:00+00 BC' WHERE id=5",
            f"UPDATE {name}.bid SET event_at=NULL,event_time=NULL,auction=NULL WHERE id=100; UPDATE {name}.auction SET lookup_key=NULL WHERE id=6",
            f"INSERT INTO {name}.bid(id,auction,price,event_at,event_time,amount) VALUES(102,1,-7,'2000-01-01','2000-01-01+00',123456789012345678901234567890.12345678901234567890); UPDATE {name}.auction SET event_at='2000-01-01',event_time='2000-01-01+00',lookup_key=1 WHERE id=1",
            f"DELETE FROM {name}.bid",
            f"INSERT INTO {name}.bid(id,auction,price,event_at,event_time,amount,enabled,token) VALUES(103,1,8,'2024-03-10 01:59:59.999999','2024-03-10 01:59:59.999999-08',0.000000000000000000000000000001,true,'a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11'); UPDATE {name}.auction SET lookup_key=1,event_at='2024-03-10 03:00:00',event_time='2024-03-10 03:00:00-07' WHERE id=2",
        ]
        for index, change in enumerate(changes,1):
            fixture.sql("SET DateStyle='German,DMY'; SET TimeZone='America/New_York'; BEGIN;"+change+';COMMIT')
            worker.event('published',minimum_time=ready['time']+index)
            fixture.verify()
        fixture.sql(f'BEGIN; DELETE FROM {name}.auction; ROLLBACK')
        fixture.verify()
        worker.abort()
        progress = fixture.sql(f'SELECT row_to_json(p) FROM {name}.pgderive_progress p')
        operator = '<' if fixture.strict else '<='
        fixture.rejected(command,'changed',fixture.query.replace(f'r.{fixture.field}{operator}l.',f'r.{fixture.field}{"<=" if fixture.strict else "<"}l.'))
        assert fixture.sql(f'SELECT row_to_json(p) FROM {name}.pgderive_progress p') == progress
        fixture.query = quoted_left(fixture.query)
        resumed = fixture.start(command,'resumed',maximum=1)
        reopened = resumed.event('ready')
        assert reopened['slot']==ready['slot'] and reopened['time']==ready['time']+len(changes)
        fixture.verify()
        fixture.sql(f"BEGIN; UPDATE {name}.auction SET category='collision'; UPDATE {name}.bid SET price=NULL,amount='Infinity'; COMMIT")
        resumed.event('published')
        resumed.finish()
        fixture.verify()
        (output/'result.json').write_text(json.dumps({'sql_oracle':'exact bag','memory_oracle':'independent native correlated LIMIT-one selection','simultaneous_inputs':True,'late_right_changes':True,'nulls':True,'restart':'cold','mode':mode})+'\n')
        fixture.cleanup()
    except BaseException:
        fixture.preserve()
        raise


if __name__=='__main__':
    output = pathlib.Path(sys.argv[1])
    command = sys.argv[2:]
    if command and command[0]=='--':
        command = command[1:]
    for mode in ('local','instant','strict','filtered','composite'):
        directory = output/mode
        directory.mkdir()
        qualify(directory,command,mode)
