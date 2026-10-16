#!/usr/bin/env python3
"""Exact UTC calendar expression MV/memory qualification through DST instants."""
import collections
import datetime
import decimal
import json
import pathlib
import sys
from native_mv_oracle import expected
from sql_compiler_harness import SqlFixture


class CalendarFixture(SqlFixture):
    def __init__(self, output):
        super().__init__(output,False)
        self.sql(f"ALTER TABLE {self.name}.bid ADD COLUMN date_time timestamptz; UPDATE {self.name}.bid SET date_time='2024-03-10 09:59:59.123456+00'::timestamptz+id*interval '1 hour'")
        self.labels=['year','month','day','minute','second','epoch','week','isoyear','isodow','dow','doy','quarter']
        self.query='SELECT '+','.join(f"extract({field} FROM date_time AT TIME ZONE 'UTC') AS {field}" for field in self.labels)+f' FROM {self.name}.bid'

    def verify(self):
        native=expected(self,self.labels)
        assert self.bag_difference(native).strip()=='0'
        rows=json.loads(self.sql(f"SELECT COALESCE(json_agg(date_time),'[]') FROM {self.name}.bid"))
        memory=collections.Counter()
        for value in rows:
            if value is None:
                memory[(None,)*len(self.labels)]+=1
                continue
            dt=datetime.datetime.fromisoformat(value).astimezone(datetime.timezone.utc)
            iso=dt.isocalendar()
            delta=dt-datetime.datetime(1970,1,1,tzinfo=datetime.timezone.utc)
            epoch=decimal.Decimal(delta.days*86400+delta.seconds)+decimal.Decimal(delta.microseconds)/1000000
            second=decimal.Decimal(dt.second)+decimal.Decimal(dt.microsecond)/1000000
            memory[(dt.year,dt.month,dt.day,dt.minute,second,epoch,iso.week,iso.year,iso.weekday,(dt.weekday()+1)%7,dt.timetuple().tm_yday,(dt.month-1)//3+1)]+=1
        actual=json.loads(self.sql(f"SELECT COALESCE(json_agg(g),'[]') FROM {self.bag_relation()} g"),parse_float=decimal.Decimal)
        assert {tuple(row['tuple'][1]):row['weight'] for row in actual}==dict(memory)


def qualify(output,command):
    fixture=CalendarFixture(output)
    with fixture.managed():
        worker=fixture.start(command,'first')
        ready=worker.event('ready')
        fixture.verify()
        changes=[
            f"UPDATE {fixture.name}.bid SET date_time='2024-11-03 01:30:00-07' WHERE id=1; UPDATE {fixture.name}.bid SET date_time='2024-11-03 01:30:00-08' WHERE id=2",
            f"UPDATE {fixture.name}.bid SET date_time=NULL WHERE id=3; DELETE FROM {fixture.name}.bid WHERE id=4",
            f"INSERT INTO {fixture.name}.bid(id,auction,price,date_time) VALUES(100,1,NULL,'1969-12-31 23:59:59.999999+00')",
        ]
        for index,change in enumerate(changes,1):
            fixture.sql('BEGIN;'+change+';COMMIT')
            worker.event('published',minimum_time=ready['time']+index)
            fixture.verify()
        worker.abort()
        resumed=fixture.start(command,'resumed',maximum=1)
        reopened=resumed.event('ready')
        assert reopened['slot']==ready['slot'] and reopened['time']==ready['time']+len(changes)
        fixture.sql(f'DELETE FROM {fixture.name}.bid WHERE id=100')
        resumed.event('published')
        resumed.finish()
        fixture.verify()
        (output/'result.json').write_text(json.dumps({'native_mv':'exact identical SQL','memory':'independent datetime/Decimal UTC','dst_instants':True,'cold_restart':True,'fields':fixture.labels})+'\n')


if __name__=='__main__':
    command=sys.argv[2:]
    if command and command[0]=='--':
        command=command[1:]
    qualify(pathlib.Path(sys.argv[1]),command)
