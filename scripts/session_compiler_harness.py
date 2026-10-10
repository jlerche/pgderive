#!/usr/bin/env python3
"""Explicit PostgreSQL session connectivity and temporal extrema; no expiry."""
import collections
import datetime
import json
import pathlib
import sys
from native_compiler_harness import NativeFixture, encoded, instant


def offset(text, seconds):
    if text is None or text in ('infinity','-infinity'):
        return text
    value = datetime.datetime.fromisoformat(text)+datetime.timedelta(seconds=seconds)
    return value.isoformat(sep=' ').replace('+00:00','+00')


class SessionFixture(NativeFixture):
    evidence = {'session':'adjacent gaps strictly below ten seconds; NULL time excluded', 'expiry':'none', 'oracle':'direct connected components'}

    def __init__(self, output, mode):
        super().__init__(output, 'projection')
        self.mode = mode
        self.temporal = 'event_time' if mode.endswith('instant') else 'event_at'
        self.grouped = mode.startswith('extrema')
        self.frames = mode.startswith('frames')
        self.offsets = mode.startswith('offsets')
        self.sql(f"SET TimeZone='UTC'; UPDATE {self.name}.bid SET event_at=TIMESTAMP '2000-01-01 00:00:00'+INTERVAL '1 second'*(id%31),event_time=TIMESTAMPTZ '2000-01-01 00:00:00+00'+INTERVAL '1 second'*(id%31); UPDATE {self.name}.bid SET event_at=NULL,event_time=NULL WHERE id%7=0; UPDATE {self.name}.bid SET event_at='-infinity',event_time='-infinity',auction=1 WHERE id IN(1,4); UPDATE {self.name}.bid SET event_at='infinity',event_time='infinity',auction=1 WHERE id IN(2,3); INSERT INTO {self.name}.bid(id,auction,price,event_at,event_time) VALUES(200,900,NULL,'2000-01-01 00:00:10','2000-01-01 00:00:10+00'),(201,900,NULL,'2000-01-01 00:00:20','2000-01-01 00:00:20+00'),(203,901,NULL,'2000-01-01 00:00:00.000001','2000-01-01 00:00:00.000001+00'),(204,901,NULL,'2000-01-01 00:00:10','2000-01-01 00:00:10+00')")
        if self.offsets:
            self.query = f"SELECT b.id AS bidder,b.{self.temporal}-INTERVAL '10 seconds' AS start,b.{self.temporal}+INTERVAL '24 hours' AS finish,b.auction AS n FROM {self.name}.bid b"
        elif self.frames:
            frame = ' OVER(PARTITION BY b.auction ORDER BY b.id ROWS BETWEEN 1 FOLLOWING AND 1 FOLLOWING)'
            self.query = f'SELECT b.id AS bidder,MIN(b.{self.temporal}){frame} AS start,MAX(b.{self.temporal}){frame} AS finish,COUNT(*){frame} AS n FROM {self.name}.bid b'
        elif self.grouped:
            self.query = f"SELECT b.auction AS bidder,MIN(b.{self.temporal}) FILTER(WHERE b.id%2=0) AS start,MAX(b.{self.temporal}) FILTER(WHERE b.id%2=1) AS finish,COUNT(*) AS n FROM {self.name}.bid b GROUP BY b.auction"
        else:
            previous = f'SELECT b.id,b.auction,b.{self.temporal} AS time,lag(b.{self.temporal}) OVER(PARTITION BY b.auction ORDER BY b.{self.temporal},b.id) AS previous FROM {self.name}.bid b WHERE b.{self.temporal} IS NOT NULL'
            flags = f"SELECT q.id,q.auction,q.time,CASE WHEN q.previous IS NULL THEN 1 WHEN q.time=q.previous THEN 0 WHEN q.time-q.previous>=INTERVAL '10 seconds' THEN 1 ELSE 0 END AS flag FROM({previous}) q"
            numbered = f'SELECT z.auction,z.time,SUM(z.flag) OVER(PARTITION BY z.auction ORDER BY z.time,z.id ROWS UNBOUNDED PRECEDING) AS session FROM({flags}) z'
            grouped = f'SELECT s.auction,MIN(s.time) AS start,MAX(s.time) AS last,COUNT(*) AS n FROM({numbered}) s GROUP BY s.auction,s.session'
            self.query = f"SELECT g.auction AS bidder,g.start AS start,g.last+INTERVAL '10 seconds' AS finish,g.n AS n FROM({grouped}) g"
        self.labels = ['bidder','start','finish','n']

    def verify(self):
        prefix = "SET DateStyle='ISO,MDY'; SET TimeZone='UTC'; "
        expected = f"SELECT jsonb_build_array(NULL,jsonb_build_array({','.join(self.labels)})) tuple,count(*)::bigint weight FROM ({self.query}) q GROUP BY 1"
        difference = self.sql(prefix+f'''WITH expected AS ({expected}),actual AS
            (SELECT tuple,weight FROM {self.name}.groups),difference AS
            ((SELECT * FROM expected EXCEPT ALL SELECT * FROM actual) UNION ALL
            (SELECT * FROM actual EXCEPT ALL SELECT * FROM expected)) SELECT count(*) FROM difference''')
        assert difference.strip() == '0','session/extrema output differs from PostgreSQL'
        source = json.loads(self.sql(prefix+f"SELECT COALESCE(json_agg(q),'[]') FROM(SELECT id,auction,{self.temporal}::text AS time FROM {self.name}.bid) q"))
        groups = collections.defaultdict(list)
        for row in source:
            groups[row['auction']].append(row)
        memory = collections.Counter()
        timezone = self.temporal == 'event_time'
        for bidder, group in groups.items():
            if self.offsets:
                for row in group:
                    start = offset(row['time'],-10)
                    finish = offset(row['time'],86400)
                    memory[(row['id'],encoded(start,timezone),encoded(finish,timezone),bidder)] += 1
                continue
            if self.frames:
                group.sort(key=lambda row:row['id'])
                for index,row in enumerate(group):
                    following = group[index+1:index+2]
                    value = following[0]['time'] if following else None
                    memory[(row['id'],encoded(value,timezone),encoded(value,timezone),len(following))] += 1
                continue
            if self.grouped:
                minima = [row['time'] for row in group if row['time'] is not None and row['id']%2 == 0]
                maxima = [row['time'] for row in group if row['time'] is not None and row['id']%2 == 1]
                start = min(minima,key=instant) if minima else None
                finish = max(maxima,key=instant) if maxima else None
                memory[(bidder,encoded(start,timezone),encoded(finish,timezone),len(group))] += 1
                continue
            times = sorted([row['time'] for row in group if row['time'] is not None],key=instant)
            components = []
            for time in times:
                connected = False
                if components:
                    last = components[-1][-1]
                    if time == last:
                        connected = True
                    elif time not in ('infinity','-infinity') and last not in ('infinity','-infinity'):
                        connected = datetime.datetime.fromisoformat(time)-datetime.datetime.fromisoformat(last) < datetime.timedelta(seconds=10)
                if connected:
                    components[-1].append(time)
                else:
                    components.append([time])
            for component in components:
                finish = component[-1]
                finish = offset(finish,10)
                memory[(bidder,encoded(component[0],timezone),encoded(finish,timezone),len(component))] += 1
        actual = json.loads(self.sql(f"SELECT COALESCE(json_agg(g),'[]') FROM {self.name}.groups g"))
        assert {tuple(row['tuple'][1]):row['weight'] for row in actual} == dict(memory),'session/extrema output differs from connected-component oracle'
        assert self.sql(f'''SELECT count(*) FROM pg_replication_slots s JOIN {self.name}.pgderive_worker_registration r ON r.slot_name=s.slot_name JOIN {self.name}.pgderive_progress p ON true WHERE s.confirmed_flush_lsn>p.end_lsn''').strip() == '0'


def qualify(output, command, mode):
    fixture = SessionFixture(output,mode)
    try:
        fixture.rejected(command,'unsupported',fixture.query+' ORDER BY bidder')
        assert fixture.sql(f"SELECT to_regclass('{fixture.name}.pgderive_worker_registration') IS NULL").strip() == 't'
        worker = fixture.start(command,'first')
        ready = worker.event('ready')
        fixture.verify()
        changes = [
            f"INSERT INTO {fixture.name}.bid(id,auction,price,event_at,event_time) VALUES(202,900,NULL,'2000-01-01 00:00:15','2000-01-01 00:00:15+00'),(205,NULL,NULL,NULL,NULL)",
            f"DELETE FROM {fixture.name}.bid WHERE id=202; UPDATE {fixture.name}.auction SET group_id=NULL WHERE id=1",
            f"INSERT INTO {fixture.name}.bid(id,auction,price,event_at,event_time) VALUES(202,900,NULL,'2000-01-01 00:00:15','2000-01-01 00:00:15+00'); UPDATE {fixture.name}.bid SET auction=902 WHERE id=201",
            f"UPDATE {fixture.name}.bid SET auction=900,event_at='2000-01-01 00:00:25',event_time='2000-01-01 00:00:25+00' WHERE id=201; DELETE FROM {fixture.name}.bid WHERE id=202",
            f"UPDATE {fixture.name}.bid SET event_at=NULL,event_time=NULL WHERE id IN(203,204); DELETE FROM {fixture.name}.bid WHERE id IN(200,201)",
            f"UPDATE {fixture.name}.bid SET event_at='2024-03-10 01:59:59.999999',event_time='2024-03-10 01:59:59.999999-08' WHERE id=1; UPDATE {fixture.name}.bid SET event_at='2024-03-10 03:00:00',event_time='2024-03-10 03:00:00-07' WHERE id=2",
        ]
        fixture.sql(f"BEGIN; DELETE FROM {fixture.name}.bid; ROLLBACK")
        for index, change in enumerate(changes,1):
            fixture.sql("SET DateStyle='German,DMY'; SET TimeZone='America/New_York'; BEGIN;"+change+';COMMIT')
            worker.event('published',minimum_time=ready['time']+index)
            fixture.verify()
        worker.abort()
        prior = fixture.sql(f'SELECT row_to_json(p) FROM {fixture.name}.pgderive_progress p')
        fixture.rejected(command,'changed',fixture.query.replace(' AS bidder',' AS changed',1))
        if mode.startswith('session'):
            fixture.rejected(command,'changed-gap',fixture.query.replace('10 seconds','11 seconds'))
        assert fixture.sql(f'SELECT row_to_json(p) FROM {fixture.name}.pgderive_progress p') == prior
        fixture.query = fixture.query.replace('b.','"B".').replace('bid b','bid AS "B"')
        resumed = fixture.start(command,'resumed',maximum=1)
        reopened = resumed.event('ready')
        assert reopened['slot']==ready['slot'] and reopened['time']==ready['time']+len(changes)
        fixture.verify()
        fixture.sql(f"DELETE FROM {fixture.name}.bid")
        resumed.event('published')
        resumed.finish()
        fixture.verify()
        (output/'result.json').write_text(json.dumps({'mode':mode,'sql_oracle':'exact bag','memory_oracle':'direct components/extrema','restart':'cold',**fixture.evidence})+'\n')
        fixture.cleanup()
    except BaseException:
        fixture.preserve()
        raise


if __name__ == '__main__':
    output = pathlib.Path(sys.argv[1])
    command = sys.argv[2:]
    if command and command[0]=='--':
        command = command[1:]
    for mode in ('session-local','session-instant','extrema-local','extrema-instant','frames-local','frames-instant','offsets-local','offsets-instant'):
        directory = output/mode
        directory.mkdir()
        qualify(directory,command,mode)
