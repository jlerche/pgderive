#!/usr/bin/env python3
"""Native calendar values/metadata and independent exact arithmetic through CDC."""
import collections
from decimal import Decimal
import json
import subprocess

from native_mv_oracle import expected, binary_difference
from numeric_arithmetic_oracle import clean
from sql_compiler_harness import SqlFixture
from temporal_oracle import (Civil, Interval, parse_civil, parse_interval, encoded,
    date_integer, date_difference, timestamp_add, timestamp_difference,
    interval_add, negate, extract, truncate, special, compare)
from worker_harness import blocked_diagnostic, History, run_modes

DATE_FIELDS = ('year','month','day','quarter','week','isoyear','dow','isodow','doy','decade','century','millennium','epoch','julian')
STAMP_FIELDS = DATE_FIELDS+('hour','minute','second','milliseconds','microseconds')
INTERVAL_FIELDS = ('year','month','day','hour','minute','second','milliseconds','microseconds','quarter','decade','century','millennium','epoch')
UNITS = ('millennium','century','decade','year','quarter','month','week','day','hour','minute','second','milliseconds','microseconds')
MODES = ('projection','interval-identities','date-wide','date-comparison','date-casts','date-integer','date-interval','stamp-interval','instant-interval',
    'interval-arithmetic','interval-predicate','stamp-difference','instant-difference',
    'extract-date','extract-stamp','extract-instant','extract-interval',
    'trunc-date','trunc-stamp','trunc-instant','trunc-interval',
    'group-trunc','group-extract','ordered-trunc','join-trunc','derived','guarded',
    'date-group','date-distinct','date-order','date-join','mixed-in','date-set','date-set-mixed',
    'calendar-error','date-error','cast-error','interval-error','difference-error','trunc-error')


def literal(value):
    return 'NULL' if value is None else "'"+value.replace("'","''")+"'" if isinstance(value,str) else str(value)


def fields(kind):
    return {'date':DATE_FIELDS,'stamp':STAMP_FIELDS,
        'instant':STAMP_FIELDS+('timezone','timezone_hour','timezone_minute'),
        'interval':INTERVAL_FIELDS}[kind]


def column(kind):
    return {'date':'d','stamp':'stamp','instant':'instant','interval':'iv'}[kind]


def expressions(mode):
    if mode.startswith('extract-'):
        kind = mode.removeprefix('extract-')
        return [f'EXTRACT({unit} FROM {column(kind)})' for unit in fields(kind)]
    if mode.startswith('trunc-') and mode != 'trunc-error':
        kind = mode.removeprefix('trunc-')
        return [f"date_trunc('{unit}',{column(kind)})" for unit in UNITS if kind != 'interval' or unit != 'week']
    return {
        'projection':['d','stamp','instant','iv'],
        'date-wide':['d','EXTRACT(year FROM d)','EXTRACT(epoch FROM d)','EXTRACT(julian FROM d)'],
        'date-casts':['d::timestamp','d::timestamptz','stamp::date','instant::date'],
        'date-comparison':['d>stamp','d=stamp','stamp<d','d<instant','instant>=d','d<>stamp'],
        'date-integer':['d+days','days+d','d-days','CASE WHEN id=4 OR id=5 THEN NULL ELSE d-d2 END'],
        'date-interval':['d+iv','d-iv','iv+d','d+other'],
        'stamp-interval':['stamp+iv','stamp-iv','iv+stamp','stamp+other'],
        'instant-interval':['instant+iv','instant-iv','iv+instant','instant+other'],
        'interval-arithmetic':['iv+other','iv-other','-iv'],
        'interval-predicate':['iv','other','iv=other','iv<other','iv>other'],
        'stamp-difference':['stamp-stamp2','stamp2-stamp'],
        'instant-difference':['instant-instant2','instant2-instant'],
        'guarded':["CASE WHEN id<999 THEN date_trunc('month',stamp) ELSE stamp+iv END",
                   "COALESCE(stamp,stamp+iv)"],
        'calendar-error':['CASE WHEN id<999 THEN stamp ELSE stamp+iv END'],
        'date-error':['CASE WHEN id<999 THEN d ELSE d+days END'],
        'cast-error':['d::timestamp'],
        'interval-error':['CASE WHEN id<999 THEN iv ELSE iv+other END'],
        'difference-error':['CASE WHEN id<999 THEN iv ELSE stamp-stamp2 END'],
        'trunc-error':["CASE WHEN id<999 THEN stamp ELSE date_trunc('millennium',stamp) END"],
    }[mode]


def outputs(mode, row):
    d,t,z,iv,other = (row[name] for name in ('d','stamp','instant','iv','other'))
    if mode.startswith('extract-'):
        kind = mode.removeprefix('extract-')
        return [clean(extract(row[column(kind)],kind,unit)) for unit in fields(kind)]
    if mode.startswith('trunc-') and mode != 'trunc-error':
        kind = mode.removeprefix('trunc-')
        result_kind = 'instant' if kind == 'date' else kind
        return [encoded(truncate(row[column(kind)],kind,unit),result_kind) for unit in UNITS if kind != 'interval' or unit != 'week']
    if mode in ('projection','date-wide'):
        return [encoded(d,'date'),encoded(t,'stamp'),encoded(z,'instant'),encoded(iv,'interval')] if mode == 'projection' else [encoded(d,'date')]+[clean(extract(d,'date',unit)) for unit in ('year','epoch','julian')]
    if mode == 'date-integer':
        return [encoded(date_integer(d,row['days']),'date'),encoded(date_integer(d,row['days']),'date'),
                encoded(date_integer(d,-row['days'] if row['days'] is not None else None),'date'),
                None if row['id'] in (4,5) else date_difference(d,row['d2'])]
    if mode in ('date-casts','cast-error'):
        def midnight(value):
            return value if value is None or special(value) else Civil(value.day)
        values = [encoded(d,'stamp'),encoded(d,'instant'),encoded(midnight(t),'date'),encoded(midnight(z),'date')]
        return values[:1] if mode == 'cast-error' else values
    if mode == 'date-comparison':
        first,second = compare(d,t),compare(d,z)
        return [None if first is None else first>0,None if first is None else first==0,
                None if first is None else first>0,None if second is None else second<0,
                None if second is None else second<=0,None if first is None else first!=0]
    if mode in ('date-interval','stamp-interval','instant-interval'):
        value,kind = (d,'stamp') if mode == 'date-interval' else (t,'stamp') if mode == 'stamp-interval' else (z,'instant')
        return [encoded(value,kind) for value in (timestamp_add(value,iv),timestamp_add(value,iv,True),timestamp_add(value,iv),timestamp_add(value,other))]
    if mode == 'interval-arithmetic':
        return [encoded(value,'interval') for value in (interval_add(iv,other),interval_add(iv,other,True),negate(iv))]
    if mode == 'interval-predicate':
        comparison = compare(iv,other)
        return [encoded(iv,'interval'),encoded(other,'interval'),comparison==0,comparison<0,comparison>0]
    if mode in ('stamp-difference','instant-difference'):
        first,second = (t,row['stamp2']) if mode == 'stamp-difference' else (z,row['instant2'])
        return [encoded(timestamp_difference(first,second),'interval'),encoded(timestamp_difference(second,first),'interval')]
    if mode == 'guarded':
        return [encoded(truncate(t,'stamp','month'),'stamp'),encoded(t if t is not None else timestamp_add(t,iv),'stamp')]
    return [encoded({'calendar-error':t,'date-error':d,'interval-error':iv,'difference-error':iv,'trunc-error':t}[mode],
                    'date' if mode == 'date-error' else 'interval' if mode in ('interval-error','difference-error') else 'stamp')]


class TemporalFixture(SqlFixture):
    def __init__(self, output, mode):
        super().__init__(output,False)
        self.mode = mode
        # All helper/native-oracle psql calls share ISO interval output too.
        self.pg[1:1] = ['-q','-c',"SET DateStyle='ISO,MDY'; SET TimeZone='UTC'; SET IntervalStyle='iso_8601'"]
        a,b = self.name+'.auction',self.name+'.bid'
        self.sql(f'''ALTER TABLE {b} ADD COLUMN d date,ADD COLUMN d2 date,
            ADD COLUMN stamp timestamp,ADD COLUMN stamp2 timestamp,
            ADD COLUMN instant timestamptz,ADD COLUMN instant2 timestamptz,
            ADD COLUMN iv interval,ADD COLUMN other interval,ADD COLUMN days integer;
            ALTER TABLE {a} ADD COLUMN on_date date,ADD COLUMN anchor timestamp;
            UPDATE {a} SET on_date='2024-01-31',anchor='2024-01-31';
            UPDATE {a} SET on_date='2016-01-01',anchor='2016-01-01' WHERE id=2;
            DELETE FROM {b}''')
        rows = [
            ('2024-01-31','2024-01-31 23:59:59.999999','P1M','P30D'),
            ('2024-02-29','2024-02-29 12:00:00.000001','P1Y','P12M'),
            ('2016-01-01','2016-01-01 00:00:00','P-1M2DT-3H-4M-5.123456S','P1DT25H'),
            ('infinity','infinity','P2D','PT0S'),
            ('-infinity','-infinity','P-2D','PT0S'),
            (None,None,None,'PT0S'),
            ('1969-12-31','1969-12-31 23:59:59.999999','PT0.000001S','PT0S'),
            ('0001-02-29 BC','0001-02-29 12:34:56.123456 BC','P1M','PT0S'),
            ('0002-03-01 BC','0002-03-01 00:00:00 BC','P-1M','PT0S'),
            ('0011-01-01 BC','0011-01-01 00:00:00 BC','PT25H','P1DT1H'),
            ('0100-12-31 BC','0100-12-31 23:59:59 BC','P1D','PT0S'),
            ('294276-12-31','294276-12-31 23:59:59.999999','PT0S','PT0S'),
            ('2024-01-02','2024-01-02 01:00:00','PT25H','P1DT1H'),
            ('2000-01-01','2000-01-01 00:00:00','infinity','PT0S'),
            ('2000-01-01','2000-01-01 00:00:00','-infinity','PT0S'),
            ('2024-01-31','2024-01-31 00:00:00','PT720H','P30D'),
            ('2024-01-31','2024-01-31 00:00:00','P30D','PT720H'),
            ('294247-01-10','294247-01-10 04:00:54.775807','PT0S','PT0S'),
            ('294276-12-31','294276-12-31 23:59:59.999499','PT0S','PT0S'),
        ]
        for identifier,(date,stamp,interval,other) in enumerate(rows,1):
            stamp2 = '294276-12-30 23:59:59.999999' if identifier in (12,19) else '294247-01-09 04:00:54.775807' if identifier == 18 else '2024-01-01 00:00:00' if identifier == 13 else '1970-01-01 00:00:00'
            instant = None if stamp is None else stamp.removesuffix(' BC')+'+00'+(' BC' if stamp.endswith(' BC') else '') if stamp not in ('infinity','-infinity') else stamp
            instant2 = stamp2+'+00'
            self.sql(f'INSERT INTO {b}(id,auction,price,d,d2,stamp,stamp2,instant,instant2,iv,other,days) VALUES('+','.join(map(literal,(identifier,identifier%3,identifier-5,date,'2000-01-01',stamp,stamp2,instant,instant2,interval,other,identifier%5-2)))+')')
        if mode in ('date-wide','date-comparison'):
            self.sql(f"UPDATE {b} SET d='5874897-12-31' WHERE id=12; UPDATE {b} SET d='4714-11-24 BC' WHERE id=8")
        if mode == 'mixed-in':
            self.sql(f"UPDATE {a} SET on_date='5874897-12-31' WHERE id=3")
        if mode == 'mixed-in':
            self.sql(f"UPDATE {b} SET d='5874897-12-31' WHERE id=12")
        if mode == 'guarded':
            self.sql(f"UPDATE {b} SET iv='P1M' WHERE id=12")
        if mode == 'interval-arithmetic':
            self.sql(f"UPDATE {b} SET iv='PT2562047788H54.775806S',other='PT0.000001S' WHERE id=3; UPDATE {b} SET iv='PT-2562047788H-54.775807S',other='PT0.000001S' WHERE id=6")
        self.labels,self.query = self.statement()

    def statement(self):
        mode,b = self.mode,self.name+'.bid'
        if mode == 'interval-identities':
            return ['v0'],f'SELECT iv AS v0 FROM {b} WHERE id=1 OR id=16 OR id=17'
        if mode in ('group-trunc','group-extract','date-group'):
            key = "date_trunc('month',stamp)" if mode == 'group-trunc' else 'EXTRACT(year FROM stamp)' if mode == 'group-extract' else 'd'
            return ['k','n','total'],f'SELECT {key} AS k,COUNT(*) AS n,SUM(price) AS total FROM {b} GROUP BY {key}'
        if mode == 'date-distinct':
            return ['k'],f'SELECT DISTINCT d AS k FROM {b}'
        if mode == 'date-set':
            return ['k'],f'SELECT d AS k FROM {b} UNION SELECT d FROM {b}'
        if mode == 'date-set-mixed':
            return ['k'],f'SELECT d AS k FROM {b} UNION SELECT stamp FROM {b}'
        if mode in ('date-order','ordered-trunc'):
            key = 'd' if mode == 'date-order' else "date_trunc('day',stamp)"
            return ['id','k'],f'SELECT id,{key} AS k FROM {b} ORDER BY k NULLS FIRST,id LIMIT 5'
        if mode == 'date-join':
            return ['aid','bid','k'],f'SELECT a.id AS aid,b.bid AS bid,on_date AS k FROM {self.name}.auction a JOIN(SELECT id AS bid,d AS on_date FROM {b}) b USING(on_date)'
        if mode == 'join-trunc':
            return ['aid','bid','k'],f"SELECT a.id AS aid,b.id AS bid,b.d AS k FROM {self.name}.auction a JOIN {b} b ON a.anchor=date_trunc('day',b.stamp)"
        if mode == 'mixed-in':
            dates = f'SELECT a.on_date FROM {self.name}.auction a'
            stamps = f'SELECT a.anchor FROM {self.name}.auction a'
            return ['id','v0','v1','v2','v3'],f'SELECT id,stamp IN({dates}) AS v0,stamp NOT IN({dates}) AS v1,d IN({stamps}) AS v2,d NOT IN({stamps}) AS v3 FROM {b}'
        if mode == 'derived':
            return ['id','k','e'],f"SELECT q.id,date_trunc('day',q.t) AS k,EXTRACT(epoch FROM q.t) AS e FROM(SELECT id,date_trunc('month',stamp)+iv AS t FROM {b}) q"
        values = expressions(mode)
        labels = ['id']+['v'+str(index) for index in range(len(values))]
        query = 'SELECT id,'+','.join(value+' AS '+label for value,label in zip(values,labels[1:]))+f' FROM {b}'
        if mode == 'interval-predicate':
            query += ' WHERE iv IS NOT NULL AND other IS NOT NULL'
        return labels,query

    def memory(self, rows, auctions=()):
        mode,result = self.mode,collections.Counter()
        if mode == 'interval-identities':
            return collections.Counter((encoded(row['iv'],'interval'),) for row in rows if row['id'] in (1,16,17))
        if mode == 'mixed-in':
            for row in rows:
                stamp = contains(row['stamp'],[a['on_date'] for a in auctions])
                date = contains(row['d'],[a['anchor'] for a in auctions])
                result[(row['id'],stamp,None if stamp is None else not stamp,date,None if date is None else not date)] += 1
            return result
        if mode in ('group-trunc','group-extract','date-group'):
            grouped = collections.defaultdict(list)
            for row in rows:
                key = encoded(truncate(row['stamp'],'stamp','month'),'stamp') if mode == 'group-trunc' else clean(extract(row['stamp'],'stamp','year')) if mode == 'group-extract' else encoded(row['d'],'date')
                grouped[key].append(row['price'])
            for key,values in grouped.items():
                present = [value for value in values if value is not None]
                result[(key,len(values),sum(present) if present else None)] += 1
            return result
        if mode in ('date-distinct','date-set'):
            return collections.Counter({(encoded(row['d'],'date'),):1 for row in rows})
        if mode == 'date-set-mixed':
            return collections.Counter({(encoded(value,'stamp'),):1 for row in rows for value in (row['d'],row['stamp'])})
        if mode in ('date-order','ordered-trunc'):
            ranked = [(row['id'],row['d'] if mode == 'date-order' else truncate(row['stamp'],'stamp','day')) for row in rows]
            def key(pair):
                value = pair[1]
                return (0,) if value is None else (1,) if value == '-infinity' else (3,) if value == 'infinity' else (2,value.day,value.micros)
            return collections.Counter((identifier,encoded(value,'date' if mode == 'date-order' else 'stamp')) for identifier,value in sorted(ranked,key=lambda pair:(key(pair),pair[0]))[:5])
        if mode in ('join-trunc','date-join'):
            for a in auctions:
                for row in rows:
                    first,second = (a['on_date'],row['d']) if mode == 'date-join' else (a['anchor'],truncate(row['stamp'],'stamp','day'))
                    if first is not None and second is not None and first == second:
                        result[(a['id'],row['id'],encoded(row['d'],'date'))] += 1
            return result
        for row in rows:
            if mode == 'derived':
                value = timestamp_add(truncate(row['stamp'],'stamp','month'),row['iv'])
                result[(row['id'],encoded(truncate(value,'stamp','day'),'stamp'),clean(extract(value,'stamp','epoch')))] += 1
            elif mode != 'interval-predicate' or (row['iv'] is not None and row['other'] is not None):
                result[(row['id'],*outputs(mode,row))] += 1
        return result

    def verify(self):
        self.verify_sql(expected(self,self.labels))
        assert binary_difference(self) == '0','temporal native send representation differs'
        metadata = "SELECT json_agg(json_build_array(attname,atttypid,atttypmod,attcollation) ORDER BY attnum) FROM pg_attribute WHERE attrelid='%s'::regclass AND attnum>0 AND NOT attisdropped"
        assert self.sql(metadata % (self.name+'.groups')) == self.sql(metadata % (self.name+'.pgderive_native_oracle')),'temporal output metadata differs'
        rows = json.loads(self.sql(f"SELECT COALESCE(json_agg(r),'[]') FROM(SELECT id,auction,price,d::text,d2::text,stamp::text,stamp2::text,instant::text,instant2::text,iv::text,other::text,days FROM {self.name}.bid) r"))
        for row in rows:
            for name in ('d','d2','stamp','stamp2','instant','instant2'):
                row[name] = parse_civil(row[name])
            for name in ('iv','other'):
                row[name] = parse_interval(row[name])
        auctions = json.loads(self.sql(f"SELECT json_agg(r) FROM(SELECT id,on_date::text,anchor::text FROM {self.name}.auction) r"))
        for row in auctions:
            row['on_date'],row['anchor'] = parse_civil(row['on_date']),parse_civil(row['anchor'])
        self.verify_memory(dict(self.memory(rows,auctions)),parse_float=Decimal)


def interval_equal(left, right):
    return compare(left,right) == 0


def contains(value, candidates):
    comparisons = [compare(value,candidate) for candidate in candidates]
    return True if 0 in comparisons else None if None in comparisons else False


def changes(fixture):
    a,b = fixture.name+'.auction',fixture.name+'.bid'
    result = [
        f"INSERT INTO {b}(id,auction,price,d,d2,stamp,stamp2,instant,instant2,iv,other,days) VALUES(100,1,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL),(101,1,7,'2024-01-31','2024-02-29','2024-01-31 12:00:00','2024-01-01','2024-01-31 12:00:00+00','2024-01-01+00','P1M','P30D',-1); UPDATE {a} SET on_date='2024-01-31',anchor='2024-01-31' WHERE id=1",
        f"UPDATE {b} SET d='2024-02-29',stamp='2024-02-29 01:02:03.000004',instant='2024-02-28 17:02:03.000004-08',iv='P1Y',other='P12M' WHERE id=1; UPDATE {a} SET on_date='2024-02-29',anchor='2024-02-29' WHERE id=1",
        f"UPDATE {b} SET iv='P-1M2DT-3H-4M-5.123456S',other='P-30D',days=-2 WHERE id=2; UPDATE {b} SET iv='P1D' WHERE id=16; DELETE FROM {b} WHERE id=4",
        f"UPDATE {b} SET d=NULL,stamp=NULL,instant=NULL,iv=NULL WHERE id IN(1,2,5); UPDATE {b} SET iv='P2D' WHERE id=17; UPDATE {a} SET on_date=NULL,anchor=NULL WHERE id=2",
        f'DELETE FROM {b}',
        f"INSERT INTO {b}(id,auction,price,d,d2,stamp,stamp2,instant,instant2,iv,other,days) VALUES(102,1,-3,'2016-01-01','2015-12-31','2016-01-01 23:59:59.999999','2016-01-01','2016-01-01 23:59:59.999999+00','2016-01-01+00','PT25H','P1DT1H',1); UPDATE {a} SET on_date='2016-01-01',anchor='2016-01-01' WHERE id=1",
    ]
    if fixture.mode == 'interval-identities':
        result[-1] += f"; INSERT INTO {b}(id,iv) VALUES(1,'P1M'),(16,'PT720H'),(17,'P30D')"
    if fixture.mode == 'mixed-in':
        result[1] += f"; UPDATE {a} SET on_date='2016-01-01' WHERE id=3"
        result[2] += f"; UPDATE {a} SET on_date='5874897-12-31' WHERE id=3"
    if fixture.mode == 'mixed-in':
        result[0] += f'; UPDATE {a} SET on_date=NULL,anchor=NULL WHERE id=4'
    return result


def qualify(output, command, mode):
    fixture = TemporalFixture(output,mode)
    b = fixture.name+'.bid'
    with fixture.managed():
        if mode == 'projection':
            for label,query in {
                'interval-group':f'SELECT iv FROM {b} GROUP BY iv',
                'interval-distinct':f'SELECT DISTINCT iv FROM {b}',
                'interval-order':f'SELECT iv FROM {b} ORDER BY iv LIMIT 1',
                'interval-hidden-order':f'SELECT iv FROM {b} ORDER BY id LIMIT 1',
                'interval-set':f'SELECT iv FROM {b} UNION SELECT iv FROM {b}',
                'date-minimum':f'SELECT MIN(d) FROM {b}',
            }.items():
                fixture.rejected(command,label,query)
            for kind in ('INNER','LEFT','RIGHT','FULL'):
                fixture.rejected(command,'mixed-using-'+kind.lower(),f'SELECT b.bid FROM {fixture.name}.auction a {kind} JOIN(SELECT id AS bid,stamp AS on_date FROM {b}) b USING(on_date)')
            assert fixture.sql(f"SELECT to_regclass('{fixture.name}.pgderive_worker_registration') IS NULL").strip() == 't'
        final = f"UPDATE {b} SET d='0001-02-29 BC',stamp='0001-02-29 23:59:59.999999 BC',instant='0001-02-29 23:59:59.999999+00 BC',iv='P1M',days=-1 WHERE id=102"
        if mode == 'interval-identities':
            final += f"; UPDATE {b} SET iv='P31D' WHERE id=1"
        fixture.run_history(command,History(changes=changes(fixture),final=final,
            normalized=fixture.query.replace('SELECT','select'),rejected=[]))
        if mode.endswith('-error'):
            before = fixture.sql(f"SELECT to_jsonb(p)-'fence' FROM {fixture.name}.pgderive_progress p")
            sink = fixture.sql(f"SELECT COALESCE(jsonb_agg(jsonb_build_array(tuple,weight) ORDER BY tuple),'[]') FROM {fixture.bag_relation()}")
            worker = fixture.start(command,'expected-error'); worker.event('ready')
            bad = {
                'calendar-error':"'2000-01-01','294276-12-31 23:59:59.999999','2000-01-01','P1M','PT0S',0",
                'date-error':"'5874897-12-31','2000-01-01','2000-01-01','PT0S','PT0S',1",
                'cast-error':"'5874897-12-31','2000-01-01','2000-01-01','PT0S','PT0S',0",
                'interval-error':"'2000-01-01','2000-01-01','2000-01-01','P2147483647D','P1D',0",
                'difference-error':"'2000-01-01','infinity','infinity','PT0S','PT0S',0",
                'trunc-error':"'2000-01-01','4714-11-24 00:00:00 BC','2000-01-01','PT0S','PT0S',0",
            }[mode]
            fixture.sql(f'INSERT INTO {b}(id,d,stamp,stamp2,iv,other,days) VALUES(999,{bad})')
            blocked_diagnostic(worker)
            native = subprocess.run(fixture.pg+[fixture.query],env=fixture.environment,capture_output=True,text=True)
            (output/'native-error.txt').write_text(native.stderr)
            assert native.returncode != 0,'native PostgreSQL accepted temporal error input'
            assert fixture.sql(f"SELECT to_jsonb(p)-'fence' FROM {fixture.name}.pgderive_progress p") == before
            assert fixture.sql(f"SELECT COALESCE(jsonb_agg(jsonb_build_array(tuple,weight) ORDER BY tuple),'[]') FROM {fixture.bag_relation()}") == sink
            assert fixture.acknowledgement_ahead().strip() == '0'
        (output/'result.json').write_text(json.dumps({'mode':mode,'native_mv_send_metadata':True,
            'integer_calendar_decimal_memory':True,'bc_infinities_nulls':True,
            'simultaneous_inputs':True,'cold_restart':True,'error_boundary':mode.endswith('-error')})+'\n')


if __name__ == '__main__':
    run_modes(MODES,qualify)
