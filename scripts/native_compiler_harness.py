#!/usr/bin/env python3
"""Native PostgreSQL timestamp/numeric source identity through snapshot/CDC/restart."""
import collections
from decimal import Decimal
import json
import pathlib
import sys
from numeric_compiler_harness import average
from sql_compiler_harness import SqlFixture


def instant(text):
    if text is None:
        return (3,)
    if text == '-infinity':
        return (0,)
    if text == 'infinity':
        return (2,)
    value = text.removesuffix(' BC')
    date, time = value.split(' ')
    year, month, day = map(int, date.split('-'))
    if text.endswith(' BC'):
        year = 1-year
    hour, minute, second = time.removesuffix('+00').split(':')
    return (1,year,month,day,int(hour),int(minute),Decimal(second))


def encoded(text, timezone=False):
    if text is None or text in ('infinity', '-infinity'):
        return text
    value = text.replace(' ', 'T', 1)
    if timezone:
        value = value.replace('+00', '+00:00')
    return value


class NativeFixture(SqlFixture):
    def __init__(self, output, mode):
        super().__init__(output, False)
        self.pg.insert(1, '-q')
        self.mode = mode
        self.sql(f'''ALTER TABLE {self.name}.bid ADD COLUMN event_at timestamp,
            ADD COLUMN event_time timestamptz, ADD COLUMN amount numeric;
            UPDATE {self.name}.bid SET event_at=timestamp '1999-12-31 23:59:59.999999',
            event_time=timestamptz '1999-12-31 15:59:59.999999-08',amount=id::numeric/3;
            UPDATE {self.name}.bid SET event_at=NULL,event_time=NULL,amount=NULL WHERE id%7=0;
            UPDATE {self.name}.bid SET event_at=timestamp '294276-12-31 23:59:59.999999',event_time=timestamptz '294276-12-31 23:59:59.999999+00',amount=123456789012345678901234567890.123456789012345678901234567890 WHERE id=1;
            UPDATE {self.name}.bid SET event_at=timestamp '4714-11-24 00:00:00 BC',event_time=timestamptz '4714-11-24 00:00:00+00 BC',amount='NaN' WHERE id=2;
            UPDATE {self.name}.bid SET event_at='infinity',event_time='infinity',amount='Infinity' WHERE id=3;
            UPDATE {self.name}.bid SET event_at='-infinity',event_time='-infinity',amount='-Infinity' WHERE id=4;
            UPDATE {self.name}.bid SET event_at=timestamp '0001-02-29 00:00:00 BC',event_time=timestamptz '0001-02-29 00:00:00+00 BC',amount=1.2300 WHERE id=5;''')
        if mode == 'join':
            self.sql(f"ALTER TABLE {self.name}.auction ADD COLUMN event_time timestamptz; UPDATE {self.name}.auction SET event_time='1999-12-31 23:59:59.999999+00'; UPDATE {self.name}.auction SET event_time='infinity' WHERE id=3")
            self.query = f'SELECT a.id AS auction,b.id AS bid,b.event_time AS instant,b.amount AS amount FROM {self.name}.auction a JOIN {self.name}.bid b ON a.event_time=b.event_time WHERE b.event_at IS NOT NULL'
            self.labels = ['auction','bid','instant','amount']
        elif mode == 'projection':
            self.query = f'SELECT b.id AS id,b.event_at AS local,b.event_time AS instant,b.amount AS amount FROM {self.name}.bid b WHERE b.event_time<\'2000-01-01 00:00:00+00\' OR b.event_time IS NULL OR b.id=1 OR b.id=3'
            self.labels = ['id','local','instant','amount']
        elif mode == 'grouped':
            self.query = f'SELECT b.event_at AS local,COUNT(*) AS n,COUNT(b.amount) AS present,AVG(b.price) AS mean FROM {self.name}.bid b GROUP BY b.event_at'
            self.labels = ['local','n','present','mean']
        else:
            frame = ' OVER(PARTITION BY b.auction ORDER BY b.event_time DESC NULLS FIRST,b.id ROWS BETWEEN 2 PRECEDING AND 1 FOLLOWING)'
            self.query = f'SELECT b.id AS id,b.event_time AS instant,COUNT(*){frame} AS n,AVG(b.price){frame} AS mean FROM {self.name}.bid b'
            self.labels = ['id','instant','n','mean']

    def verify(self):
        prefix = "SET DateStyle='ISO,MDY'; SET TimeZone='UTC'; "
        query = f"SELECT jsonb_build_array(NULL,jsonb_build_array({','.join(self.labels)})) tuple,count(*)::bigint weight FROM ({self.query}) q GROUP BY 1"
        difference = self.sql(prefix + f'''WITH expected AS ({query}),actual AS
            (SELECT tuple,weight FROM {self.name}.groups),difference AS
            ((SELECT * FROM expected EXCEPT ALL SELECT * FROM actual) UNION ALL
            (SELECT * FROM actual EXCEPT ALL SELECT * FROM expected)) SELECT count(*) FROM difference''')
        assert difference.strip().splitlines()[-1] == '0', 'native output differs from PostgreSQL'
        raw = self.sql(prefix + f"SELECT COALESCE(json_agg(x),'[]') FROM (SELECT id,auction,price,event_at::text local,event_time::text instant,amount::text amount FROM {self.name}.bid) x").strip()
        source = json.loads(raw)
        expected = collections.Counter()
        if self.mode == 'join':
            auctions = json.loads(self.sql(prefix + f"SELECT json_agg(x) FROM (SELECT id,event_time::text instant FROM {self.name}.auction) x").strip())
            for left in auctions:
                for right in source:
                    if left['instant'] is not None and right['instant'] == left['instant'] and right['local'] is not None:
                        amount = right['amount']
                        if amount is not None and amount not in ('NaN','Infinity','-Infinity'):
                            amount = Decimal(amount)
                        expected[(left['id'],right['id'],encoded(right['instant'],True),amount)] += 1
        elif self.mode == 'projection':
            for row in source:
                if row['id'] in (1,3) or row['instant'] is None or instant(row['instant']) < instant('2000-01-01 00:00:00+00'):
                    amount = row['amount']
                    if amount is not None and amount not in ('NaN','Infinity','-Infinity'):
                        amount = Decimal(amount)
                    expected[(row['id'],encoded(row['local']),encoded(row['instant'],True),amount)] += 1
        else:
            groups = collections.defaultdict(list)
            for row in source:
                groups[row['local'] if self.mode == 'grouped' else row['auction']].append(row)
            for key, group in groups.items():
                # NULLS FIRST DESC, then ascending id, independent tuple-time comparator.
                group.sort(key=lambda row: row['id'])
                group.sort(key=lambda row: instant(row['instant']), reverse=True)
                for index in range(len(group) if self.mode == 'rows' else 1):
                    frame = group[max(0,index-2):min(len(group),index+2)] if self.mode == 'rows' else group
                    prices = [row['price'] for row in frame if row['price'] is not None]
                    values = (encoded(key),len(frame),sum(row['amount'] is not None for row in frame),average(prices)) if self.mode == 'grouped' else (group[index]['id'],encoded(group[index]['instant'],True),len(frame),average(prices))
                    expected[values] += 1
        actual = json.loads(self.sql(f"SELECT COALESCE(json_agg(g),'[]') FROM {self.name}.groups g"), parse_float=Decimal)
        assert {tuple(row['tuple'][1]):row['weight'] for row in actual} == dict(expected)
        assert self.sql(f'''SELECT count(*) FROM pg_replication_slots s JOIN {self.name}.pgderive_worker_registration r ON r.slot_name=s.slot_name JOIN {self.name}.pgderive_progress p ON true WHERE s.confirmed_flush_lsn>p.end_lsn''').strip() == '0'


def qualify(output, command, mode):
    fixture = NativeFixture(output, mode)
    try:
        fixture.rejected(command,'unsupported',fixture.query + ' ORDER BY id')
        assert fixture.sql(f"SELECT to_regclass('{fixture.name}.pgderive_worker_registration') IS NULL").strip() == 't'
        worker = fixture.start(command,'first')
        ready = worker.event('ready')
        fixture.verify()
        changes = [
            f"INSERT INTO {fixture.name}.bid(id,auction,price,event_at,event_time,amount) VALUES(100,NULL,NULL,NULL,NULL,NULL),(101,1,-7,'2000-01-01 00:00:00','1999-12-31 16:00:00-08',0.000000000000000000000000000001)",
            f"UPDATE {fixture.name}.bid SET event_at='2024-03-10 01:59:59.999999',event_time='2024-03-10 01:59:59.999999-08',amount=1.00 WHERE id=1; UPDATE {fixture.name}.bid SET event_at='2024-03-10 03:00:00',event_time='2024-03-10 03:00:00-07',amount=1.000 WHERE id=2; UPDATE {fixture.name}.auction SET group_id=NULL WHERE id=1",
            f"UPDATE {fixture.name}.bid SET event_time=NULL,event_at=NULL,amount=NULL WHERE id IN(1,2,3,5)",
            f"DELETE FROM {fixture.name}.bid WHERE id IN(4,101); UPDATE {fixture.name}.bid SET event_time='infinity',event_at='infinity',auction=2 WHERE id=6",
            f"UPDATE {fixture.name}.auction SET group_id=3 WHERE id=2",
        ]
        if mode == 'join':
            changes[1] += f"; UPDATE {fixture.name}.auction SET event_time='2024-03-10 03:00:00-07' WHERE id=2"
        for index, change in enumerate(changes,1):
            fixture.sql("SET DateStyle='German,DMY'; SET TimeZone='America/New_York'; BEGIN;"+change+';COMMIT')
            worker.event('published',minimum_time=ready['time']+index)
            fixture.verify()
        worker.abort()
        prior = fixture.sql(f'SELECT row_to_json(p) FROM {fixture.name}.pgderive_progress p')
        fixture.rejected(command,'changed',fixture.query.replace('AS instant','AS changed').replace('AS local','AS changed'))
        assert fixture.sql(f'SELECT row_to_json(p) FROM {fixture.name}.pgderive_progress p') == prior
        fixture.query = fixture.query.replace('b.','"B".').replace('bid b','bid AS "B"')
        resumed = fixture.start(command,'resumed',maximum=1)
        reopened = resumed.event('ready')
        assert reopened['slot']==ready['slot'] and reopened['time']==ready['time']+len(changes)
        fixture.verify()
        fixture.sql(f"UPDATE {fixture.name}.bid SET event_time='1999-12-31 23:59:59.999999+00',event_at='1999-12-31 23:59:59.999999',amount=9223372036854775806.6666666666666667 WHERE id=100")
        resumed.event('published')
        resumed.finish()
        fixture.verify()
        (output/'result.json').write_text(json.dumps({'mode':mode,'sql_oracle':'exact bag','memory_oracle':'native scalar/partition recomputation','timestamp_range':'BC through 294276, infinities','restart':'cold','numeric':'exact and special values'})+'\n')
        fixture.cleanup()
    except BaseException:
        fixture.preserve()
        raise


if __name__ == '__main__':
    output = pathlib.Path(sys.argv[1])
    command = sys.argv[2:]
    if command and command[0]=='--':
        command = command[1:]
    for mode in ('projection','grouped','rows','join'):
        directory = output/mode
        directory.mkdir()
        qualify(directory,command,mode)
