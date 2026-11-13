#!/usr/bin/env python3
"""Qualify local q14 EXTRACT/CASE; retain the unsupported timezone variant."""
import collections
from decimal import Decimal
import json
import pathlib
from engine_rejection import qualify_unavailable
from native_mv_oracle import expected,binary_difference
from temporal_oracle import parse_civil,extract,encoded
from sql_compiler_harness import SqlFixture
from worker_harness import harness_arguments,History

SQL = pathlib.Path(__file__).resolve().parents[1]/'queries/nexmark/q14.sql'


class Q14Fixture(SqlFixture):
    def __init__(self, output, mode):
        super().__init__(output, False)
        self.mode = mode
        self.pg[1:1] = ['-q','-c',"SET DateStyle='ISO,MDY'; SET TimeZone='UTC'"]
        temporal = 'timestamp' if mode == 'local' else 'timestamptz'
        self.sql(f'ALTER TABLE {self.name}.bid ADD COLUMN bidder bigint, ADD COLUMN date_time {temporal}, ADD COLUMN extra text')
        self.query = SQL.read_text().replace('nexmark.', self.name+'.').strip().removesuffix(';')
        if mode == 'local':
            self.query = self.query.replace("date_time AT TIME ZONE 'UTC'", 'date_time')
            self.sql(f'''UPDATE {self.name}.bid SET bidder=id%4,price=2000000+id,
                date_time=timestamp '2024-03-01'+(ARRAY[6,7,8,18,19,20,23])[1+(id%7)]*interval '1 hour',
                extra=CASE WHEN id%4=0 THEN NULL ELSE 'céc🙂c' END;
                UPDATE {self.name}.bid SET date_time=NULL WHERE id=1;
                UPDATE {self.name}.bid SET date_time='infinity' WHERE id=2;
                UPDATE {self.name}.bid SET date_time='-infinity' WHERE id=3;
                UPDATE {self.name}.bid SET price=NULL WHERE id=4;
                UPDATE {self.name}.bid SET price=1000000 WHERE id=5;
                UPDATE {self.name}.bid SET price=60000000 WHERE id=6''')
        self.labels = ['auction','bidder','price','bidTimeType','date_time','extra','c_counts']

    def verify(self):
        self.verify_sql(expected(self,self.labels))
        assert binary_difference(self) == '0','q14 native representation differs'
        attrs = "SELECT json_agg(json_build_array(attname,atttypid,atttypmod,attcollation) ORDER BY attnum) FROM pg_attribute WHERE attrelid='%s'::regclass AND attnum>0 AND NOT attisdropped"
        assert self.sql(attrs % (self.name+'.groups')) == self.sql(attrs % (self.name+'.pgderive_native_oracle'))
        rows = json.loads(self.sql(f"SELECT json_agg(r) FROM(SELECT auction,bidder,price,date_time::text,extra FROM {self.name}.bid) r"))
        memory = collections.Counter()
        for row in rows:
            if row['price'] is None:
                continue
            price = Decimal('0.908')*row['price']
            if not Decimal(1000000) < price < Decimal(50000000):
                continue
            stamp = parse_civil(row['date_time'])
            hour = extract(stamp,'stamp','hour')
            label = 'otherTime' if hour is None else 'dayTime' if 8 <= hour <= 18 else 'nightTime' if hour <= 6 or hour >= 20 else 'otherTime'
            extra = row['extra']
            memory[(row['auction'],row['bidder'],price,label,encoded(stamp,'stamp'),extra,None if extra is None else extra.count('c'))] += 1
        self.verify_memory(dict(memory),parse_float=Decimal)


def qualify_local(fixture, command):
    b = fixture.name+'.bid'
    with fixture.managed():
        changes = [
            f"UPDATE {b} SET date_time='2024-03-10 08:59:59.999999',price=3000000,extra='ccc' WHERE id=1",
            f"UPDATE {b} SET date_time='2024-03-10 19:00:00' WHERE id=1; UPDATE {b} SET price=900000 WHERE id=2; UPDATE {b} SET date_time='2024-03-10 20:00:00' WHERE id=3",
            f'DELETE FROM {b} WHERE id IN(3,7); UPDATE {b} SET extra=NULL WHERE id=1',
            f"INSERT INTO {b}(id,auction,price,bidder,date_time,extra) VALUES(100,1,2000000,9,'2024-03-10 06:00:00','c')",
        ]
        fixture.run_history(command,History(changes=changes,
            final=f"UPDATE {b} SET date_time='2024-10-31 07:00:00',price=3000000 WHERE id=100",
            normalized=fixture.query.replace('SELECT','select'),rejected=[]))
        (fixture.output/'result.json').write_text(json.dumps({'mode':'local','native_mv_send_metadata':True,
            'decimal_memory':True,'local_extract_case':True,'cold_restart':True})+'\n')


if __name__ == '__main__':
    output, command = harness_arguments()
    for mode in ('local', 'instant'):
        directory = output/mode
        directory.mkdir()
        fixture = Q14Fixture(directory,mode)
        if mode == 'local':
            qualify_local(fixture,command)
        else:
            qualify_unavailable(fixture,command)
