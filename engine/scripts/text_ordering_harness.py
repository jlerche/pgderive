#!/usr/bin/env python3
"""Compare text extrema and wider ordering with PostgreSQL through retractions."""
import json

from native_mv_oracle import binary_difference, expected
from sql_compiler_harness import SqlFixture
from worker_harness import History, run_modes


class OrderingFixture(SqlFixture):
    def __init__(self, output, mode):
        super().__init__(output, False)
        self.mode = mode
        self.source = self.name + '.bid'
        self.sql(f'''ALTER TABLE {self.source}
            ADD COLUMN payload varchar(12) COLLATE "C",
            ADD COLUMN duration interval, ADD COLUMN observed date''')
        self.sql(f'DELETE FROM {self.source}')
        if mode == 'extrema':
            self.labels = ['auction', 'lo', 'hi']
            self.query = f'''SELECT auction, min(DISTINCT payload) AS lo,
                max(payload) FILTER (WHERE id <> 104) AS hi
                FROM {self.source} GROUP BY auction'''
        elif mode == 'date-extrema':
            self.labels = ['auction', 'lo', 'hi']
            self.query = f'''SELECT auction, min(DISTINCT observed) AS lo,
                max(observed) FILTER (WHERE id <> 104) AS hi
                FROM {self.source} GROUP BY auction'''
        elif mode == 'interval-distinct':
            self.labels = ['id', 'duration']
            self.query = f'''SELECT DISTINCT ON (duration) id, duration FROM {self.source}
                ORDER BY duration DESC NULLS LAST, id'''
        elif mode == 'numeric-range':
            self.labels = ['id', 'n', 'wide']
            self.query = f'''SELECT id,
                count(*) OVER (ORDER BY price::numeric / 2 DESC NULLS FIRST
                    RANGE BETWEEN 0.5 PRECEDING AND 1.5 FOLLOWING) AS n,
                count(*) OVER (ORDER BY CASE WHEN id=100 THEN '-Infinity'::numeric
                    WHEN id=101 THEN 'Infinity'::numeric WHEN id=102 THEN 'NaN'::numeric
                    ELSE price::numeric END DESC NULLS LAST
                    RANGE BETWEEN 'Infinity'::numeric PRECEDING AND 'Infinity'::numeric FOLLOWING)
                    AS wide FROM {self.source}'''
        elif mode == 'window':
            self.labels = ['id', 'r', 'lo']
            self.query = f'''SELECT id, rank() OVER (ORDER BY payload DESC NULLS LAST) AS r,
                min(payload) OVER (ORDER BY duration NULLS FIRST GROUPS
                    BETWEEN CURRENT ROW AND CURRENT ROW) AS lo FROM {self.source}'''
        else:
            self.labels = ['id', 'duration']
            self.query = f'''SELECT id, duration FROM {self.source}
                ORDER BY duration DESC NULLS LAST FETCH FIRST 2 ROWS WITH TIES'''

    def verify(self):
        self.verify_sql(expected(self, self.labels))
        assert binary_difference(self) == '0'
        if self.mode in ('extrema', 'date-extrema'):
            columns = self.sql(f"""SELECT attname,atttypid,atttypmod,attcollation
                FROM pg_attribute WHERE attrelid IN ('{self.name}.groups'::regclass,
                    '{self.name}.pgderive_native_oracle'::regclass)
                    AND attname IN ('lo','hi') ORDER BY attname,attrelid""").splitlines()
            assert len(columns) == 4 and columns[0] == columns[1] and columns[2] == columns[3], columns


def qualify(output, command, mode):
    fixture = OrderingFixture(output, mode)
    source = fixture.source
    with fixture.managed():
        fixture.run_history(command, History(
            changes=[
                f"""INSERT INTO {source}(id,auction,payload,duration,price) VALUES
                    (100,1,'a','1 month',1),(101,1,'a','30 days',2),
                    (102,1,'A','1 day',3),(103,2,NULL,NULL,NULL),(104,1,'é','10 hours',4),
                    (105,2,'','-1 day',4),(106,1,'ab','2 months',7);
                    UPDATE {source} SET observed = CASE id
                        WHEN 100 THEN date 'infinity'
                        WHEN 101 THEN date '5874897-12-31'
                        WHEN 102 THEN date '0001-01-01 BC'
                        WHEN 104 THEN date '-infinity'
                        ELSE NULL END""",
                f"""UPDATE {source} SET payload='zz',duration='-2 days',observed=date '2025-01-01' WHERE id=106""",
                f'DELETE FROM {source} WHERE id IN (100,102,105)',
                f'DELETE FROM {source}',
            ],
            final=f"""INSERT INTO {source}(id,auction,payload,duration,price) VALUES
                (107,1,'z','1 day',1),(108,1,NULL,NULL,NULL)""",
            normalized=fixture.query.replace('SELECT', 'select'),
            rejected=[],
        ))
        (output/'result.json').write_text(json.dumps({
            'mode': mode, 'native_binary_and_bag': True, 'cold_restart': True,
        })+'\n')


if __name__ == '__main__':
    run_modes(('extrema', 'window', 'interval-ties', 'date-extrema', 'interval-distinct', 'numeric-range'), qualify)
