#!/usr/bin/env python3
"""Compare text extrema and wider ordering with PostgreSQL through retractions."""
import json
import os
import subprocess
import uuid

from native_mv_oracle import binary_difference, expected
from sql_compiler_harness import SqlFixture
from worker_case_shard import select_cases
from worker_harness import History, run_modes


class OrderingFixture(SqlFixture):
    def __init__(self, output, mode, database):
        super().__init__(output, False, database=database)
        self.mode = mode
        collation = '"C"'
        if mode.startswith('codepoint-'):
            builtin = self.sql("SELECT to_regcollation('pg_catalog.pg_unicode_fast') IS NOT NULL")
            collation = 'pg_unicode_fast' if builtin.strip() == 't' else 'pg_c_utf8'
        self.source = self.name + '.bid'
        clause = '' if mode.startswith('default-') else f' COLLATE {collation}'
        if mode == 'default-override':
            clause = ' COLLATE pg_catalog."en-US-x-icu"'
        self.sql(f'''ALTER TABLE {self.source}
            ADD COLUMN payload varchar(12){clause},
            ADD COLUMN duration interval, ADD COLUMN observed date''')
        self.sql(f'DELETE FROM {self.source}')
        if mode in ('extrema', 'codepoint-extrema', 'default-extrema'):
            override = ' COLLATE pg_catalog.ucs_basic' if mode == 'codepoint-extrema' else ''
            self.labels = ['auction', 'lo', 'hi']
            self.query = f'''SELECT auction, min(DISTINCT payload) AS lo,
                max(payload{override}) FILTER (WHERE id <> 104) AS hi
                FROM {self.source} GROUP BY auction'''
        elif mode in ('codepoint-topk', 'default-topk'):
            explicit = 'pg_unicode_fast' if collation == 'pg_unicode_fast' else 'ucs_basic'
            order = 'payload' if mode == 'default-topk' else f'payload COLLATE pg_catalog.{explicit}'
            self.labels = ['id', 'payload']
            self.query = f'''SELECT id, payload FROM {self.source}
                ORDER BY {order} DESC NULLS LAST, id LIMIT 3'''
        elif mode == 'default-override':
            default = 'payload COLLATE pg_catalog."default"'
            self.labels = ['id', 'payload', 'r']
            self.query = f"""SELECT id, payload, rank() OVER (ORDER BY {default}) AS r
                FROM {self.source} ORDER BY {default} DESC NULLS LAST, id LIMIT 3"""
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
        elif mode == 'temporal-range':
            self.labels = ['id', 'dates', 'durations', 'unbounded']
            self.query = f'''SELECT id,
                count(*) OVER (ORDER BY CASE WHEN id=101 THEN date '0001-01-01 BC'
                    ELSE observed END NULLS FIRST
                    RANGE BETWEEN interval '1 month' PRECEDING AND CURRENT ROW) AS dates,
                count(*) OVER (ORDER BY duration DESC NULLS LAST
                    RANGE BETWEEN CURRENT ROW AND interval '1 day' FOLLOWING) AS durations,
                count(*) OVER (ORDER BY CASE WHEN id=100 THEN '-infinity'::interval
                    WHEN id=101 THEN 'infinity'::interval ELSE duration END
                    RANGE BETWEEN 'infinity'::interval PRECEDING AND 'infinity'::interval FOLLOWING)
                    AS unbounded FROM {self.source}'''
        elif mode in ('window', 'default-window'):
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
        if self.mode in ('extrema', 'codepoint-extrema', 'default-extrema', 'date-extrema'):
            columns = self.sql(f"""SELECT attname,atttypid,atttypmod,attcollation
                FROM pg_attribute WHERE attrelid IN ('{self.name}.groups'::regclass,
                    '{self.name}.pgderive_native_oracle'::regclass)
                    AND attname IN ('lo','hi') ORDER BY attname,attrelid""").splitlines()
            assert len(columns) == 4 and columns[0] == columns[1] and columns[2] == columns[3], columns


def qualify(output, command, mode):
    if not mode.startswith('default-'):
        return run_history(output, command, mode, 'pgderive_dev')
    database = f'pgderive_ordering_{uuid.uuid4().hex[:16]}'
    port = os.environ.get('PGDERIVE_TEST_POSTGRES_PORT', '55434')
    pg = ['psql', '-h', '127.0.0.1', '-p', port, '-U', 'postgres',
          '-d', 'postgres', '-v', 'ON_ERROR_STOP=1', '-Atc']
    environment = {**os.environ, 'PGPASSWORD': os.environ.get('PGPASSWORD', 'postgres')}
    if mode == 'default-extrema':
        locale = "LOCALE_PROVIDER libc LOCALE 'C'"
    else:
        version = int(subprocess.check_output(pg + ['SHOW server_version_num'], env=environment))
        name = 'PG_UNICODE_FAST' if mode == 'default-topk' and version >= 180000 else 'C.UTF-8'
        locale = f"LOCALE_PROVIDER builtin BUILTIN_LOCALE '{name}'"
    subprocess.run(pg + [f"CREATE DATABASE {database} TEMPLATE template0 ENCODING 'UTF8' {locale}"],
                   env=environment, check=True)
    (output/'database.json').write_text(json.dumps({'owned_database': database, 'port': port})+'\n')
    try:
        run_history(output, command, mode, database)
    except BaseException:
        print('Preserving ordering failure database:', database, flush=True)
        raise
    else:
        subprocess.run(pg + [f'DROP DATABASE {database}'], env=environment, check=True)


def run_history(output, command, mode, database):
    fixture = OrderingFixture(output, mode, database)
    source = fixture.source
    with fixture.managed():
        fixture.run_history(command, History(
            changes=[
                f"""INSERT INTO {source}(id,auction,payload,duration,price) VALUES
                    (100,1,'a','1 month',1),(101,1,'a','30 days',2),
                    (102,1,'A','1 day',3),(103,2,NULL,NULL,NULL),(104,1,'é','10 hours',4),
                    (105,2,'','-1 day',4),(106,1,'ab','2 months',7),
                    (109,1,U&'e\\0301','3 days',8),(110,1,U&'\\+01F600','4 days',9);
                    UPDATE {source} SET observed = CASE id
                        WHEN 100 THEN date 'infinity'
                        WHEN 101 THEN date '5874897-12-31'
                        WHEN 102 THEN date '0001-01-01 BC'
                        WHEN 104 THEN date '-infinity'
                        ELSE NULL END""",
                f"""UPDATE {source} SET payload='zz',duration='-2 days',observed=date '2025-01-01' WHERE id IN (106,110)""",
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
    modes = (
        'extrema', 'codepoint-extrema', 'codepoint-topk', 'window', 'interval-ties',
        'date-extrema', 'interval-distinct', 'numeric-range', 'temporal-range',
        'default-extrema', 'default-topk', 'default-window', 'default-override',
    )
    run_modes(select_cases(modes), qualify)
