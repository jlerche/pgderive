#!/usr/bin/env python3
"""Initialization binding drift and steady-state inspection tests in a separately owned database.

Catalog mutation is confined to this database. Failures retain it and all logs;
never alter shared pgderive_dev built-ins, even for a rollback-only test.
"""
import json
import os
import subprocess
import sys
import time
from worker_harness import Fixture, harness_arguments


LOCAL_FUNCTIONS = {
    'local-bin': "'pg_catalog.date_bin(interval,timestamp,timestamp)'::regprocedure",
    'local-mod': "'pg_catalog.mod(bigint,bigint)'::regprocedure",
    'local-aggregate': "'pg_catalog.count()'::regprocedure",
    'local-window': "'pg_catalog.row_number()'::regprocedure",
    'planner-support': "(SELECT prosupport FROM pg_catalog.pg_proc WHERE oid='pg_catalog.generate_series(integer,integer)'::regprocedure)",
    'aggregate-helper': "(SELECT aggtransfn FROM pg_catalog.pg_aggregate WHERE aggfnoid='pg_catalog.count()'::regprocedure)",
    'interval-operator': "'pg_catalog.interval_mul(interval,double precision)'::regprocedure",
    'timestamp-interval': "'pg_catalog.timestamp_mi_interval(timestamp,interval)'::regprocedure",
    'float-coercion': "'pg_catalog.float8(integer)'::regprocedure",
}


class BindingFixture(Fixture):
    def __init__(self, output, mode, database):
        super().__init__(output, 'binding_'+mode.replace('-', '_'), rows=8, auctions=2, database=database)
        self.computed = mode in ('expression', 'hot-computed')
        if mode == 'varchar':
            self.sql(f'ALTER TABLE {self.name}.bid ADD COLUMN note varchar(12)')

    def config(self, maximum=0):
        path = super().config(maximum)
        expression = 'price+1' if self.computed else 'price'
        text = path.read_text().split('[worker.query]')[0]
        text += f'[worker.query]\nsql="SELECT {expression} AS value FROM {self.name}.bid"\n'
        path.write_text(text.replace('retry_attempts=8', 'retry_attempts=1'))
        return path

    def verify(self):
        expression = 'price+1' if self.computed else 'price'
        expected = f'SELECT jsonb_build_array(NULL,jsonb_build_array({expression})) tuple,count(*) weight FROM {self.name}.bid GROUP BY {expression}'
        assert self.bag_difference(expected).strip() == '0'
        assert self.acknowledgement_ahead().strip() == '0'

    def boundary(self):
        return self.sql(f'''SELECT jsonb_build_object(
            'progress',(SELECT to_jsonb(p)-'fence' FROM {self.name}.pgderive_progress p),
            'queries',(SELECT jsonb_agg(to_jsonb(q) ORDER BY query_id) FROM {self.name}.pgderive_queries q),
            'members',(SELECT jsonb_agg(to_jsonb(a) ORDER BY query_id,arrangement_id) FROM {self.name}.pgderive_arrangements a),
            'sink',(SELECT jsonb_agg(to_jsonb(g) ORDER BY tuple) FROM {self.bag_relation()} g))''')

    def failed(self, worker, label):
        worker.process.wait(timeout=60)
        assert worker.process.returncode != 0, label+' unexpectedly succeeded'
        worker.abort()
        error = (self.output/label/'stderr.log').read_text()
        assert 'contract changed' in error or 'environment changed' in error or 'destination changed' in error, error


def qualify(output, command, mode, database):
    fixture = BindingFixture(output, mode, database)
    with fixture.managed():
        worker = fixture.start(command, 'first')
        ready = worker.event('ready')
        fixture.verify()
        worker.abort()  # Quiesce before administrative semantic changes.
        before = fixture.boundary()
        acknowledged = fixture.sql(f"SELECT confirmed_flush_lsn FROM pg_replication_slots WHERE slot_name='{ready['slot']}'")
        if mode in ('type', 'varchar'):
            oid = 1043 if mode == 'varchar' else 20
            change = f"UPDATE pg_catalog.pg_type SET typdelim=';' WHERE oid={oid}"
            restore = f"UPDATE pg_catalog.pg_type SET typdelim=',' WHERE oid={oid}"
        elif mode in LOCAL_FUNCTIONS:
            change = f"UPDATE pg_catalog.pg_proc SET proleakproof=NOT proleakproof WHERE oid={LOCAL_FUNCTIONS[mode]}"
            restore = change
        elif mode == 'aggregate-sort':
            change = "UPDATE pg_catalog.pg_operator SET oprcanmerge=NOT oprcanmerge WHERE oid=(SELECT aggsortop FROM pg_catalog.pg_aggregate WHERE aggfnoid='pg_catalog.min(anyarray)'::regprocedure)"
            restore = change
        elif mode == 'expression':
            change = "UPDATE pg_catalog.pg_proc SET proleakproof=NOT proleakproof WHERE oid='pg_catalog.int8pl(bigint,bigint)'::regprocedure"
            restore = change
        elif mode == 'bootstrap-helper':
            change = "UPDATE pg_catalog.pg_proc SET proisstrict=NOT proisstrict WHERE oid='pg_catalog.octet_length(text)'::regprocedure"
            restore = change
        else:
            change = "UPDATE pg_catalog.pg_proc SET proisstrict=NOT proisstrict WHERE oid='pg_catalog.jsonb_array_elements(jsonb)'::regprocedure"
            restore = change
        fixture.sql(change)
        fixture.sql(f'UPDATE {fixture.name}.bid SET price=price+10 WHERE id=1')
        rejected = fixture.start(command, 'incompatible-restart')
        fixture.failed(rejected, 'incompatible-restart')
        assert fixture.boundary() == before, 'rejected initialization changed durable state'
        assert fixture.sql(f"SELECT confirmed_flush_lsn FROM pg_replication_slots WHERE slot_name='{ready['slot']}'") == acknowledged
        fixture.sql(restore)
        resumed = fixture.start(command, 'resumed', maximum=1)
        reopened = resumed.event('ready')
        resumed.event('published', minimum_time=reopened['time']+1)
        resumed.finish()
        fixture.verify()
        (output/'result.json').write_text(json.dumps({'mode':mode,'initialization_rejected':True,
            'unchanged_sink_membership_progress_ack':True,'restored_environment_replayed':True,
            'database':database})+'\n')



def context(output, command, database):
    fixture = BindingFixture(output, 'context', database)
    with fixture.managed():
        fixture.sql(f"ALTER DATABASE {database} SET search_path TO public,pg_catalog; ALTER DATABASE {database} SET TimeZone='America/Los_Angeles'; ALTER DATABASE {database} SET DateStyle='SQL,DMY'; ALTER DATABASE {database} SET IntervalStyle='postgres'; ALTER DATABASE {database} SET extra_float_digits=-15")
        fixture.sql("CREATE FUNCTION public.current_setting(text) RETURNS text LANGUAGE sql IMMUTABLE AS $$ SELECT 'shadow'::text $$")
        worker = fixture.start(command, 'first', maximum=1)
        worker.event('ready')
        fixture.sql(f"""CREATE FUNCTION {fixture.name}.assert_context() RETURNS trigger LANGUAGE plpgsql AS $$
            BEGIN
              IF pg_catalog.current_setting('search_path') <> 'pg_catalog'
                OR pg_catalog.current_setting('TimeZone') <> 'UTC'
                OR pg_catalog.current_setting('DateStyle') <> 'ISO, MDY'
                OR pg_catalog.current_setting('IntervalStyle') <> 'iso_8601'
                OR pg_catalog.current_setting('extra_float_digits') <> '3'
              THEN RAISE EXCEPTION 'uncontrolled publication settings'; END IF;
              RETURN NEW;
            END $$;
            CREATE TRIGGER assert_context BEFORE UPDATE ON {fixture.name}.pgderive_progress
            FOR EACH ROW EXECUTE FUNCTION {fixture.name}.assert_context()""")
        fixture.sql(f'UPDATE {fixture.name}.bid SET price=price+10 WHERE id=1')
        worker.event('published')
        worker.finish()
        fixture.verify()
        (output/'result.json').write_text(json.dumps({'hostile_database_defaults':True,
            'search_path_shadow_ignored':True,'controlled_publication_settings':True})+'\n')


def inspections(path):
    return [json.loads(line)['binding_inspection'] for line in path.read_text().splitlines()
            if 'binding_inspection' in json.loads(line)]


def hot_path(output, command, mode, database):
    fixture = BindingFixture(output, mode, database)
    log = output/'observed.jsonl'
    with log.open('w') as stream:
        proxy = subprocess.Popen([sys.executable, '-u', 'scripts/pg_commit_proxy.py',
            '--mode', 'observe', '--upstream', fixture.port], stdout=stream, stderr=stream)
    try:
        with fixture.managed():
            deadline = time.monotonic()+10
            while time.monotonic() < deadline:
                lines = log.read_text().splitlines()
                if lines:
                    fixture.environment['PGDERIVE__POSTGRES__PORT'] = str(json.loads(lines[0])['port'])
                    break
                time.sleep(.05)
            else:
                raise RuntimeError('binding observer did not become ready')
            worker = fixture.start(command, 'first', maximum=3)
            ready = worker.event('ready')
            initial = inspections(log)
            assert 'catalog' in initial, 'initialization did not inspect catalog'
            assert 'session_configuration' in initial, 'initialization did not configure sessions'
            assert 'terminal_functions' not in initial
            changes = [f'UPDATE {fixture.name}.bid SET price=-3 WHERE id=1',
                       f"UPDATE {fixture.name}.auction SET category='unused' WHERE id=1",
                       f'DELETE FROM {fixture.name}.bid WHERE id=2']
            for tick, change in enumerate(changes, 1):
                fixture.sql(change)
                worker.event('published', minimum_time=ready['time']+tick)
            worker.finish()
            fixture.verify()
            assert inspections(log) == initial, 'publication performed semantic inspection'
            resumed = fixture.start(command, 'resumed', maximum=1)
            resumed.event('ready')
            reopened = inspections(log)
            assert len(reopened) > len(initial), 'restart skipped semantic inspection'
            fixture.sql(f'INSERT INTO {fixture.name}.bid VALUES(99,1,-7)')
            resumed.event('published')
            resumed.finish()
            fixture.verify()
            assert inspections(log) == reopened, 'resumed publication performed semantic inspection'
            (output/'result.json').write_text(json.dumps({'mode':mode,
                'initialization_and_restart_inspections':True,
                'steady_state_semantic_inspections':0,'steady_state_session_configuration':0,'transactions':4})+'\n')
    finally:
        proxy.terminate()
        proxy.wait(timeout=10)


def main():
    output, command = harness_arguments()
    database = f'pgderive_binding_{os.getpid()}'
    port = os.environ.get('PGDERIVE_TEST_POSTGRES_PORT', '55434')
    control = ['psql','-h','127.0.0.1','-p',port,'-U','postgres','-d','postgres','-v','ON_ERROR_STOP=1','-Atc']
    environment = {**os.environ, 'PGPASSWORD':os.environ.get('PGPASSWORD','postgres')}
    subprocess.run(control+[f'CREATE DATABASE {database} TEMPLATE template0'], env=environment, check=True)
    (output/'database.json').write_text(json.dumps({'owned_database':database,'port':port})+'\n')
    try:
        for mode in ['type','varchar','helper','bootstrap-helper','expression',*LOCAL_FUNCTIONS,'aggregate-sort']:
            directory = output/mode
            directory.mkdir()
            qualify(directory, command, mode, database)
        for mode in ['hot-plain','hot-computed']:
            directory = output/mode
            directory.mkdir()
            hot_path(directory, command, mode, database)
        directory = output/'context'
        directory.mkdir()
        context(directory, command, database)
    except BaseException:
        print('Preserving binding failure database:',database, flush=True)
        raise
    else:
        subprocess.run(control+[f'DROP DATABASE {database}'], env=environment, check=True)


if __name__ == '__main__':
    main()
