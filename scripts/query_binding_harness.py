#!/usr/bin/env python3
"""Binding drift and atomic failure tests in a separately owned database.

Catalog mutation is confined to this database. Failures retain it and all logs;
never alter shared pgderive_dev built-ins, even for a rollback-only test.
"""
import json
import os
import subprocess
from worker_harness import Fixture, Worker, harness_arguments


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
        self.terminal = mode in ('terminal', 'expression')
        if mode == 'varchar':
            self.sql(f'ALTER TABLE {self.name}.bid ADD COLUMN note varchar(12)')

    def config(self, maximum=0):
        path = super().config(maximum)
        expression = 'abs(price)' if self.terminal else 'price'
        text = path.read_text().split('[worker.query]')[0]
        text += f'[worker.query]\nsql="SELECT {expression} AS value FROM {self.name}.bid"\n'
        path.write_text(text.replace('retry_attempts=8', 'retry_attempts=1'))
        return path

    def verify(self):
        expression = 'abs(price)' if self.terminal else 'price'
        expected = f'SELECT jsonb_build_array(NULL,jsonb_build_array({expression})) tuple,count(*) weight FROM {self.name}.bid GROUP BY {expression}'
        assert self.bag_difference(expected).strip() == '0'
        assert self.acknowledgement_ahead().strip() == '0'

    def boundary(self):
        return self.sql(f'''SELECT jsonb_build_object(
            'progress',(SELECT to_jsonb(p)-'fence' FROM {self.name}.pgderive_progress p),
            'queries',(SELECT jsonb_agg(to_jsonb(q) ORDER BY query_id) FROM {self.name}.pgderive_queries q),
            'members',(SELECT jsonb_agg(to_jsonb(a) ORDER BY query_id,arrangement_id) FROM {self.name}.pgderive_arrangements a),
            'sink',(SELECT jsonb_agg(to_jsonb(g) ORDER BY tuple) FROM {self.name}.groups g))''')

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
            change = "UPDATE pg_catalog.pg_proc SET proleakproof=NOT proleakproof WHERE oid='pg_catalog.abs(bigint)'::regprocedure"
            restore = change
        elif mode == 'bootstrap-helper':
            change = "UPDATE pg_catalog.pg_proc SET proisstrict=NOT proisstrict WHERE oid='pg_catalog.octet_length(text)'::regprocedure"
            restore = change
        else:
            change = "UPDATE pg_catalog.pg_proc SET proisstrict=NOT proisstrict WHERE oid='pg_catalog.jsonb_array_elements(jsonb)'::regprocedure"
            restore = change
        if mode == 'terminal':
            fixture.sql(f'''CREATE FUNCTION {fixture.name}.change_helper() RETURNS trigger LANGUAGE plpgsql AS $$
                BEGIN {change}; RETURN NEW; END $$;
                CREATE TRIGGER change_helper BEFORE UPDATE ON {fixture.name}.pgderive_progress
                FOR EACH ROW EXECUTE FUNCTION {fixture.name}.change_helper()''')
        else:
            fixture.sql(change)
        fixture.sql(f'UPDATE {fixture.name}.bid SET price=price+10 WHERE id=1')
        fixture.failed(worker, 'first')
        assert fixture.boundary() == before, 'failed publication changed durable state'
        assert fixture.sql(f"SELECT confirmed_flush_lsn FROM pg_replication_slots WHERE slot_name='{ready['slot']}'") == acknowledged
        if mode == 'terminal':
            fixture.sql(f'DROP TRIGGER change_helper ON {fixture.name}.pgderive_progress; DROP FUNCTION {fixture.name}.change_helper()')
            assert fixture.sql("SELECT proisstrict FROM pg_catalog.pg_proc WHERE oid='pg_catalog.jsonb_array_elements(jsonb)'::regprocedure").strip() == 't', 'helper mutation escaped rollback'
        else:
            rejected = fixture.start(command, 'incompatible-restart')
            fixture.failed(rejected, 'incompatible-restart')
            assert fixture.boundary() == before
            fixture.sql(restore)
        resumed = fixture.start(command, 'resumed', maximum=1)
        resumed.event('published', minimum_time=ready['time']+1)
        resumed.finish()
        fixture.verify()
        (output/'result.json').write_text(json.dumps({'mode':mode,'publication_rejected':True,
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


def main():
    output, command = harness_arguments()
    database = f'pgderive_binding_{os.getpid()}'
    port = os.environ.get('PGDERIVE_TEST_POSTGRES_PORT', '55434')
    control = ['psql','-h','127.0.0.1','-p',port,'-U','postgres','-d','postgres','-v','ON_ERROR_STOP=1','-Atc']
    environment = {**os.environ, 'PGPASSWORD':os.environ.get('PGPASSWORD','postgres')}
    subprocess.run(control+[f'CREATE DATABASE {database} TEMPLATE template0'], env=environment, check=True)
    (output/'database.json').write_text(json.dumps({'owned_database':database,'port':port})+'\n')
    try:
        for mode in ['type','varchar','helper','bootstrap-helper','expression','terminal',*LOCAL_FUNCTIONS,'aggregate-sort']:
            directory = output/mode
            directory.mkdir()
            qualify(directory, command, mode, database)
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
