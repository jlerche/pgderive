#!/usr/bin/env python3
"""Database-wide semantic leases and durable rebuild revisions, in an owned DB."""
import json
import os
import subprocess
import time
from pathlib import Path
from query_binding_harness import BindingFixture
from worker_harness import harness_arguments
from harness_control import admin


class EnvironmentFixture(BindingFixture):
    def cleanup(self):
        for worker in self.workers:
            worker.abort()
        slots = self.sql("SELECT slot_name FROM pg_replication_slots WHERE database=current_database()").splitlines()
        for slot in slots:
            self.sql(f"SELECT pg_drop_replication_slot('{slot}')")
        self.sql(f'DROP PUBLICATION {self.name}; DROP SCHEMA {self.name} CASCADE')


def command(fixture, binary, scope, action, success=True):
    return admin(fixture, [str(binary)], action, scope=scope, success=success, timeout=180)


def validator_exclusion(fixture, admin):
    environment = {**fixture.environment, 'PGAPPNAME': 'pgderive-maintenance-validator'}
    with (fixture.output/'validator.log').open('w') as log:
        validator = subprocess.Popen(fixture.pg + [
            "SELECT pg_advisory_lock_shared(hashtextextended('pgderive-semantic-environment-v1',0)); SELECT pg_sleep(60)"],
            env=environment, stdout=log, stderr=log)
        try:
            for _ in range(100):
                if fixture.sql("SELECT count(*) FROM pg_locks l JOIN pg_stat_activity a USING(pid) WHERE a.application_name='pgderive-maintenance-validator' AND l.locktype='advisory' AND l.mode='ShareLock' AND l.granted").strip() == '1':
                    break
                time.sleep(.02)
            else:
                raise AssertionError('validator lease not witnessed')
            assert 'all workers and validators' in command(fixture, admin, 'environment', 'begin', False)
        finally:
            fixture.sql("SELECT pg_terminate_backend(pid) FROM pg_stat_activity WHERE application_name='pgderive-maintenance-validator'")
            validator.wait(timeout=10)


def qualify(output, worker_command, database):
    fixture = EnvironmentFixture(output, 'expression', database)
    admin = Path(worker_command[0]).with_name('pgderive')
    with fixture.managed():
        worker = fixture.start(worker_command, 'first')
        worker.event('ready')
        fixture.verify()
        before = fixture.boundary()
        error = command(fixture, admin, 'environment', 'begin', False)
        assert 'all workers and validators' in error
        assert fixture.boundary() == before
        assert fixture.sql("SELECT to_regclass('pgderive_environment.revision') IS NULL").strip() == 't'
        fixture.sql(f'UPDATE {fixture.name}.bid SET price=price+1 WHERE id=1')
        worker.event('published')
        worker.abort()
        before = fixture.boundary()
        validator_exclusion(fixture, admin)
        first = command(fixture, admin, 'environment', 'begin')
        assert first['revision'] == 1 and first['pending'] is True
        assert command(fixture, admin, 'environment', 'begin') == first
        pending = fixture.start(worker_command, 'pending')
        pending.process.wait(timeout=60)
        assert pending.process.returncode != 0
        pending.abort()
        assert 'maintenance is pending' in (output/'pending/stderr.log').read_text()
        assert fixture.boundary() == before
        assert 'maintenance is pending' in command(fixture, admin, 'query', 'rebuild', False)
        assert 'maintenance is pending' in command(fixture, admin, 'query', 'validate', False)
        # Catalog contents can be restored, but the revision still forbids old state.
        fixture.sql("BEGIN; UPDATE pg_proc SET procost=procost+1 WHERE oid='pg_catalog.abs(bigint)'::regprocedure; UPDATE pg_proc SET procost=procost-1 WHERE oid='pg_catalog.abs(bigint)'::regprocedure; COMMIT")
        fixture.sql(f'ALTER TABLE {fixture.name}.bid ADD COLUMN note text; UPDATE {fixture.name}.bid SET note=\'new\',price=price+3 WHERE id=2')
        finished = command(fixture, admin, 'environment', 'finish')
        assert finished['pending'] is False and finished['revision'] == 1
        assert command(fixture, admin, 'environment', 'finish') == finished
        fixture.failed(fixture.start(worker_command, 'stale'), 'stale')
        assert fixture.boundary() == before
        assert fixture.acknowledgement_ahead().strip() == '0'
        # Remove layout change so rejection is independently established by epoch alone.
        fixture.sql(f'ALTER TABLE {fixture.name}.bid DROP COLUMN note')
        fixture.failed(fixture.start(worker_command, 'stale-epoch'), 'stale-epoch')
        assert fixture.boundary() == before
        command(fixture, admin, 'query', 'rebuild')
        fixture.verify()
        resumed = fixture.start(worker_command, 'resumed', maximum=1)
        resumed.event('ready')
        fixture.sql(f'UPDATE {fixture.name}.bid SET price=price+2 WHERE id=3')
        resumed.event('published')
        resumed.finish()
        fixture.verify()
        assert command(fixture, admin, 'environment', 'begin')['revision'] == 2
        command(fixture, admin, 'environment', 'finish')
        fixture.failed(fixture.start(worker_command, 'stale-again'), 'stale-again')
        (output/'result.json').write_text(json.dumps({'active_workers':'exclusive maintenance refused',
            'pending':'startup and rebuild rejected', 'revision':'monotonic and idempotent',
            'old_state':'rejected even after catalog restore', 'durability':'sink/progress/ACK unchanged',
            'rebuild':'fresh snapshot, compatible public table, resumed CDC', 'database_scope':True})+'\n')


def main():
    output, worker_command = harness_arguments()
    database = f'pgderive_environment_{os.getpid()}'
    port = os.environ.get('PGDERIVE_TEST_POSTGRES_PORT', '55434')
    pg = ['psql','-h','127.0.0.1','-p',port,'-U','postgres','-d','postgres','-v','ON_ERROR_STOP=1','-Atc']
    environment = {**os.environ, 'PGPASSWORD':os.environ.get('PGPASSWORD','postgres')}
    subprocess.run(pg+[f'CREATE DATABASE {database} TEMPLATE template0'], env=environment, check=True)
    (output/'database.json').write_text(json.dumps({'owned_database':database,'port':port})+'\n')
    try:
        qualify(output, worker_command, database)
    except BaseException:
        print('Preserving environment failure database:', database, flush=True)
        raise
    else:
        subprocess.run(pg+[f'DROP DATABASE {database}'], env=environment, check=True)


if __name__ == '__main__':
    main()
