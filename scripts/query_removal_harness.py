#!/usr/bin/env python3
"""Sequential owned-fixture retirement faults and destructive-ownership checks."""
import json
import subprocess
import sys
import time

from query_lifecycle_harness import configured, grouped, inspect, rejected, verify_result
from worker_harness import Worker, harness_arguments


def setup(output, command, public=True):
    fixture = grouped(output)
    config = configured(fixture) if public else fixture.config()
    validated = inspect(command, fixture, 'validate', config)
    logs = output/'worker'
    logs.mkdir()
    worker = Worker(command+['query', 'create'], config, logs, fixture.environment)
    fixture.workers.append(worker)
    worker.event('ready')
    if public:
        verify_result(fixture, [column['name'] for column in validated['columns']])
    else:
        fixture.verify()
    worker.finish(graceful=True)
    return fixture, config


def cleanup(fixture):
    for slot in fixture.sql(f"SELECT slot_name FROM pg_replication_slots WHERE slot_name LIKE '{fixture.name}%'").splitlines():
        fixture.sql(f"SELECT pg_drop_replication_slot('{slot}')")
    fixture.sql(f'DROP PUBLICATION {fixture.name}; DROP SCHEMA {fixture.name} CASCADE')


def wait_records(path, predicate):
    deadline = time.monotonic()+20
    while time.monotonic() < deadline:
        records = [json.loads(line) for line in path.read_text().splitlines()]
        if predicate(records):
            return records
        time.sleep(.05)
    raise AssertionError(f'fault witness missing: {path}')


def fault(output, command, stage, mode):
    fixture, config = setup(output, command)
    proxy = None
    source_before = [fixture.sql(f'SELECT jsonb_agg(to_jsonb(t) ORDER BY id) FROM {fixture.name}.{table} t') for table in ('auction', 'bid')]
    try:
        marker = 'pgderive_removals VALUES' if stage == 'intent' else 'pgderive_removals SET state'
        gate = output/'release'
        evidence = output/'fault.jsonl'
        with evidence.open('w') as log:
            args = [sys.executable, '-u', 'scripts/pg_commit_proxy.py', '--mode', mode,
                    '--once', '--arm-query', marker]
            if mode == 'hold':
                args += ['--gate', str(gate)]
            proxy = subprocess.Popen(args, stdout=log, stderr=log)
        records = wait_records(evidence, lambda rows: rows and rows[0].get('ready'))
        fixture.environment['PGDERIVE__POSTGRES__PORT'] = str(records[0]['port'])
        if mode == 'hold':
            with (output/'drop.stdout').open('w') as stdout, (output/'drop.stderr').open('w') as stderr:
                process = subprocess.Popen(command+['query', 'drop', str(config)],
                                           env=fixture.environment, stdout=stdout, stderr=stderr)
                try:
                    wait_records(evidence, lambda rows: any(row.get('fault') == 'held_commit' for row in rows))
                    process.kill()
                    process.wait(timeout=10)
                    gate.touch()
                    wait_records(evidence, lambda rows: any(row.get('fault') == 'released_commit' for row in rows))
                finally:
                    if process.poll() is None:
                        process.kill()
                        process.wait(timeout=10)
            deadline = time.monotonic()+5
            while fixture.sql(f"SELECT pg_try_advisory_lock(hashtextextended('pgderive-worker:{fixture.name}:grouped',0))").strip() != 't':
                assert time.monotonic() < deadline, 'killed control connection still owns query'
                time.sleep(.05)
        else:
            rejected(command, fixture, 'drop', config)
            wait_records(evidence, lambda rows: any(row.get('fault') == ('before_commit' if mode == 'before' else 'lost_commit_response') for row in rows))
        fixture.environment.pop('PGDERIVE__POSTGRES__PORT')
        state = inspect(command, fixture, 'status', config)['state']
        expected = 'activated' if stage == 'intent' and mode == 'before' else (
            'dropped' if stage == 'finish' and mode != 'before' else 'dropping')
        assert state == expected, (stage, mode, state)
        if state != 'dropped':
            fixture.verify()
        assert inspect(command, fixture, 'drop', config)['state'] == 'dropped'
        assert inspect(command, fixture, 'drop', config)['state'] == 'dropped'
        assert fixture.sql(f"SELECT count(*) FROM pg_replication_slots WHERE slot_name LIKE '{fixture.name}%'").strip() == '0'
        assert fixture.sql(f'SELECT count(*) FROM {fixture.name}.pgderive_queries').strip() == '0'
        assert [fixture.sql(f'SELECT jsonb_agg(to_jsonb(t) ORDER BY id) FROM {fixture.name}.{table} t') for table in ('auction', 'bid')] == source_before, 'removal fault mutated sources'
        (output/'qualification.json').write_text(json.dumps({'stage': stage, 'fault': mode,
            'witness': str(evidence), 'observed_state': state, 'retry': 'dropped'})+'\n')
        cleanup(fixture)
    except BaseException:
        fixture.preserve()
        raise
    finally:
        if proxy is not None:
            proxy.terminate()
            proxy.wait(timeout=10)


def competing_sink(output, command):
    fixture, config = setup(output, command, public=False)
    try:
        before = fixture.sql(f'SELECT * FROM {fixture.name}.groups ORDER BY group_key')
        other = output/'other.toml'
        other.write_text(config.read_text().replace('query_id="grouped"', 'query_id="other"').replace(
            f'slot="{fixture.name}"', f'slot="{fixture.name}_other"').replace('retry_attempts=8', 'retry_attempts=1'))
        assert 'destination belongs to another query' in rejected(command, fixture, 'create', other)
        assert inspect(command, fixture, 'drop', other)['state'] == 'dropped'
        assert fixture.sql(f'SELECT * FROM {fixture.name}.groups ORDER BY group_key') == before
        fixture.verify()
        inspect(command, fixture, 'drop', config)
        (output/'qualification.json').write_text(json.dumps({'unactivated_competitor': 'removed', 'active_sink': 'preserved'})+'\n')
        cleanup(fixture)
    except BaseException:
        fixture.preserve()
        raise


def unowned_sink(output, command):
    fixture = grouped(output)
    try:
        fixture.sql(f'CREATE TABLE {fixture.name}.groups(group_key jsonb PRIMARY KEY,row_count bigint,total bigint)')
        oid = fixture.sql(f"SELECT '{fixture.name}.groups'::regclass::oid")
        config = fixture.config()
        config.write_text(config.read_text().replace('retry_attempts=8', 'retry_attempts=1'))
        rejected(command, fixture, 'create', config)
        assert fixture.sql(f'SELECT count(*) FROM {fixture.name}.pgderive_progress').strip() == '0'
        inspect(command, fixture, 'drop', config)
        assert fixture.sql(f"SELECT '{fixture.name}.groups'::regclass::oid") == oid
        (output/'qualification.json').write_text(json.dumps({'preexisting_empty_table': 'never adopted or removed', 'oid': oid.strip()})+'\n')
        cleanup(fixture)
    except BaseException:
        fixture.preserve()
        raise


def guarded_drop(output, command, kind):
    fixture, config = setup(output, command, public=kind != 'replaced-sink')
    try:
        if kind == 'dependent-view':
            fixture.sql(f'CREATE VIEW {fixture.name}.consumer AS SELECT * FROM {fixture.name}.result')
            assert 'depend' in rejected(command, fixture, 'drop', config)
            assert inspect(command, fixture, 'status', config)['state'] == 'dropping'
            fixture.sql(f'DROP VIEW {fixture.name}.consumer')
        elif kind == 'altered-view':
            column = fixture.sql(f"SELECT attname FROM pg_attribute WHERE attrelid='{fixture.name}.result'::regclass AND attnum=1").strip()
            quote = '"'+column.replace('"', '""')+'"'
            fixture.sql(f'ALTER VIEW {fixture.name}.result RENAME COLUMN {quote} TO tampered')
            assert 'removal result view was altered' in rejected(command, fixture, 'drop', config)
            assert inspect(command, fixture, 'status', config)['state'] == 'dropping'
            fixture.sql(f'ALTER VIEW {fixture.name}.result RENAME COLUMN tampered TO {quote}')
        else:
            fixture.sql(f'ALTER TABLE {fixture.name}.groups RENAME TO held_groups; CREATE TABLE {fixture.name}.groups (LIKE {fixture.name}.held_groups INCLUDING ALL)')
            assert 'owned destination was removed or replaced' in rejected(command, fixture, 'drop', config)
            assert inspect(command, fixture, 'status', config)['state'] == 'activated'
            assert fixture.sql(f"SELECT count(*) FROM pg_replication_slots WHERE slot_name LIKE '{fixture.name}%'").strip() == '1'
            fixture.sql(f'DROP TABLE {fixture.name}.groups; ALTER TABLE {fixture.name}.held_groups RENAME TO groups')
        fixture.verify()
        assert inspect(command, fixture, 'drop', config)['state'] == 'dropped'
        (output/'qualification.json').write_text(json.dumps({'guard': kind, 'rejected': True, 'repair_and_retry': 'dropped'})+'\n')
        cleanup(fixture)
    except BaseException:
        fixture.preserve()
        raise


if __name__ == '__main__':
    root, command = harness_arguments()
    for stage in ('intent', 'finish'):
        for mode in ('before', 'after', 'hold'):
            output = root/f'{stage}-{mode}'
            output.mkdir()
            fault(output, command, stage, mode)
    for name, qualify in [('competing-sink', competing_sink), ('unowned-sink', unowned_sink)]:
        output = root/name
        output.mkdir()
        qualify(output, command)
    for kind in ('dependent-view', 'altered-view', 'replaced-sink'):
        output = root/kind
        output.mkdir()
        guarded_drop(output, command, kind)
