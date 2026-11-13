#!/usr/bin/env python3
"""Sequential owned-fixture retirement faults and destructive-ownership checks."""
import json
import subprocess
import time
from gc_fixture import paged_garbage, empty_inventory

from query_lifecycle_harness import configured, grouped, inspect, rejected, verify_result
from worker_harness import Worker, harness_arguments
from harness_control import commit_proxy, wait_records


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





def fault(output, command, stage, mode):
    fixture, config = setup(output, command)
    proxy = None
    source_before = [fixture.sql(f'SELECT jsonb_agg(to_jsonb(t) ORDER BY id) FROM {fixture.name}.{table} t') for table in ('auction', 'bid')]
    try:
        marker = '/* lifecycle:removal-intent */' if stage == 'intent' else '/* lifecycle:removal-complete */'
        gate = output/'release'
        evidence = output/'fault.jsonl'
        proxy, port = commit_proxy(evidence, mode, once=True, marker=marker,
                                   gate=gate if mode == 'hold' else None)
        fixture.environment['PGDERIVE__POSTGRES__PORT'] = str(port)
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
        expected_state = 'activated' if stage == 'intent' and mode == 'before' else 'dropping'
        assert state == expected_state, (stage, mode, state)
        if fixture.sql(f"SELECT to_regclass('{fixture.name}.groups') IS NOT NULL").strip() == 't':
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
        before = fixture.sql(f'SELECT * FROM {fixture.bag_relation()} ORDER BY tuple')
        other = output/'other.toml'
        other.write_text(config.read_text().replace('query_id="grouped"', 'query_id="other"').replace(
            f'slot="{fixture.name}"', f'slot="{fixture.name}_other"').replace('retry_attempts=8', 'retry_attempts=1'))
        assert 'destination belongs to another query' in rejected(command, fixture, 'create', other)
        assert inspect(command, fixture, 'drop', other)['state'] == 'dropped'
        assert fixture.sql(f'SELECT * FROM {fixture.bag_relation()} ORDER BY tuple') == before
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
        fixture.sql(f'CREATE TABLE {fixture.name}.groups(tuple jsonb PRIMARY KEY,weight bigint)')
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
            fixture.sql(f'CREATE VIEW {fixture.name}.consumer AS SELECT * FROM {fixture.name}.groups')
            assert 'depend' in rejected(command, fixture, 'drop', config)
            assert inspect(command, fixture, 'status', config)['state'] == 'dropping'
            fixture.sql(f'DROP VIEW {fixture.name}.consumer')
        elif kind == 'altered-table':
            column = fixture.sql(f"SELECT attname FROM pg_attribute WHERE attrelid='{fixture.name}.groups'::regclass AND attnum=1").strip()
            quote = '"'+column.replace('"', '""')+'"'
            fixture.sql(f'ALTER TABLE {fixture.name}.groups RENAME COLUMN {quote} TO tampered')
            assert 'registered result table layout changed' in rejected(command, fixture, 'drop', config)
            assert inspect(command, fixture, 'status', config)['state'] == 'activated'
            fixture.sql(f'ALTER TABLE {fixture.name}.groups RENAME COLUMN tampered TO {quote}')
        else:
            fixture.sql(f'ALTER TABLE {fixture.name}.groups RENAME TO held_groups; CREATE TABLE {fixture.name}.groups (LIKE {fixture.name}.held_groups INCLUDING ALL)')
            assert 'owned destination was removed or replaced' in rejected(command, fixture, 'drop', config)
            assert inspect(command, fixture, 'status', config)['state'] == 'activated'
            assert fixture.sql(f"SELECT count(*) FROM pg_replication_slots WHERE slot_name LIKE '{fixture.name}%'").strip() == '1'
            fixture.sql(f'DROP TABLE {fixture.name}.groups; ALTER TABLE {fixture.name}.held_groups RENAME TO groups')
        fixture.verify()
        seeded = paged_garbage(fixture) if kind == 'dependent-view' else 0
        dropped = inspect(command, fixture, 'drop', config)
        assert dropped['state'] == 'dropped'
        if seeded:
            empty_inventory(fixture, fixture.name)
            assert dropped.get('collection_passes_complete') is True
        (output/'qualification.json').write_text(json.dumps({'guard': kind, 'rejected': True, 'repair_and_retry': 'dropped', 'seeded_orphans':seeded, 'paged_cleanup':bool(seeded)})+'\n')
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
    for kind in ('dependent-view', 'altered-table', 'replaced-sink'):
        output = root/kind
        output.mkdir()
        guarded_drop(output, command, kind)
