#!/usr/bin/env python3
"""Invalidate a slot only on a fresh owned tmpfs PostgreSQL server."""
import json
import os
import pathlib
import sys
import time

from query_health_harness import health
from query_lifecycle_harness import configured, grouped, rejected
from query_rebuild_harness import command, verify
from worker_harness import Worker
from harness_control import Server


def qualify(output, binary):
    name = f'pgderive-health-slot-{os.getpid()}'
    completed = False
    old_port = os.environ.get('PGDERIVE_TEST_POSTGRES_PORT')
    server = Server(output, 'postgres', name=name, size='512m', flags=['max_slot_wal_keep_size=64MB', 'max_wal_size=128MB', 'min_wal_size=32MB'],
                    metadata='postgres.json', failure='postgres-failure.log')
    try:
        port = server.port
        os.environ['PGDERIVE_TEST_POSTGRES_PORT'] = port
        fixture = grouped(output)
        try:
            config = configured(fixture)
            config.write_text(config.read_text().replace('retry_attempts=8', 'retry_attempts=1').replace(
                '[worker.query]', '[worker.health]\nretained_wal_alert_bytes=8388608\nbacklog_alert_bytes=8388608\n[worker.query]'))
            labels = [column['name'] for column in command(fixture,binary,'validate',config)['columns']]
            logs = output/'initial'
            logs.mkdir()
            worker = Worker(binary+['query','create'], config, logs, fixture.environment)
            fixture.workers.append(worker)
            ready = worker.event('ready')
            worker.finish(graceful=True)
            before = command(fixture,binary,'status',config)['progress']
            feedback = health(fixture,binary,config)['observation']['slot']['confirmed_flush_lsn']
            fixture.sql(f'BEGIN; UPDATE {fixture.name}.auction SET group_id=13 WHERE id=1; '
                        f'UPDATE {fixture.name}.bid SET price=NULL WHERE id=1; COMMIT')
            for _ in range(3):
                fixture.sql('SELECT pg_switch_wal(); CHECKPOINT')
                warning = health(fixture,binary,config)
                if {'wal_retention','wal_backlog'} <= set(warning['alerts']):
                    break
            assert warning['observation']['max_slot_wal_keep_size'] == '64MB', warning
            assert {'wal_retention','wal_backlog'} <= set(warning['alerts']), warning
            assert 'slot_invalidated' not in warning['alerts'], warning
            assert warning['observation']['slot']['wal_status'] in ('reserved','extended'), warning
            assert command(fixture,binary,'status',config)['progress'] == before
            assert not warning['observation']['slot']['confirmed_past_durable'], warning
            assert warning['observation']['slot']['confirmed_flush_lsn'] == feedback, warning
            resumed = output/'catch-up'
            resumed.mkdir()
            worker = Worker(binary+['query','restart'],config,resumed,fixture.environment)
            fixture.workers.append(worker)
            worker.event('ready')
            worker.event('published',minimum_time=ready['time']+1)
            verify(fixture,labels)
            worker.finish(graceful=True)
            before = command(fixture,binary,'status',config)['progress']
            feedback = health(fixture,binary,config)['observation']['slot']['confirmed_flush_lsn']
            for _ in range(12):
                fixture.sql('SELECT pg_switch_wal(); CHECKPOINT')
                invalid = health(fixture,binary,config)
                if 'slot_invalidated' in invalid['alerts']:
                    break
            assert 'slot_invalidated' in invalid['alerts'] and 'slot_wal_unsafe' in invalid['alerts'], invalid
            assert invalid['observation']['slot']['wal_status'] == 'lost', invalid
            assert invalid['observation']['slot']['confirmed_flush_lsn'] == feedback, invalid
            assert not invalid['ready']
            rejected(binary,fixture,'restart',config)
            assert health(fixture,binary,config)['observation']['slot']['confirmed_flush_lsn'] == feedback
            after = command(fixture,binary,'status',config)['progress']
            assert {key:value for key,value in after.items() if key != 'fence'} == {key:value for key,value in before.items() if key != 'fence'}, 'invalid slot advanced authoritative data progress'
            began = time.monotonic()
            command(fixture,binary,'rebuild',config)
            rebuild_seconds = time.monotonic()-began
            verify(fixture,labels)
            command(fixture,binary,'drop',config)
            (output/'qualification.json').write_text(json.dumps({'slot':ready['slot'],
                'warning':warning,'catch_up_before_invalidation':True,
                'invalidated':invalid,'explicit_rebuild':True,'rebuild_seconds':rebuild_seconds,
                'retention_cap_bytes':67108864,'alert_threshold_bytes':8388608,
                'native_and_memory_equality':True}, indent=2)+'\n')
            completed = True
        except BaseException:
            fixture.preserve()
            raise
    finally:
        if old_port is None:
            os.environ.pop('PGDERIVE_TEST_POSTGRES_PORT',None)
        else:
            os.environ['PGDERIVE_TEST_POSTGRES_PORT'] = old_port
        server.finish(completed)


if __name__ == '__main__':
    output = pathlib.Path(sys.argv[1])
    output.mkdir(parents=True,exist_ok=True)
    binary = sys.argv[2:]
    if binary and binary[0] == '--':
        binary = binary[1:]
    qualify(output,binary)
