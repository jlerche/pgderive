#!/usr/bin/env python3
"""Owned health observations: running, deliberately stopped, resumed and missing slot."""
import json
import os
import pathlib
import signal
import sys
import time

from query_lifecycle_harness import configured, grouped, rejected
from query_rebuild_harness import command, verify
from worker_harness import Worker


def health(fixture,binary,config):
    return command(fixture,binary,'status',config)['health']


def qualify(output,binary):
    fixture = grouped(output)
    worker = None
    try:
        config = configured(fixture)
        config.write_text(config.read_text().replace('[worker.query]',
            '[worker.health]\nheartbeat_secs=1\nstale_secs=2\nretained_wal_alert_bytes=1\nbacklog_alert_bytes=1\n[worker.query]'))
        labels = [column['name'] for column in command(fixture,binary,'validate',config)['columns']]
        assert health(fixture,binary,config)['ready'] is False
        logs = output/'initial'
        logs.mkdir()
        worker = Worker(binary+['query','create'],config,logs,fixture.environment)
        fixture.workers.append(worker)
        ready = worker.event('ready')
        running = health(fixture,binary,config)
        assert running['ready'],running
        assert running['worker']['owner_live'] and running['worker']['fence_current']
        verify(fixture,labels)
        before = command(fixture,binary,'status',config)['progress']
        os.kill(worker.process.pid,signal.SIGSTOP)
        fixture.sql(f'BEGIN; UPDATE {fixture.name}.auction SET group_id=13 WHERE id=1; UPDATE {fixture.name}.bid SET price=NULL WHERE id=1; COMMIT')
        deadline = time.monotonic()+10
        while True:
            stalled = health(fixture,binary,config)
            if not stalled['ready']:
                break
            assert time.monotonic() < deadline,'stopped worker remained ready'
            time.sleep(.1)
        assert stalled['worker']['owner_live'] and not stalled['worker']['recent']
        assert 'wal_backlog' in stalled['alerts'] and 'wal_retention' in stalled['alerts']
        assert command(fixture,binary,'status',config)['progress'] == before
        os.kill(worker.process.pid,signal.SIGCONT)
        worker.event('published',minimum_time=ready['time']+1)
        verify(fixture,labels)
        worker.finish(graceful=True)
        stopped = health(fixture,binary,config)
        assert not stopped['ready'] and not stopped['worker']['owner_live']
        assert stopped['worker']['diagnostics']['phase'] == 'stopped',stopped
        fixture.sql(f"SELECT pg_drop_replication_slot('{ready['slot']}')")
        lost = health(fixture,binary,config)
        assert not lost['ready'] and 'slot_missing' in lost['alerts']
        config.write_text(config.read_text().replace('retry_attempts=8','retry_attempts=1'))
        rejected(binary,fixture,'restart',config)
        startup = command(fixture,binary,'status',config)['worker_attempt']
        assert startup['diagnostics']['phase'] == 'exhausted',startup
        assert startup['diagnostics']['consecutive_failures'] == 1,startup
        command(fixture,binary,'rebuild',config)
        verify(fixture,labels)
        assert not health(fixture,binary,config)['ready'],'bootstrap alone claimed readiness'
        command(fixture,binary,'drop',config)
        (output/'qualification.json').write_text(json.dumps({'running':running,'stalled':stalled,
            'stopped':stopped,'missing_slot':lost,'startup_failure':startup,'explicit_rebuild':True},indent=2)+'\n')
        fixture.sql(f'DROP PUBLICATION {fixture.name}; DROP SCHEMA {fixture.name} CASCADE')
    except BaseException:
        if worker is not None and worker.process.poll() is None:
            os.kill(worker.process.pid,signal.SIGCONT)
        fixture.preserve()
        raise


if __name__ == '__main__':
    output = pathlib.Path(sys.argv[1])
    output.mkdir(parents=True,exist_ok=True)
    binary = sys.argv[2:]
    if binary and binary[0] == '--':
        binary = binary[1:]
    qualify(output,binary)
