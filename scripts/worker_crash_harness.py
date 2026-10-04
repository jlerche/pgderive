#!/usr/bin/env python3
"""Witnessed process kills inside object upload/recovery and PostgreSQL COMMIT.

Every fixture is isolated. Failed fixtures and logs remain available for diagnosis.
"""
import argparse
import json
import os
import pathlib
import subprocess
import sys
import time
from worker_harness import Fixture


def wait_for(predicate, description, seconds=60):
    deadline = time.monotonic() + seconds
    while time.monotonic() < deadline:
        if predicate():
            return
        time.sleep(.02)
    raise TimeoutError(description)


def proxy(output, kind, gate, marker=None):
    log = (output / 'proxy.jsonl').open('w')
    error = (output / 'proxy-stderr.log').open('w')
    if kind == 's3':
        command = [sys.executable, '-u', 'scripts/s3_latency_proxy.py', '--port', '0',
                   '--scale', '0', '--gate', str(gate)]
    else:
        command = [sys.executable, '-u', 'scripts/pg_commit_proxy.py', '--mode', 'hold',
                   '--once', '--arm-query', marker, '--gate', str(gate)]
    process = subprocess.Popen(command, stdout=log, stderr=error)
    try:
        wait_for(lambda: bool((output / 'proxy.jsonl').read_text()), 'proxy readiness')
        port = json.loads((output / 'proxy.jsonl').read_text().splitlines()[0])['port']
        return process, log, error, port
    except BaseException:
        process.terminate()
        process.wait(timeout=10)
        log.close()
        error.close()
        raise


def durable(fixture):
    return fixture.sql(f'''SELECT q.logical_time,p.end_lsn::text,q.epoch FROM
      {fixture.name}.pgderive_queries q JOIN {fixture.name}.pgderive_progress p USING(query_id)''').strip()


def arm(gate, method):
    pathlib.Path(str(gate) + '.arm').write_text(method)


def object_kill(output, command, index, restoring, collecting=False):
    fixture = Fixture(output, index)
    gate = output / 'object.release'
    process, log, error, port = proxy(output, 's3', gate)
    fixture.environment['PGDERIVE__OBJECT_STORE__ENDPOINT'] = f'http://127.0.0.1:{port}'
    try:
        if collecting:
            arm(gate, 'POST') # object_store batches GC via S3 DeleteObjects
            victim = fixture.start(command, 'victim')
            victim.event('ready')
            for tick in range(3):
                fixture.sql(f'UPDATE {fixture.name}.bid SET price=price+1 WHERE id={tick+1}')
                victim.event('published', minimum_time=tick+2)
            phase = {}
        elif restoring:
            first = fixture.start(command, 'initial')
            first.event('ready')
            first.finish(graceful=True)
            prior = durable(fixture)
            # Independent external reader lifetime: a new worker may retire only
            # private recovery pins, never this public protection.
            fixture.sql(f'''INSERT INTO {fixture.name}.pgderive_protections
              (token,query_id,owner_fence,uploading,roots)
              SELECT 'external-reader','grouped',p.fence,false,
              (SELECT jsonb_agg(reference) FROM {fixture.name}.pgderive_objects)
              FROM {fixture.name}.pgderive_progress p''')
            arm(gate, 'GET')
        else:
            arm(gate, 'PUT')
        if not collecting:
            victim = fixture.start(command, 'victim')
            phase = victim.event('recovering' if restoring else 'bootstrap_upload')
        wait_for(lambda: pathlib.Path(str(gate) + '.pending').exists(), 'held object request')
        slot = fixture.sql(f'SELECT slot_name FROM {fixture.name}.pgderive_worker_registration').strip()
        if collecting:
            assert durable(fixture).split('|')[0] == '4'
        elif restoring:
            assert fixture.sql(f"SELECT active FROM {fixture.name}.pgderive_protections WHERE token='{phase['pin']}'").strip() == 't'
            assert durable(fixture) == prior
        else:
            assert fixture.sql(f'SELECT count(*) FROM {fixture.name}.pgderive_progress').strip() == '0'
        victim.abort()
        gate.touch()
        wait_for(lambda: any(json.loads(line).get('method') == ('POST' if collecting else 'GET' if restoring else 'PUT') for line in (output / 'proxy.jsonl').read_text().splitlines()[1:]), 'held request release')
        survivor = fixture.start(command, 'survivor', maximum=3 if collecting else 1)
        ready = survivor.event('ready')
        assert ready['slot'] == slot
        assert ready['time'] == (4 if collecting else 1)
        fixture.verify()
        assert fixture.sql(f'SELECT count(*) FROM {fixture.name}.pgderive_protections WHERE recovering AND active').strip() == '0'
        if collecting:
            assert durable(fixture).split('|')[0] == '4'
        elif restoring:
            assert fixture.sql(f"SELECT active FROM {fixture.name}.pgderive_protections WHERE token='external-reader'").strip() == 't'
        for tick in range(3 if collecting else 1):
            fixture.sql(f'UPDATE {fixture.name}.bid SET price=price+1 WHERE id=1')
            survivor.event('published', minimum_time=ready['time'] + tick + 1)
        survivor.finish()
        fixture.verify()
        (output / 'result.json').write_text(json.dumps({'killed': 'gc_delete' if collecting else 'pinned_restore' if restoring else 'bootstrap_put', 'resumed_time': ready['time'], 'slot': slot}) + '\n')
        fixture.cleanup()
    except BaseException:
        fixture.preserve()
        raise
    finally:
        process.terminate()
        process.wait(timeout=10)
        log.close()
        error.close()


def commit_kill(output, command, index, maintaining):
    fixture = Fixture(output, index)
    gate = output / 'commit.release'
    marker = 'binding,fence,commit_lsn::text,end_lsn::text,xid FROM' if maintaining else 'pgderive_progress SET commit_lsn'
    process, log, error, port = proxy(output, 'sql', gate, marker)
    fixture.environment['PGDERIVE__POSTGRES__PORT'] = str(port)
    try:
        victim = fixture.start(command, 'victim')
        ready = victim.event('ready')
        prior = durable(fixture)
        count = 3 if maintaining else 1
        for tick in range(count):
            fixture.sql(f'UPDATE {fixture.name}.bid SET price=price+1 WHERE id={tick+1}')
            if maintaining:
                victim.event('published', minimum_time=tick+2)
        wait_for(lambda: pathlib.Path(str(gate) + '.pending').exists(), 'held PostgreSQL COMMIT')
        held = durable(fixture)
        if maintaining:
            assert held.split('|')[0] == '4'
        else:
            assert held == prior, 'uncommitted source publication became visible'
        victim.abort()
        # Model the original server completing its transaction after client death.
        # The next worker must serialize with it and recover its authoritative result.
        gate.touch()
        wait_for(lambda: 'released_commit' in (output / 'proxy.jsonl').read_text(), 'original COMMIT completion')
        survivor = fixture.start(command, 'survivor', maximum=1)
        resumed = survivor.event('ready')
        assert resumed['slot'] == ready['slot']
        assert resumed['time'] == count+1
        fixture.verify()
        fixture.sql(f'UPDATE {fixture.name}.bid SET price=price+1 WHERE id=1')
        survivor.event('published', minimum_time=count+2)
        survivor.finish()
        fixture.verify()
        (output / 'result.json').write_text(json.dumps({'killed': 'maintenance_commit' if maintaining else 'publication_commit', 'resumed_time': resumed['time']}) + '\n')
        fixture.cleanup()
    except BaseException:
        fixture.preserve()
        raise
    finally:
        process.terminate()
        process.wait(timeout=10)
        log.close()
        error.close()


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('output', type=pathlib.Path)
    parser.add_argument('command', nargs=argparse.REMAINDER)
    args = parser.parse_args()
    command = args.command[1:] if args.command and args.command[0] == '--' else args.command
    if not command:
        parser.error('provide worker command after --')
    for index, name in enumerate(('bootstrap-put', 'restore', 'publication', 'maintenance', 'gc-delete')):
        output = args.output / name
        output.mkdir()
        if index < 2 or index == 4:
            object_kill(output, command, index, index == 1, collecting=index == 4)
        else:
            commit_kill(output, command, index, index == 3)
