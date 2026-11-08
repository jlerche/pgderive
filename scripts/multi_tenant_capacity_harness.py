#!/usr/bin/env python3
"""Qualify tenant attribution, shared ceilings, SIGTERM and foreign ownership denial."""
from contextlib import ExitStack
import json
import os
import pathlib
import signal
import subprocess
import time

from multi_tenant_memory_harness import database, fixture_group, check
from multi_query_memory_harness import configure, start, wait_for_fence
from worker_harness import Worker, harness_arguments
from portfolio_cadence_fixture import DemoFixture
from application_portfolio_harness import ApplicationFixture
from application_model import generated, query


def stop(worker):
    worker.process.send_signal(signal.SIGTERM)
    worker.finish()


def replay_pressure(output, command):
    output.mkdir()
    initial, _ = generated(7001, rows=128, accounts=16, transactions=0)
    with ApplicationFixture(output, {'initial':initial,'spec':query('group',7001)}).managed() as fixture:
        fixture.tenant_id = 'replay-tenant'
        path = configure(fixture)
        worker = start(command, [fixture], [path], output, 'shared', 'source')[0]
        charges = []
        for tick in range(5):
            target = fixture.sql(f"INSERT INTO {fixture.name}.entry SELECT {7000000+tick*600}+i,1+i%16,{10000+tick*600}+i FROM generate_series(0,599) i; SELECT pg_current_wal_insert_lsn()::text").splitlines()[-1]
            wait_for_fence([fixture], target)
            published_time = int(fixture.sql(f'SELECT published_time FROM {fixture.name}.pgderive_progress'))
            charges.append(worker.event('published', minimum_time=published_time)['memtable_bytes'])
        check([fixture])
        assert fixture.sql(f'SELECT checkpoint_lsn<end_lsn FROM {fixture.name}.pgderive_progress').strip() == 't'
        final_time = int(fixture.sql(f'SELECT published_time FROM {fixture.name}.pgderive_progress'))
        worker.abort()
        policy = output/'process.toml'
        def limits(size):
            policy.write_text(f'memtable_bytes={size}\nmemtable_bytes_per_tenant={size}\n')
        limits(1)
        oversized = subprocess.run(command+['--resources',str(policy),str(path)], env=fixture.environment, capture_output=True,text=True,timeout=60)
        (output/'oversized.stdout').write_text(oversized.stdout)
        (output/'oversized.stderr').write_text(oversized.stderr)
        assert oversized.returncode != 0 and 'replay epoch exceeds' in oversized.stdout+oversized.stderr
        reduced = max(charges[0]*2, charges[0]+65536)
        assert reduced < charges[-1], charges
        limits(reduced)
        reopened = start(command+['--resources',str(policy)], [fixture], [path], output, 'shared', 'replayed')[0]
        check([fixture])
        rows = [json.loads(line) for line in (output/'replayed-0/metrics.jsonl').read_text().splitlines()]
        assert any(row['event']=='maintenance' and row['time'] < final_time for row in rows), rows
        stop(reopened)
        check([fixture])
        (output/'report.json').write_text(json.dumps({'passed':True,'initial_and_final_charges':charges,'reduced_pool':reduced,'partial_replay_checkpoint':True,'oversized_replay_rejected':True,'ack_safe':True},indent=2))


def qualify(output, command):
    with ExitStack() as cleanup:
        fixtures, streams = [], []
        for tenant in range(3):
            directory = output/f'tenant-{tenant}'
            directory.mkdir()
            db = cleanup.enter_context(database(f'pgderive_capacity_{os.getpid()}_{tenant}', directory))
            queries, sources = fixture_group(cleanup, directory, tenant, db)
            fixtures.extend(queries)
            streams.extend(sources)
        paths = [configure(f, f.source if isinstance(f, DemoFixture) else f) for f in fixtures]
        # Exercise the production scoped disk-cache open and fixed pool reservation.
        paths[0].write_text(paths[0].read_text()+f'\n[encoded_cache]\ndirectory={json.dumps(str((output/"encoded-cache").resolve()))}\nmemory_bytes=1048576\ndisk_bytes=67108864\n')
        limits = output/'process.toml'
        limits.write_text('max_queries=6\nmax_queries_per_tenant=2\ncache_bytes=8388608\ncache_bytes_per_tenant=4194304\nmemtable_bytes=3145728\nmemtable_bytes_per_tenant=2097152\n')
        pooled = command+['--resources', str(limits)]
        workers = start(pooled, fixtures, paths, output, 'shared', 'worker')
        check(fixtures)
        for tick in range(8):
            for fixture, table, column, predicate, readers in streams:
                target = fixture.sql(f"UPDATE {table} SET {column}={100+tick} WHERE {predicate}; SELECT pg_current_wal_insert_lsn()::text").splitlines()[-1]
                wait_for_fence(readers, target)
        check(fixtures)
        for worker in workers:
            stop(worker)
        check(fixtures)
        reopened = start(pooled, fixtures, paths, output, 'shared', 'reopened')
        check(fixtures)
        for worker in reopened:
            stop(worker)
        events = [json.loads(line) for path in output.glob('*/metrics.jsonl') for line in path.read_text().splitlines()]
        assert any(row['event']=='checkpoint_backpressure' and row.get('reason')=='shared_memtable' for row in events), 'shared memtable pressure was not exercised'
        identities = {row['worker']['tenant_id'] for row in events if 'worker' in row}
        assert identities == {'tenant-0', 'tenant-1', 'tenant-2'}, identities
        for event in events:
            if 'capacity' in event:
                for stats in event['capacity'].values():
                    assert stats['used'] <= stats['limit'] and stats['peak'] <= stats['limit'], stats
                    assert stats['tenant_used'] <= stats['tenant_limit'], stats
        foreign = output/'foreign.toml'
        foreign.write_text(paths[0].read_text().replace('tenant_id="tenant-0"', 'tenant_id="foreign"'))
        denied = subprocess.run(command+[str(foreign)], env=fixtures[0].environment, capture_output=True, text=True, timeout=60)
        (output/'foreign.stdout').write_text(denied.stdout)
        (output/'foreign.stderr').write_text(denied.stderr)
        assert denied.returncode != 0 and 'catalog belongs to another tenant' in denied.stdout+denied.stderr
        check(fixtures)
        # A rejected tenant must not terminate a healthy tenant in the same process.
        isolated = output/'failure-isolation'
        isolated.mkdir()
        survivor = Worker(pooled+[str(foreign)], paths[2], isolated, fixtures[0].environment)
        fixtures[0].workers.append(survivor)
        survivor.event('ready')
        deadline = time.monotonic()+60
        while not any(row.get('event') == 'query_finished' and row.get('status') == 'failed'
                      for row in map(json.loads, (isolated/'metrics.jsonl').read_text().splitlines())):
            assert survivor.process.poll() is None
            assert time.monotonic() < deadline
            time.sleep(.1)
        fixture, table, column, predicate, _readers = streams[1]
        target = fixture.sql(f"UPDATE {table} SET {column}=777 WHERE {predicate}; SELECT pg_current_wal_insert_lsn()::text").splitlines()[-1]
        wait_for_fence([fixtures[2]], target)
        assert survivor.process.poll() is None
        survivor.process.send_signal(signal.SIGTERM)
        survivor.process.wait(timeout=60)
        survivor.close_logs()
        assert survivor.process.returncode != 0
        check(fixtures)
        (output/'report.json').write_text(json.dumps({'passed': True, 'tenants':3,'queries':6,'sigterm_drain':True,'cold_reopen':True,'sql_and_memory_oracles':True,'foreign_catalog_denied':True,'query_failure_isolated':True,'charged_limits_hold':True}, indent=2))


if __name__ == '__main__':
    output, command = harness_arguments()
    replay_pressure(output/'replay-pressure', command)
    qualify(output, command)
