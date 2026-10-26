#!/usr/bin/env python3
"""Sequential owned-fixture failures with no ACK and native/memory recovery checks."""
import json
import pathlib
import sys
import subprocess
import signal
import time

from query_health_harness import health
from query_lifecycle_harness import configured, grouped
from query_rebuild_harness import command, verify
from worker_harness import Worker


def qualify(output, binary, kind):
    fixture = grouped(output)
    proxy = None
    blocker = None
    original_environment = fixture.environment.copy()
    try:
        config = configured(fixture)
        text = config.read_text().replace('retry_attempts=8', 'retry_attempts=1')
        text = text.replace('[worker.query]', '[worker.health]\nheartbeat_secs=1\nstale_secs=2\noperation_timeout_secs=2\n[worker.query]')
        if kind == 'transaction_limit':
            text += '\n[listener]\nmax_transaction_changes=1\n'
        if kind == 'engine_scratch':
            text = text.replace('maintenance_ticks=4', 'maintenance_ticks=64')+'\n'
            text = text.replace('[execution]', '[execution]\nscratch_bytes=65536')
        if kind == 'scratch_directory':
            scratch = output/'scratch'
            scratch.mkdir()
            fixture.environment['TMPDIR'] = str(scratch.resolve())
        if kind == 'storage_outage':
            text = text.replace('[worker.health]', 'checkpoint_interval_ms=100\n[worker.health]')
            log_path = output/'storage-proxy.jsonl'
            with log_path.open('w') as log:
                proxy = subprocess.Popen([sys.executable, '-u', 'scripts/s3_latency_proxy.py',
                    '--port', '0', '--scale', '0'], stdout=log, stderr=log)
            deadline = time.monotonic()+10
            while not log_path.read_text().splitlines():
                assert proxy.poll() is None and time.monotonic() < deadline
                time.sleep(.05)
            address = json.loads(log_path.read_text().splitlines()[0])
            fixture.environment['PGDERIVE__OBJECT_STORE__ENDPOINT'] = f"http://127.0.0.1:{address['port']}"
        config.write_text(text)
        labels = [column['name'] for column in command(fixture, binary, 'validate', config)['columns']]
        logs = output/'failing'
        logs.mkdir()
        worker = Worker(binary+['query', 'create'], config, logs, fixture.environment)
        fixture.workers.append(worker)
        ready = worker.event('ready')
        if kind == 'engine_scratch':
            for ordinal in range(2):
                first = 1000+ordinal*100
                fixture.sql(f'INSERT INTO {fixture.name}.bid SELECT i,2,i FROM generate_series({first},{first+99}) i')
                worker.event('published', minimum_time=ready['time']+ordinal+1)
        if kind == 'storage_outage':
            # Warm the relevant base blocks, then finish a checkpoint. Cold reads
            # legitimately require storage; this case tests resident WAL-backed work.
            fixture.sql(f'UPDATE {fixture.name}.bid SET price=111 WHERE id=1')
            worker.event('published', minimum_time=ready['time']+1)
            worker.event('maintenance')
        before = command(fixture, binary, 'status', config)['progress']
        if kind == 'storage_outage':
            proxy.send_signal(signal.SIGSTOP)
        if kind == 'scratch_directory':
            scratch.rmdir()
        if kind == 'engine_scratch':
            fixture.sql(f'UPDATE {fixture.name}.auction SET group_id=13 WHERE id=2')
        elif kind not in ('diagnostic_connection','diagnostic_lock'):
            fixture.sql(f'BEGIN; UPDATE {fixture.name}.auction SET group_id=13 WHERE id=1; UPDATE {fixture.name}.bid SET price=NULL WHERE id=1; COMMIT')
        elif kind == 'diagnostic_lock':
            block_environment = fixture.environment.copy()
            application = fixture.name+'_health_lock'
            block_environment['PGAPPNAME'] = application
            lock_sql = f"BEGIN; SELECT query_id FROM {fixture.name}.pgderive_worker_health FOR UPDATE; SELECT pg_sleep(60)"
            with (output/'diagnostic-lock.log').open('w') as log:
                blocker = subprocess.Popen(fixture.pg+[lock_sql], env=block_environment, stdout=log, stderr=log)
            deadline = time.monotonic()+10
            while not fixture.sql(f"SELECT pid FROM pg_stat_activity WHERE application_name='{application}' AND wait_event='PgSleep'").strip():
                assert blocker.poll() is None and time.monotonic() < deadline
                time.sleep(.05)
        else:
            # Terminate only the diagnostic connection inside this owned fixture.
            pids = fixture.sql(f"SELECT pid FROM pg_stat_activity WHERE query LIKE 'INSERT INTO {fixture.name}.pgderive_worker_health%' AND pid <> pg_backend_pid()")
            assert len(pids.split()) == 1, pids
            fixture.sql(f'SELECT pg_terminate_backend({int(pids.strip())})')
        busy = None
        if kind == 'storage_outage':
            worker.event('published', minimum_time=before['logical_time']+1)
            # Uploads run in the background; ready is valid until the bounded
            # checkpoint deadline. Witness pending durable work, not a phase
            # reserved for forced foreground drains.
            pending = command(fixture, binary, 'status', config)['progress']
            assert pending['logical_time'] == before['logical_time']+1, pending
            for field in ('checkpoint_lsn', 'checkpoint_time', 'epoch'):
                assert pending[field] == before[field], pending
            deadline = time.monotonic()+2
            while time.monotonic() < deadline:
                uploading = fixture.sql(f"SELECT count(*) FROM {fixture.name}.pgderive_protections WHERE active AND uploading AND NOT quiescent AND token LIKE 'worker:%:maintenance:%'").strip()
                if uploading != '0':
                    break
                time.sleep(.02)
            assert uploading != '0', 'no protected checkpoint upload observed'
            busy = health(fixture, binary, config)
            assert busy['worker']['recent'] and busy['worker']['owner_live'] and busy['worker']['fence_current'], busy
        worker.process.wait(timeout=30)
        assert worker.process.returncode != 0, 'failure was silently accepted'
        worker.abort()
        failed = health(fixture, binary, config)
        assert not failed['ready'], failed
        after = command(fixture, binary, 'status', config)['progress']
        if kind == 'storage_outage':
            assert after['logical_time'] == before['logical_time']+1
            assert after['checkpoint_lsn'] == before['checkpoint_lsn']
            assert after['checkpoint_time'] == before['checkpoint_time']
            assert after['epoch'] == before['epoch']
            verify(fixture, labels)
        else:
            assert before == after, 'failed input changed authoritative progress'
        ahead = fixture.sql(f"SELECT confirmed_flush_lsn > '{before['checkpoint_lsn']}'::pg_lsn FROM pg_replication_slots WHERE slot_name='{ready['slot']}'")
        assert ahead.strip() == 'f', 'failed input was acknowledged'
        if kind not in ('diagnostic_connection','diagnostic_lock'):
            assert failed['worker']['diagnostics']['phase'] == 'failed', failed
            expected_category = {'transaction_limit':'resource_limit', 'storage_outage':'operation_timeout',
                'scratch_directory':'execution_or_io','engine_scratch':'resource_limit'}[kind]
            assert failed['worker']['diagnostics']['failure']['category'] == expected_category, failed
            assert 'retry_budget_exhausted' in failed['alerts'], failed
            config.write_text(text.replace('max_transaction_changes=1', 'max_transaction_changes=100000').replace('scratch_bytes=65536\n', 'scratch_bytes=1073741824\n'))
        if blocker is not None:
            fixture.sql(f"SELECT pg_terminate_backend(pid) FROM pg_stat_activity WHERE application_name='{application}'")
            blocker.wait(timeout=10)
            blocker = None
        if proxy is not None:
            proxy.send_signal(signal.SIGCONT)
        fixture.environment = original_environment
        if kind == 'scratch_directory':
            scratch.mkdir()
        recovery_logs = output/'recovered'
        recovery_logs.mkdir()
        recovered = Worker(binary+['query', 'restart'], config, recovery_logs, fixture.environment)
        fixture.workers.append(recovered)
        recovered.event('ready')
        if kind == 'storage_outage':
            fixture.sql(f'UPDATE {fixture.name}.bid SET price=987 WHERE id=1')
            recovered.event('published', minimum_time=after['logical_time']+1)
        elif kind not in ('diagnostic_connection','diagnostic_lock'):
            recovered.event('published', minimum_time=before['logical_time']+1)
        else:
            fixture.sql(f'UPDATE {fixture.name}.bid SET price=price+1 WHERE id=1')
            recovered.event('published', minimum_time=before['logical_time']+1)
        verify(fixture, labels)
        recovered.finish(graceful=True)
        command(fixture, binary, 'drop', config)
        (output/'qualification.json').write_text(json.dumps({'case':kind, 'before':before,
            'after_failure':after, 'health':failed, 'busy':busy, 'no_ack':True, 'native_and_memory_recovery':True}, indent=2)+'\n')
        fixture.sql(f'DROP PUBLICATION {fixture.name}; DROP SCHEMA {fixture.name} CASCADE')
    except BaseException:
        fixture.preserve()
        raise
    finally:
        if blocker is not None:
            fixture.sql(f"SELECT pg_terminate_backend(pid) FROM pg_stat_activity WHERE application_name='{application}'")
            blocker.wait(timeout=10)
        if proxy is not None:
            if proxy.poll() is None:
                proxy.send_signal(signal.SIGCONT)
            proxy.terminate()
            proxy.wait(timeout=10)


if __name__ == '__main__':
    root = pathlib.Path(sys.argv[1])
    binary = sys.argv[2:]
    if binary and binary[0] == '--':
        binary = binary[1:]
    for case in ['transaction_limit', 'diagnostic_connection', 'diagnostic_lock', 'engine_scratch', 'scratch_directory', 'storage_outage']:
        directory = root/case
        directory.mkdir(parents=True, exist_ok=True)
        qualify(directory, binary, case)
