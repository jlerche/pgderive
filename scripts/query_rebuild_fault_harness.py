#!/usr/bin/env python3
"""Witnessed rebuild COMMIT failures and process kills in sequential owned fixtures."""
import json
import pathlib
import subprocess
import sys
import time

from query_removal_harness import setup, cleanup, wait_records
from query_lifecycle_harness import rejected
from query_rebuild_harness import command, verify
from worker_harness import Worker
from harness_control import commit_proxy


def fault(output, binary, stage, mode):
    fixture, config = setup(output,binary)
    proxy = None
    try:
        labels = [column['name'] for column in command(fixture,binary,'validate',config)['columns']]
        old = command(fixture,binary,'status',config)
        old_slot = old['registration']['slot_name']
        source = [fixture.sql(f'SELECT jsonb_agg(to_jsonb(t) ORDER BY id) FROM {fixture.name}.{table} t') for table in ('auction','bid')]
        markers = {'intent':'pgderive_generations VALUES',
                   'activation':'pgderive_progress(query_id,binding,sink_table,fence,commit_lsn,end_lsn,',
                   'cutover':'/* lifecycle:cutover */',
                   'retirement':'/* lifecycle:removal-complete */'}
        evidence = output/'fault.jsonl'
        gate = output/'release'
        proxy, port = commit_proxy(evidence, mode, once=True, marker=markers[stage],
                                   gate=gate if mode == 'hold' else None)
        fixture.environment['PGDERIVE__POSTGRES__PORT'] = str(port)
        if mode == 'hold':
            with (output/'rebuild.stdout').open('w') as stdout, (output/'rebuild.stderr').open('w') as stderr:
                process = subprocess.Popen(binary+['query','rebuild',str(config)],env=fixture.environment,stdout=stdout,stderr=stderr)
                try:
                    wait_records(evidence,lambda rows:any(row.get('fault')=='held_commit' for row in rows))
                    process.kill()
                    process.wait(timeout=10)
                    gate.touch()
                    wait_records(evidence,lambda rows:any(row.get('fault')=='released_commit' for row in rows))
                finally:
                    if process.poll() is None:
                        process.kill()
                        process.wait(timeout=10)
        else:
            rejected(binary,fixture,'rebuild',config)
            witness = 'before_commit' if mode == 'before' else 'lost_commit_response'
            wait_records(evidence,lambda rows:any(row.get('fault')==witness for row in rows))
        fixture.environment.pop('PGDERIVE__POSTGRES__PORT')
        deadline = time.monotonic()+10
        while fixture.sql(f"SELECT pg_try_advisory_lock(hashtextextended('pgderive-worker:{fixture.name}:grouped',0))").strip() != 't':
            assert time.monotonic() < deadline, 'faulted process still owns logical query'
            time.sleep(.05)
        observed = command(fixture,binary,'status',config)
        lifecycle = observed['lifecycle']
        pending = lifecycle['pending']['query'] if lifecycle and lifecycle['pending'] else None
        if stage == 'intent' and mode == 'before':
            assert lifecycle is None
        elif stage in ('intent','activation') or (stage == 'cutover' and mode == 'before'):
            assert lifecycle['phase'] == 'Building'
            rejected(binary,fixture,'restart',config)
        else:
            assert lifecycle['phase'] == 'Active'
        # A fully committed retirement needs inspection/restart, not another rebuild.
        if not (stage == 'retirement' and mode != 'before'):
            command(fixture,binary,'rebuild',config)
        final = command(fixture,binary,'status',config)
        assert final['lifecycle']['phase'] == 'Active'
        assert final['lifecycle']['retired'] == []
        if pending:
            assert final['lifecycle']['active']['query'] == pending
        if lifecycle and lifecycle['phase'] == 'Active':
            assert final['lifecycle']['active'] == lifecycle['active']
        assert fixture.sql(f"SELECT count(*) FROM pg_replication_slots WHERE slot_name='{old_slot}'").strip() == '0'
        assert [fixture.sql(f'SELECT jsonb_agg(to_jsonb(t) ORDER BY id) FROM {fixture.name}.{table} t') for table in ('auction','bid')] == source
        verify(fixture,labels)
        logs = output/'resumed'
        logs.mkdir()
        worker = Worker(binary+['query','restart'],config,logs,fixture.environment)
        fixture.workers.append(worker)
        ready = worker.event('ready')
        fixture.sql(f'BEGIN; UPDATE {fixture.name}.auction SET group_id=11 WHERE id=1; UPDATE {fixture.name}.bid SET price=NULL WHERE id=1; COMMIT')
        worker.event('published',minimum_time=ready['time']+1)
        verify(fixture,labels)
        worker.finish(graceful=True)
        command(fixture,binary,'drop',config)
        (output/'qualification.json').write_text(json.dumps({'stage':stage,'mode':mode,
            'witness':str(evidence),'same_pending_generation':pending,
            'native_mv':True,'memory_oracle':True,'cold_restart':True})+'\n')
        cleanup(fixture)
    except BaseException:
        fixture.preserve()
        raise
    finally:
        if proxy is not None:
            proxy.terminate()
            proxy.wait(timeout=10)


if __name__ == '__main__':
    output = pathlib.Path(sys.argv[1])
    output.mkdir(parents=True,exist_ok=True)
    binary = sys.argv[2:]
    if binary and binary[0] == '--':
        binary = binary[1:]
    for stage in ('intent','activation','cutover','retirement'):
        for mode in ('before','after','hold'):
            directory = output/(stage+'-'+mode)
            directory.mkdir()
            fault(directory,binary,stage,mode)
