#!/usr/bin/env python3
"""Prove retained-WAL replay rebuilds state without repeating sink DML."""
import json
import pathlib
import sys
from worker_harness import Fixture


def qualify(output, command):
    fixture = Fixture(output, 'memtable', rows=16, auctions=4)
    original = fixture.config

    def config(maximum=0):
        path = original(maximum)
        text = path.read_text().replace('maintenance_ticks=4', 'maintenance_ticks=64\ncheckpoint_interval_ms=60000')
        path.write_text(text)
        return path

    fixture.config = config
    with fixture.managed():
        first = fixture.start(command, 'first')
        ready = first.event('ready')
        for tick in range(3):
            fixture.sql(f'''BEGIN;
              UPDATE {fixture.name}.auction SET category='replay{tick%2}' WHERE id=1;
              UPDATE {fixture.name}.bid SET auction=1,price=CASE WHEN id%2=0 THEN NULL ELSE {tick+10} END WHERE id BETWEEN 1 AND 3;
              INSERT INTO {fixture.name}.bid VALUES({100+tick},1,7); COMMIT''')
            first.event('published', minimum_time=tick+2)
            fixture.verify()
        boundary = json.loads(fixture.sql(f'SELECT row_to_json(p) FROM {fixture.name}.pgderive_progress p'))
        assert boundary['published_time'] == 4
        assert fixture.sql(f'SELECT format_version FROM {fixture.name}.pgderive_queries').strip() == '2'
        assert fixture.sql(f'SELECT logical_time FROM {fixture.name}.pgderive_queries').strip() == '1'
        assert fixture.sql(f'SELECT checkpoint_lsn<end_lsn FROM {fixture.name}.pgderive_progress').strip() == 't'
        audit_sql = f'SELECT json_agg(json_build_array(g.ctid::text,g.xmin::text,to_jsonb(g)) ORDER BY g.ctid) FROM {fixture.name}.groups g'
        audit = fixture.sql(audit_sql)
        first.abort()
        for iteration in range(2):
            resumed = fixture.start(command, f'replay-{iteration}')
            reopened = resumed.event('ready')
            assert reopened['time'] == 4 and reopened['slot'] == ready['slot']
            assert fixture.sql(audit_sql) == audit, 'replay repeated sink writes'
            fixture.verify()
            if iteration == 1:
                fixture.sql(f'UPDATE {fixture.name}.bid SET price=456 WHERE id=1')
                resumed.event('published', minimum_time=5)
                fixture.verify()
            resumed.abort()
        # A timer flush checkpoints the suffix and only then permits feedback.
        fixture.config = lambda maximum=0: timer_config(original, maximum)
        flushed = fixture.start(command, 'timer-flush')
        flushed.event('ready')
        flushed.event('maintenance')
        assert fixture.sql(f'SELECT checkpoint_lsn=end_lsn AND published_time=5 FROM {fixture.name}.pgderive_progress').strip() == 't'
        flushed.finish(graceful=True)
        fixture.verify()
        # Simulate external premature ACK in this isolated slot; startup must reject it.
        fixture.config = config
        victim = fixture.start(command, 'lost-wal-victim')
        victim.event('ready')
        fixture.sql(f'UPDATE {fixture.name}.bid SET price=123 WHERE id=1')
        victim.event('published', minimum_time=6)
        fixture.verify()
        after_victim = fixture.sql(audit_sql)
        victim.abort()
        progress = json.loads(fixture.sql(f'SELECT row_to_json(p) FROM {fixture.name}.pgderive_progress p'))
        slot = ready['slot']
        fixture.sql(f"SELECT pg_replication_slot_advance('{slot}','{progress['end_lsn']}')")
        rejected = fixture.start(command, 'lost-wal-rejected')
        rejected.process.wait(timeout=60)
        rejected.reader.join(timeout=10)
        assert rejected.process.returncode != 0, 'advanced slot was accepted'
        rejected.metrics.close()
        rejected.stderr.close()
        assert 'source slot advanced beyond durable engine position' in pathlib.Path(rejected.stderr.name).read_text()
        assert fixture.sql(audit_sql) == after_victim, 'rejected startup changed sink'
        (output/'result.json').write_text(json.dumps({'passed': True, 'sink_replay_writes': 0,
            'repeated_crashes': 2, 'timer_checkpoint': True, 'premature_ack_rejected': True,
            'published_suffix_transactions': 3, 'boundary_before_replay': boundary}, indent=2)+'\n')


def timer_config(original, maximum):
    path = original(maximum)
    path.write_text(path.read_text().replace('maintenance_ticks=4', 'maintenance_ticks=64\ncheckpoint_interval_ms=100'))
    return path


def memory_pressure(output, command):
    output.mkdir()
    fixture = Fixture(output, 'pressure', rows=16, auctions=4)
    original = fixture.config
    cap = 64 * 1024 * 1024

    def config(maximum=0):
        path = original(maximum)
        path.write_text(path.read_text().replace('maintenance_ticks=4', f'maintenance_ticks=64\ncheckpoint_interval_ms=60000\nmemtable_bytes={cap}'))
        return path

    fixture.config = config
    with fixture.managed():
        calibrate = fixture.start(command, 'calibrate')
        calibrate.event('ready')
        fixture.sql(f'UPDATE {fixture.name}.bid SET price=id+100')
        event = calibrate.event('published')
        cap = event['memtable_bytes'] * 3 // 2
        calibrate.finish(graceful=True)
        worker = fixture.start(command, 'pressure')
        ready = worker.event('ready')
        for tick in range(4):
            fixture.sql(f'UPDATE {fixture.name}.bid SET price=id+{200+tick}')
            event = worker.event('published', minimum_time=ready['time']+tick+1)
            assert event['memtable_bytes'] <= cap
            fixture.verify()
        worker.finish(graceful=True)
        events = [json.loads(line) for line in (output/'pressure/metrics.jsonl').read_text().splitlines()]
        assert sum(row['event']=='maintenance' for row in events) >= 2, 'memory pressure failed to checkpoint before stop'
        (output/'result.json').write_text(json.dumps({'passed':True,'cap':cap,'checkpoints':sum(row['event']=='maintenance' for row in events)})+'\n')


if __name__ == '__main__':
    output = pathlib.Path(sys.argv[1])
    command = sys.argv[2:]
    command = command[1:] if command and command[0] == '--' else command
    qualify(output, command)
    memory_pressure(output/'pressure', command)
