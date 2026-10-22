#!/usr/bin/env python3
"""Crash-replay a backlog of complete source transactions grouped into epochs."""
import json
import pathlib
import signal
import time
from worker_harness import Fixture


def qualify(output, command):
    output.mkdir(parents=True)
    fixture = Fixture(output, 'adaptive_epochs', rows=16, auctions=4)
    original = fixture.config

    def config(maximum=0):
        path = original(maximum)
        path.write_text(path.read_text().replace(
            'maintenance_ticks=4',
            'maintenance_ticks=64\ncheckpoint_interval_ms=60000\nphase_metrics=true'))
        return path

    fixture.config = config
    with fixture.managed():
        first = fixture.start(command, 'first')
        first.event('ready')
        first.process.send_signal(signal.SIGSTOP)
        try:
            transactions = []
            for tick in range(40):
                # Both sides change; ephemeral rows cancel across transactions.
                transactions.append(f'''BEGIN;
                  UPDATE {fixture.name}.auction SET category='epoch{tick%3}' WHERE id=1;
                  UPDATE {fixture.name}.bid SET auction=1,price=CASE WHEN id%2=0 THEN NULL ELSE {tick} END WHERE id<=3;
                  INSERT INTO {fixture.name}.bid VALUES({100+tick},1,{tick});
                  DELETE FROM {fixture.name}.bid WHERE id={99+tick}; COMMIT;''')
            fixture.sql('\n'.join(transactions))
            target = fixture.sql('SELECT pg_current_wal_lsn()').strip()
        finally:
            first.process.send_signal(signal.SIGCONT)
        # The last source commit is before target; wait for the exact final row.
        deadline = time.monotonic() + 60
        while time.monotonic() < deadline:
            first.event('published')
            # Wait for every complete transaction, then compare all result rows.
            if fixture.sql(f'''SELECT COALESCE(sum(transactions),0)
                FROM {fixture.name}.pgderive_epoch_boundaries
                WHERE query_id='grouped'
                ''').strip() == '40':
                try:
                    fixture.verify()
                    break
                except AssertionError:
                    pass
        else:
            raise TimeoutError('adaptive epochs did not reach final native result')
        boundary = json.loads(fixture.sql(f'SELECT row_to_json(p) FROM {fixture.name}.pgderive_progress p'))
        journal = json.loads(fixture.sql(f'''SELECT json_agg(row_to_json(e) ORDER BY logical_time)
            FROM {fixture.name}.pgderive_epoch_boundaries e'''))
        assert sum(epoch['transactions'] for epoch in journal) == 40
        assert any(epoch['transactions'] > 1 for epoch in journal)
        assert len(journal) < 40
        assert fixture.sql(f'SELECT format_version FROM {fixture.name}.pgderive_queries').strip() == '3'
        audit_sql = f'SELECT json_agg(json_build_array(g.ctid::text,g.xmin::text,to_jsonb(g)) ORDER BY g.ctid) FROM {fixture.name}.groups g'
        audit = fixture.sql(audit_sql)
        first.abort()
        for iteration in range(2):
            resumed = fixture.start(command, f'replay-{iteration}')
            ready = resumed.event('ready')
            assert ready['time'] == boundary['published_time']
            assert fixture.sql(audit_sql) == audit, 'epoch replay repeated sink DML'
            fixture.verify()
            resumed.abort()
        # A missing epoch boundary must fail closed rather than silently fall
        # back to transaction-at-a-time replay and rewrite the sink.
        fixture.sql(f'''DELETE FROM {fixture.name}.pgderive_epoch_boundaries
            WHERE logical_time=(SELECT min(logical_time)
                FROM {fixture.name}.pgderive_epoch_boundaries)''')
        broken = fixture.start(command, 'missing-boundary')
        broken.process.wait(timeout=60)
        broken.abort()
        assert broken.process.returncode != 0, 'missing epoch boundary was accepted'
        assert 'epoch journal' in pathlib.Path(broken.stderr.name).read_text()
        assert fixture.sql(audit_sql) == audit, 'rejected epoch journal changed sink'
        (output/'epoch-evidence.json').write_text(json.dumps({
            'complete_source_transactions': 40, 'published_epochs': len(journal),
            'repeated_recovery': 2, 'missing_boundary_rejected': True, 'sink_replay_writes': 0,
            'source_target_lsn': target, 'boundary': boundary, 'journal': journal}, indent=2)+'\n')
