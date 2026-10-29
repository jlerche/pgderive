#!/usr/bin/env python3
"""Pace owned demo activity at two source transactions per second."""
import json
import os
from pathlib import Path
import signal
import subprocess
import time
from workload import changes_sql, Phases, TICKS_PER_SECOND, MEMBERSHIP_SECONDS

ROOT = Path(os.environ.get('DEMO_ROOT', Path(__file__).resolve().parents[1]))
COMPOSE = ['docker', 'compose', '--env-file', str(ROOT/'stack-r2/.env'),
           '-f', str(ROOT/'stack-r2/compose.yaml'), '-f', str(ROOT/'stack-r2/engines.compose.yaml')]
RUNNING = True


def stop(_signal, _frame):
    global RUNNING
    RUNNING = False


def sql(statement):
    return subprocess.check_output([*COMPOSE, 'exec', '-T', 'db', 'psql', '-X', '-qAt',
        '-v', 'ON_ERROR_STOP=1', '-U', 'supabase_admin', '-d', 'postgres', '-c', statement],
        stdin=subprocess.DEVNULL, text=True, timeout=20).strip()


def main():
    for sig in (signal.SIGINT, signal.SIGTERM):
        signal.signal(sig, stop)
    path = ROOT/'generator/activity-state.json'
    previous = json.loads(path.read_text()) if path.exists() else {}
    tick = previous.get('transactions', 0)
    changes = previous.get('source_changes', 0)
    recent = previous.get('recent', [])[-255:]
    phases = Phases(previous.get('phase', 0), time.monotonic())
    phase = None
    while RUNNING:
        began = time.monotonic()
        lag = json.loads(sql("""SELECT json_build_object('queries',count(*),'bytes',max(greatest(0,pg_wal_lsn_diff(pg_current_wal_lsn(),end_lsn))))
          FROM (SELECT end_lsn FROM demo_ivm.pgderive_progress WHERE sink_table='book_totals_keyed'
          UNION ALL SELECT end_lsn FROM demo_topk.pgderive_progress WHERE sink_table='book_top10') p"""))
        if lag['queries'] != 2:
            raise RuntimeError('Both maintained queries must be active before generator startup')
        if lag['bytes'] > 16 * 1024 * 1024:
            print(json.dumps({'event': 'generator_backpressure', 'durable_wal_lag_bytes': lag['bytes']}), flush=True)
            time.sleep(.5)
            continue
        statement, next_phase = changes_sql(tick, phases.current(time.monotonic()), phase)
        counted = ''.join('WITH changed AS (' + dml + ' RETURNING 1) SELECT count(*) FROM changed;'
                          for dml in statement.split(';') if dml)
        result = sql('BEGIN;' + counted + 'SELECT pg_current_wal_insert_lsn();COMMIT;').splitlines()
        target = result[-1]
        if next_phase != phase:
            print(json.dumps({'event': 'membership_phase', 'phase': next_phase,
                              'interval_seconds': MEMBERSHIP_SECONDS}), flush=True)
        phase = next_phase
        tick += 1
        emitted = sum(int(count) for count in result[:-1])
        changes += emitted
        sample = {'tick': tick, 'target_lsn': target, 'committed_at_unix': time.time(),
                  'source_changes': emitted, 'mode': 'explicit', 'total_source_changes': changes,
                  'elapsed_ms': round((time.monotonic()-began)*1000, 3)}
        recent.append(sample)
        del recent[:-256]
        temporary = path.with_suffix('.tmp')
        temporary.write_text(json.dumps({'transactions': tick, 'source_changes': changes, 'phase': phase, 'recent': recent})+'\n')
        os.replace(temporary, path)
        print(json.dumps({'event': 'source_transaction_committed', **sample}), flush=True)
        while RUNNING and time.monotonic()-began < 1/TICKS_PER_SECOND:
            time.sleep(max(0, min(.05, 1/TICKS_PER_SECOND-(time.monotonic()-began))))


if __name__ == '__main__':
    main()
