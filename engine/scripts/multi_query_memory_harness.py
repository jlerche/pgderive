#!/usr/bin/env python3
"""Sequential isolated storefront memory comparison: one process vs one per query."""
import argparse
from contextlib import ExitStack
import json
import pathlib
import statistics
import time

from performance.common import process_stats
from portfolio_cadence_fixture import DemoFixture
from worker_harness import Worker


def memory(worker):
    assert worker.process.poll() is None, 'worker exited during measurement'
    pid = worker.process.pid
    result = process_stats(pid)
    # RSS sums count shared pages repeatedly; PSS apportions them across processes.
    rows = pathlib.Path(f'/proc/{pid}/smaps_rollup').read_text().splitlines()
    result['pss_bytes'] = int(next(row.split()[1] for row in rows if row.startswith('Pss:')))*1024
    return result


def start(command, fixtures, paths, output, mode, label):
    workers = []
    groups = [paths] if mode == 'shared' else [[path] for path in paths]
    for index, group in enumerate(groups):
        directory = output/f'{label}-{index}'
        directory.mkdir()
        worker = Worker(command+[str(p) for p in group[:-1]], group[-1], directory, fixtures[0].environment)
        fixtures[0].workers.append(worker)
        workers.append(worker)
        for _ in group:
            worker.event('ready')
    return workers


def configure(fixture, source=None):
    source = source or fixture
    path = fixture.config()
    text = path.read_text().replace('tenant_id="fixture"', 'tenant_id='+json.dumps(getattr(fixture, 'tenant_id', 'fixture'))).replace(f'publication="{fixture.name}"', f'publication="{source.name}"')
    text = text.split('[execution]', 1)[0]
    text = text.replace('maintenance_ticks=4',
                        'maintenance_ticks=0\ncheckpoint_interval_ms=60000\ngc_interval_ms=300000\nmemtable_bytes=33554432')
    text += '\n[execution]\ncache_bytes=33554432\ncache_entries=4096\n'
    path.write_text(text)
    return path


def progress(fixtures, target):
    result = []
    for fixture in fixtures:
        row = json.loads(fixture.sql(f"""SELECT json_build_object(
          'query',p.query_id,'end_lsn',p.end_lsn::text,'included',p.end_lsn>='{target}'::pg_lsn,
          'lag_bytes',greatest(0,pg_wal_lsn_diff('{target}'::pg_lsn,p.end_lsn)),
          'ack_safe',s.confirmed_flush_lsn<=p.checkpoint_lsn AND p.checkpoint_lsn<=p.end_lsn)
          FROM {fixture.name}.pgderive_progress p
          JOIN {fixture.name}.pgderive_worker_registration r USING(query_id)
          JOIN pg_replication_slots s ON s.slot_name=r.slot_name"""))
        assert row['ack_safe'], 'slot acknowledged beyond durable checkpoint'
        row.update(database=fixture.database, schema=fixture.name)
        result.append(row)
    return result


def wait_for_fence(fixtures, target):
    deadline = time.monotonic()+60
    while not all(row['included'] for row in progress(fixtures, target)):
        assert time.monotonic() < deadline, 'query did not consume final source transaction'
        time.sleep(.1)


def verify(fixtures):
    for fixture in fixtures:
        fixture.verify()


def qualify(output, command, count, seconds, mode):
    output.mkdir()
    with ExitStack() as cleanup:
        source = cleanup.enter_context(DemoFixture(output, output.name.replace('-', '_')).managed())
        _, lines = source.relations()
        source.sql(f'INSERT INTO {lines} SELECT 7000000+i,1+i%20,0,1 FROM generate_series(0,599) i')
        fixtures = [source]
        for index in range(1, count):
            directory = output/f'query-{index}'
            directory.mkdir()
            fixtures.append(cleanup.enter_context(DemoFixture(directory, output.name.replace('-', '_')+f'_{index}',
                                                              source=source, top10=index%2 == 1).managed()))
        paths = [configure(fixture, source) for fixture in fixtures]
        workers = start(command, fixtures, paths, output, mode, 'worker')
        verify(fixtures)
        samples = []
        started = time.monotonic()
        for tick in range(seconds):
            time.sleep(max(0, started+tick-time.monotonic()))
            mutation = json.loads(source.sql(f'''WITH changed AS (
              UPDATE {lines} SET quantity={1 if tick%2 == 0 else -1}
              WHERE line_id BETWEEN 7000000 AND 7000599 RETURNING 1)
              SELECT json_build_object('changes',count(*),'lsn',pg_current_wal_insert_lsn()::text) FROM changed'''))
            assert mutation['changes'] == 600
            target = mutation['lsn']
            samples.append({'seconds': time.monotonic()-started, 'processes': [memory(w) for w in workers],
                            'progress': progress(fixtures, target)})
            (output/'samples.json').write_text(json.dumps(samples, indent=2))
        time.sleep(max(0, started+seconds-time.monotonic()))
        elapsed = time.monotonic()-started
        wait_for_fence(fixtures, target)
        verify(fixtures)
        for worker in workers:
            worker.finish(graceful=True)
        verify(fixtures)
        reopened = start(command, fixtures, paths, output, mode, 'reopened')
        verify(fixtures)
        for worker in reopened:
            worker.finish(graceful=True)
        rss = [sum(p['rss_bytes'] for p in s['processes']) for s in samples]
        pss = [sum(p['pss_bytes'] for p in s['processes']) for s in samples]
        report = {'passed': True, 'mode': mode, 'queries': count, 'seconds': elapsed,
                  'source_changes': seconds*600, 'source_changes_per_second': seconds*600/elapsed,
                  'query_changes': seconds*600*count, 'final_source_fence': target, 'all_queries_included_final_fence': True, 'rss_mean_bytes': statistics.mean(rss),
                  'rss_sampled_max_bytes': max(rss), 'pss_mean_bytes': statistics.mean(pss),
                  'pss_sampled_max_bytes': max(pss), 'oracle_equal': True, 'cold_reopen_equal': True,
                  'scope': 'Fixed 20-product state, alternating totals/top-ten; 32 MiB cache and memtable per query. Linux RSS/PSS; no PostgreSQL memory. Short runs are not plateau proofs.'}
        (output/'report.json').write_text(json.dumps(report, indent=2))
        return report


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('output', type=pathlib.Path)
    parser.add_argument('--counts', default='1,2,4,8')
    parser.add_argument('--seconds', type=int, default=300)
    parser.add_argument('--mode', choices=['shared', 'separate', 'both'], default='both')
    args, command = parser.parse_known_args()
    if command[:1] == ['--']:
        command = command[1:]
    counts = [int(value) for value in args.counts.split(',')]
    assert command and args.seconds > 0 and counts and all(1 <= count <= 32 for count in counts)
    args.output.mkdir(exist_ok=True)
    reports = []
    for count in counts:
        for mode in (['shared', 'separate'] if args.mode == 'both' else [args.mode]):
            reports.append(qualify(args.output/f'{mode}-{count}', command, count, args.seconds, mode))
            (args.output/'comparison.json').write_text(json.dumps(reports, indent=2))
            print(json.dumps(reports[-1]), flush=True)


if __name__ == '__main__':
    main()
