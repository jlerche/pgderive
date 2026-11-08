#!/usr/bin/env python3
"""Independent databases, schemas, source data and query recipes in one worker process."""
import argparse
from contextlib import ExitStack, contextmanager
import json
import os
import pathlib
import statistics
import subprocess
import time

from application_model import generated, query, state
from application_portfolio_harness import ApplicationFixture
from multi_query_memory_harness import configure, memory, progress, start, verify, wait_for_fence
from portfolio_cadence_fixture import DemoFixture
from sql_compiler_harness import SqlFixture


@contextmanager
def database(name, output):
    command = ['psql', '-h', '127.0.0.1', '-p', os.environ.get('PGDERIVE_TEST_POSTGRES_PORT', '55434'),
               '-U', 'postgres', '-d', 'postgres', '-v', 'ON_ERROR_STOP=1', '-Atc']
    environment = os.environ.copy()
    environment['PGPASSWORD'] = os.environ.get('PGPASSWORD', 'postgres')
    def execute(sql):
        result = subprocess.run(command+[sql], env=environment, capture_output=True, text=True)
        with (output/'database-commands.log').open('a') as log:
            log.write(sql+'\n'+result.stdout+result.stderr)
        result.check_returncode()
    execute(f'CREATE DATABASE {name}')
    (output/'database.json').write_text(json.dumps({'database': name, 'owned': True}))
    # On failure leave this owned database intact as failure evidence.
    yield name
    execute(f'DROP DATABASE {name}')


def fixture_group(cleanup, directory, tenant, db):
    fixtures, streams = [], []
    for index in range(2):
        output = directory/f'query-{index}'
        output.mkdir()
        seed = 1009+tenant*107+index*13
        tag = directory.parent.name.replace('-', '_')+f'_tenant{tenant}_{index}'
        if tenant%3 == 0:
            fixture = DemoFixture(output, tag, source=fixtures[0] if index else None,
                                  top10=index == 1, database=db)
            if index == 0:
                _, table = fixture.relations()
                fixture.sql(f'INSERT INTO {table} SELECT 7000000+i,1+i%20,0,{seed} FROM generate_series(0,599) i')
                streams.append((fixture, table, 'quantity', 'line_id BETWEEN 7000000 AND 7000599', [fixture]))
            else:
                streams[0][4].append(fixture)
        elif tenant%3 == 1:
            initial, _history = generated(seed, rows=1024*(tenant+1)+index*128, accounts=32*(tenant+1), transactions=0)
            trace = {'initial': initial, 'spec': query('group' if index == 0 else 'window', seed)}
            fixture = ApplicationFixture(output, trace, database=db)
            fixture.sql(f'UPDATE {fixture.name}.entry SET account_id=1+id%{32*(tenant+1)},amount=id*{seed}')
            streams.append((fixture, fixture.name+'.entry', 'amount', 'id BETWEEN 1 AND 600', [fixture]))
        else:
            fixture = SqlFixture(output, False, typed=index == 0, remainder=index == 1, database=db, index=tag)
            fixture.sql(f'''INSERT INTO {fixture.name}.bid SELECT 7000000+i,1+i%8,i*{seed}
              FROM generate_series(0,599) i;
              UPDATE {fixture.name}.auction SET category='tenant_{tenant}_'||id::text''')
            streams.append((fixture, fixture.name+'.bid', 'price', 'id BETWEEN 7000000 AND 7000599', [fixture]))
        fixtures.append(cleanup.enter_context(fixture.managed()))
    return fixtures, streams


def check(fixtures):
    for fixture in fixtures:
        if isinstance(fixture, ApplicationFixture):
            # Reuse the independent application oracle over actual raw source rows.
            raw = {table: json.loads(fixture.sql(f'SELECT json_agg(t) FROM {fixture.name}.{table} t'))
                   for table in ['account', 'entry']}
            fixture.model = state(raw)
    verify(fixtures)


def snapshot(workers, streams):
    return {'processes': [memory(worker) for worker in workers],
            'progress': [row for fixture, target in streams.items() for row in progress([fixture], target)]}


def qualify(output, command, tenants, seconds, mode):
    output.mkdir()
    with ExitStack() as cleanup:
        fixtures, streams = [], []
        for tenant in range(tenants):
            directory = output/f'tenant-{tenant}'
            directory.mkdir()
            name = f'pgderive_memory_{os.getpid()}_{mode}_{tenants}_{tenant}'
            db = cleanup.enter_context(database(name, directory))
            queries, sources = fixture_group(cleanup, directory, tenant, db)
            fixtures.extend(queries)
            streams.extend(sources)
        paths = [configure(f, f.source if isinstance(f, DemoFixture) else f) for f in fixtures]
        workers = start(command, fixtures, paths, output, mode, 'worker')
        check(fixtures)
        samples, targets = [], {}
        began = time.monotonic()
        for tick in range(seconds):
            time.sleep(max(0, began+tick-time.monotonic()))
            for fixture, table, column, predicate, readers in streams:
                value = (tick%2*2-1)*(1009+tick%17)
                mutation = json.loads(fixture.sql(f'''WITH changed AS (
                  UPDATE {table} SET {column}={value} WHERE {predicate} RETURNING 1)
                  SELECT json_build_object('changes',count(*),'lsn',pg_current_wal_insert_lsn()::text) FROM changed'''))
                assert mutation['changes'] == 600
                for reader in readers:
                    targets[reader] = mutation['lsn']
            sample = snapshot(workers, targets)
            sample['seconds'] = time.monotonic()-began
            samples.append(sample)
            (output/'samples.json').write_text(json.dumps(samples, indent=2))
        time.sleep(max(0, began+seconds-time.monotonic()))
        elapsed = time.monotonic()-began
        for fixture, target in targets.items():
            wait_for_fence([fixture], target)
        check(fixtures)
        for worker in workers:
            worker.finish(graceful=True)
        check(fixtures)
        reopened = start(command, fixtures, paths, output, mode, 'reopened')
        check(fixtures)
        for worker in reopened:
            worker.finish(graceful=True)
        rss = [sum(p['rss_bytes'] for p in s['processes']) for s in samples]
        pss = [sum(p['pss_bytes'] for p in s['processes']) for s in samples]
        report = {'passed': True, 'mode': mode, 'tenants': tenants, 'queries': len(fixtures),
                  'databases': [f.database for f in fixtures][::2], 'independent_source_streams': len(streams),
                  'seconds': elapsed, 'changes_per_second_per_stream': 600*seconds/elapsed,
                  'source_changes': len(streams)*600*seconds, 'query_changes': len(fixtures)*600*seconds,
                  'rss_mean_bytes': statistics.mean(rss), 'rss_sampled_max_bytes': max(rss),
                  'pss_mean_bytes': statistics.mean(pss), 'pss_sampled_max_bytes': max(pss),
                  'final_fences': {f.database+'/'+f.name: target for f, target in targets.items()},
                  'oracle_equal': True, 'cold_reopen_equal': True,
                  'scope': 'Separate databases, 2 queries/tenant, 3 schema/data/query profiles. Target 600 changes/s per independent source stream and per query; measured throughput reported separately. 32 MiB private cache and memtable. Worker RSS/PSS only. Single finite run, not a plateau proof.'}
        (output/'report.json').write_text(json.dumps(report, indent=2))
        return report


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('output', type=pathlib.Path)
    parser.add_argument('--tenants', default='1,2,3')
    parser.add_argument('--seconds', type=int, default=300)
    parser.add_argument('--mode', choices=['shared', 'separate', 'both'], default='both')
    args, command = parser.parse_known_args()
    if command[:1] == ['--']:
        command = command[1:]
    counts = [int(value) for value in args.tenants.split(',')]
    assert command and args.seconds > 0 and counts and all(1 <= count <= 12 for count in counts)
    args.output.mkdir(exist_ok=True)
    reports = []
    for count in counts:
        for mode in (['shared', 'separate'] if args.mode == 'both' else [args.mode]):
            reports.append(qualify(args.output/f'{mode}-{count}', command, count, args.seconds, mode))
            (args.output/'comparison.json').write_text(json.dumps(reports, indent=2))
            print(json.dumps(reports[-1]), flush=True)


if __name__ == '__main__':
    main()
