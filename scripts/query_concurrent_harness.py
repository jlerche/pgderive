#!/usr/bin/env python3
"""Two independently supervised queries over one publication, with restart isolation."""
import collections
import json
import pathlib
import sys

from harness_control import admin
from worker_harness import Fixture, Worker, harness_arguments

sys.path.insert(0, str(pathlib.Path(__file__).resolve().parents[1]/'examples/portfolio'))
from render import TOTALS_SQL, TOP10_SQL
from workload import initial_sql, changes_sql


def qualify(output, command):
    fixture = Fixture(output, 'concurrent', rows=4, auctions=2)
    other = fixture.name+'_topk'
    source = fixture.name+'.positions'
    configs = {}
    workers = {}
    ticks = {}
    queries = {'totals': TOTALS_SQL.replace('public.demo_positions', source),
               'top10': TOP10_SQL.replace('public.demo_positions', source)}
    schemas = {'totals': fixture.name, 'top10': other}
    try:
        fixture.sql(f'''CREATE SCHEMA {other};
          CREATE TABLE {source}(id bigint PRIMARY KEY,book_id bigint NOT NULL,quantity bigint);
          ALTER TABLE {source} REPLICA IDENTITY FULL;
          ALTER PUBLICATION {fixture.name} ADD TABLE {source};''' + initial_sql().replace('public.demo_positions', source))
        for name, query in queries.items():
            original = fixture.config(query=query).read_text()
            text = original.replace(f'catalog_schema="{fixture.name}"', f'catalog_schema="{schemas[name]}"')
            text = text.replace('query_id="grouped"', f'query_id="{name}"')
            text = text.replace('sink_table="groups"', 'sink_table="groups"\nsink_key=["book_id"]')
            text = text.replace(f'slot="{fixture.name}"', f'slot="{fixture.name}_{name}"')
            text = text.replace(f'object_prefix="{fixture.name}"', f'object_prefix="{fixture.name}_{name}"')
            path = output/(name+'.toml'); path.write_text(text); configs[name] = path
            fixture.sql(f'CREATE MATERIALIZED VIEW {fixture.name}.native_{name} AS {query}')

        def start(name, action, label):
            logs = output/label; logs.mkdir()
            worker = Worker(command+['query', action], configs[name], logs, fixture.environment)
            fixture.workers.append(worker); workers[name] = worker
            ready = worker.event('ready'); ticks[name] = ready['time']
            return ready

        def progress(name):
            return fixture.sql(f'SELECT end_lsn FROM {schemas[name]}.pgderive_progress').strip()

        def verify(name):
            schema = schemas[name]
            fixture.sql(f'REFRESH MATERIALIZED VIEW {fixture.name}.native_{name}')
            expected = fixture.name+'.native_'+name; actual = schema+'.groups'
            assert fixture.sql(f'''SELECT count(*) FROM ((SELECT * FROM {expected} EXCEPT ALL SELECT * FROM {actual})
                UNION ALL (SELECT * FROM {actual} EXCEPT ALL SELECT * FROM {expected})) d''').strip() == '0'
            rows = json.loads(fixture.sql(f'SELECT json_agg(p) FROM {source} p'))
            groups = collections.defaultdict(list)
            for row in rows:
                groups[row['book_id']].append(row['quantity'])
            memory = []
            for book, values in groups.items():
                present = [value for value in values if value is not None]
                memory.append((book, len(values), sum(present) if present else None))
            if name == 'top10':
                memory.sort(key=lambda row: (row[2] is None, -(row[2] or 0), row[0]))
                memory = memory[:10]
            public = json.loads(fixture.sql(f'SELECT json_agg(g) FROM {actual} g'))
            assert collections.Counter((row['book_id'], row['positions'], row['units']) for row in public) == collections.Counter(memory)
            assert fixture.sql(f'''SELECT count(*)=1 AND bool_and(s.confirmed_flush_lsn<=p.checkpoint_lsn AND p.checkpoint_lsn<=p.end_lsn)
              FROM {schema}.pgderive_progress p JOIN {schema}.pgderive_worker_registration r USING(query_id)
              JOIN pg_replication_slots s ON s.slot_name=r.slot_name''').strip() == 't'

        def change(tick, phase, names):
            sql, _ = changes_sql(tick, phase, None)
            fixture.sql('BEGIN;'+sql.replace('public.demo_positions', source)+'COMMIT')
            for name in names:
                ticks[name] += 1
                workers[name].event('published', minimum_time=ticks[name])
                verify(name)

        initial = {name: start(name, 'create', name+'-create') for name in queries}
        assert initial['totals']['slot'] != initial['top10']['slot']
        for name in queries: verify(name)
        change(1, 0, queries)
        workers['totals'].finish(graceful=True)
        stopped = progress('totals'); peer_pid = workers['top10'].process.pid
        change(70, 1, ['top10'])  # Outside book enters while totals is stopped.
        assert progress('totals') == stopped
        resumed = start('totals', 'restart', 'totals-restart')
        assert resumed['slot'] == initial['totals']['slot']
        ticks['totals'] += 1
        workers['totals'].event('published', minimum_time=ticks['totals'])
        assert workers['top10'].process.pid == peer_pid and workers['top10'].process.poll() is None
        for name in queries: verify(name)
        workers['top10'].finish(graceful=True)
        stopped = progress('top10'); peer_pid = workers['totals'].process.pid
        change(140, 2, ['totals'])  # Winner exits; retained book ten refills later.
        assert progress('top10') == stopped
        resumed = start('top10', 'restart', 'top10-restart')
        assert resumed['slot'] == initial['top10']['slot']
        ticks['top10'] += 1
        workers['top10'].event('published', minimum_time=ticks['top10'])
        assert workers['totals'].process.pid == peer_pid and workers['totals'].process.poll() is None
        for name in queries: verify(name)
        change(210, 3, queries)  # Exact cutoff tie preserves smaller book ID.
        workers['totals'].finish(graceful=True)
        assert admin(fixture, command, 'drop', configs['totals'])['state'] == 'dropped'
        change(280, 4, ['top10'])  # Dropping one registration cannot retire the other.
        workers['top10'].finish(graceful=True)
        assert admin(fixture, command, 'drop', configs['top10'])['state'] == 'dropped'
        fixture.sql(f'DROP PUBLICATION {fixture.name}; DROP SCHEMA {other} CASCADE; DROP SCHEMA {fixture.name} CASCADE')
        (output/'result.json').write_text(json.dumps({'queries': 2, 'same_source_publication': True,
            'distinct_catalog_prefix_slot_sink': True, 'native_and_memory_oracles': True,
            'independent_pause_restart_drop': True, 'cold_replay_and_ack_order': True})+'\n')
    except BaseException:
        fixture.preserve()
        raise
    finally:
        for worker in fixture.workers: worker.abort()


if __name__ == '__main__':
    qualify(*harness_arguments())
