#!/usr/bin/env python3
"""Tenant-wide packs across independent databases, including offline-source GC."""
import argparse
from contextlib import ExitStack
import json
import pathlib
import signal
import subprocess
import time
import uuid
from types import SimpleNamespace

from harness_control import held_transaction, finish_transaction, wait_records
from multi_query_memory_harness import configure, start, wait_for_fence
from multi_tenant_memory_harness import check, database, fixture_group


def qualify(output, command):
    output.mkdir(exist_ok=True)
    schema = 'tenant_pack_'+uuid.uuid4().hex
    with ExitStack() as cleanup:
        fixtures, streams, databases = [], [], []
        for index in range(3):
            directory = output/f'source-{index}'
            directory.mkdir()
            db = cleanup.enter_context(database(f'{schema}_{index}', directory))
            databases.append(db)
            queries, sources = fixture_group(cleanup, directory, index, db)
            for fixture in queries:
                fixture.tenant_id = 'alice' if index < 2 else 'bob'
            fixtures.extend(queries)
            streams.extend(sources)
        control = fixtures[-1]

        def sql(statement):
            args = control.pg.copy()
            args[args.index('-d')+1] = 'pgderive_dev'
            return subprocess.check_output(args+[statement], env=control.environment, text=True).strip()

        def manifest():
            return json.loads(sql(f'''SELECT coalesce(json_agg(t),'[]') FROM (
              SELECT c.tenant,c.catalog::text,c.name,c.pack::text,c.length,p.path,p.owner::text
              FROM {schema}.chunks c JOIN {schema}.packs p ON p.id=c.pack
              ORDER BY c.tenant,c.catalog,c.name) t'''))

        paths = []
        for fixture in fixtures:
            path = configure(fixture, getattr(fixture, 'source', None) or fixture)
            text = path.read_text().replace('checkpoint_interval_ms=60000', 'checkpoint_interval_ms=250')
            text = text.replace('gc_interval_ms=300000', 'gc_interval_ms=1000')
            text = text.replace(f'schema="{fixture.name}_storage"', f'schema="{schema}"')
            text = text.replace(f'object_prefix="tenant-{fixture.name}"', f'object_prefix="{schema}"')
            # Give concurrently ready independent queries a generous test batch window.
            text += '\n[tenant_storage.batching]\ndelay_ms=100\n'
            path.write_text(text)
            paths.append(path)

        # Keep the production collector from consuming the fragmentation witness
        # before the outage. Logical GC still runs normally. Releasing this lock
        # after disconnecting DB0 proves physical reclamation needs no DB0 session.
        control_pg = control.pg.copy()
        control_pg[control_pg.index('-d')+1] = 'pgderive_dev'
        barrier = cleanup.enter_context(held_transaction(
            SimpleNamespace(pg=control_pg, environment=control.environment),
            output/'reclamation-barrier.log',
            "SELECT 'held' FROM (SELECT pg_advisory_xact_lock(hashtextextended('pgderive-tenant-compaction:alice',0))) locked"))
        worker = start(command, fixtures, paths, output, 'shared', 'first')[0]
        unavailable = False
        try:
            check(fixtures)
            for tick in range(8):
                for fixture, table, column, predicate, readers in streams:
                    target = fixture.sql(f'''UPDATE {table} SET {column}={100+tick}
                      WHERE {predicate}; SELECT pg_current_wal_insert_lsn()::text''').splitlines()[-1]
                    wait_for_fence(readers, target)
                time.sleep(.3)
            check(fixtures)
            initial = manifest()
            (output/'manifest-before.json').write_text(json.dumps(initial, indent=2))
            packs = {}
            for row in initial:
                packs.setdefault(row['pack'], []).append(row)
            assert all(len({row['tenant'] for row in rows}) == 1 for rows in packs.values())
            assert any(len({row['catalog'] for row in rows}) > 1 for rows in packs.values()), 'no cross-catalog shared pack'
            catalogs = json.loads(sql(f"SELECT json_object_agg(catalog::text,source->>'database') FROM {schema}.catalogs"))
            uploaders = {}
            for row in initial:
                uploaders.setdefault(row['owner'], set()).add(catalogs[row['catalog']])
            assert any(len(databases) > 1 for databases in uploaders.values()), 'independent databases did not share an uploader'
            offline_ids = {f.sql(f'SELECT catalog_id::text FROM {f.name}.pgderive_tenant_owner').strip() for f in fixtures[:2]}
            sql(f'ALTER DATABASE {databases[0]} ALLOW_CONNECTIONS false')
            unavailable = True
            sql(f"SELECT pg_terminate_backend(pid) FROM pg_stat_activity WHERE datname='{databases[0]}'")
            wait_records(pathlib.Path(worker.metrics.name), lambda rows: any(
                row.get('event') == 'process_status' and row.get('failed_queries', 0) >= 2
                for row in rows), timeout=60)
            # Capture the stable retained objects after DB0's query tasks drain;
            # logical cleanup already in flight may legitimately finish first.
            before = {(r['catalog'], r['name']): r for r in manifest() if r['catalog'] in offline_ids}
            assert before, 'offline source has no durable objects'
            finish_transaction(barrier, 'COMMIT')
            # Healthy databases keep consuming and acknowledging through their own checkpoints.
            for tick in range(8):
                for fixture, table, column, predicate, readers in streams[1:]:
                    target = fixture.sql(f'''UPDATE {table} SET {column}={200+tick}
                      WHERE {predicate}; SELECT pg_current_wal_insert_lsn()::text''').splitlines()[-1]
                    wait_for_fence(readers, target)
                time.sleep(.3)
            check(fixtures[2:])
            # This command receives the unavailable source's configuration, but
            # must use only its independently configured manifest endpoint.
            for turn in range(24):
                result = subprocess.run([str(pathlib.Path(command[0]).with_name('pgderive')),
                                         'storage', 'collect', str(paths[0])],
                                        env=fixtures[0].environment, capture_output=True, text=True, timeout=60)
                (output/f'offline-collect-{turn}.log').write_text(result.stdout+result.stderr)
                result.check_returncode()
                after = manifest()
                moved = [r for r in after if (r['catalog'], r['name']) in before
                         and r['path'] != before[r['catalog'], r['name']]['path']]
                if moved:
                    break
            assert moved, 'offline catalog was not relocated while healthy peers reclaimed shared space'
            assert all(any(r['catalog'] == old['catalog'] and r['name'] == old['name'] for r in after)
                       for old in before.values()), 'offline logical object lost'
            (output/'manifest-offline.json').write_text(json.dumps(after, indent=2))
            sql(f'ALTER DATABASE {databases[0]} ALLOW_CONNECTIONS true')
            unavailable = False
            worker.process.send_signal(signal.SIGINT)
            worker.process.wait(timeout=240)
            worker.close_logs()
            assert worker.process.returncode != 0, 'source failure was not reported'
            worker = start(command, fixtures, paths, output, 'shared', 'reopened')[0]
            check(fixtures)
            worker.finish(graceful=True)
            check(fixtures)
            report = {'passed': True, 'databases': 3, 'queries': 6, 'tenants': 2,
                      'cross_catalog_packs': True, 'tenant_isolation': True,
                      'cross_database_uploader': True,
                      'offline_relocated_chunks': len(moved), 'healthy_queries_progressed': True,
                      'oracle_equal': True, 'cold_reopen_equal': True}
            (output/'qualification.json').write_text(json.dumps(report, indent=2))
            sql(f'DROP SCHEMA {schema} CASCADE')
            return report
        except BaseException:
            worker.abort()
            subprocess.run(['pg_dump', '-h', '127.0.0.1', '-p', control.port, '-U', 'postgres',
                            '-d', 'pgderive_dev', f'--schema={schema}', f'--file={output}/failure-storage.sql'],
                           env=control.environment, check=False)
            raise
        finally:
            if unavailable:
                sql(f'ALTER DATABASE {databases[0]} ALLOW_CONNECTIONS true')


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('output', type=pathlib.Path)
    args, command = parser.parse_known_args()
    if command[:1] == ['--']:
        command = command[1:]
    assert command
    print(json.dumps(qualify(args.output, command)), flush=True)


if __name__ == '__main__':
    main()
