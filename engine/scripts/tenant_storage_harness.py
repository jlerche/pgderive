#!/usr/bin/env python3
"""Tenant-wide packs across independent databases, including offline-source GC."""
import argparse
from contextlib import ExitStack
import json
import pathlib
import signal
import subprocess
import uuid
from types import SimpleNamespace

from harness_control import held_transaction, finish_transaction, wait_records
from multi_query_memory_harness import configure, start, wait_for_fence
from multi_tenant_memory_harness import check, database, fixture_group


def advance(streams, value):
    # Queue writes on all sources before waiting. Separate databases can
    # checkpoint together, allowing the shared uploader to batch their chunks.
    fences = []
    for fixture, table, column, predicate, readers in streams:
        target = fixture.sql(f"""WITH changed AS (
            UPDATE {table} SET {column}={value} WHERE {predicate} RETURNING 1)
            SELECT pg_current_wal_insert_lsn()::text FROM changed LIMIT 1""").strip()
        assert target, f'{table} produced no WAL fence'
        fences.append((readers, target))
    for readers, target in fences:
        wait_for_fence(readers, target)


def verify_packs(rows, catalogs):
    packs = {}
    uploaders = {}
    for row in rows:
        packs.setdefault(row['pack'], []).append(row)
        uploaders.setdefault(row['owner'], set()).add(catalogs[row['catalog']])
    assert all(len({row['tenant'] for row in members}) == 1 for members in packs.values())
    return any(len({catalogs[row['catalog']] for row in members}) > 1
               for members in packs.values()) and any(len(sources) > 1 for sources in uploaders.values())


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
            queries, sources = fixture_group(cleanup, directory, index, db, churn_rows=40, application_rows=80)
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
            text = path.read_text().replace('checkpoint_interval_ms=60000', 'checkpoint_interval_ms=100')
            text = text.replace('block_rows=64', 'block_rows=256')
            # Keep the production retry count while waiting on the actual
            # failed-query witness. The timer policy is checked separately.
            text = text.replace('retry_delay_ms=100', 'retry_delay_ms=1')
            text = text.replace('gc_interval_ms=300000', 'gc_interval_ms=1000')
            text = text.replace(f'schema="{fixture.name}_storage"', f'schema="{schema}"')
            text = text.replace(f'object_prefix="tenant-{fixture.name}"', f'object_prefix="{schema}"')
            # Give concurrently ready independent queries a generous test batch window.
            text += '\n[tenant_storage.batching]\ndelay_ms=175\n'
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
            catalogs = json.loads(sql(f"SELECT json_object_agg(catalog::text,source->>'database') FROM {schema}.catalogs"))
            for tick in range(8):
                advance(streams, 100+tick)
                initial = manifest()
                if verify_packs(initial, catalogs):
                    break
            else:
                raise AssertionError('no pack shared by catalogs in independent source databases')
            check(fixtures)
            (output/'manifest-before.json').write_text(json.dumps(initial, indent=2))
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
            # Keep the healthy sources progressing while collecting. Stop as
            # soon as relocation is witnessed in the central manifest.
            for turn in range(24):
                if turn < 8:
                    advance(streams[1:], 200+turn)
                # The unavailable source config must still collect through
                # its independently configured manifest endpoint.
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
            check(fixtures[2:])
            assert moved, 'offline catalog was not relocated while healthy peers reclaimed shared space'
            assert before.keys() <= {(r['catalog'], r['name']) for r in after}, 'offline logical object lost'
            (output/'manifest-offline.json').write_text(json.dumps(after, indent=2))
            sql(f'ALTER DATABASE {databases[0]} ALLOW_CONNECTIONS true')
            unavailable = False
            worker.process.send_signal(signal.SIGINT)
            worker.process.wait(timeout=240)
            worker.close_logs()
            assert worker.process.returncode != 0, 'source failure was not reported'
            worker = start(command, fixtures, paths, output, 'shared', 'reopened')[0]
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
