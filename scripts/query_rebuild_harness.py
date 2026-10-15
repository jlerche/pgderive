#!/usr/bin/env python3
"""Owned fresh-generation rebuild tests; preserve failed fixture and process evidence."""
import collections
import json
import pathlib
import subprocess
import sys
import urllib.request
import urllib.error
import hashlib

from query_lifecycle_harness import configured, grouped, rejected
from native_mv_oracle import expected as native_expected
from worker_harness import Worker


def command(fixture, binary, action, config):
    result = subprocess.run(binary + ['query', action, str(config)],
                            env=fixture.environment, text=True, capture_output=True, timeout=240)
    with (fixture.output/'commands.jsonl').open('a') as evidence:
        evidence.write(json.dumps({'action': action, 'code': result.returncode,
                                   'stdout': result.stdout, 'stderr': result.stderr})+'\n')
    assert result.returncode == 0, result.stderr
    return json.loads(result.stdout.splitlines()[-1])


def verify(fixture, labels):
    if getattr(fixture, 'native_oracle_ready', False) and fixture.native_oracle_sql != fixture.query:
        fixture.sql(f'DROP MATERIALIZED VIEW {fixture.name}.pgderive_native_oracle')
        fixture.native_oracle_ready = False
    native_expected(fixture, labels)
    oracle = fixture.name+'.pgderive_native_oracle'
    result = fixture.name+'.result'
    assert fixture.sql(f'''SELECT count(*) FROM
        ((SELECT * FROM {oracle} EXCEPT ALL SELECT * FROM {result}) UNION ALL
         (SELECT * FROM {result} EXCEPT ALL SELECT * FROM {oracle})) d''').strip() == '0'
    auctions = json.loads(fixture.sql(f'SELECT json_agg(a) FROM {fixture.name}.auction a'))
    bids = json.loads(fixture.sql(f'SELECT json_agg(b) FROM {fixture.name}.bid b'))
    groups = {}
    for auction in auctions:
        for bid in bids:
            if (auction['category'] is None or auction['id'] != bid['auction']
                    or (fixture.null_only and bid['price'] is not None)):
                continue
            counts = groups.setdefault(auction['group_id'], [0, None])
            counts[0] += 1
            if bid['price'] is not None:
                counts[1] = (counts[1] or 0) + bid['price']
    memory = collections.Counter((group, *counts) for group, counts in groups.items())
    rows = json.loads(fixture.sql(f'SELECT coalesce(json_agg(r),\'[]\'::json) FROM {result} r'))
    actual = collections.Counter(tuple(row[label] for label in labels) for row in rows)
    assert actual == memory, 'rebuilt public bag differs from independent raw-source oracle'


def pin_roots(fixture):
    roots = json.loads(fixture.sql(f"SELECT jsonb_agg(reference) FROM {fixture.name}.pgderive_objects"))
    assert roots
    fixture.sql(f"INSERT INTO {fixture.name}.pgderive_protections(token,query_id,owner_fence,uploading,roots) SELECT 'rebuild-reader','grouped',0,false,jsonb_agg(reference) FROM {fixture.name}.pgderive_objects")
    return roots


def object_bytes(fixture, path):
    endpoint = fixture.environment.get('PGDERIVE__OBJECT_STORE__ENDPOINT','http://127.0.0.1:8333')
    with urllib.request.urlopen(f'{endpoint}/pgderive-mvp-tests/{fixture.name}/{path}',timeout=10) as response:
        return response.read()


def verify_pins(fixture, roots):
    for root in roots:
        content = object_bytes(fixture,root['path'])
        index = content
        assert hashlib.sha256(index).hexdigest() == root['hash']
        for block in json.loads(index)['blocks']:
            content = object_bytes(fixture,root['namespace']+'/'+block['path'])
            assert hashlib.sha256(content).hexdigest() == block['hash']


def verify_collected(fixture, roots):
    for root in roots:
        try:
            object_bytes(fixture,root['path'])
            raise AssertionError('released retired root was not collected')
        except urllib.error.HTTPError as error:
            assert error.code == 404


def qualify(output, binary):
    fixture = grouped(output)
    try:
        config = configured(fixture)
        labels = [column['name'] for column in command(fixture,binary,'validate',config)['columns']]
        worker_dir = output/'initial'
        worker_dir.mkdir()
        worker = Worker(binary+['query','create'],config,worker_dir,fixture.environment)
        fixture.workers.append(worker)
        first = worker.event('ready')
        verify(fixture,labels)
        worker.finish(graceful=True)
        oid = fixture.sql(f"SELECT '{fixture.name}.result'::regclass::oid").strip()
        fixture.sql(f'CREATE VIEW {fixture.name}.consumer AS SELECT * FROM {fixture.name}.result')
        fixture.sql(f'''BEGIN; UPDATE {fixture.name}.bid SET price=NULL WHERE id=1;
            UPDATE {fixture.name}.auction SET category='changed' WHERE id=1;
            DELETE FROM {fixture.name}.bid WHERE id=2; COMMIT''')
        roots = pin_roots(fixture)
        rebuilt = command(fixture,binary,'rebuild',config)
        assert rebuilt['state'] == 'activated'
        assert rebuilt['lifecycle']['phase'] == 'Active'
        assert rebuilt['lifecycle']['retired'] == []
        verify_pins(fixture,roots)
        assert fixture.sql(f"SELECT '{fixture.name}.result'::regclass::oid").strip() == oid
        assert fixture.sql(f"SELECT count(*) FROM pg_replication_slots WHERE slot_name='{first['slot']}'").strip() == '0'
        verify(fixture,labels)
        resumed_dir = output/'resumed'
        resumed_dir.mkdir()
        worker = Worker(binary+['query','restart'],config,resumed_dir,fixture.environment)
        fixture.workers.append(worker)
        ready = worker.event('ready')
        assert ready['slot'] != first['slot']
        fixture.sql(f'''BEGIN; UPDATE {fixture.name}.bid SET price=123 WHERE id=3;
            UPDATE {fixture.name}.auction SET group_id=9 WHERE id=1; COMMIT''')
        worker.event('published',minimum_time=ready['time']+1)
        verify(fixture,labels)
        worker.finish(graceful=True)
        # Changed relational semantics require a new snapshot and plan, never reuse.
        fixture.null_only = True
        fixture.where += 'AND b.price IS NULL '
        fixture.query = fixture.query.replace('GROUP BY', 'AND b.price IS NULL GROUP BY')
        config = configured(fixture)
        rejected(binary,fixture,'restart',config)
        command(fixture,binary,'rebuild',config)
        verify(fixture,labels)
        # An incompatible public layout keeps the prior view/pointer when consumers exist.
        old_generation = command(fixture,binary,'status',config)['lifecycle']['active']
        fixture.query = fixture.query.replace('"row count"', '"new count"')
        config = configured(fixture)
        rejected(binary,fixture,'rebuild',config)
        pending = command(fixture,binary,'status',config)
        assert pending['lifecycle']['phase'] == 'Building'
        assert pending['lifecycle']['active'] == old_generation
        rejected(binary,fixture,'restart',config)
        original = config.read_text()
        config.write_text(original.replace('new count', 'different count'))
        rejected(binary,fixture,'rebuild',config)
        config.write_text(original)
        fixture.sql(f'DROP VIEW {fixture.name}.consumer')
        command(fixture,binary,'rebuild',config)
        labels[1] = 'new count'
        verify(fixture,labels)
        command(fixture,binary,'drop',config)
        assert command(fixture,binary,'status',config)['state'] == 'Dropped'
        verify_pins(fixture,roots)
        fixture.sql(f"UPDATE {fixture.name}.pgderive_protections SET active=false WHERE token='rebuild-reader'")
        command(fixture,binary,'drop',config)
        verify_collected(fixture,roots)
        # Explicit create after removal reserves an entirely new physical generation.
        recreated_dir = output/'recreated'
        recreated_dir.mkdir()
        worker = Worker(binary+['query','create'],config,recreated_dir,fixture.environment)
        fixture.workers.append(worker)
        recreated = worker.event('ready')
        assert recreated['slot'] != ready['slot']
        verify(fixture,labels)
        worker.finish(graceful=True)
        command(fixture,binary,'drop',config)
        (output/'qualification.json').write_text(json.dumps({'rebuild':True,'native_mv':True,
            'memory_oracle':True,'cold_restart':True,'dependent_view_oid_preserved':True,'reader_pin_release_gc':True})+'\n')
        fixture.sql(f'DROP PUBLICATION {fixture.name}; DROP SCHEMA {fixture.name} CASCADE')
    except BaseException:
        fixture.preserve()
        raise


if __name__ == '__main__':
    output = pathlib.Path(sys.argv[1])
    output.mkdir(parents=True,exist_ok=True)
    binary = sys.argv[2:]
    if binary and binary[0] == '--':
        binary = binary[1:]
    qualify(output,binary)
