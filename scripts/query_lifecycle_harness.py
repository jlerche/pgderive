#!/usr/bin/env python3
"""Owned CLI/result-table qualification; preserve dumps and process logs on failure."""
import json
import collections
import pathlib
import re
import subprocess
import time
import urllib.error
import urllib.request
import hashlib

from sql_compiler_harness import SqlFixture
from nexmark_projection_harness import ProjectionFixture
from nexmark_q1_harness import Q1Fixture
from native_compiler_harness import NativeFixture
from native_mv_oracle import expected as native_expected
from worker_harness import Worker, harness_arguments


def configured(fixture):
    return fixture.config()



def inspect(command, fixture, action, config):
    result = subprocess.run(command + ['query', action, str(config)],
                            env=fixture.environment, text=True, capture_output=True)
    with (fixture.output/'commands.jsonl').open('a') as evidence:
        evidence.write(json.dumps({'action': action, 'code': result.returncode,
                                   'stdout': result.stdout, 'stderr': result.stderr})+'\n')
    assert result.returncode == 0, result.stderr
    return json.loads(result.stdout)


def rejected(command, fixture, action, config):
    result = subprocess.run(command+['query', action, str(config)],
                            env=fixture.environment, text=True, capture_output=True, timeout=40)
    with (fixture.output/'commands.jsonl').open('a') as evidence:
        evidence.write(json.dumps({'action': action, 'expected': 'rejection',
                                   'code': result.returncode, 'stderr': result.stderr})+'\n')
    assert result.returncode != 0, 'invalid query/result interface was accepted'
    return result.stderr


def verify_result(fixture, labels):
    fixture.verify()
    native_expected(fixture, labels)
    oracle = fixture.name+'.pgderive_native_oracle'
    result = fixture.name+'.groups'
    difference = fixture.sql(f'''SELECT count(*) FROM
        ((SELECT * FROM {oracle} EXCEPT ALL SELECT * FROM {result}) UNION ALL
         (SELECT * FROM {result} EXCEPT ALL SELECT * FROM {oracle})) differences''')
    assert difference.strip() == '0', 'typed public result differs from native materialized view'
    metadata = fixture.sql(f'''SELECT json_agg(json_build_array(attname,atttypid,atttypmod,attcollation) ORDER BY attnum)
        FROM pg_attribute WHERE attrelid='{result}'::regclass AND attnum>0 AND NOT attisdropped''')
    original = fixture.sql(f'''SELECT json_agg(json_build_array(attname,atttypid,atttypmod,attcollation) ORDER BY attnum)
        FROM pg_attribute WHERE attrelid='{oracle}'::regclass AND attnum>0 AND NOT attisdropped''')
    assert json.loads(metadata) == json.loads(original), 'public result loses native output type/modifier/collation'


def qualify(output, command, factory):
    fixture = factory(output)
    try:
        config = configured(fixture)
        validated = inspect(command, fixture, 'validate', config)
        labels = [column['name'] for column in validated['columns']]
        assert 'placement' not in validated
        assert 'terminal' not in validated['compiled']['relational']['output']

        assert fixture.sql(f"SELECT to_regclass('{fixture.name}.pgderive_worker_registration') IS NULL").strip() == 't'
        assert fixture.sql(f"SELECT count(*) FROM pg_replication_slots WHERE slot_name LIKE '{fixture.name}%'").strip() == '0'
        assert inspect(command, fixture, 'status', config)['state'] == 'unregistered'
        rejected(command, fixture, 'restart', config)
        rejected(command, fixture, 'drop', config)
        original = config.read_text()
        before, after = original.split('[worker.query]')
        _, execution = after.split('[execution]')
        for bad in [f'SELECT now() FROM {fixture.name}.bid',
                    f'SELECT price AS duplicate,price AS duplicate FROM {fixture.name}.bid']:
            config.write_text(before+'[worker.query]\nsql='+json.dumps(bad)+'\n[execution]'+execution)
            rejected(command, fixture, 'validate', config)
            assert fixture.sql(f"SELECT to_regclass('{fixture.name}.pgderive_worker_registration') IS NULL").strip() == 't'
        config.write_text(original)
        first = output/'first'
        first.mkdir()
        worker = Worker(command+['query', 'create'], config, first, fixture.environment)
        fixture.workers.append(worker)
        ready = worker.event('ready')
        for action in ('create', 'restart', 'drop'):
            assert 'another worker owns this query' in rejected(command, fixture, action, config)
        verify_result(fixture, labels)
        assert inspect(command, fixture, 'status', config)['progress']['logical_time'] == ready['time']
        changes = [
            f"UPDATE {fixture.name}.bid SET price=NULL WHERE id=1; UPDATE {fixture.name}.auction SET category='changed' WHERE id=1",
            f"UPDATE {fixture.name}.bid SET price=123 WHERE id=2",
            f"DELETE FROM {fixture.name}.bid WHERE id=2",
        ]
        for index, change in enumerate(changes, 1):
            fixture.sql('BEGIN;'+change+';COMMIT')
            worker.event('published', minimum_time=ready['time']+index)
            verify_result(fixture, labels)
        worker.finish(graceful=True)
        rejected(command, fixture, 'create', config)
        other = original.replace('query_id="grouped"', 'query_id="different"').replace(
            f'object_prefix="{fixture.name}"', f'object_prefix="{fixture.name}_other"').replace(
            'retry_attempts=8', 'retry_attempts=1')
        config.write_text(other)
        assert 'one object-store prefix' in rejected(command, fixture, 'create', config)
        config.write_text(original)
        logical = fixture.sql(f"SELECT logical_time FROM {fixture.name}.pgderive_queries")
        progress = fixture.sql(f"SELECT end_lsn FROM {fixture.name}.pgderive_progress")
        first_column = '"'+labels[0].replace('"', '""')+'"'
        fixture.sql(f'ALTER TABLE {fixture.name}.groups RENAME COLUMN {first_column} TO tampered')
        config.write_text(original.replace('retry_attempts=8', 'retry_attempts=1'))
        rejected(command, fixture, 'restart', config)
        assert fixture.sql(f"SELECT logical_time FROM {fixture.name}.pgderive_queries") == logical
        assert fixture.sql(f"SELECT end_lsn FROM {fixture.name}.pgderive_progress") == progress
        fixture.sql(f'ALTER TABLE {fixture.name}.groups RENAME COLUMN tampered TO {first_column}')
        config.write_text(original)
        resumed = output/'resumed'
        resumed.mkdir()
        worker = Worker(command+['query', 'restart'], config, resumed, fixture.environment)
        fixture.workers.append(worker)
        reopened = worker.event('ready')
        assert reopened['slot'] == ready['slot']
        verify_result(fixture, labels)
        worker.finish(graceful=True)
        (output/'result.json').write_text(json.dumps({
            'validation': 'no slot or durable registration', 'columns': validated['columns'],
            'oracles': 'native materialized view EXCEPT ALL and independent memory',
            'history': 'bootstrap NULL update simultaneous inputs delete cold restart',
        })+'\n')
        retire(command, fixture, config, validated)
        fixture.sql(f'DROP PUBLICATION {fixture.name}; DROP SCHEMA {fixture.name} CASCADE')
    except BaseException:
        fixture.preserve()
        raise



def retire(command, fixture, config, validated):
    def source_rows():
        result = []
        for relation in validated['source']['relations']:
            quote = lambda name: '"'+name.replace('"', '""')+'"'
            table = quote(relation['schema'])+'.'+quote(relation['table'])
            result.append(fixture.sql(f'SELECT coalesce(jsonb_agg(row ORDER BY row::text),\'[]\'::jsonb) FROM (SELECT to_jsonb(t) row FROM {table} t) rows'))
        return result
    before = source_rows()
    roots = json.loads(fixture.sql(f"SELECT jsonb_agg(reference) FROM {fixture.name}.pgderive_objects"))
    assert roots, 'retirement fixture has no immutable roots'
    fixture.sql(f"""INSERT INTO {fixture.name}.pgderive_protections(token,query_id,owner_fence,uploading,roots)
        SELECT 'external-reader','grouped',0,false,jsonb_agg(reference)
        FROM {fixture.name}.pgderive_objects""")
    endpoint = fixture.environment.get('PGDERIVE__OBJECT_STORE__ENDPOINT', 'http://127.0.0.1:8333')
    def get(path):
        with urllib.request.urlopen(f'{endpoint}/pgderive-mvp-tests/{fixture.name}/{path}', timeout=10) as response:
            return response.read()
    retired = inspect(command, fixture, 'drop', config)
    assert retired['state'] == 'dropped'
    assert source_rows() == before, 'drop mutated source rows'
    assert fixture.sql(f"SELECT count(*) FROM {fixture.name}.pgderive_queries").strip() == '0'
    assert fixture.sql(f"SELECT count(*) FROM pg_replication_slots WHERE slot_name LIKE '{fixture.name}%'").strip() == '0'
    assert inspect(command, fixture, 'status', config)['state'] == 'dropped'
    assert 'query removal is pending or complete' in rejected(command, fixture, 'restart', config)
    for root in roots:
        content = get(root['path'])
        index_bytes = content
        assert hashlib.sha256(index_bytes).hexdigest() == root['hash']
        index = json.loads(index_bytes)
        for block in index['blocks']:
            child = get(root['namespace']+'/'+block['path'])
            assert hashlib.sha256(child).hexdigest() == block['hash']
    fixture.sql(f"UPDATE {fixture.name}.pgderive_protections SET active=false WHERE token='external-reader'")
    inspect(command, fixture, 'drop', config)
    for root in roots:
        try:
            get(root['path'])
            raise AssertionError('released retired root was not collected')
        except urllib.error.HTTPError as error:
            assert error.code == 404


def namespace_race(output, command):
    """Hold registration serialization until both independent creates contend."""
    fixture = grouped(output)
    blocker = None
    try:
        config = configured(fixture)
        original = config.read_text()
        # Install catalog DDL without registering unsupported SQL.
        before, after = original.split('[worker.query]')
        _, execution = after.split('[execution]')
        config.write_text(before+'[worker.query]\nsql='+json.dumps(
            f'SELECT now() FROM {fixture.name}.bid')+'\n[execution]'+execution)
        rejected(command, fixture, 'create', config)
        fixture.sql(f"""CREATE TABLE {fixture.name}.pgderive_worker_registration(
            query_id text PRIMARY KEY,format_version integer NOT NULL CHECK(format_version=1),
            logical_slot text NOT NULL UNIQUE,slot_name text NOT NULL UNIQUE,
            source jsonb NOT NULL,sink jsonb NOT NULL,plan_identity text NOT NULL,
            object_prefix text NOT NULL)""")
        blocker = subprocess.Popen(fixture.pg[:-1]+['-At'], env=fixture.environment,
                                   stdin=subprocess.PIPE, stdout=subprocess.PIPE, text=True)
        blocker.stdin.write(f"BEGIN; SELECT pg_advisory_xact_lock(hashtextextended('pgderive-worker-registration:{fixture.name}',0)); SELECT 'held';\n")
        blocker.stdin.flush()
        while blocker.stdout.readline().strip() != 'held':
            assert blocker.poll() is None, 'registration lock holder exited'
        workers = []
        for index in range(2):
            path = output/f'create-{index}.toml'
            text = original.replace('retry_attempts=8', 'retry_attempts=1')
            if index:
                text = text.replace('query_id="grouped"', 'query_id="different"').replace(
                    f'slot="{fixture.name}"', f'slot="{fixture.name}_other"').replace(
                    'sink_table="groups"', 'sink_table="other_groups"').replace(
                    f'object_prefix="{fixture.name}"', f'object_prefix="{fixture.name}_other"')
            path.write_text(text)
            logs = output/f'create-{index}'
            logs.mkdir()
            worker = Worker(command+['query', 'create'], path, logs, fixture.environment)
            workers.append(worker)
            fixture.workers.append(worker)
        deadline = time.monotonic()+8
        while True:
            waiting = int(fixture.sql("SELECT count(*) FROM pg_stat_activity WHERE wait_event='advisory' AND query LIKE 'SELECT pg_catalog.pg_advisory_xact_lock%'"))
            if waiting == 2:
                break
            assert time.monotonic() < deadline, 'both registrations did not contend'
            time.sleep(.05)
        blocker.stdin.write('COMMIT;\n'+chr(92)+'q\n')
        blocker.stdin.flush()
        blocker.wait(timeout=10)
        deadline = time.monotonic()+30
        while all(worker.process.poll() is None for worker in workers):
            assert time.monotonic() < deadline, 'incompatible create was not rejected'
            time.sleep(.05)
        losers = [worker for worker in workers if worker.process.poll() is not None]
        assert len(losers) == 1 and losers[0].process.returncode != 0
        assert 'one object-store prefix' in pathlib.Path(losers[0].stderr.name).read_text()
        winner = next(worker for worker in workers if worker not in losers)
        winner.event('ready')
        winner.finish(graceful=True)
        assert fixture.sql(f'SELECT count(*) FROM {fixture.name}.pgderive_worker_registration').strip() == '1'
        assert fixture.sql(f"SELECT count(*) FROM pg_replication_slots WHERE slot_name LIKE '{fixture.name}%'").strip() == '1'
        (output/'qualification.json').write_text(json.dumps({'contending_creates':2,'registered':1,'physical_slots':1})+'\n')
        fixture.cleanup()
    except BaseException:
        fixture.preserve()
        raise
    finally:
        if blocker is not None and blocker.poll() is None:
            blocker.kill()
            blocker.wait(timeout=10)


def grouped(output):
    fixture = SqlFixture(output, False)
    fixture.query = fixture.query.replace('COUNT(*)', 'COUNT(*) AS "row count"')
    return fixture


class ScalarFixture(SqlFixture):
    def __init__(self, output):
        super().__init__(output, False)
        self.labels = ['auction', 'price', 'small', 'flag', 'token', 'label', 'moment', 'amount']
        self.sql(f'''ALTER TABLE {self.name}.bid ADD COLUMN small smallint,
            ADD COLUMN flag boolean,ADD COLUMN token uuid,ADD COLUMN label varchar(32) COLLATE "C",
            ADD COLUMN moment timestamp(3),ADD COLUMN amount numeric(12,3);
            UPDATE {self.name}.bid SET small=-32768,flag=true,
            token='a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11',label='é🙂',
            moment='2000-01-01 12:00:00.123',amount='1.230';
            INSERT INTO {self.name}.bid(id,auction,price) VALUES(300,NULL,NULL),(301,NULL,NULL)''')
        self.query = f"SELECT {','.join(self.labels)} FROM {self.name}.bid"

    def verify(self):
        raw = json.loads(self.sql(f"SELECT json_agg(b) FROM {self.name}.bid b"), parse_float=str)
        memory = collections.Counter(tuple(row[label] for label in self.labels) for row in raw)
        actual = json.loads(self.sql(f"SELECT json_agg(g) FROM {self.bag_relation()} g"), parse_float=str)
        assert {tuple(row['tuple'][1]): row['weight'] for row in actual} == dict(memory)


class WideningFixture(SqlFixture):
    def __init__(self, output):
        super().__init__(output, False)
        self.sql(f"ALTER TABLE {self.name}.bid ALTER COLUMN price TYPE integer; ALTER TABLE {self.name}.bid ADD COLUMN small_value smallint; UPDATE {self.name}.bid SET small_value=CASE WHEN id%4=0 THEN NULL WHEN id%4=1 THEN -32768 ELSE 32767 END; UPDATE {self.name}.bid SET price=2147483647 WHERE id IN(1,5); UPDATE {self.name}.bid SET price=-2147483648 WHERE id=2")
        self.query = f"SELECT price::bigint AS wide,price::bigint AS again,small_value::integer AS medium,small_value::bigint AS big,CASE WHEN id>0 THEN 1 ELSE 0 END AS flag FROM {self.name}.bid WHERE id>0"

    def verify(self):
        raw = json.loads(self.sql(f"SELECT json_agg(b) FROM {self.name}.bid b WHERE id>0"))
        memory = collections.Counter((row['price'],row['price'],row['small_value'],row['small_value'],1) for row in raw)
        actual = json.loads(self.sql(f"SELECT json_agg(g) FROM {self.bag_relation()} g"))
        assert {tuple(row['tuple'][1]):row['weight'] for row in actual} == dict(memory)


if __name__ == '__main__':
    output, command = harness_arguments()
    race = output/'namespace-race'
    race.mkdir()
    namespace_race(race, command)
    for name, factory in [
        ('grouped', grouped),
        ('projection-local', lambda out: ProjectionFixture(out, 'q0', 'local')),
        ('projection-instant', lambda out: ProjectionFixture(out, 'q0', 'instant')),
        ('numeric', lambda out: Q1Fixture(out, 'int8')),
        ('native-range', lambda out: NativeFixture(out, 'projection')),
        ('native-scalars', ScalarFixture),
        ('deferred-widening', WideningFixture),
    ]:
        directory = output/name
        directory.mkdir()
        qualify(directory, command, factory)
