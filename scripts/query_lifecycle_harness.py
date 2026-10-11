#!/usr/bin/env python3
"""Owned CLI/result-view qualification; preserve dumps and process logs on failure."""
import json
import collections
import pathlib
import re
import subprocess
import sys

from sql_compiler_harness import SqlFixture
from nexmark_projection_harness import ProjectionFixture
from nexmark_q1_harness import Q1Fixture
from nexmark_q14_harness import Q14Fixture
from native_compiler_harness import NativeFixture
from native_mv_oracle import expected as native_expected
from worker_harness import Worker


def configured(fixture):
    config = fixture.config()
    config.write_text(config.read_text().replace(
        'sink_table="groups"', 'sink_table="groups"\nresult_view="result"'))
    return config


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


def verify_result(fixture, labels):
    fixture.verify()
    native_expected(fixture, labels)
    oracle = fixture.name+'.pgderive_native_oracle'
    result = fixture.name+'.result'
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
        assert fixture.sql(f"SELECT to_regclass('{fixture.name}.pgderive_worker_registration') IS NULL").strip() == 't'
        assert fixture.sql(f"SELECT count(*) FROM pg_replication_slots WHERE slot_name LIKE '{fixture.name}%'").strip() == '0'
        assert inspect(command, fixture, 'status', config)['state'] == 'unregistered'
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
        verify_result(fixture, labels)
        assert inspect(command, fixture, 'status', config)['progress']['logical_time'] == ready['time']
        changes = [
            f"UPDATE {fixture.name}.bid SET price=NULL WHERE id=1; UPDATE {fixture.name}.auction SET category='changed' WHERE id=1",
            f"UPDATE {fixture.name}.bid SET price=123 WHERE id=2",
            f"DELETE FROM {fixture.name}.bid WHERE id=2",
        ]
        if isinstance(fixture, ArrayFixture):
            changes.append(f"UPDATE {fixture.name}.bid SET label='channel_id=updated' WHERE id=1")
        for index, change in enumerate(changes, 1):
            fixture.sql('BEGIN;'+change+';COMMIT')
            worker.event('published', minimum_time=ready['time']+index)
            verify_result(fixture, labels)
        worker.finish(graceful=True)
        logical = fixture.sql(f"SELECT logical_time FROM {fixture.name}.pgderive_queries")
        progress = fixture.sql(f"SELECT end_lsn FROM {fixture.name}.pgderive_progress")
        first_column = '"'+labels[0].replace('"', '""')+'"'
        fixture.sql(f'ALTER VIEW {fixture.name}.result RENAME COLUMN {first_column} TO tampered')
        config.write_text(original.replace('retry_attempts=8', 'retry_attempts=1'))
        rejected(command, fixture, 'restart', config)
        assert fixture.sql(f"SELECT logical_time FROM {fixture.name}.pgderive_queries") == logical
        assert fixture.sql(f"SELECT end_lsn FROM {fixture.name}.pgderive_progress") == progress
        fixture.sql(f'ALTER VIEW {fixture.name}.result RENAME COLUMN tampered TO {first_column}')
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
        fixture.cleanup()
    except BaseException:
        fixture.preserve()
        raise


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
        actual = json.loads(self.sql(f"SELECT json_agg(g) FROM {self.name}.groups g"), parse_float=str)
        assert {tuple(row['tuple'][1]): row['weight'] for row in actual} == dict(memory)


class ArrayFixture(ScalarFixture):
    def __init__(self, output):
        super().__init__(output)
        self.sql(f"UPDATE {self.name}.bid SET label=CASE WHEN id%4=0 THEN NULL WHEN id%4=1 THEN 'channel_id=' WHEN id%4=2 THEN 'a&channel_id=é🙂' ELSE 'missing' END")
        self.query = f'''SELECT pg_catalog.regexp_match(label COLLATE pg_catalog."C",'(&|^)channel_id=([^&]*)') AS captures FROM {self.name}.bid'''

    def verify(self):
        labels = json.loads(self.sql(f"SELECT json_agg(label) FROM {self.name}.bid"))
        memory = collections.Counter()
        for label in labels:
            match = None if label is None else re.search('(&|^)channel_id=([^&]*)', label)
            memory[None if match is None else match.groups()] += 1
        actual = json.loads(self.sql(f"SELECT json_agg(g) FROM {self.name}.groups g"))
        assert {(None if row['tuple'][1][0] is None else tuple(row['tuple'][1][0])):
                row['weight'] for row in actual} == dict(memory)


if __name__ == '__main__':
    output = pathlib.Path(sys.argv[1])
    command = sys.argv[2:]
    if command and command[0] == '--':
        command = command[1:]
    for name, factory in [
        ('grouped', grouped),
        ('projection-local', lambda out: ProjectionFixture(out, 'q0', 'local')),
        ('projection-instant', lambda out: ProjectionFixture(out, 'q0', 'instant')),
        ('numeric', lambda out: Q1Fixture(out, 'int8')),
        ('terminal-program', lambda out: Q14Fixture(out, 'instant')),
        ('native-range', lambda out: NativeFixture(out, 'projection')),
        ('native-scalars', ScalarFixture),
        ('native-array', ArrayFixture),
    ]:
        directory = output/name
        directory.mkdir()
        qualify(directory, command, factory)
