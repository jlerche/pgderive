#!/usr/bin/env python3
"""Owned sequential projection CDC, native bag codec and restart qualification."""
import collections
import json
import pathlib
import sys
import subprocess
import time
from sql_compiler_harness import SqlFixture


class ProjectionFixture(SqlFixture):
    def __init__(self, output):
        super().__init__(output, False)
        self.sql(f"UPDATE {self.name}.auction SET category=NULL,group_id=NULL,enabled=NULL,token='00000000-0000-0000-0000-000000000000' WHERE id IN (1,2)")
        self.query = (f'SELECT a.group_id AS g, a.category AS category, a.enabled AS active, '
                      f'a.token AS token, a.group_id AS again FROM {self.name}.auction a '
                      'WHERE a.id > 0 AND (a.enabled OR a.group_id IS NULL)')

    def verify(self):
        self.sql(f'EXPLAIN {self.query}')
        expected = (f"SELECT jsonb_build_array(NULL,jsonb_build_array(a.group_id,a.category,a.enabled,a.token,a.group_id)) tuple, count(*)::bigint weight "
                    f"FROM {self.name}.auction a WHERE a.id > 0 AND (a.enabled OR a.group_id IS NULL) GROUP BY 1")
        difference = self.sql(f'''WITH expected AS ({expected}), actual AS
            (SELECT tuple,weight FROM {self.name}.groups), difference AS
            ((SELECT * FROM expected EXCEPT ALL SELECT * FROM actual) UNION ALL
            (SELECT * FROM actual EXCEPT ALL SELECT * FROM expected)) SELECT count(*) FROM difference''')
        assert difference.strip() == '0', 'projection sink differs from PostgreSQL bag'
        # Independent raw-source recomputation with explicit true-only qualification.
        rows = json.loads(self.sql(f'SELECT json_agg(a) FROM {self.name}.auction a')) or []
        expected_memory = collections.Counter()
        for row in rows:
            if row['id'] > 0 and (row['enabled'] is True or row['group_id'] is None):
                expected_memory[tuple(row[key] for key in ('group_id', 'category', 'enabled', 'token', 'group_id'))] += 1
        actual = json.loads(self.sql(f"SELECT COALESCE(json_agg(g),'[]') FROM {self.name}.groups g"))
        assert {tuple(row['tuple'][1]): row['weight'] for row in actual} == dict(expected_memory)
        assert all(row['tuple'][0] is None and row['weight'] > 0 for row in actual)
        beyond = self.sql(f'''SELECT count(*) FROM pg_replication_slots s JOIN
            {self.name}.pgderive_worker_registration r ON r.slot_name=s.slot_name JOIN
            {self.name}.pgderive_progress p ON true WHERE s.confirmed_flush_lsn>p.end_lsn''')
        assert beyond.strip() == '0', 'slot acknowledged beyond publication'


def qualify(output, command):
    fixture = ProjectionFixture(output)
    proxy = None
    proxy_log = None
    try:
        fixture.rejected(command, 'unsupported', fixture.query.replace('a.enabled AS active', 'now() AS active'))
        assert fixture.sql(f"SELECT to_regclass('{fixture.name}.pgderive_worker_registration') IS NULL").strip() == 't'
        assert fixture.sql(f"SELECT count(*) FROM pg_replication_slots WHERE slot_name LIKE '{fixture.name}%'").strip() == '0'
        proxy_log = (output / 'sql-fault.jsonl').open('w')
        proxy = subprocess.Popen([sys.executable, '-u', 'scripts/pg_commit_proxy.py',
                                  '--mode', 'after', '--once', '--arm-query', 'pgderive_progress SET commit_lsn'],
                                 stdout=proxy_log, stderr=proxy_log)
        deadline = time.monotonic() + 10
        while time.monotonic() < deadline:
            lines = (output / 'sql-fault.jsonl').read_text().splitlines()
            if lines:
                fixture.environment['PGDERIVE__POSTGRES__PORT'] = str(json.loads(lines[0])['port'])
                break
            time.sleep(.05)
        else:
            raise RuntimeError('projection SQL fault proxy did not start')
        worker = fixture.start(command, 'first')
        ready = worker.event('ready')
        fixture.verify()
        assert fixture.sql(f'SELECT max(weight) FROM {fixture.name}.groups').strip() == '2'
        changes = [
            f"UPDATE {fixture.name}.auction SET group_id=1,enabled=false WHERE id=2; UPDATE {fixture.name}.bid SET price=9 WHERE id=2",
            f"UPDATE {fixture.name}.auction SET group_id=NULL,enabled=NULL WHERE id=2; DELETE FROM {fixture.name}.auction WHERE id=1",
            f"INSERT INTO {fixture.name}.auction(id,category,group_id,enabled,token) VALUES(100,NULL,NULL,NULL,'00000000-0000-0000-0000-000000000000')",
            f"UPDATE {fixture.name}.auction SET enabled=true,category='reentered',group_id=2 WHERE id=2",
            f"DELETE FROM {fixture.name}.auction WHERE id=100; UPDATE {fixture.name}.bid SET price=price+1 WHERE id=3",
            f"UPDATE {fixture.name}.bid SET price=price+1 WHERE id=4",  # complete empty query delta tick
        ]
        for index, change in enumerate(changes, 1):
            fixture.sql('BEGIN;' + change + ';COMMIT')
            worker.event('ready' if index == 1 else 'published', minimum_time=ready['time'] + index)
            fixture.verify()
        worker.abort()
        faults = [json.loads(line) for line in (output / 'sql-fault.jsonl').read_text().splitlines()[1:]]
        assert len(faults) == 1 and faults[0]['server_committed'] is True, faults
        prior = fixture.sql(f'SELECT row_to_json(p) FROM {fixture.name}.pgderive_progress p')
        fixture.rejected(command, 'changed-order', fixture.query.replace('a.group_id AS g, a.category AS category', 'a.category AS category, a.group_id AS g'))
        fixture.rejected(command, 'changed-alias', fixture.query.replace('AS active', 'AS renamed'))
        assert fixture.sql(f'SELECT row_to_json(p) FROM {fixture.name}.pgderive_progress p') == prior
        fixture.query = fixture.query.replace('a.', '"X".').replace('auction a ', 'auction AS "X" ').replace('a.enabled', '"X".enabled')
        resumed = fixture.start(command, 'resumed', maximum=1)
        reopened = resumed.event('ready')
        assert reopened['slot'] == ready['slot'] and reopened['time'] == ready['time'] + len(changes)
        fixture.sql(f"INSERT INTO {fixture.name}.auction(id,category,group_id,enabled,token) VALUES(101,'after restart',NULL,false,NULL)")
        resumed.event('published')
        resumed.finish()
        fixture.verify()
        (output / 'result.json').write_text(json.dumps({'sql_oracle': 'exact native weighted bag',
            'memory_oracle': 'independent source recomputation', 'restart': 'normalized SQL same slot',
            'projection_collisions': 'weight two then exact retractions', 'empty_tick': 'other relation transaction',
            'changed_layout': 'order and alias rejected without progress', 'lost_commit': 'authoritative recovery after committed response loss', 'process_restart': 'killed worker cold resumes', 'transactions': len(changes)+1}) + '\n')
        fixture.cleanup()
    except BaseException:
        fixture.preserve()
        raise
    finally:
        if proxy:
            proxy.terminate()
            proxy.wait(timeout=10)
        if proxy_log:
            proxy_log.close()


if __name__ == '__main__':
    output = pathlib.Path(sys.argv[1])
    command = sys.argv[2:]
    if command and command[0] == '--':
        command = command[1:]
    qualify(output, command)
