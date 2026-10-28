#!/usr/bin/env python3
"""Owned sequential projection CDC, native bag codec and restart qualification."""
from worker_harness import History, run_cases
from harness_control import commit_proxy
import collections
import json
from sql_compiler_harness import SqlFixture


class ProjectionFixture(SqlFixture):
    def __init__(self, output, local=False):
        super().__init__(output, False)
        self.local = local
        self.sql(f"UPDATE {self.name}.auction SET category=NULL,group_id=NULL,enabled=NULL,token='00000000-0000-0000-0000-000000000000' WHERE id IN (1,2)")
        self.query = (f'SELECT a.group_id AS g, a.category AS category, a.enabled AS active, '
                      f'a.token AS token, a.group_id AS again FROM {self.name}.auction a '
                      'WHERE a.id > 0 AND (a.enabled OR a.group_id IS NULL)')
        if local:
            self.sql(f"ALTER TABLE {self.name}.auction ADD COLUMN small_value smallint, ADD COLUMN large_value bigint, ADD COLUMN vartext varchar(64)")
            self.sql(f"UPDATE {self.name}.auction SET small_value=CASE WHEN id=1 THEN -2 ELSE 2 END,large_value=CASE WHEN id=1 THEN -3 ELSE 3 END,vartext=CASE WHEN id=1 THEN 'é🙂' ELSE '界x' END WHERE id IN (1,2)")
            self.sql(f"UPDATE {self.name}.auction SET category=CASE WHEN id=1 THEN 'é🙂' ELSE '界x' END,group_id=CASE WHEN id=1 THEN -2 ELSE 2 END,enabled=true WHERE id IN (1,2)")
            self.query = self.query.replace('a.group_id AS', '(0-a.group_id) AS').replace(' FROM ', ',(0-a.small_value) AS sm,(0::bigint-a.large_value) AS lg,a.vartext AS vl FROM ')

    def verify(self):
        self.sql(f'EXPLAIN {self.query}')
        expected = (f"SELECT jsonb_build_array(NULL,jsonb_build_array(a.group_id,a.category,a.enabled,a.token,a.group_id)) tuple, count(*)::bigint weight "
                    f"FROM {self.name}.auction a WHERE a.id > 0 AND (a.enabled OR a.group_id IS NULL) GROUP BY 1")
        if self.local:
            expected = expected.replace('jsonb_build_array(a.group_id,a.category,a.enabled,a.token,a.group_id)', 'jsonb_build_array((0-a.group_id),a.category,a.enabled,a.token,(0-a.group_id),(0-a.small_value),(0::bigint-a.large_value),a.vartext)')
        self.verify_sql(expected)
        # Independent raw-source recomputation with explicit true-only qualification.
        rows = json.loads(self.sql(f'SELECT json_agg(a) FROM {self.name}.auction a')) or []
        expected_memory = collections.Counter()
        for row in rows:
            if row['id'] > 0 and (row['enabled'] is True or row['group_id'] is None):
                values = tuple(row[key] for key in ('group_id', 'category', 'enabled', 'token', 'group_id'))
                if self.local:
                    group = None if row['group_id'] is None else -row['group_id']
                    length = row['category']
                    values = (group,length,row['enabled'],row['token'],group,
                              None if row['small_value'] is None else -row['small_value'],
                              None if row['large_value'] is None else -row['large_value'],
                              row['vartext'])
                expected_memory[values] += 1
        actual = json.loads(self.sql(f"SELECT COALESCE(json_agg(g),'[]') FROM {self.bag_relation()} g"))
        assert {tuple(row['tuple'][1]): row['weight'] for row in actual} == dict(expected_memory)
        assert all(row['tuple'][0] is None and row['weight'] > 0 for row in actual)
        beyond = self.acknowledgement_ahead()
        assert beyond.strip() == '0', 'slot acknowledged beyond publication'


def qualify(output, command, local=False):
    fixture = ProjectionFixture(output, local)
    proxy = None
    try:
        fixture.rejected(command, 'unsupported', fixture.query.replace('a.enabled AS active', 'now() AS active'))
        assert fixture.sql(f"SELECT to_regclass('{fixture.name}.pgderive_worker_registration') IS NULL").strip() == 't'
        assert fixture.sql(f"SELECT count(*) FROM pg_replication_slots WHERE slot_name LIKE '{fixture.name}%'").strip() == '0'
        proxy, port = commit_proxy(output/'sql-fault.jsonl', 'after', once=True,
                                   marker='/* pgderive:epoch */')
        fixture.environment['PGDERIVE__POSTGRES__PORT'] = str(port)
        changes = [
            f"UPDATE {fixture.name}.auction SET group_id=1,enabled=false WHERE id=2; UPDATE {fixture.name}.bid SET price=9 WHERE id=2",
            f"UPDATE {fixture.name}.auction SET group_id=NULL,enabled=NULL WHERE id=2; DELETE FROM {fixture.name}.auction WHERE id=1",
            f"INSERT INTO {fixture.name}.auction(id,category,group_id,enabled,token) VALUES(100,NULL,NULL,NULL,'00000000-0000-0000-0000-000000000000')",
            f"UPDATE {fixture.name}.auction SET enabled=true,category='reentered',group_id=2 WHERE id=2",
            f"DELETE FROM {fixture.name}.auction WHERE id=100; UPDATE {fixture.name}.bid SET price=price+1 WHERE id=3",
            f"UPDATE {fixture.name}.bid SET price=price+1 WHERE id=4",  # complete empty query delta tick
        ]
        def initial():
            assert fixture.sql(f'SELECT max(weight) FROM {fixture.bag_relation()}').strip() == '2'
        def committed_fault():
            faults = [json.loads(line) for line in (output / 'sql-fault.jsonl').read_text().splitlines()[1:]]
            assert len(faults) == 1 and faults[0]['server_committed'] is True, faults
        ready = fixture.run_history(command, History(changes,
            f"INSERT INTO {fixture.name}.auction(id,category,group_id,enabled,token) VALUES(101,'after restart',NULL,false,NULL)",
            fixture.query.replace('a.', '"X".').replace('auction a ', 'auction AS "X" ').replace('a.enabled', '"X".enabled'),
            [('changed-order',fixture.query.replace('(0-a.group_id) AS g, a.category AS category', 'a.category AS category, (0-a.group_id) AS g') if local else fixture.query.replace('a.group_id AS g, a.category AS category', 'a.category AS category, a.group_id AS g')),
             ('changed-alias',fixture.query.replace('AS active', 'AS renamed'))], verify_reopen=False),
            initial=initial, after_stop=committed_fault)
        if local:
            before = fixture.sql(f"SELECT to_jsonb(p)-'fence' FROM {fixture.name}.pgderive_progress p")
            sink_before = fixture.sql(f"SELECT COALESCE(jsonb_agg(jsonb_build_array(tuple,weight) ORDER BY tuple),'[]') FROM {fixture.bag_relation()}")
            bad = fixture.start(command, 'overflow')
            bad.event('ready')
            fixture.sql(f"INSERT INTO {fixture.name}.auction(id,category,group_id,enabled) VALUES(999,'overflow',-2147483648,true)")
            bad.process.wait(timeout=60)
            assert bad.process.returncode != 0, 'local integer overflow was accepted'
            bad.abort()
            assert fixture.sql(f"SELECT to_jsonb(p)-'fence' FROM {fixture.name}.pgderive_progress p") == before
            assert fixture.sql(f"SELECT COALESCE(jsonb_agg(jsonb_build_array(tuple,weight) ORDER BY tuple),'[]') FROM {fixture.bag_relation()}") == sink_before
            beyond = fixture.sql(f"SELECT count(*) FROM pg_replication_slots WHERE slot_name='{ready['slot']}' AND confirmed_flush_lsn > (SELECT checkpoint_lsn FROM {fixture.name}.pgderive_progress)")
            assert beyond.strip() == '0', 'failed local map advanced slot ACK'
        (output / 'result.json').write_text(json.dumps({'sql_oracle': 'exact native weighted bag',
            'memory_oracle': 'independent source recomputation', 'restart': 'normalized SQL same slot',
            'projection_collisions': 'weight two then exact retractions', 'empty_tick': 'other relation transaction',
            'changed_layout': 'order and alias rejected without progress', 'lost_commit': 'authoritative recovery after committed response loss', 'process_restart': 'killed worker cold resumes', 'transactions': len(changes)+1, 'local': local, 'integer_overflow': 'sink/progress/ACK unchanged' if local else 'not applicable'}) + '\n')
        fixture.cleanup()
    except BaseException:
        fixture.preserve()
        raise
    finally:
        if proxy:
            proxy.terminate()
            proxy.wait(timeout=10)


if __name__ == '__main__':
    run_cases(((mode, mode == 'local') for mode in ('plain', 'local')), qualify)
