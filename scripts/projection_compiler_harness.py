#!/usr/bin/env python3
"""Owned sequential projection CDC, native bag codec and restart qualification."""
from worker_harness import blocked_diagnostic, blocked_recovery, History, run_cases
from harness_control import commit_proxy
import collections
import json
from sql_compiler_harness import SqlFixture
from native_mv_oracle import binary_difference, expected as native_expected


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
            tests = ('TRUE', 'NOT TRUE', 'FALSE', 'NOT FALSE', 'UNKNOWN', 'NOT UNKNOWN')
            projected = ','.join(f'a.enabled IS {test} AS flag_{i}' for i,test in enumerate(tests))
            distinct = ('a.group_id IS DISTINCT FROM a.small_value AS changed,'
                        'a.group_id IS NOT DISTINCT FROM a.small_value AS same')
            simple = ("CASE a.group_id + 0 WHEN a.small_value THEN 'same' "
                      "WHEN NULL THEN 'null match' WHEN 1.0 THEN 'one' ELSE 'other' END AS bucket,"
                      "CASE a.enabled WHEN true THEN 1 WHEN false THEN 2::bigint END AS state,"
                      "CASE a.id WHEN 1 THEN a.vartext ELSE a.vartext END AS label")
            nullif = ("NULLIF(a.group_id, 1.00),"
                      "NULLIF(a.vartext, 'é🙂') AS nonmatching_text,"
                      "NULLIF(a.group_id, a.small_value) AS nonmatching_group")
            ranges = ("a.group_id BETWEEN a.small_value AND 2.0 AS within_range,"
                      "a.group_id NOT BETWEEN NULL AND 1 AS beyond_upper")
            joined = "a.category || '🙂' || a.vartext::text AS joined"
            extrema = ("GREATEST(a.small_value, a.group_id),"
                       "LEAST(a.group_id + 0.00, a.small_value + 0.000, NULL) AS least_numeric")
            bitwise = "a.group_id & 3 AS low_bits, a.small_value << 1 AS shifted"
            remainder = ("mod(a.group_id - 3, a.id) AS integral_remainder,"
                         "mod(a.group_id + 0.00, (a.id - 96) * 1.250) AS numeric_remainder")
            text_joins = ("concat(a.category, a.vartext::text) AS combined,"
                          "concat_ws(a.category, 'é', a.vartext::text, '', NULL) AS separated")
            casts = ("a.small_value::text AS small_text,a.group_id::text AS group_text,"
                     "a.large_value::text AS large_text,a.enabled::text AS enabled_text,"
                     "a.token::text AS token_text")
            self.query = self.query.replace(' FROM ', f',{projected},{distinct},{simple},{nullif},{ranges},{joined},{extrema},{bitwise},{remainder},{text_joins},{casts} FROM ')
            self.query = self.query.replace('(a.enabled OR a.group_id IS NULL)',
                                            "((a.enabled OR a.group_id IS NULL) IS TRUE "
                                            "OR a.group_id IS NOT DISTINCT FROM 1) "
                                            "AND (a.category || '🙂') IS DISTINCT FROM 'excluded🙂' "
                                            "AND CASE a.id WHEN 0 THEN false ELSE true END "
                                            "AND NULLIF(GREATEST(a.id, 1), 7) IS NOT NULL "
                                            "AND a.id BETWEEN 1 AND 101 "
                                            "AND a.id NOT BETWEEN 98 AND 99")

    def verify(self):
        self.sql(f'EXPLAIN {self.query}')
        expected = (f"SELECT jsonb_build_array(NULL,jsonb_build_array(a.group_id,a.category,a.enabled,a.token,a.group_id)) tuple, count(*)::bigint weight "
                    f"FROM {self.name}.auction a WHERE a.id > 0 AND (a.enabled OR a.group_id IS NULL) GROUP BY 1")
        if self.local:
            expected = native_expected(self, ['g','category','active','token','again','sm','lg','vl']
                                       + [f'flag_{i}' for i in range(6)] + ['changed', 'same', 'bucket', 'state', 'label', 'nullif', 'nonmatching_text', 'nonmatching_group',
                                          'within_range', 'beyond_upper', 'joined', 'greatest', 'least_numeric', 'low_bits', 'shifted', 'integral_remainder', 'numeric_remainder', 'combined', 'separated',
                                          'small_text', 'group_text', 'large_text', 'enabled_text', 'token_text'])
            def columns(table):
                return self.sql(f"""SELECT json_agg(json_build_array(attname,atttypid,atttypmod,attcollation) ORDER BY attnum)
                    FROM pg_attribute WHERE attrelid='{self.name}.{table}'::regclass AND attnum>0 AND NOT attisdropped""")
            assert columns('groups') == columns('pgderive_native_oracle')
            assert binary_difference(self) == '0'
        self.verify_sql(expected)
        # Independent raw-source recomputation with explicit true-only qualification.
        rows = json.loads(self.sql(f'SELECT json_agg(a) FROM {self.name}.auction a')) or []
        expected_memory = collections.Counter()
        for row in rows:
            qualifies = row['enabled'] is True or row['group_id'] is None
            if self.local:
                qualifies = ((qualifies or row['group_id'] == 1)
                             and row['category'] != 'excluded' and row['id'] != 7
                             and 1 <= row['id'] <= 101 and not 98 <= row['id'] <= 99)
            if row['id'] > 0 and qualifies:
                values = tuple(row[key] for key in ('group_id', 'category', 'enabled', 'token', 'group_id'))
                if self.local:
                    group = None if row['group_id'] is None else -row['group_id']
                    length = row['category']
                    values = (group,length,row['enabled'],row['token'],group,
                              None if row['small_value'] is None else -row['small_value'],
                              None if row['large_value'] is None else -row['large_value'],
                              row['vartext'])
                    active = row['enabled']
                    values += (active is True, active is not True, active is False,
                               active is not False, active is None, active is not None,
                               row['group_id'] != row['small_value'],
                               row['group_id'] == row['small_value'])
                    key = row['group_id']
                    bucket = 'other'
                    if key is not None and key == row['small_value']:
                        bucket = 'same'
                    elif key == 1:
                        bucket = 'one'
                    state = None if active is None else (1 if active else 2)
                    values += (bucket, state, row['vartext'],
                               None if key == 1 else key,
                               None if row['vartext'] == 'é🙂' else row['vartext'],
                               None if key == row['small_value'] else key)
                    low = row['small_value']
                    within = None
                    if key is not None:
                        if key > 2 or (low is not None and key < low):
                            within = False
                        elif low is not None:
                            within = True
                    values += (within, True if key is not None and key > 1 else None)
                    text = row['vartext']
                    joined = None if length is None or text is None else length + '🙂' + text
                    candidates = [value for value in (key, low) if value is not None]
                    values += (joined, max(candidates, default=None), min(candidates, default=None))
                    values += (None if key is None else key & 3,
                               None if low is None else low * 2,
                               None if key is None else -((3 - key) % row['id']))
                    if key is None:
                        values += (None,)
                    else:
                        quarters = (abs(key) * 4) % (abs(row['id'] - 96) * 5)
                        values += ((-quarters if key < 0 else quarters) / 4,)
                    combined = ''.join(value for value in (length, text) if value is not None)
                    separated = None if length is None else length.join(
                        value for value in ('é', text, '') if value is not None)
                    values += (combined, separated)
                    values += tuple(None if value is None else str(value)
                                    for value in (low, key, row['large_value']))
                    values += (None if active is None else str(active).lower(), row['token'])
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
        if local:
            changes[3] += f"; UPDATE {fixture.name}.auction SET category='excluded' WHERE id=100"
        def initial():
            assert fixture.sql(f'SELECT max(weight) FROM {fixture.bag_relation()}').strip() == '2'
        def committed_fault():
            faults = [json.loads(line) for line in (output / 'sql-fault.jsonl').read_text().splitlines()[1:]]
            assert len(faults) == 1 and faults[0]['server_committed'] is True, faults
        ready = fixture.run_history(command, History(changes,
            f"INSERT INTO {fixture.name}.auction(id,category,group_id,enabled,token) VALUES(101,'after restart',NULL,false,NULL)",
            fixture.query.replace('mod(a.group_id - 3, a.id)', '((a.group_id - 3) % a.id)').replace('a.', '"X".').replace('auction a ', 'auction AS "X" ').replace('a.enabled', '"X".enabled'),
            [('changed-order',fixture.query.replace('(0-a.group_id) AS g, a.category AS category', 'a.category AS category, (0-a.group_id) AS g') if local else fixture.query.replace('a.group_id AS g, a.category AS category', 'a.category AS category, a.group_id AS g')),
             ('changed-alias',fixture.query.replace('AS active', 'AS renamed'))], verify_reopen=False),
            initial=initial, after_stop=committed_fault)
        if local:
            worker = fixture.start(command, 'numeric-remainder-recovery')
            worker.event('ready')
            fixture.sql(f"INSERT INTO {fixture.name}.auction(id,category,group_id,enabled) VALUES(96,'zero divisor',1,true)")
            assert 'division by zero' in blocked_recovery(
                fixture, worker, f"DELETE FROM {fixture.name}.auction WHERE id=96")
            fixture.verify()
            before = fixture.sql(f"SELECT to_jsonb(p)-'fence' FROM {fixture.name}.pgderive_progress p")
            sink_before = fixture.sql(f"SELECT COALESCE(jsonb_agg(jsonb_build_array(tuple,weight) ORDER BY tuple),'[]') FROM {fixture.bag_relation()}")
            bad = fixture.start(command, 'overflow')
            bad.event('ready')
            fixture.sql(f"INSERT INTO {fixture.name}.auction(id,category,group_id,enabled) VALUES(97,'overflow',-2147483648,true)")
            blocked_diagnostic(bad)
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
