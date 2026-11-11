#!/usr/bin/env python3
"""Native Boolean and UUID coercion and pairwise set types through CDC."""
from collections import Counter
import json

from native_mv_oracle import binary_difference, expected
from sql_compiler_harness import SqlFixture
from worker_harness import History, run_modes


OPERATIONS = {
    'union-all': 'UNION ALL',
    'union': 'UNION',
    'intersect-all': 'INTERSECT ALL',
    'intersect': 'INTERSECT',
    'except-all': 'EXCEPT ALL',
    'except': 'EXCEPT',
}


class ResolutionFixture(SqlFixture):
    def verify_native(self, cast=None):
        oracle_query = self.query
        if cast is not None:
            # PostgreSQL SELECT accepts this collated unknown, while view creation
            # rejects its collation on the coerced noncollatable result column.
            self.verify_sql()
            oracle_query = self.query.replace('COLLATE "C"', cast)
        self.verify_sql(expected(self, self.labels, query_sql=oracle_query))
        assert binary_difference(self) == '0'
        attributes = ("SELECT json_agg(json_build_array(attname,atttypid,atttypmod,attcollation) "
                      "ORDER BY attnum) FROM pg_attribute WHERE attrelid='%s'::regclass "
                      "AND attnum>0 AND NOT attisdropped")
        assert self.sql(attributes % (self.name+'.groups')) == self.sql(
            attributes % (self.name+'.pgderive_native_oracle'))

    def verify_set(self, left, right):
        if self.mode.endswith('-all'):
            values = self.apply_set_operation(left, right)
        else:
            values = self.apply_set_operation(Counter(dict.fromkeys(left, 1)),
                                              Counter(dict.fromkeys(right, 1)))
            values = dict.fromkeys(values, 1)
        self.verify_memory(values)

    def apply_set_operation(self, left, right):
        if self.mode.startswith('union'):
            return left + right
        if self.mode.startswith('intersect'):
            return left & right
        return left - right


class BooleanResolutionFixture(ResolutionFixture):
    def __init__(self, output, mode):
        super().__init__(output, False)
        self.mode = mode
        a = self.name + '.auction'
        b = self.name + '.bid'
        if mode == 'scalar':
            self.query = f'''SELECT id,
                CASE WHEN enabled THEN 'off' ELSE enabled END AS searched,
                CASE enabled WHEN 'yes' COLLATE "C" THEN '0' WHEN NULL THEN 'no'
                    ELSE enabled END AS simple,
                COALESCE(enabled, 'Y' COLLATE "C", NULL) AS fallback,
                COALESCE(CASE WHEN enabled THEN 'off' ELSE NULL::boolean END,' on ') AS nested,
                CASE WHEN 'yes' COLLATE "C" THEN enabled ELSE 'off' END AS condition,
                CASE WHEN enabled THEN NULL END AS null_case,
                COALESCE(NULL,NULL) AS null_coalesce
                FROM {a}'''
            self.labels = ['id', 'searched', 'simple', 'fallback', 'nested',
                           'condition', 'null_case', 'null_coalesce']
        elif mode == 'unknown':
            self.query = (f'SELECT NULL AS flag FROM {a} WHERE enabled '
                          f'UNION ALL SELECT NULL FROM {b} WHERE price IS NULL')
            self.labels = ['flag']
        else:
            self.query = (f'SELECT enabled AS flag FROM {a} {OPERATIONS[mode]} '
                          f'(SELECT false AS flag WHERE false UNION ALL '
                          f'SELECT \' On \' COLLATE "C" FROM {b} WHERE price>0 UNION ALL '
                          f'SELECT NULL FROM {b} WHERE price IS NULL)')
            self.labels = ['flag']

    def verify(self):
        self.verify_native('::boolean' if self.mode in OPERATIONS else None)
        rows = json.loads(self.sql(f"SELECT COALESCE(json_agg(a),'[]') FROM {self.name}.auction a"))
        if self.mode == 'scalar':
            values = Counter()
            for row in rows:
                flag = row['enabled']
                case = None if flag is None else False
                values[(row['id'], case, case, flag is not False, flag is not True,
                        flag, None, None)] += 1
            self.verify_memory(values)
            return
        prices = json.loads(self.sql(f"SELECT COALESCE(json_agg(price),'[]') FROM {self.name}.bid"))
        if self.mode == 'unknown':
            count = sum(row['enabled'] is True for row in rows) + prices.count(None)
            self.verify_memory({(None,): count} if count else {})
            return
        left = Counter((row['enabled'],) for row in rows)
        right = Counter((True if price is not None else None,) for price in prices
                        if price is None or price > 0)
        self.verify_set(left, right)


UUID = 'a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11'
ZERO_UUID = '00000000-0000-0000-0000-000000000000'
RAW_UUID = '{A0EE-BC99-9C0B-4EF8-BB6D-6BB9-BD38-0A11}'


class UuidResolutionFixture(ResolutionFixture):
    def __init__(self, output, mode):
        super().__init__(output, False)
        self.mode = mode
        a = self.name + '.auction'
        b = self.name + '.bid'
        if mode == 'scalar':
            self.query = f"""SELECT id,
                CASE WHEN enabled THEN '{RAW_UUID}' COLLATE "C" ELSE token END AS searched,
                CASE token WHEN '{RAW_UUID}' THEN '{ZERO_UUID}'
                    WHEN NULL THEN '{RAW_UUID}' ELSE token END AS simple,
                COALESCE(token, '{RAW_UUID}', NULL) AS fallback,
                CASE WHEN enabled THEN token END AS typed_null
                FROM {a}"""
            self.labels = ['id', 'searched', 'simple', 'fallback', 'typed_null']
        else:
            self.query = (f"SELECT token AS value FROM {a} {OPERATIONS[mode]} "
                          f"(SELECT NULL::uuid AS value WHERE false UNION ALL "
                          f"SELECT '{RAW_UUID}' COLLATE \"C\" FROM {b} WHERE price>0 UNION ALL "
                          f"SELECT NULL FROM {b} WHERE price IS NULL)")
            self.labels = ['value']

    def verify(self):
        self.verify_native('::uuid' if self.mode in OPERATIONS else None)
        rows = json.loads(self.sql(f"SELECT COALESCE(json_agg(a),'[]') FROM {self.name}.auction a"))
        if self.mode == 'scalar':
            values = Counter()
            for row in rows:
                token = row['token']
                enabled = row['enabled'] is True
                values[(row['id'], UUID if enabled else token,
                        ZERO_UUID if token == UUID else token,
                        token if token is not None else UUID, token if enabled else None)] += 1
            self.verify_memory(values)
            return
        prices = json.loads(self.sql(f"SELECT COALESCE(json_agg(price),'[]') FROM {self.name}.bid"))
        self.verify_set(Counter((row['token'],) for row in rows),
                        Counter((UUID if price is not None else None,) for price in prices
                                if price is None or price > 0))


def qualify(output, command, mode):
    fixture = BooleanResolutionFixture(output, mode)
    a = fixture.name + '.auction'
    b = fixture.name + '.bid'
    with fixture.managed():
        if mode == 'scalar':
            fixture.rejected(command, 'invalid-unselected-literal',
                             f"SELECT COALESCE(true,'bad') AS flag FROM {a}")
        fixture.run_history(command, History(
            changes=[
                f'UPDATE {a} SET enabled=NULL WHERE id=2; INSERT INTO {b} VALUES (100,1,NULL)',
                f'UPDATE {a} SET enabled=true WHERE id=3; UPDATE {b} SET price=9 WHERE id=100',
                f'UPDATE {a} SET enabled=false WHERE id=2; DELETE FROM {b} WHERE price IS NULL',
            ],
            final=f'UPDATE {a} SET enabled=NULL WHERE id=3; UPDATE {b} SET price=NULL WHERE id=100',
            normalized=fixture.query,
            rejected=[],
        ))
        (output/'result.json').write_text(json.dumps({
            'mode': mode, 'native_binary_and_metadata': True, 'independent_bag': True,
            'cold_restart': True,
        })+'\n')


def qualify_uuid(output, command, mode):
    fixture = UuidResolutionFixture(output, mode.removeprefix('uuid-'))
    a = fixture.name + '.auction'
    b = fixture.name + '.bid'
    with fixture.managed():
        if fixture.mode == 'scalar':
            fixture.rejected(command, 'invalid-unselected-literal',
                             f"SELECT COALESCE('{UUID}'::uuid,'bad') AS value FROM {a}")
        fixture.run_history(command, History(
            changes=[
                f'UPDATE {a} SET token=NULL WHERE id=2; INSERT INTO {b} VALUES (100,1,NULL)',
                f"UPDATE {a} SET token='{ZERO_UUID}',enabled=true WHERE id=3; "
                f'UPDATE {b} SET price=9 WHERE id=100',
                f"UPDATE {a} SET token='{UUID}' WHERE id=2; DELETE FROM {b} WHERE price IS NULL",
            ],
            final=f'UPDATE {a} SET token=NULL WHERE id=3; UPDATE {b} SET price=NULL WHERE id=100',
            normalized=fixture.query,
            rejected=[],
        ))
        (output/'result.json').write_text(json.dumps({
            'mode': mode, 'native_binary_and_metadata': True, 'independent_bag': True,
            'cold_restart': True,
        })+'\n')


if __name__ == '__main__':
    run_modes(('scalar', 'unknown', *OPERATIONS), qualify)
    run_modes(tuple('uuid-'+mode for mode in ('scalar', *OPERATIONS)), qualify_uuid)
