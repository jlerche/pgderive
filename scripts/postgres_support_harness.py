#!/usr/bin/env python3
"""Execute the declared scalar support matrix against native PostgreSQL and Python."""
import collections
import json
import pathlib
from functions_compiler_harness import substring
from native_mv_oracle import binary_difference, expected
from sql_compiler_harness import SqlFixture
from worker_harness import History, harness_arguments

ROOT = pathlib.Path(__file__).resolve().parent.parent
MATRIX = ROOT / 'docs/postgres-support-matrix.json'


def load_matrix(path=MATRIX):
    matrix = json.loads(path.read_text())
    cases = matrix['scalar_functions'] + matrix['coercion_cases']
    keys = [(case['name'], tuple(case['input_oids'])) for case in matrix['scalar_functions']]
    if not keys or len(keys) != len(set(keys)):
        raise ValueError('Scalar signatures must be nonempty and unique')
    for case in cases:
        if not case['expression'] or len(case['arguments']) != len(case['input_oids']):
            raise ValueError('Every signature requires an expression and one oracle argument per input')
        for argument in case['arguments']:
            if set(argument) not in ({'column'}, {'literal'}):
                raise ValueError('Oracle arguments must identify a column or literal')
    ids = [case['id'] for case in matrix['rejections']]
    if not ids or len(ids) != len(set(ids)):
        raise ValueError('Rejection identifiers must be nonempty and unique')
    for case in matrix['rejections']:
        if not case['expression'] or not case['error_contains']:
            raise ValueError('Each rejected query requires SQL and a diagnostic')
    rejected_oids = [case['oid'] for case in matrix['source_rejections']]
    if len(rejected_oids) != len(set(rejected_oids)) or set(rejected_oids) != set(matrix['unsupported_source_type_oids']):
        raise ValueError('Source rejection cases must exactly cover declared rejected OIDs')
    admitted = matrix['source_type_oids']
    if len(admitted) != len(set(admitted)) or set(admitted).intersection(rejected_oids):
        raise ValueError('Admitted source OIDs must be unique and disjoint from rejections')
    for feature in matrix['features']:
        for field in ['implementation', 'qualification']:
            for name in feature[field]:
                if not (ROOT / name).is_file():
                    raise ValueError('Missing support-matrix reference: ' + name)
    return matrix


def evaluate(case, row):
    args = [row[argument['column']] if 'column' in argument else argument['literal']
            for argument in case['arguments']]
    if any(value is None for value in args):
        return None
    text, *rest = args
    name = case['name']
    if name in ['length', 'char_length', 'character_length']:
        return len(text)
    if name in ['octet_length', 'bit_length']:
        return len(text.encode('utf8')) * (8 if name == 'bit_length' else 1)
    if name == 'ascii':
        return ord(text[0]) if text else 0
    if name == 'chr':
        return chr(text)
    if name == 'reverse':
        return text[::-1]
    if name == 'left':
        return text[:rest[0]]
    if name == 'right':
        return text[-rest[0]:] if rest[0] else ''
    if name in ['ltrim', 'rtrim', 'btrim']:
        method = {'ltrim': str.lstrip, 'rtrim': str.rstrip, 'btrim': str.strip}[name]
        return method(text, rest[0] if rest else ' ')
    if name == 'replace':
        return text.replace(*rest) if rest[0] else text
    if name == 'translate':
        mapping = {}
        for index, character in enumerate(rest[0]):
            if character not in mapping:
                mapping[character] = rest[1][index] if index < len(rest[1]) else ''
        return ''.join(mapping.get(character, character) for character in text)
    if name == 'strpos':
        return text.find(rest[0]) + 1
    if name == 'starts_with':
        return text.startswith(rest[0])
    if name == 'repeat':
        return text * max(rest[0], 0)
    if name == 'split_part':
        pieces = text.split(rest[0]) if rest[0] else [text]
        position = rest[1] - 1 if rest[1] > 0 else rest[1]
        return pieces[position] if -len(pieces) <= position < len(pieces) else ''
    if name in ['substr', 'substring']:
        return substring(text, *rest)
    if name == 'to_hex':
        width = 64 if case['input_oids'][0] == 20 else 32
        return format(text & ((1 << width) - 1), 'x')
    if name == 'abs':
        return abs(text)
    raise ValueError('No independent oracle for ' + name)


class SupportFixture(SqlFixture):
    def __init__(self, output, matrix):
        super().__init__(output, False)
        self.cases = matrix['scalar_functions'] + matrix['coercion_cases']
        self.labels = ['v' + str(index) for index in range(len(self.cases))]
        self.sql(f'''ALTER TABLE {self.name}.auction
            ADD COLUMN small_value smallint, ADD COLUMN large_value bigint,
            ADD COLUMN codepoint integer, ADD COLUMN vartext varchar(64),
            ADD COLUMN event_time timestamptz, ADD COLUMN local_time timestamp, ADD COLUMN amount numeric;
            UPDATE {self.name}.auction SET
            category=CASE WHEN id IN(1,2) THEN 'é🙂á é' WHEN id=3 THEN ''
                          WHEN id=4 THEN NULL ELSE 'skip me' END,
            group_id=CASE WHEN id IN(1,2) THEN 2 WHEN id=3 THEN -2 ELSE NULL END,
            small_value=CASE WHEN id=4 THEN NULL ELSE -7 END,
            large_value=CASE WHEN id=4 THEN NULL ELSE -2147483649 END,
            codepoint=CASE WHEN id=4 THEN NULL WHEN id=3 THEN 65 ELSE 128578 END,
            vartext=CASE WHEN id=4 THEN NULL ELSE '界🙂' END,
            event_time='2024-03-10 01:30:00-05',local_time='2024-03-10 01:30:00',amount=3.14''')
        self.query = ('SELECT ' + ','.join(case['expression'] + ' AS ' + label
                      for case, label in zip(self.cases, self.labels))
                      + f" FROM {self.name}.auction a WHERE NOT starts_with(COALESCE(a.category,''),'skip')")

    def verify(self):
        self.verify_sql(expected(self, self.labels))
        assert binary_difference(self) == '0', 'Native send representations differ'
        def columns(table):
            return self.sql(f"SELECT json_agg(json_build_array(attname,atttypid,atttypmod,attcollation) ORDER BY attnum) FROM pg_attribute WHERE attrelid='{self.name}.{table}'::regclass AND attnum>0 AND NOT attisdropped").strip()
        assert columns('groups') == columns('pgderive_native_oracle'), 'Sink column metadata differs'
        rows = json.loads(self.sql(f'SELECT json_agg(a) FROM {self.name}.auction a')) or []
        memory = collections.Counter(tuple(evaluate(case, row) for case in self.cases)
                    for row in rows if not (row['category'] or '').startswith('skip'))
        self.verify_memory(dict(memory))


def qualify(output, command):
    matrix = load_matrix()
    fixture = SupportFixture(output, matrix)
    with fixture.managed():
        admitted = json.loads(fixture.sql(f"SELECT json_agg(DISTINCT a.atttypid::integer) FROM pg_publication_rel pr JOIN pg_publication p ON p.oid=pr.prpubid JOIN pg_attribute a ON a.attrelid=pr.prrelid AND a.attnum>0 AND NOT a.attisdropped WHERE p.pubname='{fixture.name}'"))
        assert set(admitted) == set(matrix['source_type_oids']), admitted
        for case in fixture.cases:
            actual = int(fixture.sql(f"SELECT pg_typeof({case['expression']})::oid FROM {fixture.name}.auction a LIMIT 1"))
            assert actual == case['result_oid'], (case, actual)
            for argument, oid in zip(case['arguments'], case['input_oids']):
                if 'column' in argument:
                    actual = int(fixture.sql(f"SELECT pg_typeof(a.{argument['column']})::oid FROM {fixture.name}.auction a LIMIT 1"))
                    assert actual == oid, (case, argument, actual)
        for case in matrix['rejections']:
            query = f"SELECT {case['expression']} FROM {fixture.name}.auction a"
            fixture.sql('EXPLAIN ' + query)  # Valid PostgreSQL SQL, unsupported by this engine.
            fixture.rejected(command, case['id'], query)
            diagnostic = (output / case['id'] / 'stderr.log').read_text()
            assert case['error_contains'] in diagnostic, (case['id'], diagnostic)
            assert fixture.sql(f"SELECT to_regclass('{fixture.name}.pgderive_worker_registration') IS NULL").strip() == 't'
            assert fixture.sql(f"SELECT to_regclass('{fixture.name}.groups') IS NULL").strip() == 't'
            assert fixture.sql(f"SELECT count(*) FROM pg_replication_slots WHERE slot_name='{fixture.name}'").strip() == '0'
        for case in matrix['source_rejections']:
            assert int(fixture.sql(f"SELECT pg_typeof(NULL::{case['sql_type']})::oid")) == case['oid']
            fixture.sql(f"ALTER TABLE {fixture.name}.auction ADD COLUMN unsupported {case['sql_type']}")
            fixture.rejected(command, 'source-' + str(case['oid']), fixture.query)
            diagnostic = (output / ('source-' + str(case['oid'])) / 'stderr.log').read_text()
            assert case['error_contains'] in diagnostic, diagnostic
            assert fixture.sql(f"SELECT to_regclass('{fixture.name}.pgderive_worker_registration') IS NULL").strip() == 't'
            assert fixture.sql(f"SELECT count(*) FROM pg_replication_slots WHERE slot_name='{fixture.name}'").strip() == '0'
            assert fixture.sql(f"SELECT to_regclass('{fixture.name}.groups') IS NULL").strip() == 't'
            fixture.sql(f'ALTER TABLE {fixture.name}.auction DROP COLUMN unsupported')
        changes = [
            f"UPDATE {fixture.name}.auction SET category='🙂é🙂',group_id=-1,codepoint=233 WHERE id=2",
            f"DELETE FROM {fixture.name}.auction WHERE id=1; UPDATE {fixture.name}.auction SET category='é🙂',group_id=0 WHERE id=3",
            f"INSERT INTO {fixture.name}.auction(id,category,group_id) VALUES(100,NULL,NULL),(101,'é🙂',0)",
            f"UPDATE {fixture.name}.auction SET category='skip now' WHERE id=2; UPDATE {fixture.name}.auction SET category='é🙂',group_id=2 WHERE id=5",
            f"DELETE FROM {fixture.name}.auction WHERE id=100; UPDATE {fixture.name}.auction SET category='',group_id=-2 WHERE id=101",
        ]
        def initial():
            assert fixture.sql(f'SELECT max(weight) FROM {fixture.bag_relation()}').strip() == '2'
        fixture.run_history(command, History(changes=changes,
            final=f"UPDATE {fixture.name}.auction SET category='é🙂',group_id=1 WHERE id=101",
            normalized=fixture.query, rejected=[]), initial=initial)
        (output / 'result.json').write_text(json.dumps({
            'strict_signatures': len(matrix['scalar_functions']),
            'coercion_cases': len(matrix['coercion_cases']), 'rejected_cases': len(matrix['rejections']),
            'unsupported_source_types': len(matrix['source_rejections']),
            'native_oracle': 'refreshed original SQL, weighted bag, send bytes and column metadata',
            'independent_oracle': 'Python Unicode and integer-width rules',
            'transactions': len(changes) + 1, 'restart': 'same slot and durable state',
            'duplicates_nulls_retractions': 'exact'}) + '\n')


if __name__ == '__main__':
    qualify(*harness_arguments())
