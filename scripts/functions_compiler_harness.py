#!/usr/bin/env python3
"""Typed local functions: native materialized view + independent memory through CDC/restart."""
import collections
import json
from sql_compiler_harness import SqlFixture
from worker_harness import harness_arguments, History

EXPRESSIONS = [
    'abs(a.group_id)', 'abs(a.small_value)', 'abs(a.large_value)',
    'length(a.category)', 'char_length(a.category)', 'character_length(a.category)',
    'octet_length(a.category)', 'bit_length(a.category)', 'ascii(a.category)',
    'chr(128578)', 'reverse(a.category)', 'left(a.category,a.group_id)',
    'right(a.category,a.group_id)', 'btrim(a.category)', "ltrim(a.category,'é🙂 ')",
    "rtrim(a.category,'é🙂 ')", "replace(a.category,'é','xy')",
    "translate(a.category,'é🙂é','abz')", "strpos(a.category,'🙂')",
    "starts_with(a.category,'é')", 'repeat(a.category,a.group_id)',
    "split_part(a.category,'🙂',-2)", 'substr(a.category,a.group_id,2)',
    'substring(a.category,a.group_id)', 'to_hex(a.group_id)', 'to_hex(a.large_value)',
    'length(reverse(a.vartext))', "COALESCE(reverse(a.category),'missing')",
    "CASE WHEN a.group_id>0 THEN left(a.category,1) ELSE 'other' END",
]
WHERE = "NOT starts_with(COALESCE(a.category,''),'skip') AND length(COALESCE(a.category,''))>=0"


def strict(function, *arguments):
    return None if any(value is None for value in arguments) else function(*arguments)


def substring(text, start, length=None):
    offset = max(start, 1) - 1
    return text[offset:] if length is None else text[offset:offset + max(start + length - max(start, 1), 0)]


def memory(row):
    text, number, small, large = (row[key] for key in ('category', 'group_id', 'small_value', 'large_value'))
    split = lambda text: (text.split('🙂')[-2] if len(text.split('🙂')) >= 2 else '')
    return (
        strict(abs, number), strict(abs, small), strict(abs, large),
        strict(len, text), strict(len, text), strict(len, text),
        strict(lambda value: len(value.encode()), text), strict(lambda value: len(value.encode())*8, text),
        strict(lambda value: ord(value[0]) if value else 0, text), '🙂', strict(lambda value: value[::-1], text),
        strict(lambda value, n: value[:n], text, number),
        strict(lambda value, n: value[-n:] if n > 0 else value[-n:] if n < 0 else '', text, number),
        strict(lambda value: value.strip(' '), text), strict(lambda value: value.lstrip('é🙂 '), text),
        strict(lambda value: value.rstrip('é🙂 '), text), strict(lambda value: value.replace('é', 'xy'), text),
        strict(lambda value: value.translate(str.maketrans({'é': 'a', '🙂': 'b'})), text),
        strict(lambda value: value.find('🙂')+1, text), strict(lambda value: value.startswith('é'), text),
        strict(lambda value, n: value*max(n, 0), text, number), strict(split, text),
        strict(lambda value, n: substring(value, n, 2), text, number), strict(substring, text, number),
        strict(lambda value: format(value & 0xffffffff, 'x'), number),
        strict(lambda value: format(value & 0xffffffffffffffff, 'x'), large),
        strict(len, row['vartext']), text[::-1] if text is not None else 'missing',
        strict(lambda value: value[:1], text) if number is not None and number > 0 else 'other',
    )


class FunctionsFixture(SqlFixture):
    def __init__(self, output):
        super().__init__(output, False)
        self.sql(f"ALTER TABLE {self.name}.auction ADD COLUMN small_value smallint,ADD COLUMN large_value bigint,ADD COLUMN vartext varchar(64)")
        self.sql(f"UPDATE {self.name}.auction SET category=CASE WHEN id IN(1,2) THEN 'é🙂á é' WHEN id=3 THEN '' WHEN id=4 THEN NULL ELSE 'skip me' END,group_id=CASE WHEN id IN(1,2) THEN 2 WHEN id=3 THEN -2 ELSE NULL END,small_value=-7,large_value=-2147483649,vartext='界🙂'")
        self.query = 'SELECT ' + ','.join(f'{expression} AS v{index}' for index, expression in enumerate(EXPRESSIONS)) + f' FROM {self.name}.auction a WHERE {WHERE}'
        self.sql(f'CREATE MATERIALIZED VIEW {self.name}.oracle AS {self.query}')

    def verify(self):
        self.verify_materialized(','.join(f'v{index}' for index in range(len(EXPRESSIONS))), 'typed function sink differs from refreshed native MV')
        rows = json.loads(self.sql(f'SELECT json_agg(a) FROM {self.name}.auction a')) or []
        expected_memory = collections.Counter(memory(row) for row in rows if not (row['category'] or '').startswith('skip'))
        self.verify_memory(dict(expected_memory))


def qualify(output, command):
    fixture = FunctionsFixture(output)
    with fixture.managed():
        fixture.rejected(command, 'overload', fixture.query.replace('abs(a.group_id)', 'abs(a.group_id::numeric)'))
        assert fixture.sql(f"SELECT count(*) FROM pg_replication_slots WHERE slot_name LIKE '{fixture.name}%'").strip() == '0'
        changes = [
            f"UPDATE {fixture.name}.auction SET category='🙂é🙂',group_id=-1 WHERE id=2",
            f"DELETE FROM {fixture.name}.auction WHERE id=1; UPDATE {fixture.name}.auction SET category='é🙂',group_id=0 WHERE id=3",
            f"INSERT INTO {fixture.name}.auction(id,category,group_id) VALUES(100,NULL,NULL),(101,'é🙂',0)",
            f"UPDATE {fixture.name}.auction SET category='skip now' WHERE id=2; UPDATE {fixture.name}.auction SET category='é🙂',group_id=2 WHERE id=5",
            f"DELETE FROM {fixture.name}.auction WHERE id=100; UPDATE {fixture.name}.auction SET category='',group_id=-2 WHERE id=101",
        ]
        def initial():
            assert fixture.sql(f'SELECT max(weight) FROM {fixture.bag_relation()}').strip() == '2'
        fixture.run_history(command, History(
            changes=changes,
            final=f"UPDATE {fixture.name}.auction SET category='é🙂',group_id=1 WHERE id=101",
            normalized=fixture.query,
            rejected=[],
        ), initial=initial)
        (output/'result.json').write_text(json.dumps({'sql_oracle':'refreshed native materialized view',
            'memory_oracle':'independent Python Unicode/width semantics', 'projection_columns':len(EXPRESSIONS),
            'transactions':len(changes)+1,'restart':'same slot, durable object state',
            'duplicates_nulls_retractions':'exact', 'overload':'rejected before slot'})+'\n')


if __name__ == '__main__':
    qualify(*harness_arguments())
