#!/usr/bin/env python3
"""Native and independent oracles for integral RANGE windows across CDC."""
from collections import Counter, defaultdict
import json

from native_mv_oracle import binary_difference, expected
from sql_compiler_harness import SqlFixture
from worker_harness import History, run_modes


class IntegralRangeFixture(SqlFixture):
    def __init__(self, output, mode):
        super().__init__(output, False)
        width = {'smallint': 16, 'integer': 32, 'bigint': 64}[mode]
        minimum, maximum = -(2 ** (width - 1)), 2 ** (width - 1) - 1
        table = self.name + '.bid'
        self.sql(f'''ALTER TABLE {table} ALTER COLUMN price TYPE {mode};
            UPDATE {table} SET auction=1,price=CASE id
                WHEN 1 THEN '{minimum}'::{mode} WHEN 2 THEN '{minimum + 1}'::{mode}
                WHEN 3 THEN '{maximum}'::{mode} WHEN 4 THEN '{maximum - 1}'::{mode}
                ELSE CASE WHEN id%4=0 THEN NULL ELSE (id%8)*2 END END
            WHERE id<=8;
            UPDATE {table} SET price=CASE WHEN id%4=0 THEN NULL ELSE (id%8)*2 END WHERE id>8''')
        self.frames = [
            (False, False, -1, 2, "1 PRECEDING AND (1+1)::smallint FOLLOWING"),
            (True, True, 1, 2, "1 FOLLOWING AND 2 FOLLOWING"),
            (False, True, None, -1, "UNBOUNDED PRECEDING AND 1 PRECEDING"),
            (True, False, 0, None, "0 FOLLOWING AND UNBOUNDED FOLLOWING"),
            (False, False, -2, -3, "2 PRECEDING AND 3 PRECEDING"),
            (False, False, -9223372036854775807, 9223372036854775807,
             "9223372036854775807 PRECEDING AND 9223372036854775807 FOLLOWING"),
        ]
        fields = ['b.id']
        self.labels = ['id']
        for index, (descending, nulls_first, _, _, bounds) in enumerate(self.frames):
            order = ('DESC' if descending else 'ASC') + (' NULLS FIRST' if nulls_first else ' NULLS LAST')
            window = f'OVER (PARTITION BY b.auction ORDER BY b.price {order} RANGE BETWEEN {bounds})'
            for label, function in [('n', 'count(*)'), ('first', 'first_value(b.price)'),
                                    ('last', 'last_value(b.price)')]:
                alias = f'{label}_{index}'
                fields.append(f'{function} {window} AS {alias}')
                self.labels.append(alias)
        self.query = f"SELECT {','.join(fields)} FROM {table} b"

    def verify(self):
        self.verify_sql(expected(self, self.labels))
        assert binary_difference(self) == '0'
        source = json.loads(self.sql(f"SELECT COALESCE(json_agg(b),'[]') FROM {self.name}.bid b"))
        groups = defaultdict(list)
        for row in source:
            groups[row['auction']].append(row)
        memory = Counter()
        for group in groups.values():
            for row in group:
                fields = [row['id']]
                for descending, nulls_first, start, end, _ in self.frames:
                    orient = -1 if descending else 1

                    def key(value):
                        if value is None:
                            return (0 if nulls_first else 2, 0)
                        return (1, orient * value)

                    current = key(row['price'])
                    lower = None if start is None else (current[0], current[1] + start if current[0] == 1 else 0)
                    upper = None if end is None else (current[0], current[1] + end if current[0] == 1 else 0)
                    frame = sorted((item['price'] for item in group
                                    if (lower is None or key(item['price']) >= lower)
                                    and (upper is None or key(item['price']) <= upper)), key=key)
                    fields.extend((len(frame), frame[0] if frame else None, frame[-1] if frame else None))
                memory[tuple(fields)] += 1
        self.verify_memory(memory)


def qualify(output, command, mode):
    fixture = IntegralRangeFixture(output, mode)
    table = fixture.name + '.bid'
    with fixture.managed():
        fixture.run_history(command, History(
            changes=[
                f'INSERT INTO {table} VALUES (100,1,NULL),(101,1,2),(102,1,2)',
                f'UPDATE {table} SET auction=2,price=4 WHERE id IN (1,100,101)',
                f'UPDATE {table} SET price=NULL WHERE id IN (3,4,102)',
                f'DELETE FROM {table} WHERE id IN (2,5,100)',
            ],
            final=f'UPDATE {table} SET auction=1,price=-3 WHERE id IN (1,101); DELETE FROM {table} WHERE id=4',
            normalized=fixture.query.replace('b.', '"B".').replace('bid b', 'bid AS "B"'),
            rejected=[('changed-bound', fixture.query.replace('1 PRECEDING', '2 PRECEDING', 1))],
        ))
        (output/'result.json').write_text(json.dumps({
            'mode': mode, 'native_bag_and_binary': True, 'independent_oracle': True,
            'cold_restart': True,
        })+'\n')


if __name__ == '__main__':
    run_modes(('smallint', 'integer', 'bigint'), qualify)
