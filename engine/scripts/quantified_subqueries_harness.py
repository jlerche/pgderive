#!/usr/bin/env python3
"""Compare quantified subqueries with PostgreSQL and a raw-source oracle."""
from collections import Counter
import json
import operator

from native_mv_oracle import binary_difference, expected
from sql_compiler_harness import SqlFixture
from worker_harness import History, run_cases


COMPARISONS = (
    ('lt', '<', operator.lt), ('le', '<=', operator.le), ('eq', '=', operator.eq),
    ('ne', '!=', operator.ne), ('ge', '>=', operator.ge), ('gt', '>', operator.gt),
)
GROUPS = {'lower': COMPARISONS[:3], 'upper': COMPARISONS[3:]}


def compare(probe, values, operation, all_rows):
    results = [None if probe is None or value is None else operation(probe, value) for value in values]
    if (not all_rows) in results:
        return not all_rows
    if None in results:
        return None
    return all_rows


class QuantifiedFixture(SqlFixture):
    def __init__(self, output, mode, group):
        super().__init__(output, False)
        self.mode = mode
        self.comparisons = GROUPS[group]
        self.labels = ['id']
        self.outer = self.name + '.auction'
        self.inner = self.name + '.bid'
        self.sql(f'''DELETE FROM {self.outer}; DELETE FROM {self.inner};
            INSERT INTO {self.outer}(id,group_id) VALUES(1,1),(2,1),(3,2),(4,3),(5,NULL);
            ALTER TABLE {self.outer} ADD COLUMN correlation_key integer;
            UPDATE {self.outer} SET correlation_key=id WHERE id<>5;
            INSERT INTO {self.inner} VALUES(1,1,1),(2,1,2),(3,1,NULL),(4,1,2),
                (5,2,NULL),(6,NULL,100);''')
        key = 'a.correlation_key' if mode == 'correlated' else '1'
        rhs = f'SELECT b.price FROM {self.inner} b WHERE b.auction={key}'
        projections = ['a.id']
        for label, symbol, _ in self.comparisons:
            for quantifier in ('ANY', 'ALL'):
                name = f'{label}_{quantifier.lower()}'
                self.labels.append(name)
                projections.append(f'a.group_id {symbol} {quantifier} ({rhs}) AS {name}')
        self.query = f"SELECT {', '.join(projections)} FROM {self.outer} a"

    def verify(self):
        self.verify_sql(expected(self, self.labels))
        assert binary_difference(self) == '0'
        outer = json.loads(self.sql(f"SELECT COALESCE(json_agg(a),'[]') FROM {self.outer} a"))
        inner = json.loads(self.sql(f"SELECT COALESCE(json_agg(b),'[]') FROM {self.inner} b"))
        memory = Counter()
        for row in outer:
            key = row['correlation_key'] if self.mode == 'correlated' else 1
            values = [item['price'] for item in inner if key is not None and item['auction'] == key]
            truth = tuple(
                compare(row['group_id'], values, operation, all_rows)
                for _, _, operation in self.comparisons for all_rows in (False, True)
            )
            memory[(row['id'], *truth)] += 1
        self.verify_memory(memory)


def qualify(output, command, mode, group):
    fixture = QuantifiedFixture(output, mode, group)
    a, b = fixture.outer, fixture.inner
    with fixture.managed():
        fixture.run_history(command, History(
            changes=[
                f'DELETE FROM {b} WHERE id IN (1,2)',
                f'DELETE FROM {b} WHERE id=4; UPDATE {a} SET group_id=NULL WHERE id=1',
                f'DELETE FROM {b} WHERE id=3',
                f'INSERT INTO {b} VALUES(7,1,2),(8,1,2); UPDATE {a} SET group_id=1 WHERE id=1',
            ],
            final=f'UPDATE {b} SET auction=4 WHERE id IN (7,8)',
            normalized=fixture.query.replace('ANY', 'SOME').replace('!=', '<>'),
            rejected=[],
        ))
        (output/'result.json').write_text(json.dumps({
            'mode': mode, 'operators': [symbol for _, symbol, _ in fixture.comparisons],
            'cold_restart': True, 'native_bag_and_binary': True,
            'independent_oracle': True,
        })+'\n')


if __name__ == '__main__':
    run_cases(((f'{mode}-{group}', mode, group)
               for mode in ('uncorrelated', 'correlated') for group in GROUPS), qualify)
