#!/usr/bin/env python3
"""Compare quantified subqueries with PostgreSQL and a raw-source oracle."""
from collections import Counter
import json

from native_mv_oracle import binary_difference, expected
from sql_compiler_harness import SqlFixture
from worker_harness import History, run_modes


def compare(probe, values, all_rows):
    results = [None if probe is None or value is None else probe < value for value in values]
    if (not all_rows) in results:
        return not all_rows
    if None in results:
        return None
    return all_rows


class QuantifiedFixture(SqlFixture):
    labels = ['id', 'any_less', 'all_less']

    def __init__(self, output, mode):
        super().__init__(output, False)
        self.mode = mode
        self.outer = self.name + '.auction'
        self.inner = self.name + '.bid'
        self.sql(f'''DELETE FROM {self.outer}; DELETE FROM {self.inner};
            INSERT INTO {self.outer}(id,group_id) VALUES(1,1),(2,1),(3,2),(4,3),(5,NULL);
            INSERT INTO {self.inner} VALUES(1,1,1),(2,1,2),(3,1,NULL),(4,1,2),
                (5,2,NULL),(6,NULL,100);''')
        key = 'a.id' if mode == 'correlated' else '1'
        rhs = f'SELECT b.price FROM {self.inner} b WHERE b.auction={key}'
        self.query = f'''SELECT a.id, a.group_id < ANY ({rhs}) AS any_less,
            a.group_id < ALL ({rhs}) AS all_less FROM {self.outer} a'''

    def verify(self):
        self.verify_sql(expected(self, self.labels))
        assert binary_difference(self) == '0'
        outer = json.loads(self.sql(f"SELECT COALESCE(json_agg(a),'[]') FROM {self.outer} a"))
        inner = json.loads(self.sql(f"SELECT COALESCE(json_agg(b),'[]') FROM {self.inner} b"))
        memory = Counter()
        for row in outer:
            key = row['id'] if self.mode == 'correlated' else 1
            values = [item['price'] for item in inner if item['auction'] == key]
            probe = row['group_id']
            memory[(row['id'], compare(probe, values, False), compare(probe, values, True))] += 1
        self.verify_memory(memory)


def qualify(output, command, mode):
    fixture = QuantifiedFixture(output, mode)
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
            normalized=fixture.query.replace('ANY', 'SOME'),
            rejected=[],
        ))
        (output/'result.json').write_text(json.dumps({
            'mode': mode, 'cold_restart': True, 'native_bag_and_binary': True,
            'independent_oracle': True,
        })+'\n')


if __name__ == '__main__':
    run_modes(('uncorrelated', 'correlated'), qualify)
