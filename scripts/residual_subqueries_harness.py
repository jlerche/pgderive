#!/usr/bin/env python3
"""Check keyed inequality correlation against native PostgreSQL and a raw-source oracle."""
from collections import Counter
import json
import operator

from native_mv_oracle import binary_difference, expected
from quantified_subqueries_harness import COMPARISONS, compare
from sql_compiler_harness import SqlFixture
from worker_harness import History, run_cases


QUANTIFIED = tuple(item for item in COMPARISONS if item[0] in ('lt', 'eq', 'ge'))


def membership(probe, values):
    if not values:
        return False
    if probe is None:
        return None
    if probe in values:
        return True
    return None if None in values else False


class ResidualFixture(SqlFixture):
    def __init__(self, output, mode, keyed):
        super().__init__(output, False)
        self.mode = mode
        self.keyed = keyed
        self.outer = self.name + '.auction'
        self.inner = self.name + '.bid'
        a, b = self.outer, self.inner
        self.sql(f'''DELETE FROM {a}; DELETE FROM {b};
            ALTER TABLE {a} ADD COLUMN probe bigint, ADD COLUMN ceiling bigint, ADD COLUMN floor integer;
            ALTER TABLE {b} ADD COLUMN cutoff integer;
            INSERT INTO {a}(id,group_id,probe,ceiling) VALUES
                (1,1,7,10),(2,1,7,3),(3,1,7,NULL),(4,1,NULL,10),
                (5,NULL,7,10),(6,2,7,10),(7,3,NULL,10),(8,1,7,20),(9,1,7,10);
            UPDATE {a} SET floor=100 WHERE id<>9;
            INSERT INTO {b}(id,auction,price,cutoff) VALUES
                (1,1,7,3),(2,1,7,3),(3,1,NULL,12),(4,1,NULL,NULL),
                (5,NULL,7,3),(6,2,NULL,2),(8,1,9,5),(99,1,7,1);''')
        keys = 'b.id<>99'
        if keyed:
            keys += ' AND b.auction=a.group_id'
        rhs = f'SELECT b.price FROM {b} b WHERE {keys} AND NOT (a.ceiling<=b.cutoff AND (b.id<=a.floor OR b.id<a.id))'
        if mode == 'quantified':
            projections = ['a.id']
            self.labels = ['id']
            for label, symbol, _ in QUANTIFIED:
                for quantifier in ('ANY', 'ALL'):
                    name = f'{label}_{quantifier.lower()}'
                    self.labels.append(name)
                    projections.append(f'a.probe {symbol} {quantifier} ({rhs}) AS {name}')
            derived = f'(SELECT id, group_id, probe, floor, ceiling-1 AS ceiling FROM {a})'
            self.query = f"SELECT {', '.join(projections)} FROM {derived} a"
        elif mode == 'all-filter':
            self.labels = ['id']
            self.query = f'SELECT a.id FROM {a} a WHERE a.probe < ALL ({rhs})'
        elif mode in ('in', 'in-filter'):
            if mode == 'in':
                self.labels = ['id', 'member', 'excluded']
                self.query = f'''SELECT a.id, a.probe IN ({rhs}) AS member,
                    a.probe NOT IN ({rhs}) AS excluded FROM {a} a'''
            else:
                self.labels = ['id']
                self.query = f'SELECT a.id FROM {a} a WHERE a.probe IN ({rhs})'
        else:
            negation = 'NOT ' if mode == 'not-exists' else ''
            self.labels = ['id']
            self.query = f'''SELECT a.id FROM {a} a WHERE {negation}EXISTS
                (SELECT 1 FROM {b} b WHERE {keys} AND NOT (b.cutoff>=a.ceiling AND (a.floor>=b.id OR b.id<a.id)))'''

    def verify(self):
        self.verify_sql(expected(self, self.labels))
        assert binary_difference(self) == '0'
        outer = json.loads(self.sql(f"SELECT COALESCE(json_agg(a),'[]') FROM {self.outer} a"))
        inner = json.loads(self.sql(f"SELECT COALESCE(json_agg(b),'[]') FROM {self.inner} b"))
        memory = Counter()
        for row in outer:
            key, ceiling, floor = row['group_id'], row['ceiling'], row['floor']
            if self.mode == 'quantified' and ceiling is not None:
                ceiling -= 1
            values = [
                item['price'] for item in inner
                if (not self.keyed or (key is not None and key == item['auction'])) and item['id'] != 99
                and (
                    (ceiling is not None and item['cutoff'] is not None and item['cutoff'] < ceiling)
                    or (floor is not None and floor < item['id'] and item['id'] >= row['id'])
                )
            ]
            if self.mode == 'quantified':
                truth = tuple(
                    compare(row['probe'], values, operation, all_rows)
                    for _, _, operation in QUANTIFIED for all_rows in (False, True)
                )
                memory[(row['id'], *truth)] += 1
            elif self.mode == 'all-filter':
                if compare(row['probe'], values, operator.lt, True) is True:
                    memory[(row['id'],)] += 1
            elif self.mode in ('in', 'in-filter'):
                mark = membership(row['probe'], values)
                if self.mode == 'in':
                    memory[(row['id'], mark, None if mark is None else not mark)] += 1
                elif mark is True:
                    memory[(row['id'],)] += 1
            elif bool(values) == (self.mode == 'exists'):
                memory[(row['id'],)] += 1
        self.verify_memory(memory)


def qualify(output, command, mode, keyed):
    fixture = ResidualFixture(output, mode, keyed)
    a, b = fixture.outer, fixture.inner
    with fixture.managed():
        fixture.run_history(command, History(
            changes=[
                f'DELETE FROM {b} WHERE id=1; UPDATE {a} SET floor=2 WHERE id IN (1,3)',
                f'UPDATE {b} SET cutoff=4 WHERE id=3; DELETE FROM {b} WHERE id IN (2,8)',
                f'UPDATE {b} SET cutoff=NULL WHERE id=3; UPDATE {a} SET ceiling=20 WHERE id=2',
                f'UPDATE {b} SET auction=3,cutoff=1 WHERE id=3; UPDATE {a} SET probe=7 WHERE id=7',
                f'DELETE FROM {b}',
                f'INSERT INTO {b}(id,auction,price,cutoff) VALUES (3,3,NULL,1)',
            ],
            final=f'UPDATE {b} SET auction=1,price=7 WHERE id=3',
            normalized=fixture.query.replace('SELECT', 'select').replace('ANY', 'SOME'),
            rejected=[],
        ))
        (output/'result.json').write_text(json.dumps({
            'mode': mode, 'keyed': keyed, 'cold_restart': True, 'native_bag_and_binary': True,
            'independent_oracle': True,
        })+'\n')


if __name__ == '__main__':
    modes = ('exists', 'not-exists', 'in', 'in-filter', 'quantified', 'all-filter')
    run_cases(((mode if keyed else f'{mode}-inequality-only', mode, keyed)
               for keyed in (True, False) for mode in modes), qualify)
