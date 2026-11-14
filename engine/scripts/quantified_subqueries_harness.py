#!/usr/bin/env python3
"""Compare quantified subqueries with PostgreSQL and a raw-source oracle."""
from collections import Counter
from decimal import Decimal
import json
import operator
from uuid import UUID

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
    def __init__(self, output, mode, group, domains):
        super().__init__(output, False)
        self.mode = mode
        self.domains = domains
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
        self.probe = 'group_id'
        if domains in ('C', 'POSIX'):
            self.probe = 'probe'
            self.sql(f'''ALTER TABLE {self.outer} ADD COLUMN probe varchar COLLATE "{domains}";
                ALTER TABLE {self.inner} ADD COLUMN term text COLLATE "{domains}";
                UPDATE {self.outer} SET probe=CASE id WHEN 1 THEN 'Z' WHEN 2 THEN 'é'
                    WHEN 3 THEN 'a' WHEN 4 THEN 'a ' END;
                UPDATE {self.inner} SET term=CASE id WHEN 1 THEN 'Z' WHEN 2 THEN 'a'
                    WHEN 4 THEN 'a' WHEN 6 THEN 'é' END;''')
        elif domains == 'boolean':
            self.probe = 'probe'
            self.sql(f'''ALTER TABLE {self.outer} ADD COLUMN probe boolean;
                ALTER TABLE {self.inner} ADD COLUMN term boolean;
                UPDATE {self.outer} SET probe=(id % 2 = 0) WHERE id<>5;
                UPDATE {self.inner} SET term=(id<>1) WHERE id IN (1,2,4,6);''')
        elif domains == 'uuid':
            self.probe = 'probe'
            self.sql(f'''ALTER TABLE {self.outer} ADD COLUMN probe uuid;
                ALTER TABLE {self.inner} ADD COLUMN term uuid;
                UPDATE {self.outer} SET probe=CASE WHEN id=1
                    THEN '80000000-0000-0000-0000-000000000000'::uuid
                    ELSE '00000000-0000-0000-0000-000000000001'::uuid END WHERE id<>5;
                UPDATE {self.inner} SET term=CASE WHEN id=1
                    THEN '80000000-0000-0000-0000-000000000000'::uuid
                    ELSE 'ffffffff-ffff-ffff-ffff-ffffffffffff'::uuid END WHERE id IN (1,2,4,6);''')
        elif domains != 'integral':
            self.probe = 'probe'
            probe_type = 'bigint' if domains == 'integral-numeric' else 'numeric'
            probe_value = '9007199254740993'
            if domains == 'numeric-integral':
                probe_value += '.000000000000000001'
            self.sql(f"""ALTER TABLE {self.outer} ADD COLUMN probe {probe_type};
                UPDATE {self.outer} SET probe={probe_value} WHERE id<>5;
                UPDATE {self.inner} SET price=9007199254740993 WHERE id IN (1,2,4);""")
            if domains != 'numeric-integral':
                self.sql(f"""ALTER TABLE {self.inner} ALTER COLUMN price TYPE numeric;
                    UPDATE {self.inner} SET price=9007199254740993.000000000000000001 WHERE id=1;
                    UPDATE {self.inner} SET price=9007199254740993.00 WHERE id IN (2,4);""")
            if domains == 'numeric':
                self.sql(f"UPDATE {self.outer} SET probe=9007199254740993.000 WHERE id=1")
        key = 'a.correlation_key' if mode == 'correlated' else '1'
        self.value = 'term' if domains in ('C', 'POSIX', 'boolean', 'uuid') else 'price'
        rhs = f'SELECT b.{self.value} FROM {self.inner} b WHERE b.auction={key}'
        projections = ['a.id']
        for label, symbol, _ in self.comparisons:
            for quantifier in ('ANY', 'ALL'):
                name = f'{label}_{quantifier.lower()}'
                self.labels.append(name)
                projections.append(f'a.{self.probe} {symbol} {quantifier} ({rhs}) AS {name}')
        self.query = f"SELECT {', '.join(projections)} FROM {self.outer} a"

    def verify(self):
        self.verify_sql(expected(self, self.labels))
        assert binary_difference(self) == '0'
        outer = json.loads(self.sql(f"SELECT COALESCE(json_agg(a),'[]') FROM {self.outer} a"), parse_float=Decimal)
        inner = json.loads(self.sql(f"SELECT COALESCE(json_agg(b),'[]') FROM {self.inner} b"), parse_float=Decimal)
        memory = Counter()
        for row in outer:
            key = row['correlation_key'] if self.mode == 'correlated' else 1
            values = [item[self.value] for item in inner if key is not None and item['auction'] == key]
            probe = row[self.probe]
            if self.domains == 'uuid':
                probe = None if probe is None else UUID(probe)
                values = [None if value is None else UUID(value) for value in values]
            truth = tuple(
                compare(probe, values, operation, all_rows)
                for _, _, operation in self.comparisons for all_rows in (False, True)
            )
            memory[(row['id'], *truth)] += 1
        self.verify_memory(memory)


def qualify(output, command, mode, group, domains):
    fixture = QuantifiedFixture(output, mode, group, domains)
    a, b = fixture.outer, fixture.inner
    new_value = {'boolean': 'true', 'uuid': 'ffffffff-ffff-ffff-ffff-ffffffffffff'}.get(domains, '2')
    new_probe = '80000000-0000-0000-0000-000000000000' if domains == 'uuid' else '1'
    with fixture.managed():
        fixture.run_history(command, History(
            changes=[
                f'DELETE FROM {b} WHERE id IN (1,2)',
                f'DELETE FROM {b} WHERE id=4; UPDATE {a} SET {fixture.probe}=NULL WHERE id=1',
                f'DELETE FROM {b} WHERE id=3',
                f"INSERT INTO {b}(id,auction,{fixture.value}) VALUES(7,1,'{new_value}'),(8,1,'{new_value}'); UPDATE {a} SET {fixture.probe}='{new_probe}' WHERE id=1",
            ],
            final=f'UPDATE {b} SET auction=4 WHERE id IN (7,8)',
            normalized=fixture.query.replace('ANY', 'SOME').replace('!=', '<>'),
            rejected=[],
        ))
        (output/'result.json').write_text(json.dumps({
            'mode': mode, 'domains': domains, 'operators': [symbol for _, symbol, _ in fixture.comparisons],
            'cold_restart': True, 'native_bag_and_binary': True,
            'independent_oracle': True,
        })+'\n')


if __name__ == '__main__':
    run_cases(((f'{mode}-{group}-{domains}', mode, group, domains)
               for mode, domains in (
                   ('uncorrelated', 'integral'), ('correlated', 'integral'),
                   ('correlated', 'numeric'), ('uncorrelated', 'integral-numeric'),
                   ('correlated', 'numeric-integral'),
                   ('correlated', 'C'), ('uncorrelated', 'POSIX'),
                   ('correlated', 'boolean'), ('uncorrelated', 'boolean'),
                   ('correlated', 'uuid'), ('uncorrelated', 'uuid'),
               ) for group in GROUPS), qualify)
