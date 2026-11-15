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


def row_truth(left, right, ordered):
    if ordered:
        for a, b in zip(left, right):
            if a is None or b is None:
                return None
            if a != b:
                return a < b
        return False
    if any(a is not None and b is not None and a != b for a, b in zip(left, right)):
        return False
    return None if None in left or None in right else True


def reduce_truth(values, all_rows=False):
    if (not all_rows) in values:
        return not all_rows
    return None if None in values else all_rows


GROUP_OFFSET_MODES = ('exists-group-offset', 'exists-distinct-offset', 'exists-distinct-on-offset')


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
        rhs = f'SELECT b.price FROM {b} b WHERE {keys} AND NOT (a.ceiling::bigint<=b.cutoff::numeric AND (b.id<=a.floor OR b.id<a.id OR b.id=a.id))'
        if mode in ('in', 'all-filter'):
            rhs = rhs.replace('SELECT ', 'SELECT DISTINCT ', 1)
        if mode == 'ordered':
            selected = f"""SELECT b.price FROM {b} b WHERE {keys}
                ORDER BY b.id DESC OFFSET 1 LIMIT 2"""
            unique = f"""SELECT DISTINCT b.price FROM {b} b WHERE {keys}
                ORDER BY 1 ASC NULLS LAST OFFSET 1 LIMIT 1"""
            ties = f"""SELECT b.price FROM {b} b WHERE {keys}
                ORDER BY b.cutoff DESC NULLS LAST FETCH FIRST 1 ROW WITH TIES"""
            self.labels = ['id', 'member', 'excluded', 'earlier', 'before_all', 'unique_member', 'ties_earlier']
            self.query = f'''SELECT a.id, a.probe IN ({selected}) AS member,
                a.probe NOT IN ({selected}) AS excluded,
                a.probe < ANY ({selected}) AS earlier,
                a.probe < ALL ({selected}) AS before_all,
                a.probe IN ({unique}) AS unique_member, a.probe < ANY ({ties}) AS ties_earlier
                FROM {a} a'''
            self.sql(f'INSERT INTO {b}(id,auction,price,cutoff) VALUES (13,1,8,12)')
        elif mode == 'residual-ordered':
            selected = f"""SELECT b.price FROM {b} b WHERE {keys} AND b.id>a.id
                ORDER BY b.id DESC OFFSET 1 LIMIT 2"""
            ties = f"""SELECT b.price FROM {b} b WHERE {keys} AND b.id>a.id
                ORDER BY b.cutoff DESC NULLS LAST FETCH FIRST 1 ROW WITH TIES"""
            self.labels = ['id', 'member', 'excluded', 'earlier', 'before_all', 'ties_earlier']
            self.query = f"""SELECT a.id, a.probe IN ({selected}) AS member,
                a.probe NOT IN ({selected}) AS excluded,
                a.probe < ANY ({selected}) AS earlier,
                a.probe < ALL ({selected}) AS before_all,
                a.probe < ANY ({ties}) AS ties_earlier FROM {a} a"""
            self.sql(f'INSERT INTO {b}(id,auction,price,cutoff) VALUES (13,1,8,12)')
        elif mode == 'joined':
            rhs = rhs.replace(f'SELECT b.price FROM {b} b',
                f'SELECT c.price FROM {b} b LEFT JOIN {b} c ON c.auction=b.id AND c.id<>99')
            self.labels = ['id', 'present', 'member', 'earlier', 'before_all']
            self.query = f'''SELECT a.id, EXISTS ({rhs}) AS present, a.probe IN ({rhs}) AS member,
                a.probe < ANY ({rhs}) AS earlier, a.probe < ALL ({rhs}) AS before_all
                FROM {a} a'''
        elif mode == 'quantified':
            projections = ['a.id']
            self.labels = ['id']
            for label, symbol, _ in QUANTIFIED:
                for quantifier in ('ANY', 'ALL'):
                    name = f'{label}_{quantifier.lower()}'
                    self.labels.append(name)
                    projections.append(f'a.probe {symbol} {quantifier} ({rhs}) AS {name}')
            derived = f'(SELECT id, group_id, probe, floor, ceiling-1 AS ceiling FROM {a})'
            self.query = f"SELECT {', '.join(projections)} FROM {derived} a"
        elif mode == 'row':
            self.labels = ['id', 'member', 'excluded', 'earlier', 'before_all']
            rhs = rhs.replace('SELECT b.price', 'SELECT b.price, b.cutoff', 1)
            probe = '(a.probe, a.floor::numeric)'
            self.query = f'''SELECT a.id, {probe} IN ({rhs}) AS member,
                {probe} NOT IN ({rhs}) AS excluded, {probe} < ANY ({rhs}) AS earlier,
                {probe} < ALL ({rhs}) AS before_all FROM {a} a'''
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
            targets = 'b.price, NULL, 1 OPERATOR(pg_catalog./) 0' if mode == 'exists-projection' else 'DISTINCT b.*'
            exists = f'''EXISTS (SELECT {targets} FROM {b} b WHERE {keys}
                AND NOT (b.cutoff::numeric>=a.ceiling::bigint AND (a.floor>=b.id OR b.id<a.id OR b.id=a.id)))'''
            if mode == 'exists':
                exists = exists.replace('DISTINCT b.*', 'DISTINCT ON (b.price) b.price')
                exists = exists.removesuffix(')') + ' ORDER BY b.price, b.id LIMIT 1)'
            if mode == 'exists-offset':
                exists = exists.replace('DISTINCT b.*', 'b.price')
                exists = exists.removesuffix(')') + ' ORDER BY b.price LIMIT 1 OFFSET 2)'
            if mode in ('exists-group', 'exists-group-offset'):
                exists = exists.replace('DISTINCT b.*', 'b.price')
                offset = 1 if mode == 'exists-group-offset' else 0
                exists = exists.removesuffix(')') + f' GROUP BY b.price OFFSET {offset})'
            if mode in ('exists-distinct-offset', 'exists-distinct-on-offset'):
                projection = ('DISTINCT b.price' if mode == 'exists-distinct-offset'
                              else 'DISTINCT ON (id) b.price AS id')
                exists = exists.replace('DISTINCT b.*', projection)
                exists = exists.removesuffix(')') + ' ORDER BY 1 OFFSET 1)'
            if mode == 'exists-ties':
                plain = exists.replace('DISTINCT b.*', 'b.price').removesuffix(')')
                nonempty = plain + ' ORDER BY b.price OFFSET 1 FETCH FIRST 1 ROW WITH TIES)'
                empty = plain + ' ORDER BY b.price FETCH FIRST 0 ROWS WITH TIES)'
                self.labels = ['id', 'present', 'absent']
                self.query = f'SELECT a.id, {nonempty} AS present, NOT {empty} AS absent FROM {a} a'
            elif mode == 'exists-zero':
                empty = exists.removesuffix(')') + ' LIMIT 0)'
                self.labels = ['id', 'present', 'absent']
                self.query = f'SELECT a.id, {empty} AS present, NOT {empty} AS absent FROM {a} a'
            elif mode == 'exists-projection':
                self.labels = ['id', 'present', 'absent']
                self.query = f'SELECT a.id, {exists} AS present, NOT {exists} AS absent FROM {a} a'
            else:
                self.query = f'SELECT a.id FROM {a} a WHERE {negation}{exists} AND a.id>0'

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
            matches = [
                item for item in inner
                if (not self.keyed or (key is not None and key == item['auction'])) and item['id'] != 99
                and (
                    (ceiling is not None and item['cutoff'] is not None and item['cutoff'] < ceiling)
                    or (floor is not None and floor < item['id'] and item['id'] > row['id'])
                )
            ]
            values = [item['price'] for item in matches]
            if self.mode == 'ordered':
                bucket = sorted((item for item in inner if key is not None and key == item['auction']
                                 and item['id'] != 99), key=lambda item: -item['id'])
                values = [item['price'] for item in bucket[1:3]]
                mark = membership(row['probe'], values)
                unique = sorted({item['price'] for item in bucket},
                                key=lambda price: (price is None, price or 0))
                first = max((item['cutoff'] for item in bucket if item['cutoff'] is not None), default=None)
                peers = [item['price'] for item in bucket if item['cutoff'] == first]
                memory[(row['id'], mark, None if mark is None else not mark,
                        compare(row['probe'], values, operator.lt, False),
                        compare(row['probe'], values, operator.lt, True),
                        membership(row['probe'], unique[1:2]),
                        compare(row['probe'], peers, operator.lt, False))] += 1
            elif self.mode == 'residual-ordered':
                bucket = sorted((item for item in inner if item['id'] != 99 and item['id'] > row['id']
                                 and (not self.keyed or key is not None and key == item['auction'])),
                                key=lambda item: -item['id'])
                values = [item['price'] for item in bucket[1:3]]
                mark = membership(row['probe'], values)
                first = max((item['cutoff'] for item in bucket if item['cutoff'] is not None), default=None)
                peers = [item['price'] for item in bucket if item['cutoff'] == first]
                memory[(row['id'], mark, None if mark is None else not mark,
                        compare(row['probe'], values, operator.lt, False),
                        compare(row['probe'], values, operator.lt, True),
                        compare(row['probe'], peers, operator.lt, False))] += 1
            elif self.mode == 'joined':
                values = [
                    value for item in matches for value in
                    ([peer['price'] for peer in inner
                      if peer['auction'] == item['id'] and peer['id'] != 99] or [None])
                ]
                memory[(row['id'], bool(values), membership(row['probe'], values),
                        compare(row['probe'], values, operator.lt, False),
                        compare(row['probe'], values, operator.lt, True))] += 1
            elif self.mode == 'row':
                probe = (row['probe'], floor)
                terms = [(item['price'], item['cutoff']) for item in matches]
                member = reduce_truth([row_truth(probe, term, False) for term in terms])
                order = [row_truth(probe, term, True) for term in terms]
                memory[(row['id'], member, None if member is None else not member,
                        reduce_truth(order), reduce_truth(order, True))] += 1
            elif self.mode == 'quantified':
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
            elif self.mode == 'exists-offset':
                if len(values) > 2:
                    memory[(row['id'],)] += 1
            elif self.mode in GROUP_OFFSET_MODES:
                if len(set(values)) > 1:
                    memory[(row['id'],)] += 1
            elif self.mode == 'exists-ties':
                memory[(row['id'], len(values) > 1, True)] += 1
            elif self.mode == 'exists-zero':
                memory[(row['id'], False, True)] += 1
            elif self.mode == 'exists-projection':
                memory[(row['id'], bool(values), not values)] += 1
            elif bool(values) == (self.mode in ('exists', 'exists-group')):
                memory[(row['id'],)] += 1
        self.verify_memory(memory)


def qualify(output, command, mode, keyed):
    fixture = ResidualFixture(output, mode, keyed)
    a, b = fixture.outer, fixture.inner
    group_changes = [
        f'INSERT INTO {b}(id,auction,price,cutoff) VALUES (11,3,NULL,1)',
        f'INSERT INTO {b}(id,auction,price,cutoff) VALUES (12,3,9,1)',
        f'DELETE FROM {b} WHERE id=12',
    ] if mode in GROUP_OFFSET_MODES else []
    with fixture.managed():
        fixture.run_history(command, History(
            changes=[
                f'DELETE FROM {b} WHERE id=1; UPDATE {a} SET floor=2 WHERE id IN (1,3)',
                f'UPDATE {b} SET cutoff=4 WHERE id=3; DELETE FROM {b} WHERE id IN (2,8)',
                f'UPDATE {b} SET cutoff=NULL WHERE id=3; UPDATE {a} SET ceiling=20 WHERE id=2',
                f'UPDATE {b} SET auction=3,cutoff=1 WHERE id=3; UPDATE {a} SET probe=7 WHERE id=7',
                f'DELETE FROM {b}',
                f'INSERT INTO {b}(id,auction,price,cutoff) VALUES (3,3,NULL,1)',
                *group_changes,
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
    modes = ('exists', 'not-exists', 'in', 'in-filter', 'quantified', 'all-filter', 'exists-projection', 'row', 'joined', 'exists-zero', 'exists-group', 'exists-offset',
             'exists-ties', 'residual-ordered', *GROUP_OFFSET_MODES)
    cases = [(mode if keyed else f'{mode}-inequality-only', mode, keyed)
             for keyed in (True, False) for mode in modes]
    run_cases([*cases, ('ordered', 'ordered', True)], qualify)
