#!/usr/bin/env python3
"""Check keyed inequality correlation against native PostgreSQL and a raw-source oracle."""
from collections import Counter
import json
import operator

from native_mv_oracle import binary_difference, expected
from quantified_subqueries_harness import COMPARISONS, compare
from sql_compiler_harness import SqlFixture
from worker_case_shard import select_cases
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


def distinct_on_values(rows):
    # Sort by cutoff first and greatest id within each equal cutoff.
    sorted_rows = sorted(rows, key=lambda row: (
        row['cutoff'] is None, row['cutoff'] or 0, -row['id']))
    winners = {}
    for row in sorted_rows:
        winners.setdefault(row['cutoff'], row['price'])
    return list(winners.values())[1:3]


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
        if mode in ('ordered', 'residual-ordered'):
            where = keys + (' AND b.id>a.id' if mode == 'residual-ordered' else '')
            selected = f"""SELECT b.price FROM {b} b WHERE {where}
                ORDER BY b.id DESC OFFSET 1 LIMIT 2"""
            unique = f"""SELECT DISTINCT b.price FROM {b} b WHERE {where}
                ORDER BY 1 ASC NULLS LAST OFFSET 1 LIMIT 1"""
            ties = f"""SELECT b.price FROM {b} b WHERE {where}
                ORDER BY b.cutoff DESC NULLS LAST FETCH FIRST 1 ROW WITH TIES"""
            representatives = f"""SELECT DISTINCT ON (b.cutoff) b.price FROM {b} b
                WHERE {where} ORDER BY b.cutoff ASC NULLS LAST, b.id DESC OFFSET 1 LIMIT 2"""
            self.labels = ['id', 'member', 'excluded', 'earlier', 'before_all',
                           'unique_member', 'ties_earlier', 'representative_member']
            self.query = f"""SELECT a.id, a.probe IN ({selected}) AS member,
                a.probe NOT IN ({selected}) AS excluded,
                a.probe < ANY ({selected}) AS earlier,
                a.probe < ALL ({selected}) AS before_all,
                a.probe IN ({unique}) AS unique_member, a.probe < ANY ({ties}) AS ties_earlier,
                a.probe IN ({representatives}) AS representative_member FROM {a} a"""
            self.sql(f'INSERT INTO {b}(id,auction,price,cutoff) VALUES (13,1,8,12)')
        elif mode == 'aggregate-quantified':
            grouped = f'FROM {b} b WHERE {keys}'
            count = f'SELECT count(*) {grouped}'
            maximum = f'SELECT max(b.price) {grouped} HAVING count(*)>1'
            per_group = f'''SELECT max(b.price) FROM {b} b WHERE b.id<>99
                GROUP BY b.auction HAVING count(*)>1'''
            self.labels = ['id', 'count_any', 'max_any', 'max_all', 'group_any', 'group_all']
            self.query = f"""SELECT a.id, a.probe >= ANY ({count}) AS count_any,
                a.probe < ANY ({maximum}) AS max_any, a.probe < ALL ({maximum}) AS max_all,
                a.probe < ANY ({per_group}) AS group_any, a.probe < ALL ({per_group}) AS group_all
                FROM {a} a"""
            self.sql(f'''INSERT INTO {b}(id,auction,price,cutoff) VALUES (14,2,NULL,3);
                UPDATE {a} SET probe=2 WHERE id=9''')
        elif mode == 'keyed-grouped-comparisons':
            per_key_group = f"""SELECT max(b.price) FROM {b} b WHERE {keys}
                GROUP BY b.cutoff HAVING count(*)>1"""
            order = 'ORDER BY 1 DESC NULLS LAST'
            unique = per_key_group.replace('SELECT ', 'SELECT DISTINCT ', 1)
            second = f'{unique} {order} OFFSET 1 LIMIT 1'
            first = f'{per_key_group} ORDER BY count(*) DESC FETCH FIRST 1 ROW WITH TIES'
            sets = f"""SELECT b.price FROM {b} b WHERE {keys}
                GROUP BY GROUPING SETS ((b.price), (b.cutoff))"""
            partial_key = f"""SELECT b.auction FROM {b} b WHERE {keys}
                GROUP BY GROUPING SETS ((b.auction), (b.cutoff))"""
            self.labels = ['id', 'key_group_in', 'key_group_any', 'key_group_all',
                           'key_group_second', 'key_group_first_any', 'key_group_first_all', 'key_group_sets', 'key_group_partial']
            self.query = f"""SELECT a.id, a.probe IN ({per_key_group}) AS key_group_in,
                a.probe < ANY ({per_key_group}) AS key_group_any,
                a.probe < ALL ({per_key_group}) AS key_group_all,
                a.probe IN ({second}) AS key_group_second,
                a.probe < ANY ({first}) AS key_group_first_any,
                a.probe < ALL ({first}) AS key_group_first_all,
                a.probe >= ALL ({sets}) AS key_group_sets,
                a.probe >= ALL ({partial_key}) AS key_group_partial FROM {a} a"""
            self.sql(f"""INSERT INTO {b}(id,auction,price,cutoff) VALUES
                (14,2,NULL,2),(15,1,10,1),(16,1,8,1),(17,1,10,4),(18,1,8,4)""")
        elif mode == 'keyed-empty-grouping-sets':
            nullable = f"""SELECT max(b.price) FROM {b} b WHERE {keys}
                GROUP BY ROLLUP(b.price) HAVING count(*)=0"""
            counted = f"""SELECT count(*) FROM {b} b WHERE {keys}
                GROUP BY GROUPING SETS ((b.cutoff), (), ()) ORDER BY 1 OFFSET 1 LIMIT 1"""
            self.labels = ['id', 'empty_group_all', 'bounded_empty_group_in']
            self.query = f"""SELECT a.id, a.probe >= ALL ({nullable}) AS empty_group_all,
                a.probe IN ({counted}) AS bounded_empty_group_in FROM {a} a"""
            self.sql(f'UPDATE {a} SET probe=0 WHERE id IN (5,9)')
        elif mode == 'grouped-residual':
            matched = f"""FROM {b} b WHERE {keys}
                AND (b.id>a.floor OR b.price IS NOT DISTINCT FROM a.ceiling)"""
            grouped = f"SELECT max(b.price) {matched} GROUP BY b.cutoff HAVING count(*)>1"
            bounded = grouped.replace('SELECT ', 'SELECT DISTINCT ', 1) + ' ORDER BY 1 NULLS LAST OFFSET 1 LIMIT 1'
            empty = f"SELECT max(b.price) {matched} GROUP BY ROLLUP(b.cutoff) HAVING count(*)=0"
            masks = f"""SELECT GROUPING(b.cutoff) {matched}
                GROUP BY GROUPING SETS ((b.cutoff), (b.price)) HAVING GROUPING(b.cutoff)=1"""
            self.labels = ['id', 'member', 'earlier', 'earlier_than_all', 'empty_group_all', 'has_mask']
            self.query = f"""SELECT a.id, a.probe IN ({grouped}) AS member,
                a.probe < ANY ({bounded}) AS earlier,
                a.probe < ALL ({grouped}) AS earlier_than_all,
                a.probe < ALL ({empty}) AS empty_group_all,
                1 = ANY ({masks}) AS has_mask FROM {a} a"""
            self.sql(f"""UPDATE {a} SET floor=0 WHERE id IN (1,4,6);
                UPDATE {a} SET probe=6 WHERE id=1;
                INSERT INTO {b}(id,auction,price,cutoff) VALUES
                    (120,1,6,20),(121,1,6,20),(122,1,6,21),(123,1,6,21),
                    (124,1,10,22),(125,1,10,22)""")
        elif mode == 'null-safe-residual':
            same = f"SELECT b.price FROM {b} b WHERE {keys} AND b.price IS NOT DISTINCT FROM a.ceiling"
            different = f"SELECT b.price FROM {b} b WHERE {keys} AND b.price IS DISTINCT FROM a.ceiling"
            self.labels = ['id', 'same_exists', 'same_in', 'different_any', 'different_all']
            self.query = f"""SELECT a.id, EXISTS ({same}) AS same_exists,
                a.probe IN ({same}) AS same_in, a.probe < ANY ({different}) AS different_any,
                a.probe < ALL ({different}) AS different_all FROM {a} a"""
        elif mode == 'nested-quantified':
            scope = keys if keyed else keys + ' AND b.id > a.id'
            child = f"""SELECT c.price FROM {b} c WHERE c.id<>99
                AND c.auction=b.id AND c.id>a.id"""
            candidates = f'SELECT b.price FROM {b} b WHERE {scope}'
            self.labels = ['id', 'both_in', 'any_all', 'all_any']
            self.query = f"""SELECT a.id,
                a.probe IN ({candidates} AND b.price IN ({child})) AS both_in,
                a.probe < ANY ({candidates} AND b.price < ALL ({child})) AS any_all,
                a.probe < ALL ({candidates} AND b.price >= ANY ({child})) AS all_any
                FROM {a} a"""
        elif mode == 'nested-domain':
            scope = keys if keyed else keys + ' AND b.id > a.id'
            remaining = f"""SELECT b.price FROM {b} b WHERE {scope}
                AND (b.cutoff IS NULL OR NOT EXISTS (SELECT 1 FROM {b} c WHERE c.id<>99
                    AND c.auction=b.id AND c.price>=a.probe))"""
            mixed = f"""SELECT 1 FROM {b} b WHERE {keys}
                AND (b.id > a.id OR NOT EXISTS (SELECT 1 FROM {b} c WHERE c.id<>99
                    AND c.auction=b.id AND c.price>=b.price))"""
            self.labels = ['id', 'present', 'member', 'earlier', 'before_all', 'mixed_present']
            self.query = f"""SELECT a.id, EXISTS ({remaining}) AS present,
                a.probe IN ({remaining}) AS member, a.probe < ANY ({remaining}) AS earlier,
                a.probe < ALL ({remaining}) AS before_all,
                EXISTS ({mixed}) AS mixed_present FROM {a} a"""
        elif mode == 'nested-exists':
            scope = keys if keyed else keys + ' AND b.id>a.id'
            exists = f"""EXISTS (SELECT 1 FROM {b} b WHERE {scope}
                AND EXISTS (SELECT 1 FROM {b} c WHERE c.id<>99
                    AND c.auction=b.id AND c.price>=a.probe))"""
            shadowed = exists.removesuffix(')') + f""" AND EXISTS (
                SELECT 1 FROM {b} b WHERE b.id<>99 AND b.auction=a.group_id
                    AND b.price>=a.probe))"""
            self.labels = ['id', 'present', 'absent', 'shadow_present']
            self.query = f"""SELECT a.id, {exists} AS present, NOT {exists} AS absent,
                {shadowed} AS shadow_present FROM {a} a"""
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
            if self.mode in ('ordered', 'residual-ordered'):
                bucket = sorted((item for item in inner if item['id'] != 99
                                 and (not self.keyed or key is not None and key == item['auction'])
                                 and (self.mode != 'residual-ordered' or item['id'] > row['id'])),
                                key=lambda item: -item['id'])
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
                        compare(row['probe'], peers, operator.lt, False),
                        membership(row['probe'], distinct_on_values(bucket)))] += 1
            elif self.mode == 'aggregate-quantified':
                qualifying = [item['price'] for item in inner if item['id'] != 99
                              and key is not None and key == item['auction']]
                maximum = [max((value for value in qualifying if value is not None), default=None)
                           ] if len(qualifying) > 1 else []
                groups = {}
                for item in inner:
                    if item['id'] != 99:
                        groups.setdefault(item['auction'], []).append(item['price'])
                maxima = [max((price for price in prices if price is not None), default=None)
                          for prices in groups.values() if len(prices) > 1]
                memory[(row['id'], compare(row['probe'], [len(qualifying)], operator.ge, False),
                        compare(row['probe'], maximum, operator.lt, False),
                        compare(row['probe'], maximum, operator.lt, True),
                        compare(row['probe'], maxima, operator.lt, False),
                        compare(row['probe'], maxima, operator.lt, True))] += 1
            elif self.mode == 'keyed-grouped-comparisons':
                groups = {}
                for item in inner:
                    if item['id'] != 99 and key is not None and key == item['auction']:
                        groups.setdefault(item['cutoff'], []).append(item['price'])
                prices = {price for values in groups.values() for price in values}
                set_values = list(prices | {None}) if groups else []
                summaries = [(len(prices), max((price for price in prices if price is not None),
                                                 default=None))
                             for prices in groups.values() if len(prices) > 1]
                maxima = [price for _, price in summaries]
                unique = sorted(set(maxima), key=lambda price: (price is None, -(price or 0)))
                largest = max((count for count, _ in summaries), default=0)
                first = [price for count, price in summaries if count == largest]
                memory[(row['id'], membership(row['probe'], maxima),
                        compare(row['probe'], maxima, operator.lt, False),
                        compare(row['probe'], maxima, operator.lt, True),
                        membership(row['probe'], unique[1:2]),
                        compare(row['probe'], first, operator.lt, False),
                        compare(row['probe'], first, operator.lt, True),
                        compare(row['probe'], set_values, operator.ge, True),
                        compare(row['probe'], [key, None] if groups else [], operator.ge, True))] += 1
            elif self.mode == 'keyed-empty-grouping-sets':
                groups = Counter(item['cutoff'] for item in inner
                                 if item['id'] != 99 and key is not None and key == item['auction'])
                count = sum(groups.values())
                counts = sorted([*groups.values(), count, count])
                memory[(row['id'], compare(row['probe'], [] if count else [None], operator.ge, True),
                        membership(row['probe'], counts[1:2]))] += 1
            elif self.mode == 'grouped-residual':
                groups = {}
                for item in inner:
                    if (item['id'] != 99
                            and (not self.keyed or key is not None and key == item['auction'])
                            and (floor is not None and item['id'] > floor
                                 or item['price'] == row['ceiling'])):
                        groups.setdefault(item['cutoff'], []).append(item['price'])
                maxima = [max((p for p in prices if p is not None), default=None)
                          for prices in groups.values() if len(prices) > 1]
                bounded = sorted(set(maxima), key=lambda value: (value is None, value or 0))[1:2]
                memory[(row['id'], membership(row['probe'], maxima),
                        compare(row['probe'], bounded, operator.lt, False),
                        compare(row['probe'], maxima, operator.lt, True),
                        compare(row['probe'], [] if groups else [None], operator.lt, True),
                        bool(groups))] += 1
            elif self.mode == 'null-safe-residual':
                available = [item['price'] for item in inner if item['id'] != 99
                             and (not self.keyed or key is not None and key == item['auction'])]
                same = [price for price in available if price == row['ceiling']]
                different = [price for price in available if price != row['ceiling']]
                memory[(row['id'], bool(same), membership(row['probe'], same),
                        compare(row['probe'], different, operator.lt, False),
                        compare(row['probe'], different, operator.lt, True))] += 1
            elif self.mode == 'nested-quantified':
                paired = [
                    (item['price'], [
                        child['price'] for child in inner
                        if child['id'] != 99 and child['auction'] == item['id']
                        and child['id'] > row['id']
                    ]) for item in inner if item['id'] != 99
                    and (key is not None and key == item['auction'] if self.keyed
                         else item['id'] > row['id'])
                ]
                nested_in = [price for price, child in paired if membership(price, child) is True]
                nested_all = [price for price, child in paired
                              if compare(price, child, operator.lt, True) is True]
                nested_any = [price for price, child in paired
                              if compare(price, child, operator.ge, False) is True]
                memory[(row['id'], membership(row['probe'], nested_in),
                        compare(row['probe'], nested_all, operator.lt, False),
                        compare(row['probe'], nested_any, operator.lt, True))] += 1
            elif self.mode == 'nested-domain':
                probe = row['probe']
                values = [
                    item['price'] for item in inner if item['id'] != 99
                    and (key is not None and key == item['auction'] if self.keyed
                         else item['id'] > row['id'])
                    and (item['cutoff'] is None or not any(child['id'] != 99 and child['auction'] == item['id']
                                and probe is not None and child['price'] is not None
                                and child['price'] >= probe for child in inner))
                ]
                mixed = any(
                    item['id'] != 99
                    and (not self.keyed or key is not None and key == item['auction'])
                    and (item['id'] > row['id'] or not any(
                        child['id'] != 99 and child['auction'] == item['id']
                        and item['price'] is not None and child['price'] is not None
                        and child['price'] >= item['price'] for child in inner))
                    for item in inner
                )
                memory[(row['id'], bool(values), membership(probe, values),
                        compare(probe, values, operator.lt, False),
                        compare(probe, values, operator.lt, True), mixed)] += 1
            elif self.mode == 'nested-exists':
                probe = row['probe']
                present = any(
                    probe is not None and witness['id'] != 99
                    and (key is not None and key == witness['auction'] if self.keyed
                         else witness['id'] > row['id'])
                    and any(child['id'] != 99 and child['auction'] == witness['id']
                            and child['price'] is not None and child['price'] >= probe
                            for child in inner)
                    for witness in inner
                )
                shadow_witness = key is not None and probe is not None and any(
                    child['id'] != 99 and child['auction'] == key
                    and child['price'] is not None and child['price'] >= probe for child in inner
                )
                memory[(row['id'], present, not present, present and shadow_witness)] += 1
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
    nested_changes = [
        f"""INSERT INTO {b}(id,auction,price,cutoff)
            VALUES (20,1,NULL,1), (21,20,7,1), (22,20,7,1), (23,1,7,1)""",
        f'DELETE FROM {b} WHERE id=21',
        f'DELETE FROM {b} WHERE id=22',
    ] if mode in ('nested-exists', 'nested-domain', 'nested-quantified') else []
    with fixture.managed():
        fixture.run_history(command, History(
            changes=[
                f'DELETE FROM {b} WHERE id=1; UPDATE {a} SET floor=2 WHERE id IN (1,3)',
                f'UPDATE {b} SET cutoff=4 WHERE id=3; DELETE FROM {b} WHERE id IN (2,8)',
                *([f'DELETE FROM {b} WHERE id=14'] if mode == 'keyed-grouped-comparisons' else []),
                f'UPDATE {b} SET cutoff=NULL WHERE id=3; UPDATE {a} SET ceiling=20 WHERE id=2',
                f'UPDATE {b} SET auction=3,cutoff=1 WHERE id=3; UPDATE {a} SET probe=7 WHERE id=7',
                f'DELETE FROM {b}',
                f'INSERT INTO {b}(id,auction,price,cutoff) VALUES (3,3,NULL,1)',
                *group_changes,
                *nested_changes,
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
             'exists-ties', 'residual-ordered', 'nested-exists', 'nested-domain', 'nested-quantified', 'null-safe-residual', 'grouped-residual', *GROUP_OFFSET_MODES)
    cases = [(mode if keyed else f'{mode}-inequality-only', mode, keyed)
             for keyed in (True, False) for mode in modes]
    cases.extend([
        ('ordered', 'ordered', True),
        ('aggregate-quantified', 'aggregate-quantified', True),
        ('keyed-grouped-comparisons', 'keyed-grouped-comparisons', True),
        ('keyed-empty-grouping-sets', 'keyed-empty-grouping-sets', True),
    ])
    run_cases(select_cases(cases), qualify)
