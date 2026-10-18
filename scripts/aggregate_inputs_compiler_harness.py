#!/usr/bin/env python3
"""Computed grouped inputs: native MV, independent bags, CDC and cold restart."""
import collections
import decimal
import json
from sql_compiler_harness import SqlFixture
from worker_harness import run_modes, History

MODES = ('statistics', 'distinct', 'ordered', 'mixed', 'computed-key', 'derived')
GUARD = ' FILTER(WHERE price>0)'


class AggregateInputsFixture(SqlFixture):
    def __init__(self, output, mode):
        super().__init__(output, False)
        self.mode = mode
        self.sql(f'DELETE FROM {self.name}.bid; INSERT INTO {self.name}.bid VALUES '
                 '(1,1,2),(2,1,2),(3,-1,6),(4,NULL,NULL),(5,2,-9223372036854775808),'
                 '(6,2,NULL),(0,3,-9223372036854775808)')
        source = f'{self.name}.bid'
        key = 'abs(auction)' if mode == 'computed-key' else 'auction'
        expressions = {
            'statistics': ['COUNT(*)', 'COUNT(NULL)', 'COUNT(true)', "COUNT('x')", 'COUNT(NULL::bigint)',
                           'COUNT(abs(price))'+GUARD, 'SUM(abs(price))'+GUARD, 'AVG(abs(price))'+GUARD,
                           'COUNT(abs(price)) FILTER(WHERE price<0)', 'COUNT(to_hex(price))', 'COUNT(abs(auction))'],
            'distinct': ['COUNT(DISTINCT abs(price))'+GUARD, 'SUM(abs(price))'+GUARD,
                         'COUNT(DISTINCT abs(price)) FILTER(WHERE price>3)'],
            'ordered': ['MIN(abs(price))'+GUARD, 'MAX(abs(price))'+GUARD],
            'mixed': ['COUNT(abs(price))'+GUARD, 'MIN(abs(price))'+GUARD,
                      'MAX(abs(price))'+GUARD, 'SUM(abs(price)::numeric)'+GUARD,
                      'MIN(abs(price)::numeric)'+GUARD, 'MAX(abs(price)::numeric)'+GUARD],
            'computed-key': ['COUNT(abs(price))'+GUARD, 'SUM(abs(price))'+GUARD],
            'derived': ['COUNT(abs(price))'+GUARD, 'SUM(abs(price))'+GUARD],
        }[mode]
        # A separate negative guard must be safe even for bigint MIN.
        expressions = [value.replace('WHERE price<0', 'WHERE price=-1') for value in expressions]
        self.query = (f'SELECT {key} AS g,' + ','.join(f'{value} AS v{index}' for index, value in enumerate(expressions))
                      + f' FROM {source} WHERE id>0 GROUP BY {key}')
        if mode == 'derived':
            self.query = (f'SELECT auction AS g,COUNT(abs(price)){GUARD} AS v0,SUM(abs(price)){GUARD} AS v1 '
                          f'FROM (SELECT auction,MAX(price) AS price FROM {source} WHERE id>0 GROUP BY auction) d GROUP BY auction')
        self.labels = ['g'] + [f'v{index}' for index in range(len(expressions))]
        self.sql(f'CREATE MATERIALIZED VIEW {self.name}.oracle AS {self.query}')

    def memory(self):
        groups = collections.defaultdict(list)
        source = json.loads(self.sql(f'SELECT json_agg(b) FROM {self.name}.bid b')) or []
        for row in source:
            if row['id'] > 0:
                key = row['auction']
                if self.mode == 'computed-key' and key is not None:
                    key = abs(key)
                groups[key].append(row['price'])
        result = {}
        for key, prices in groups.items():
            if self.mode == 'derived':
                present = [price for price in prices if price is not None]
                prices = [max(present) if present else None]
            positive = [abs(price) for price in prices if price is not None and price > 0]
            total = sum(positive) if positive else None
            count = len(positive)
            if self.mode == 'statistics':
                average = (decimal.Decimal(total)/count).quantize(decimal.Decimal('1e-16')) if count else None
                values = [len(prices), 0, len(prices), len(prices), 0, count, total, average,
                          sum(price == -1 for price in prices), sum(price is not None for price in prices),
                          len(prices) if key is not None else 0]
            elif self.mode == 'distinct':
                values = [len(set(positive)), total, len({price for price in positive if price > 3})]
            elif self.mode == 'ordered':
                values = [min(positive) if positive else None, max(positive) if positive else None]
            elif self.mode == 'mixed':
                values = [count, min(positive) if positive else None, max(positive) if positive else None, total,
                          min(positive) if positive else None, max(positive) if positive else None]
            else:
                values = [count, total]
            result[(key, *values)] = 1
        return result

    def verify(self):
        self.sql(f'REFRESH MATERIALIZED VIEW {self.name}.oracle')
        expected = ('SELECT jsonb_build_array(NULL,jsonb_build_array(' + ','.join(self.labels)
                    + f')) tuple,count(*)::bigint weight FROM {self.name}.oracle GROUP BY 1')
        assert self.bag_difference(expected).strip() == '0', 'computed aggregate differs from refreshed native MV'
        self.verify_memory(self.memory(), parse_float=decimal.Decimal)


def qualify(output, command, mode):
    fixture = AggregateInputsFixture(output, mode)
    with fixture.managed():
        if mode == 'statistics':
            fixture.rejected(command, 'missing-function', fixture.query.replace('abs(price)', 'lower(price::text)'))
            assert fixture.sql(f"SELECT count(*) FROM pg_replication_slots WHERE slot_name LIKE '{fixture.name}%'").strip() == '0'
        changes = [
            f'UPDATE {fixture.name}.bid SET price=-1 WHERE id=2; UPDATE {fixture.name}.bid SET auction=-1 WHERE id=1',
            f'UPDATE {fixture.name}.bid SET price=6 WHERE id=6; DELETE FROM {fixture.name}.bid WHERE id=3',
            f'UPDATE {fixture.name}.bid SET price=NULL WHERE auction=-1',
            f'DELETE FROM {fixture.name}.bid WHERE auction=-1; INSERT INTO {fixture.name}.bid VALUES(100,4,NULL),(101,4,2)',
            f'UPDATE {fixture.name}.bid SET price=-9223372036854775808 WHERE id=101; DELETE FROM {fixture.name}.bid WHERE id=100',
        ]
        fixture.run_history(command, History(
            changes=changes,
            final=f'UPDATE {fixture.name}.bid SET price=8 WHERE id=101',
            normalized=fixture.query,
            rejected=[],
        ))
        if mode == 'statistics':
            before = fixture.sql(f"SELECT to_jsonb(p)-'fence' FROM {fixture.name}.pgderive_progress p")
            sink_before = fixture.sql(f"SELECT COALESCE(jsonb_agg(jsonb_build_array(tuple,weight) ORDER BY tuple),'[]') FROM {fixture.bag_relation()}")
            bad = fixture.start(command, 'overflow')
            bad.event('ready')
            fixture.sql(f'INSERT INTO {fixture.name}.bid VALUES(999,-2147483648,1)')
            bad.process.wait(timeout=60)
            assert bad.process.returncode != 0, 'overflow was accepted'
            bad.abort()
            assert fixture.sql(f"SELECT to_jsonb(p)-'fence' FROM {fixture.name}.pgderive_progress p") == before
            assert fixture.sql(f"SELECT COALESCE(jsonb_agg(jsonb_build_array(tuple,weight) ORDER BY tuple),'[]') FROM {fixture.bag_relation()}") == sink_before
            assert fixture.acknowledgement_ahead().strip() == '0'
        (output/'result.json').write_text(json.dumps({'shape': mode, 'sql_oracle': 'refreshed native MV exact bag',
            'memory_oracle': 'independent grouped source bags', 'transactions': len(changes)+1,
            'restart': 'cold, same slot', 'filter_where': 'excluded overflow and unknown guards',
            'identity': 'collisions, retractions, group moves, last row and all NULL groups'})+'\n')


if __name__ == '__main__':
    run_modes(MODES, qualify)
