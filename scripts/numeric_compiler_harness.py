#!/usr/bin/env python3
"""Exact PostgreSQL integer numeric aggregate CDC qualification."""
import collections
from decimal import Decimal, localcontext, ROUND_HALF_UP
import json
import pathlib
import sys
from sql_compiler_harness import SqlFixture
from partition_compiler_harness import qualify


def average(values):
    if not values:
        return None
    # Independent base-10000 leading-digit scale selection plus Decimal rounding.
    # PostgreSQL numeric.c select_div_scale requires at least 16 significant digits.
    numerator, denominator = sum(values), len(values)
    def leading(value):
        value = abs(value)
        weight = 0
        while value >= 10000:
            value //= 10000
            weight += 1
        return weight, value
    weight1, digit1 = leading(numerator)
    weight2, digit2 = leading(denominator)
    scale = max(0, min(1000, 16 - 4 * (weight1 - weight2 - (digit1 <= digit2))))
    with localcontext() as context:
        context.prec = 100
        return (Decimal(numerator) / Decimal(denominator)).quantize(Decimal(1).scaleb(-scale), rounding=ROUND_HALF_UP)


class NumericFixture(SqlFixture):
    def __init__(self, output, window, frame_spec, native):
        super().__init__(output, False)
        self.window = window
        self.frame_spec = ('2 PRECEDING AND 1 FOLLOWING', -2, 1, True)
        self.sql(f'ALTER TABLE {self.name}.bid ALTER COLUMN price TYPE {native}; UPDATE {self.name}.bid SET price=-id WHERE id%3=0; UPDATE {self.name}.bid SET auction=NULL WHERE id IN (1,2)')
        if native == 'bigint':
            self.sql(f"INSERT INTO {self.name}.bid VALUES(90,1,9223372036854775807),(91,1,9223372036854775806),(92,1,-9223372036854775808),(93,1,1),(94,1,0)")
        functions = ('COUNT(*) AS n,COUNT(b.price) AS present,SUM(b.price) AS total,'
                     'AVG(b.price) AS mean,AVG(b.price) FILTER(WHERE b.price<0) AS negative')
        if window:
            frame = ' OVER (PARTITION BY b.auction ORDER BY b.price DESC NULLS FIRST,b.id ROWS BETWEEN 2 PRECEDING AND 1 FOLLOWING)'
            functions = functions.replace(' AS ', frame + ' AS ')
            prefix = 'b.id AS id,b.auction AS auction,b.price AS price,'
            self.labels = ['id', 'auction', 'price', 'n', 'present', 'total', 'mean', 'negative']
        else:
            prefix = 'b.auction AS auction,'
            self.labels = ['auction', 'n', 'present', 'total', 'mean', 'negative']
        self.query = f'SELECT {prefix}{functions} FROM {self.name}.bid b' + ('' if window else ' GROUP BY b.auction')

    def verify(self):
        values = ','.join(self.labels)
        expected = f'SELECT jsonb_build_array(NULL,jsonb_build_array({values})) tuple,count(*)::bigint weight FROM ({self.query}) q GROUP BY 1'
        difference = self.sql(f'''WITH expected AS ({expected}), actual AS
            (SELECT tuple,weight FROM {self.name}.groups), difference AS
            ((SELECT * FROM expected EXCEPT ALL SELECT * FROM actual) UNION ALL
            (SELECT * FROM actual EXCEPT ALL SELECT * FROM expected)) SELECT count(*) FROM difference''')
        assert difference.strip() == '0', 'numeric result differs from PostgreSQL'
        source = json.loads(self.sql(f"SELECT COALESCE(json_agg(b),'[]') FROM {self.name}.bid b"))
        groups = collections.defaultdict(list)
        for row in source:
            groups[row['auction']].append(row)
        memory = collections.Counter()
        for key, group in groups.items():
            group.sort(key=lambda row: (row['price'] is not None, -(row['price'] or 0), row['id']))
            for index in range(len(group) if self.window else 1):
                frame = group[max(0, index-2):min(len(group), index+2)] if self.window else group
                prices = [row['price'] for row in frame if row['price'] is not None]
                prefix = (group[index]['id'], key, group[index]['price']) if self.window else (key,)
                memory[prefix + (len(frame),len(prices),sum(prices) if prices else None,average(prices),average([p for p in prices if p<0]))] += 1
        actual = json.loads(self.sql(f"SELECT COALESCE(json_agg(g),'[]') FROM {self.name}.groups g"), parse_float=Decimal)
        assert {tuple(row['tuple'][1]): row['weight'] for row in actual} == dict(memory)
        beyond = self.sql(f'''SELECT count(*) FROM pg_replication_slots s JOIN
            {self.name}.pgderive_worker_registration r ON r.slot_name=s.slot_name JOIN
            {self.name}.pgderive_progress p ON true WHERE s.confirmed_flush_lsn>p.end_lsn''')
        assert beyond.strip() == '0'


if __name__ == '__main__':
    output = pathlib.Path(sys.argv[1])
    command = sys.argv[2:]
    if command and command[0] == '--':
        command = command[1:]
    for native, window in [('smallint', False), ('integer', False), ('bigint', False), ('bigint', True)]:
        directory = output/(native + ('-rows' if window else '-grouped'))
        directory.mkdir()
        qualify(directory, command, window, fixture_class=lambda out, win, frame: NumericFixture(out, win, frame, native))
