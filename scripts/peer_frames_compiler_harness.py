#!/usr/bin/env python3
"""Owned PostgreSQL default RANGE and explicit GROUPS frame qualification."""
from worker_harness import harness_arguments
import collections
import json
from decimal import Decimal
from numeric_compiler_harness import average
from ranking_compiler_harness import RankingFixture, qualify


class PeerFramesFixture(RankingFixture):
    oracle_name = 'independent peer-group frame bags'

    def __init__(self, output, mode):
        super().__init__(output, mode)
        order = '' if mode == 'unordered' else ' ORDER BY b.price DESC NULLS FIRST'
        frames = {
            'default': '', 'unordered': '',
            'groups': ' GROUPS BETWEEN 1 PRECEDING AND 1 FOLLOWING',
            'current': ' RANGE BETWEEN CURRENT ROW AND CURRENT ROW',
            'empty': ' GROUPS BETWEEN 2 FOLLOWING AND 1 FOLLOWING',
            'full': ' RANGE BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING',
        }
        window = f' OVER(PARTITION BY b.auction{order}{frames[mode]})'
        functions = ('COUNT(*) AS n,COUNT(b.price) AS present,SUM(b.price) AS total,'
                     'MIN(b.price) AS lo,MAX(b.price) AS hi,COUNT(*) FILTER(WHERE b.price<0) AS negative,AVG(b.price) AS mean')
        functions = functions.replace(' AS ', window + ' AS ')
        self.query = f'SELECT b.id,b.auction,b.price,{functions} FROM {self.name}.bid b'
        self.labels = ['id', 'auction', 'price', 'n', 'present', 'total', 'lo', 'hi', 'negative', 'mean']

    def verify(self):
        self.verify_sql()
        source = json.loads(self.sql(f"SELECT COALESCE(json_agg(b),'[]') FROM {self.name}.bid b"))
        groups = collections.defaultdict(list)
        for row in source:
            groups[row['auction']].append(row)
        memory = collections.Counter()
        for group in groups.values():
            keys = sorted({row['price'] for row in group}, key=lambda price: (price is not None, -(price or 0)))
            for row in group:
                index = keys.index(row['price'])
                if self.mode in ('full', 'unordered'):
                    selected = keys
                elif self.mode == 'groups':
                    selected = keys[max(0, index - 1):index + 2]
                elif self.mode == 'current':
                    selected = [row['price']]
                elif self.mode == 'empty':
                    selected = []
                else:
                    selected = keys[:index + 1]
                frame = [other for other in group if other['price'] in selected]
                prices = [other['price'] for other in frame if other['price'] is not None]
                stats = (len(frame), len(prices), sum(prices) if prices else None,
                         min(prices) if prices else None, max(prices) if prices else None,
                         sum(price < 0 for price in prices), average(prices))
                memory[(row['id'], row['auction'], row['price']) + stats] += 1
        self.verify_memory(dict(memory), parse_float=Decimal)


if __name__ == '__main__':
    output, command = harness_arguments()
    for mode in ('default', 'groups', 'current', 'empty', 'full', 'unordered'):
        directory = output/mode
        directory.mkdir()
        qualify(directory, command, mode, fixture_class=PeerFramesFixture)
