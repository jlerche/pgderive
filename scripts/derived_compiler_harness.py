#!/usr/bin/env python3
"""Derived post-window/group filtering and top-k bags against PostgreSQL."""
import collections
import json
import pathlib
import sys
from ranking_compiler_harness import RankingFixture, qualify


class DerivedFixture(RankingFixture):
    oracle_name = 'independent ranked/grouped post-filter bags'

    def __init__(self, output, mode):
        super().__init__(output, mode)
        if mode == 'grouped':
            inner = f'SELECT b.auction,COUNT(*) AS n,COUNT(b.price) AS present FROM {self.name}.bid b GROUP BY b.auction'
            self.query = f'SELECT q.auction,q.n FROM({inner}) q WHERE q.n>=2 AND q.present>=1'
            self.labels = ['auction', 'n']
        else:
            inner = self.query
            self.query = f'SELECT q.price FROM({inner}) q WHERE q.r<=3 AND(q.price>0 OR q.price IS NULL)'
            self.labels = ['price']

    def changed_query(self):
        return self.query.replace('>=2', '>=3') if self.mode == 'grouped' else self.query.replace('<=3', '<=2')

    def verify(self):
        values = ','.join(self.labels)
        expected = f'SELECT jsonb_build_array(NULL,jsonb_build_array({values})) tuple,count(*)::bigint weight FROM ({self.query}) q GROUP BY 1'
        difference = self.sql(f'''WITH expected AS ({expected}), actual AS
            (SELECT tuple,weight FROM {self.name}.groups), difference AS
            ((SELECT * FROM expected EXCEPT ALL SELECT * FROM actual) UNION ALL
            (SELECT * FROM actual EXCEPT ALL SELECT * FROM expected)) SELECT count(*) FROM difference''')
        assert difference.strip() == '0', 'derived result differs from PostgreSQL'
        source = json.loads(self.sql(f"SELECT COALESCE(json_agg(b),'[]') FROM {self.name}.bid b"))
        groups = collections.defaultdict(list)
        for row in source:
            groups[row['auction']].append(row)
        memory = collections.Counter()
        for auction, group in groups.items():
            if self.mode == 'grouped':
                if len(group)>=2 and any(row['price'] is not None for row in group):
                    memory[(auction, len(group))] += 1
                continue
            group.sort(key=lambda row: (row['price'] is not None, -(row['price'] or 0), row['id']))
            for index, row in enumerate(group):
                price = row['price']
                if self.mode == 'number':
                    rank = index+1
                else:
                    rank = sum(other['price'] is None or (price is not None and other['price']>price) for other in group if other['price'] != price)+1
                if rank<=3 and (price is None or price>0):
                    memory[(price,)] += 1
        actual = json.loads(self.sql(f"SELECT COALESCE(json_agg(g),'[]') FROM {self.name}.groups g"))
        assert {tuple(row['tuple'][1]): row['weight'] for row in actual} == dict(memory)
        assert self.sql(f'''SELECT count(*) FROM pg_replication_slots s JOIN
            {self.name}.pgderive_worker_registration r ON r.slot_name=s.slot_name JOIN
            {self.name}.pgderive_progress p ON true WHERE s.confirmed_flush_lsn>p.end_lsn''').strip() == '0'


if __name__ == '__main__':
    output = pathlib.Path(sys.argv[1])
    command = sys.argv[2:]
    if command and command[0] == '--':
        command = command[1:]
    for mode in ('peers', 'number', 'grouped'):
        directory = output/mode
        directory.mkdir()
        qualify(directory, command, mode, fixture_class=DerivedFixture)
