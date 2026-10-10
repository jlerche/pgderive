#!/usr/bin/env python3
"""Composed retained SQL stages against PostgreSQL and raw-source recomputation."""
import collections
import json
import pathlib
import sys
from ranking_compiler_harness import RankingFixture, qualify


class CompositionFixture(RankingFixture):
    oracle_name = 'independent composed group/window bags'

    def __init__(self, output, mode):
        super().__init__(output, mode)
        grouped = f'SELECT b.auction AS g,COUNT(*) AS n FROM {self.name}.bid b WHERE b.price IS NULL OR b.price>0 GROUP BY b.auction'
        window = f'SELECT q.g,q.n,MAX(q.n) OVER() AS m FROM({grouped}) q WHERE q.n>=2'
        if mode == 'repeated-bin':
            self.sql(f"ALTER TABLE {self.name}.bid ADD COLUMN event_time timestamp DEFAULT TIMESTAMP '2000-01-01 00:00:00'")
            self.sql(f"UPDATE {self.name}.bid SET event_time=CASE WHEN id%4=0 THEN NULL ELSE TIMESTAMP '2000-01-01 00:00:00' + INTERVAL '1 second'*(id%23) END")
            first = "date_bin(INTERVAL '10 seconds',b.event_time,TIMESTAMP '2000-01-01 00:00:00')"
            inner = f'SELECT b.event_time,{first} AS bucket FROM {self.name}.bid b WHERE b.price IS NULL OR b.price>0'
            second = first.replace('b.','q.')
            self.query = f'SELECT {second} AS bucket,COUNT(*) AS n FROM({inner}) q GROUP BY {second}'
            self.labels = ['bucket','n']
        elif mode == 'group-window':
            self.query = window
            self.labels = ['g','n','m']
        elif mode == 'three-stage':
            self.query = f'SELECT z.m,COUNT(*) AS winners FROM({window}) z WHERE z.n=z.m GROUP BY z.m'
            self.labels = ['m','winners']
        elif mode == 'window-group':
            inner = f'SELECT b.auction,b.price,rank() OVER(PARTITION BY b.auction ORDER BY b.price DESC NULLS FIRST) AS r FROM {self.name}.bid b WHERE b.price IS NULL OR b.price>0'
            self.query = f'SELECT q.auction,COUNT(*) AS n,MAX(q.r) AS m FROM({inner}) q WHERE q.r<=3 GROUP BY q.auction'
            self.labels = ['auction','n','m']
        else:
            inner = f'SELECT b.id,b.auction,b.price,lag(b.price) OVER(PARTITION BY b.auction ORDER BY b.id) AS previous FROM {self.name}.bid b WHERE b.price IS NULL OR b.price>0'
            self.query = f'SELECT q.id,q.auction,q.previous,COUNT(*) OVER(PARTITION BY q.auction ORDER BY q.id ROWS UNBOUNDED PRECEDING) AS n FROM({inner}) q WHERE q.previous IS NULL OR q.previous>0'
            self.labels = ['id','auction','previous','n']

    def changed_query(self):
        return self.query.replace('>0','>1')

    def verify(self):
        values = ','.join(self.labels)
        expected = f'SELECT jsonb_build_array(NULL,jsonb_build_array({values})) tuple,count(*)::bigint weight FROM ({self.query}) q GROUP BY 1'
        difference = self.sql(f'''WITH expected AS ({expected}), actual AS
            (SELECT tuple,weight FROM {self.name}.groups), difference AS
            ((SELECT * FROM expected EXCEPT ALL SELECT * FROM actual) UNION ALL
            (SELECT * FROM actual EXCEPT ALL SELECT * FROM expected)) SELECT count(*) FROM difference''')
        assert difference.strip() == '0', 'composed stages differ from PostgreSQL'
        source = json.loads(self.sql(f"SELECT COALESCE(json_agg(b),'[]') FROM {self.name}.bid b"))
        groups = collections.defaultdict(list)
        for row in source:
            if row['price'] is None or row['price']>0:
                groups[row['auction']].append(row)
        memory = collections.Counter()
        if self.mode == 'repeated-bin':
            counts = collections.Counter()
            for group in groups.values():
                for row in group:
                    value = row['event_time']
                    bucket = None if value is None else value[:17]+f'{int(value[17:19])//10*10:02d}'
                    counts[bucket] += 1
            memory.update((bucket,count) for bucket,count in counts.items())
        elif self.mode in ('group-window','three-stage'):
            counts = {key:len(group) for key,group in groups.items() if len(group)>=2}
            if counts:
                maximum = max(counts.values())
                if self.mode == 'three-stage':
                    memory[(maximum,sum(count==maximum for count in counts.values()))] += 1
                else:
                    memory.update((key,count,maximum) for key,count in counts.items())
        elif self.mode == 'window-group':
            for key,group in groups.items():
                ranks = []
                for row in group:
                    price = row['price']
                    rank = 1+sum(other['price'] is None or (price is not None and other['price']>price) for other in group if other['price'] != price)
                    if rank<=3:
                        ranks.append(rank)
                if ranks:
                    memory[(key,len(ranks),max(ranks))] += 1
        else:
            for key,group in groups.items():
                group.sort(key=lambda row:row['id'])
                count = 0
                for index,row in enumerate(group):
                    previous = group[index-1]['price'] if index else None
                    if previous is None or previous>0:
                        count += 1
                        memory[(row['id'],key,previous,count)] += 1
        actual = json.loads(self.sql(f"SELECT COALESCE(json_agg(g),'[]') FROM {self.name}.groups g"))
        assert {tuple(row['tuple'][1]):row['weight'] for row in actual} == dict(memory), 'composed stages differ from memory oracle'
        assert self.sql(f'''SELECT count(*) FROM pg_replication_slots s JOIN
            {self.name}.pgderive_worker_registration r ON r.slot_name=s.slot_name JOIN
            {self.name}.pgderive_progress p ON true WHERE s.confirmed_flush_lsn>p.end_lsn''').strip() == '0'


if __name__ == '__main__':
    output = pathlib.Path(sys.argv[1])
    command = sys.argv[2:]
    if command and command[0] == '--':
        command = command[1:]
    for mode in ('group-window','three-stage','window-group','window-window','repeated-bin'):
        directory = output/mode
        directory.mkdir()
        qualify(directory,command,mode,fixture_class=CompositionFixture)
