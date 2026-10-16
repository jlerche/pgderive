#!/usr/bin/env python3
"""PostgreSQL integral COUNT DISTINCT with retractions, filters and composition."""
from worker_harness import harness_arguments
import collections
import json
from partition_compiler_harness import PartitionFixture, qualify


class DistinctFixture(PartitionFixture):
    def __init__(self, output, window, mode=None):
        super().__init__(output,False)
        mode = mode or 'int4'
        self.mode = mode
        typename = {'int2':'smallint','int4':'integer','int8':'bigint','nested':'bigint'}[mode]
        self.sql(f"ALTER TABLE {self.name}.bid ALTER COLUMN price TYPE {typename}; UPDATE {self.name}.bid SET price=-2 WHERE id%3=0; UPDATE {self.name}.bid SET price=NULL WHERE id%5=0")
        if mode in ('int8','nested'):
            self.sql(f'UPDATE {self.name}.bid SET price=9223372036854775807 WHERE id=1; UPDATE {self.name}.bid SET price=-9223372036854775808 WHERE id=2')
        grouped = f'SELECT b.auction AS auction,COUNT(DISTINCT b.price) AS unique_count,COUNT(DISTINCT b.auction) AS present_key,COUNT(DISTINCT b.price) FILTER(WHERE b.price<0) AS negative,COUNT(*) AS n FROM {self.name}.bid b GROUP BY b.auction'
        self.query = grouped
        self.labels = ['auction','unique_count','present_key','negative','n']
        if mode == 'nested':
            self.query = f'SELECT q.auction AS auction,q.unique_count AS unique_count,q.present_key AS present_key,q.negative AS negative,q.n AS n,MAX(q.unique_count) OVER() AS maximum FROM({grouped}) q'
            self.labels.append('maximum')

    def verify(self):
        expected = f"SELECT jsonb_build_array(NULL,jsonb_build_array({','.join(self.labels)})) tuple,count(*)::bigint weight FROM ({self.query}) q GROUP BY 1"
        difference = self.bag_difference(expected)
        assert difference.strip() == '0','distinct output differs from PostgreSQL'
        source = json.loads(self.sql(f"SELECT COALESCE(json_agg(b),'[]') FROM {self.name}.bid b"))
        groups = collections.defaultdict(list)
        for row in source:
            groups[row['auction']].append(row)
        expected_memory = collections.Counter()
        maximum = max((len({row['price'] for row in group if row['price'] is not None}) for group in groups.values()),default=0)
        for key,group in groups.items():
            values = {row['price'] for row in group if row['price'] is not None}
            output = (key,len(values),int(key is not None),sum(value<0 for value in values),len(group))
            if self.mode == 'nested':
                output += (maximum,)
            expected_memory[output] += 1
        actual = json.loads(self.sql(f"SELECT COALESCE(json_agg(g),'[]') FROM {self.bag_relation()} g"))
        assert {tuple(row['tuple'][1]):row['weight'] for row in actual} == dict(expected_memory),'distinct output differs from source-value-presence oracle'
        assert self.acknowledgement_ahead().strip() == '0'


if __name__ == '__main__':
    output, command = harness_arguments()
    for mode in ('int2','int4','int8','nested'):
        directory = output/mode
        directory.mkdir()
        qualify(directory,command,False,mode,fixture_class=DistinctFixture)
