#!/usr/bin/env python3
"""GROUP BY references across native, joined and derived SQL scopes."""
from collections import Counter
import json

from native_mv_oracle import binary_difference, expected
from sql_compiler_harness import SqlFixture
from worker_harness import History, run_modes


class GroupFixture(SqlFixture):
    def __init__(self, output, mode):
        super().__init__(output, False)
        self.mode = mode
        bid = self.name + '.bid'
        auction = self.name + '.auction'
        if mode == 'native':
            prefix = f'SELECT b.auction%2 AS "Bucket",COUNT(*) AS n FROM {bid} b'
            original, explicit = '"Bucket"', 'b.auction%2'
            suffix = ' HAVING COUNT(*)>1 ORDER BY b.auction%2'
            self.labels = ['Bucket', 'n']
        elif mode == 'join':
            prefix = (f'SELECT a.id AS g,COUNT(*) AS n FROM {auction} a '
                      f'JOIN {bid} b ON a.id=b.auction')
            original, explicit, suffix = '1', 'a.id', ''
            self.labels = ['g', 'n']
        elif mode == 'derived':
            prefix = (f'SELECT q.k AS g,COUNT(*) AS n FROM '
                      f'(SELECT auction AS k FROM {bid} WHERE price IS NOT NULL) q')
            original, explicit, suffix = 'g', 'q.k', ''
            self.labels = ['g', 'n']
        elif mode == 'precedence':
            prefix = f'SELECT b.auction AS g,COUNT(*) AS auction FROM {bid} b'
            original, explicit, suffix = 'auction', 'b.auction', ''
            self.labels = ['g', 'auction']
        elif mode == 'using':
            prefix = (f'SELECT q.n FROM (SELECT a.id AS g,id AS g,COUNT(*) AS n FROM {auction} a '
                      f'JOIN {bid} b USING (id)')
            original, explicit, suffix = 'g', 'a.id', ') q'
            self.labels = ['n']
        elif mode == 'duplicate':
            prefix = (f'SELECT q.n FROM (SELECT b.auction AS g,auction AS g,b.auction::integer AS g,COUNT(*) AS n '
                      f'FROM {bid} b')
            original, explicit, suffix = 'g', 'b.auction', ') q'
            self.labels = ['n']
        else:
            prefix = (f"SELECT q.n FROM (SELECT 1.00 AS g,'01.00'::numeric AS g,COUNT(*) AS n "
                      f"FROM {bid}")
            original, explicit, suffix = 'g', '1.00::numeric', ') q'
            self.labels = ['n']
        self.query = f'{prefix} GROUP BY {original}{suffix}'
        self.normalized = f'{prefix} GROUP BY {explicit}{suffix}'

    def verify(self):
        self.verify_sql(expected(self, self.labels))
        assert binary_difference(self) == '0'
        source = self.name + '.bid'
        rows = json.loads(self.sql(f"SELECT COALESCE(json_agg(b),'[]') FROM {source} b"))
        if self.mode in ('join', 'using'):
            ids = set(json.loads(self.sql(f'SELECT json_agg(id) FROM {self.name}.auction')))
            key = 'id' if self.mode == 'using' else 'auction'
            rows = [row for row in rows if row[key] in ids]
        if self.mode == 'derived':
            rows = [row for row in rows if row['price'] is not None]
        key = 'id' if self.mode == 'using' else 'auction'
        keys = [row[key] for row in rows]
        if self.mode == 'native':
            keys = [key % 2 if key is not None else None for key in keys]
        if self.mode == 'numeric':
            self.verify_memory({(len(rows),): 1} if rows else {})
            return
        groups = Counter(keys)
        if self.mode in ('duplicate', 'using'):
            self.verify_memory(Counter((count,) for count in groups.values()))
        else:
            self.verify_memory({(key, count): 1 for key, count in groups.items()
                                if self.mode != 'native' or count > 1})


def qualify(output, command, mode):
    fixture = GroupFixture(output, mode)
    source = fixture.name + '.bid'
    with fixture.managed():
        fixture.run_history(command, History(
            changes=[
                f'INSERT INTO {source} VALUES (100,1,5),(101,NULL,NULL),(102,NULL,7)',
                f'UPDATE {source} SET auction=2,price=NULL WHERE id=100',
                f'DELETE FROM {source} WHERE auction=1',
            ],
            final=f'DELETE FROM {source} WHERE id IN (101,102)',
            normalized=fixture.normalized,
            rejected=[],
        ))
        (output/'result.json').write_text(json.dumps({
            'mode': mode, 'native_binary_and_bag': True, 'independent_group_counts': True,
            'cold_restart_with_explicit_group': True,
        })+'\n')


if __name__ == '__main__':
    run_modes(('native', 'join', 'derived', 'precedence', 'duplicate', 'numeric', 'using'), qualify)
