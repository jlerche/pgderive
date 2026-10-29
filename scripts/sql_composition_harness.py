#!/usr/bin/env python3
"""HAVING/CTE/UNION ALL through production CDC, native SQL and raw-row oracles."""
import collections
import json
from decimal import Decimal

from sql_compiler_harness import SqlFixture
from worker_harness import History, run_modes


MODES = (
    'having-count', 'having-hidden', 'having-key', 'having-filter', 'having-window',
    'global-empty', 'global-sum', 'global-unknown', 'global-constant',
    'cte-chain', 'cte-repeated-join', 'cte-shadow', 'cte-alias', 'cte-repeated-union',
    'union-duplicates', 'union-null', 'union-widen', 'union-numeric',
    'union-group', 'union-window', 'union-branch-limit', 'union-limit',
    'union-constant', 'union-varchar', 'union-boolean', 'union-temporal', 'combined',
)


def grouped(rows):
    groups = collections.defaultdict(list)
    for row in rows:
        groups[row['auction']].append(row)
    return groups


def total(values):
    present = [value for value in values if value is not None]
    return sum(present) if present else None


def descending(values):
    return sorted(values, key=lambda value: (value is None, -(value or 0)))


class CompositionFixture(SqlFixture):
    def __init__(self, output, mode):
        super().__init__(output, False)
        self.mode = mode
        b, a = self.name+'.bid', self.name+'.auction'
        if mode == 'union-varchar':
            self.sql(f"ALTER TABLE {a} ADD COLUMN short varchar(3),ADD COLUMN long varchar(5); UPDATE {a} SET short='ab',long='alpha'")
        if mode == 'union-temporal':
            self.sql(f"ALTER TABLE {a} ADD COLUMN plain timestamp,ADD COLUMN zoned timestamptz; UPDATE {a} SET plain=TIMESTAMP '2000-01-01'+id*INTERVAL '1 second',zoned=TIMESTAMPTZ '2000-01-01 00:00:00+00'+id*INTERVAL '1 second'")
        self.labels = ['g']
        group = f'SELECT b.auction AS g,COUNT(*) AS n FROM {b} b GROUP BY b.auction'
        values = f'SELECT b.price AS g FROM {b} b'
        union = f'SELECT b.auction AS g FROM {b} b UNION ALL SELECT a.id FROM {a} a'
        queries = {
            'having-count': group+' HAVING COUNT(*)>=2',
            'having-hidden': f'SELECT b.auction AS g FROM {b} b GROUP BY b.auction HAVING SUM(b.price)>10 AND COUNT(*)>=2',
            'having-key': f'SELECT COUNT(*) AS g FROM {b} b GROUP BY b.auction HAVING b.auction>2',
            'having-filter': f'SELECT b.auction AS g FROM {b} b GROUP BY b.auction HAVING COUNT(*) FILTER(WHERE b.price>0)>=2',
            'having-window': f'SELECT q.g,q.n,MAX(q.n) OVER() AS m FROM ({group} HAVING COUNT(*)>=2) q',
            'global-empty': f'SELECT COUNT(*) AS g FROM {b} b HAVING COUNT(*)=0',
            'global-sum': f'SELECT COUNT(*) AS g,SUM(b.price) AS s FROM {b} b HAVING SUM(b.price) IS NULL OR SUM(b.price)>10',
            'global-unknown': f'SELECT COUNT(*) AS g FROM {b} b HAVING SUM(b.price)>10',
            'global-constant': f'SELECT 7::bigint AS g FROM {b} b HAVING TRUE',
            'cte-chain': f'WITH c AS ({group}), d AS (SELECT c.g,c.n FROM c WHERE c.n>=2) SELECT d.g,d.n FROM d',
            'cte-repeated-join': f'WITH c AS (SELECT b.id,b.price FROM {b} b) SELECT l.id AS g,r.price AS v FROM c l JOIN c r ON l.id=r.id',
            'cte-shadow': f'WITH c AS ({values}) SELECT q.g FROM (WITH c AS (SELECT b.id::bigint AS g FROM {b} b) SELECT c.g FROM c WHERE c.g>2) q',
            'cte-alias': f'WITH c(x) AS ({values}) SELECT c.x AS g FROM c',
            'cte-repeated-union': f'WITH c AS ({values}) SELECT c.g FROM c UNION ALL SELECT c.g FROM c',
            'union-duplicates': values+' UNION ALL '+values,
            'union-null': f'{values} WHERE b.price IS NULL UNION ALL {values} WHERE b.price IS NULL',
            'union-widen': f'SELECT a.id::smallint AS g FROM {a} a UNION ALL {values}',
            'union-numeric': f'SELECT a.id::numeric AS g FROM {a} a UNION ALL {values}',
            'union-group': f'SELECT q.g,COUNT(*) AS n FROM ({union}) q GROUP BY q.g',
            'union-window': f'SELECT q.g,COUNT(*) OVER(PARTITION BY q.g) AS n FROM ({union}) q',
            'union-branch-limit': f'({values} ORDER BY g DESC NULLS LAST LIMIT 2) UNION ALL ({values} ORDER BY g DESC NULLS LAST LIMIT 2)',
            'union-limit': values+' UNION ALL '+values+' ORDER BY g DESC NULLS LAST LIMIT 4 OFFSET 1',
            'union-constant': f'{values} UNION ALL SELECT 7::bigint AS ignored',
            'union-varchar': f'SELECT a.short AS g FROM {a} a UNION ALL SELECT a.long AS other_label FROM {a} a',
            'union-boolean': f'SELECT a.enabled AS g FROM {a} a UNION ALL SELECT NULL',
            'union-temporal': f'SELECT a.plain AS g FROM {a} a UNION ALL SELECT a.zoned FROM {a} a',
            'combined': f'WITH c AS ({union}), d AS (SELECT c.g,COUNT(*) AS n FROM c GROUP BY c.g HAVING COUNT(*)>=2) SELECT d.g,d.n FROM d ORDER BY n DESC,g ASC NULLS LAST LIMIT 4',
        }
        self.query = queries[mode]
        if mode in ('having-count', 'cte-chain', 'union-group', 'union-window', 'combined'):
            self.labels = ['g', 'n']
        elif mode == 'having-window':
            self.labels = ['g', 'n', 'm']
        elif mode == 'global-sum':
            self.labels = ['g', 's']
        elif mode == 'cte-repeated-join':
            self.labels = ['g', 'v']
        if mode.startswith('global-'):
            self.sql(f'DELETE FROM {b}')
        self.sql(f'CREATE MATERIALIZED VIEW {self.name}.oracle AS {self.query}')

    def verify(self):
        self.verify_materialized(','.join(self.labels), 'original SQL native MV differs')
        attributes = "SELECT attname,atttypid,atttypmod,attcollation FROM pg_attribute WHERE attrelid=%s::regclass AND attnum>0 AND NOT attisdropped ORDER BY attnum"
        assert self.sql(attributes % ("'"+self.name+".groups'")) == self.sql(attributes % ("'"+self.name+".oracle'")), 'result names/types/modifiers/collations differ'
        rows = json.loads(self.sql(f"SELECT COALESCE(json_agg(b),'[]') FROM {self.name}.bid b"))
        catalog = json.loads(self.sql(f'SELECT json_agg(a) FROM {self.name}.auction a'))
        self.verify_memory(dict(self.memory(rows, catalog)), parse_float=Decimal)

    def memory(self, rows, catalog):
        mode = self.mode
        groups = grouped(rows)
        result = collections.Counter()
        if mode.startswith('having-') or mode == 'cte-chain':
            selected = []
            for key, members in groups.items():
                count = len(members)
                amount = total([row['price'] for row in members])
                accept = count >= 2
                if mode == 'having-hidden':
                    accept = accept and amount is not None and amount > 10
                elif mode == 'having-key':
                    accept = key is not None and key > 2
                elif mode == 'having-filter':
                    accept = sum(row['price'] is not None and row['price'] > 0 for row in members) >= 2
                if accept:
                    selected.append((key, count))
            maximum = max((count for _, count in selected), default=None)
            for key, count in selected:
                value = (key, count, maximum) if mode == 'having-window' else (key, count) if mode in ('having-count', 'cte-chain') else (count,) if mode == 'having-key' else (key,)
                result[value] += 1
        elif mode.startswith('global-'):
            count, amount = len(rows), total([row['price'] for row in rows])
            if mode == 'global-constant':
                result[(7,)] = 1
            elif mode == 'global-empty' and count == 0:
                result[(0,)] = 1
            elif mode == 'global-sum' and (amount is None or amount > 10):
                result[(count, amount)] = 1
            elif mode == 'global-unknown' and amount is not None and amount > 10:
                result[(count,)] = 1
        elif mode == 'cte-repeated-join':
            result.update((row['id'], row['price']) for row in rows)
        elif mode == 'cte-shadow':
            result.update((row['id'],) for row in rows if row['id'] > 2)
        elif mode in ('union-group', 'union-window', 'combined'):
            counts = collections.Counter([row['auction'] for row in rows]+[row['id'] for row in catalog])
            values = list(counts.items())
            if mode == 'combined':
                values = sorted(((key, n) for key, n in values if n >= 2), key=lambda row: (-row[1], row[0] is None, row[0] or 0))[:4]
            for key, count in values:
                result[(key, count)] = count if mode == 'union-window' else 1
        elif mode == 'union-varchar':
            result.update((row[column],) for row in catalog for column in ('short', 'long'))
        elif mode == 'union-boolean':
            result.update((row['enabled'],) for row in catalog)
            result[(None,)] += 1
        elif mode == 'union-temporal':
            result.update((row['plain']+'+00:00' if row['plain'] is not None else None,) for row in catalog)
            result.update((row['zoned'],) for row in catalog)
        else:
            values = [row['price'] for row in rows]
            if mode == 'union-null':
                values = [value for value in values if value is None]*2
            elif mode in ('cte-repeated-union', 'union-duplicates'):
                values *= 2
            elif mode in ('union-widen', 'union-numeric'):
                values = [row['id'] for row in catalog]+values
            elif mode == 'union-branch-limit':
                values = descending(values)[:2]*2
            elif mode == 'union-limit':
                values = descending(values*2)[1:5]
            elif mode == 'union-constant':
                values.append(7)
            result.update((value,) for value in values)
        return result


def changes(fixture):
    b = fixture.name+'.bid'
    return [
        f'INSERT INTO {b} VALUES(100,NULL,NULL),(101,1,7),(102,1,7)',
        f"UPDATE {b} SET auction=2,price=11 WHERE id IN(100,101); INSERT INTO {fixture.name}.auction(id,category) VALUES(9,'extra')",
        f'UPDATE {b} SET price=NULL; DELETE FROM {fixture.name}.auction WHERE id=9',
        f'DELETE FROM {b}',
        f'INSERT INTO {b} VALUES(103,1,3),(104,1,3),(105,NULL,NULL)',
    ]


def qualify(output, command, mode):
    fixture = CompositionFixture(output, mode)
    with fixture.managed():
        b = fixture.name+'.bid'
        negatives = {
            'recursive': f'WITH RECURSIVE c AS (SELECT b.id FROM {b} b) SELECT c.id FROM c',
            'union-distinct': f'SELECT b.id FROM {b} b UNION SELECT b.id FROM {b} b',
            'having-alias': f'SELECT b.auction AS g,COUNT(*) AS n FROM {b} b GROUP BY b.auction HAVING n>1',
        }
        # Check fail-closed admission once; each history also rejects changed plans.
        if mode == MODES[0]:
            for label, query in negatives.items():
                fixture.rejected(command, label, query)
            assert fixture.sql(f"SELECT to_regclass('{fixture.name}.pgderive_worker_registration') IS NULL").strip() == 't'
        fixture.run_history(command, History(
            changes=changes(fixture),
            final=f'INSERT INTO {b} VALUES(106,1,7)',
            normalized=fixture.query.replace('SELECT', 'select'),
            rejected=[('changed', f'SELECT q.g FROM ({fixture.query}) q WHERE q.g IS NULL')],
        ))
        (output/'result.json').write_text(json.dumps({'mode': mode, 'original_sql_mv': True,
            'raw_row_memory_oracle': True, 'metadata': True, 'empty_and_null_inputs': True,
            'weighted_duplicates': True,
            'same_transaction_sources': mode in ('union-widen', 'union-numeric', 'union-group', 'union-window', 'combined'),
            'cold_restart': True})+'\n')


if __name__ == '__main__':
    run_modes(MODES, qualify)
