#!/usr/bin/env python3
"""Boolean aggregates through native bags, FILTER, retractions and cold restart."""
import collections
import json

from native_mv_oracle import binary_difference, expected
from sql_compiler_harness import SqlFixture
from worker_harness import History, run_modes


def boolean_values(rows):
    flags = [row['enabled'] for row in rows if row['enabled'] is not None]
    passing = [row for row in rows if row['permit'] is True]
    any_flags = [row['enabled'] for row in passing if row['enabled'] is not None]
    # The fixture uses nonzero divisors with magnitude at most ten on passing rows.
    signs = [row['divisor'] > 0 for row in passing if row['divisor'] is not None]
    return (all(flags) if flags else None, any(any_flags) if any_flags else None,
            all(signs) if signs else None)


class BooleanFixture(SqlFixture):
    def __init__(self, output, mode):
        super().__init__(output, False)
        self.mode = mode
        source = f'{self.name}.auction'
        self.sql(f'''ALTER TABLE {source} ADD COLUMN permit boolean, ADD COLUMN divisor integer;
            DELETE FROM {source};
            INSERT INTO {source}(id,group_id,enabled,permit,divisor) VALUES
                (1,1,true,true,2),(2,1,false,true,-2),(3,1,NULL,NULL,0),
                (4,2,NULL,true,NULL),(5,2,NULL,false,0),(6,NULL,true,false,0)''')
        over = ' OVER w' if mode == 'window' else ''
        fields = (f'bool_and(enabled){over} AS all_flags,'
                  f'bool_or(enabled) FILTER (WHERE permit){over} AS any_flags,'
                  f'every(10 / divisor > 0) FILTER (WHERE permit){over} AS every_safe')
        prefix = {'grouped': 'group_id AS k,', 'global': '', 'window': 'id,'}[mode]
        suffix = {'grouped': ' GROUP BY group_id', 'global': '',
                  'window': ' WINDOW w AS (PARTITION BY group_id ORDER BY id ROWS BETWEEN 2 PRECEDING AND 1 PRECEDING)'}[mode]
        self.query = f'SELECT {prefix}{fields} FROM {source}{suffix}'
        self.labels = ({'grouped': ['k'], 'global': [], 'window': ['id']}[mode]
                       + ['all_flags', 'any_flags', 'every_safe'])

    def verify(self):
        self.verify_sql(expected(self, self.labels))
        assert binary_difference(self) == '0'
        attrs = ("SELECT json_agg(json_build_array(attname,atttypid,atttypmod,attcollation) ORDER BY attnum)"
                 " FROM pg_attribute WHERE attrelid='%s'::regclass AND attnum>0 AND NOT attisdropped")
        assert self.sql(attrs % (self.name+'.groups')) == self.sql(attrs % (self.name+'.pgderive_native_oracle'))
        rows = json.loads(self.sql(f"SELECT COALESCE(json_agg(a ORDER BY id),'[]') FROM {self.name}.auction a"))
        if self.mode == 'global':
            self.verify_memory({boolean_values(rows): 1})
            return
        partitions = collections.defaultdict(list)
        for row in rows:
            partitions[row['group_id']].append(row)
        if self.mode == 'grouped':
            memory = {(key, *boolean_values(partition)): 1 for key, partition in partitions.items()}
        else:
            memory = {(row['id'], *boolean_values(partition[max(0,index-2):index])): 1
                      for partition in partitions.values() for index, row in enumerate(partition)}
        self.verify_memory(memory)


def qualify(output, command, mode):
    fixture = BooleanFixture(output, mode)
    a = fixture.name+'.auction'
    with fixture.managed():
        fixture.run_history(command, History(
            changes=[
                f"INSERT INTO {a}(id,group_id,enabled,permit,divisor) VALUES (7,1,false,true,-2)",
                f"DELETE FROM {a} WHERE id=2; UPDATE {a} SET permit=NULL,divisor=0 WHERE id=7",
                f"DELETE FROM {a} WHERE id=7; UPDATE {a} SET enabled=NULL WHERE id=1",
                f"DELETE FROM {a}",
                f"INSERT INTO {a}(id,group_id,enabled,permit,divisor) VALUES (8,NULL,false,true,2),(9,NULL,NULL,NULL,0)",
            ],
            final=f"UPDATE {a} SET enabled=true,divisor=-2 WHERE id=8",
            normalized=fixture.query.replace('every(', 'pg_catalog.bool_and('),
            rejected=[],
        ))
        (output/'result.json').write_text(json.dumps({
            'mode': mode, 'native_send_and_metadata': True, 'independent_bag': True,
            'filter_null_and_false': True, 'retractions': True, 'cold_restart': True,
        })+'\n')


if __name__ == '__main__':
    run_modes(('grouped', 'global', 'window'), qualify)
