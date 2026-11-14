#!/usr/bin/env python3
"""Compare ordered text aggregates with PostgreSQL through changes and restart."""
import json

from native_mv_oracle import binary_difference, expected
from sql_compiler_harness import SqlFixture
from worker_harness import History, run_modes


class StringAggFixture(SqlFixture):
    def __init__(self, output, mode):
        super().__init__(output, False)
        self.source = self.name + '.bid'
        self.sql(f'''ALTER TABLE {self.source}
            ADD COLUMN payload text COLLATE "C",
            ADD COLUMN delimiter text COLLATE "C"''')
        self.sql(f'DELETE FROM {self.source}')
        if mode == 'grouped':
            self.labels = ['auction', 'result']
            self.query = f'''SELECT auction,
                string_agg(payload, delimiter ORDER BY price DESC NULLS LAST, id)
                  FILTER (WHERE id <> 105) AS result
                FROM {self.source} GROUP BY auction'''
        else:
            self.labels = ['result']
            self.query = f'''SELECT string_agg(DISTINCT payload, delimiter
                ORDER BY payload DESC, delimiter NULLS FIRST) AS result FROM {self.source}'''

    def verify(self):
        self.verify_sql(expected(self, self.labels))
        assert binary_difference(self) == '0'


def qualify(output, command, mode):
    fixture = StringAggFixture(output, mode)
    source = fixture.source
    with fixture.managed():
        fixture.run_history(command, History(
            changes=[
                f"""INSERT INTO {source}(id,auction,price,payload,delimiter) VALUES
                    (100,1,5,'a','!'),(101,1,5,'a','!'),(102,1,NULL,'é',NULL),
                    (103,2,7,NULL,'?'),(104,2,8,'','/'),(105,1,9,'skip','?'),
                    (106,1,-1,'a','/')""",
                f"""UPDATE {source} SET price=10,delimiter=':' WHERE id=102""",
                f'DELETE FROM {source} WHERE id IN (101,103,104)',
                f'DELETE FROM {source}',
            ],
            final=f"""INSERT INTO {source}(id,auction,price,payload,delimiter) VALUES
                (107,1,1,'z','?'),(108,1,0,'a',NULL)""",
            normalized=fixture.query.replace('string_agg', 'pg_catalog.string_agg'),
            rejected=[],
        ))
        (output/'result.json').write_text(json.dumps({
            'mode': mode, 'native_binary_and_bag': True, 'cold_restart': True,
        })+'\n')


if __name__ == '__main__':
    run_modes(('grouped', 'distinct'), qualify)
