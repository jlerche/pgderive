#!/usr/bin/env python3
"""Constant VALUES bags against PostgreSQL through publication and cold restart."""
import collections
from decimal import Decimal
import json

from native_mv_oracle import binary_difference, expected
from sql_compiler_harness import SqlFixture
from worker_harness import History, run_modes


class ValuesFixture(SqlFixture):
    def __init__(self, output, mode):
        super().__init__(output, False)
        self.mode = mode
        if mode == 'constant':
            self.labels = ['column1', 'column2', 'column3', 'column4', 'column5', 'column6']
            self.query = """VALUES (1::smallint,'é'::varchar(2) COLLATE "C",NULL,1.2::numeric(4,2),'q' COLLATE "C",'7' COLLATE "C"),
                                   (1::bigint,'é'::varchar(2),NULL,1.20::numeric(4,2),'q'::varchar(2),7),
                                   (NULL,NULL::varchar(2),NULL,2::numeric(4,2),NULL COLLATE "C",NULL)"""
        else:
            self.labels = ['id', 'tag']
            self.query = f"""WITH v(k,tag) AS MATERIALIZED (VALUES (1,'same'),(1,'same'),(100,'new'),(NULL,'never'))
                SELECT a.id,v.tag FROM {self.name}.auction a JOIN v ON a.id=v.k"""

    def verify(self):
        self.verify_sql(expected(self, self.labels))
        assert binary_difference(self) == '0'
        attrs = ("SELECT json_agg(json_build_array(attname,atttypid,atttypmod,attcollation) ORDER BY attnum)"
                 " FROM pg_attribute WHERE attrelid='%s'::regclass AND attnum>0 AND NOT attisdropped")
        assert self.sql(attrs % (self.name+'.groups')) == self.sql(attrs % (self.name+'.pgderive_native_oracle'))
        if self.mode == 'constant':
            memory = {(1, 'é', None, Decimal('1.20'), 'q', 7): 2,
                      (None, None, None, Decimal('2.00'), None, None): 1}
        else:
            ids = set(json.loads(self.sql(f'SELECT COALESCE(json_agg(id),\'[]\') FROM {self.name}.auction')))
            memory = collections.Counter((key, tag) for key, tag in ((1,'same'), (1,'same'), (100,'new')) if key in ids)
        self.verify_memory(memory, parse_float=Decimal)


def qualify(output, command, mode):
    fixture = ValuesFixture(output, mode)
    a = fixture.name+'.auction'
    with fixture.managed():
        fixture.run_history(command, History(
            changes=[f"INSERT INTO {a}(id,category) VALUES (100,'inserted')", f'DELETE FROM {a} WHERE id=1'],
            final=f"INSERT INTO {a}(id,category) VALUES (1,'restored')",
            normalized=fixture.query.replace('VALUES', 'values'),
            rejected=[],
        ))
        (output/'result.json').write_text(json.dumps({
            'mode': mode, 'native_send_and_metadata': True, 'duplicate_rows': True,
            'independent_bag': True, 'cold_restart': True,
        })+'\n')


if __name__ == '__main__':
    run_modes(('constant', 'joined'), qualify)
