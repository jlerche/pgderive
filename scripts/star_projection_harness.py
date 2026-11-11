#!/usr/bin/env python3
"""Single-table stars through native output catalogs, CDC and cold restart."""
from collections import Counter
from decimal import Decimal
import json

from native_mv_oracle import binary_difference, expected, quote
from sql_compiler_harness import SqlFixture
from worker_harness import History, run_modes


class StarFixture(SqlFixture):
    def __init__(self, output, mode):
        super().__init__(output, False)
        self.mode = mode
        source = self.name + '.auction'
        self.sql(f'''ALTER TABLE {source} DROP COLUMN token,
            ADD COLUMN "Label" varchar(7) COLLATE "C",
            ADD COLUMN amount numeric(9,3),
            ADD COLUMN "Moment" timestamp(2);
            UPDATE {source} SET "Label"='value' || id, amount=id/10.0,
                "Moment"='2024-01-01 12:34:56.789'::timestamp''')
        self.source_labels = ['id', 'category', 'enabled', 'group_id', 'Label', 'amount', 'Moment']
        self.labels = self.source_labels.copy()
        target = '*' if mode == 'bare' else '"A".*'
        if mode == 'ordered':
            target = 'id+10 AS shifted,' + target
            self.labels.insert(0, 'shifted')
        suffix = ' ORDER BY 2 DESC LIMIT 2' if mode == 'ordered' else ''
        self.query = f'SELECT {target} FROM {source} AS "A" WHERE "A".enabled IS NOT FALSE{suffix}'
        explicit = ','.join(quote(label) for label in self.source_labels)
        self.normalized = self.query.replace('*' if mode == 'bare' else '"A".*', explicit, 1)

    def verify(self):
        self.verify_sql(expected(self, self.labels))
        assert binary_difference(self) == '0'
        attributes = ("SELECT json_agg(json_build_array(attnum,attname,atttypid,atttypmod,attcollation) "
                      "ORDER BY attnum) FROM pg_attribute WHERE attrelid='%s'::regclass "
                      "AND attnum>0 AND NOT attisdropped")
        assert self.sql(attributes % (self.name+'.groups')) == self.sql(
            attributes % (self.name+'.pgderive_native_oracle'))
        source = self.name + '.auction'
        rows = json.loads(self.sql(f"SELECT COALESCE(json_agg(a),'[]') FROM {source} a"),
                          parse_float=Decimal)
        rows = [row for row in rows if row['enabled'] is not False]
        if self.mode == 'ordered':
            rows = sorted(rows, key=lambda row: row['id'], reverse=True)[:2]
            for row in rows:
                row['shifted'] = row['id'] + 10
        self.verify_memory(Counter(tuple(row[label] for label in self.labels) for row in rows),
                           parse_float=Decimal)


def qualify(output, command, mode):
    fixture = StarFixture(output, mode)
    source = fixture.name + '.auction'
    with fixture.managed():
        fixture.rejected(command, 'hidden-table-name', f'SELECT auction.* FROM {source} a')
        fixture.run_history(command, History(
            changes=[
                f'''INSERT INTO {source}(id,category,enabled,group_id,"Label",amount,"Moment")
                    VALUES (100,'new',NULL,3,'late',7.1234,'2024-02-03 04:05:06.987')''',
                f'UPDATE {source} SET enabled=false WHERE id=100',
                f'''UPDATE {source} SET enabled=true,"Label"=NULL,amount=NULL,"Moment"=NULL
                    WHERE id=100''',
            ],
            final=f'DELETE FROM {source} WHERE id=100',
            normalized=fixture.normalized,
            rejected=[],
        ))
        (output/'result.json').write_text(json.dumps({
            'mode': mode, 'physical_column_gap': True, 'native_binary_and_metadata': True,
            'cold_restart_with_explicit_targets': True,
        })+'\n')


if __name__ == '__main__':
    run_modes(('bare', 'alias', 'ordered'), qualify)
