#!/usr/bin/env python3
"""Native PostgreSQL and independent frame-value oracles across CDC and restart."""
from collections import Counter, defaultdict
import json
from native_mv_oracle import binary_difference, expected
from ranking_compiler_harness import qualify
from sql_compiler_harness import SqlFixture
from worker_harness import run_modes


class FrameValueFixture(SqlFixture):
    oracle_name = 'independent ordered frame positions'

    def __init__(self, output, mode):
        super().__init__(output, False)
        self.mode = mode
        self.sql(f'''ALTER TABLE {self.name}.bid ADD COLUMN step smallint;
            UPDATE {self.name}.bid SET price=CASE WHEN id%4=0 THEN NULL ELSE id%3 END,
                step=CASE WHEN id%5=0 THEN NULL ELSE id%4+1 END''')
        frames = {
            'default': 'ORDER BY b.price DESC NULLS FIRST',
            'groups': 'ORDER BY b.price DESC NULLS FIRST GROUPS BETWEEN 1 PRECEDING AND 1 FOLLOWING',
            'rows': 'ORDER BY b.id ROWS BETWEEN 2 FOLLOWING AND 3 FOLLOWING',
            'unordered': '',
        }
        # With peers, make the payload identical within a peer group. SQL leaves
        # the order of occurrences inside a peer group unspecified.
        payload = 'b.price::text::varchar(12) COLLATE "C"'
        if mode == 'unordered':
            payload = "CASE WHEN b.auction IS NULL THEN NULL::varchar(12) ELSE 'partition'::varchar(12) END"
        self.query = f"""SELECT b.id,first_value({payload}) OVER w AS head,
            last_value({payload}) OVER w AS tail,
            nth_value({payload},(b.step + b.id%2)::smallint) OVER w AS picked,
            nth_value({payload},'0x2') OVER w AS second,
            nth_value({payload},NULL) OVER w AS missing,
            count(*) OVER w AS size FROM {self.name}.bid b
            WINDOW w AS (PARTITION BY b.auction {frames[mode]})"""
        self.labels = ['id', 'head', 'tail', 'picked', 'second', 'missing', 'size']

    def changed_query(self):
        return self.query.replace(",'0x2')", ",'0b11')")

    def verify(self):
        self.verify_sql(expected(self, self.labels))
        assert binary_difference(self) == '0'
        attributes = ("SELECT json_agg(json_build_array(attname,atttypid,atttypmod,attcollation) ORDER BY attnum) "
                      "FROM pg_attribute WHERE attnum>0 AND NOT attisdropped AND attrelid=")
        native, sink = (self.sql(attributes + f"'{self.name}.{table}'::regclass")
                        for table in ('pgderive_native_oracle', 'groups'))
        assert native == sink, (native, sink)
        source = json.loads(self.sql(f"SELECT COALESCE(json_agg(b),'[]') FROM {self.name}.bid b"))
        groups = defaultdict(list)
        for row in source:
            groups[row['auction']].append(row)
        memory = Counter()
        for group in groups.values():
            group.sort(key=lambda row: row['id'])
            if self.mode in ('default', 'groups'):
                group.sort(key=lambda row: (row['price'] is not None, -(row['price'] or 0)))
            keys = list(dict.fromkeys(row['price'] for row in group))
            for index, row in enumerate(group):
                if self.mode == 'rows':
                    frame = group[index+2:index+4]
                elif self.mode == 'unordered':
                    frame = group
                else:
                    peer = keys.index(row['price'])
                    selected = keys[:peer+1] if self.mode == 'default' else keys[max(0, peer-1):peer+2]
                    frame = [item for item in group if item['price'] in selected]
                values = [
                    ('partition' if item['auction'] is not None else None) if self.mode == 'unordered'
                    else (str(item['price']) if item['price'] is not None else None)
                    for item in frame
                ]
                position = None if row['step'] is None else row['step'] + row['id'] % 2 - 1
                memory[(
                    row['id'], values[0] if values else None, values[-1] if values else None,
                    values[position] if position is not None and position < len(values) else None,
                    values[1] if len(values) > 1 else None, None, len(values),
                )] += 1
        self.verify_memory(dict(memory))


if __name__ == '__main__':
    run_modes(('default', 'groups', 'rows', 'unordered'), qualify, fixture_class=FrameValueFixture)
