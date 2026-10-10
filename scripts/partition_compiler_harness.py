#!/usr/bin/env python3
"""Owned partition aggregate and non-temporal ROWS CDC qualification."""
import collections
import json
import pathlib
import sys
from sql_compiler_harness import SqlFixture


class PartitionFixture(SqlFixture):
    def __init__(self, output, window, frame_spec=None):
        super().__init__(output, False)
        self.window = window
        self.frame_spec = frame_spec or ("2 PRECEDING AND 1 FOLLOWING", -2, 1, True)
        self.sql(f'ALTER TABLE {self.name}.bid ALTER COLUMN price TYPE integer; UPDATE {self.name}.bid SET price=-id WHERE id%3=0; UPDATE {self.name}.bid SET auction=NULL WHERE id IN (1,2)')
        source = f'{self.name}.bid b'
        functions = ('COUNT(*) AS n,COUNT(b.price) AS present,SUM(b.price) AS total,'
                     'MIN(b.price) AS lo,MAX(b.price) AS hi,COUNT(*) FILTER(WHERE b.price<0) AS negative')
        if window:
            clause, _, _, descending = self.frame_spec
            order = 'DESC NULLS FIRST' if descending else 'ASC NULLS LAST'
            frame = f' OVER (PARTITION BY b.auction ORDER BY b.price {order},b.id ROWS BETWEEN {clause})' 
            functions = functions.replace(' AS ', frame + ' AS ')
            self.query = f'SELECT b.id AS id,b.auction AS auction,b.price AS price,{functions} FROM {source}'
            self.labels = ['id', 'auction', 'price', 'n', 'present', 'total', 'lo', 'hi', 'negative']
        else:
            self.query = f'SELECT b.auction AS auction,{functions} FROM {source} GROUP BY b.auction'
            self.labels = ['auction', 'n', 'present', 'total', 'lo', 'hi', 'negative']

    def verify(self):
        values = ','.join(self.labels)
        expected = f'SELECT jsonb_build_array(NULL,jsonb_build_array({values})) tuple,count(*)::bigint weight FROM ({self.query}) q GROUP BY 1'
        difference = self.sql(f'''WITH expected AS ({expected}), actual AS
            (SELECT tuple,weight FROM {self.name}.groups), difference AS
            ((SELECT * FROM expected EXCEPT ALL SELECT * FROM actual) UNION ALL
            (SELECT * FROM actual EXCEPT ALL SELECT * FROM expected)) SELECT count(*) FROM difference''')
        assert difference.strip() == '0', 'partition result differs from PostgreSQL'
        source = json.loads(self.sql(f"SELECT COALESCE(json_agg(b),'[]') FROM {self.name}.bid b"))
        groups = collections.defaultdict(list)
        for row in source:
            groups[row['auction']].append(row)
        expected_memory = collections.Counter()
        for key, group in groups.items():
            _, start, end, descending = self.frame_spec
            group.sort(key=lambda row: (row['price'] is not None, -(row['price'] or 0), row['id']) if descending else (row['price'] is None, row['price'] or 0, row['id']))
            for index in range(len(group) if self.window else 1):
                left = 0 if start is None else max(0, min(len(group), index + start))
                right = len(group) if end is None else max(0, min(len(group), index + end + 1))
                frame = group[min(left, right):right] if self.window else group
                prices = [row['price'] for row in frame if row['price'] is not None]
                stats = (len(frame), len(prices), sum(prices) if prices else None,
                         min(prices) if prices else None, max(prices) if prices else None,
                         sum(price < 0 for price in prices))
                prefix = (group[index]['id'], key, group[index]['price']) if self.window else (key,)
                expected_memory[prefix + stats] += 1
        actual = json.loads(self.sql(f"SELECT COALESCE(json_agg(g),'[]') FROM {self.name}.groups g"))
        assert {tuple(row['tuple'][1]): row['weight'] for row in actual} == dict(expected_memory)
        beyond = self.sql(f'''SELECT count(*) FROM pg_replication_slots s JOIN
            {self.name}.pgderive_worker_registration r ON r.slot_name=s.slot_name JOIN
            {self.name}.pgderive_progress p ON true WHERE s.confirmed_flush_lsn>p.end_lsn''')
        assert beyond.strip() == '0'


def qualify(output, command, window, frame_spec=None):
    fixture = PartitionFixture(output, window, frame_spec)
    try:
        fixture.rejected(command, 'unsupported', fixture.query + ' ORDER BY auction')
        if window:
            fixture.rejected(command, 'peer-order', fixture.query.replace(',b.id ROWS', ' ROWS'))
            fixture.rejected(command, 'range', fixture.query.replace('ROWS BETWEEN ' + fixture.frame_spec[0], 'RANGE UNBOUNDED PRECEDING'))
        assert fixture.sql(f"SELECT to_regclass('{fixture.name}.pgderive_worker_registration') IS NULL").strip() == 't'
        worker = fixture.start(command, 'first')
        ready = worker.event('ready')
        fixture.verify()
        changes = [
            f"INSERT INTO {fixture.name}.bid VALUES(100,NULL,NULL),(101,100,-3),(102,100,NULL)",
            f"UPDATE {fixture.name}.bid SET price=7,auction=1 WHERE id IN (100,101); UPDATE {fixture.name}.auction SET group_id=NULL WHERE id=1",
            f"UPDATE {fixture.name}.bid SET price=NULL WHERE auction=1",
            f"DELETE FROM {fixture.name}.bid WHERE auction=1; DELETE FROM {fixture.name}.bid WHERE id=102",
            f"INSERT INTO {fixture.name}.bid VALUES(103,100,NULL),(104,100,NULL)",
            f"UPDATE {fixture.name}.auction SET group_id=3 WHERE id=2",  # complete empty query tick
        ]
        for index, change in enumerate(changes, 1):
            fixture.sql('BEGIN;' + change + ';COMMIT')
            worker.event('published', minimum_time=ready['time'] + index)
            fixture.verify()
        worker.abort()
        prior = fixture.sql(f'SELECT row_to_json(p) FROM {fixture.name}.pgderive_progress p')
        changed = fixture.query.replace('ROWS BETWEEN ' + fixture.frame_spec[0], 'ROWS BETWEEN 3 PRECEDING AND CURRENT ROW') if window else fixture.query.replace('b.price<0', 'b.price<1')
        fixture.rejected(command, 'changed', changed)
        assert fixture.sql(f'SELECT row_to_json(p) FROM {fixture.name}.pgderive_progress p') == prior
        fixture.query = fixture.query.replace('b.', '"B".').replace('bid b', 'bid AS "B"')
        resumed = fixture.start(command, 'resumed', maximum=1)
        reopened = resumed.event('ready')
        assert reopened['slot'] == ready['slot'] and reopened['time'] == ready['time'] + len(changes)
        fixture.verify()
        fixture.sql(f'UPDATE {fixture.name}.bid SET price=-7 WHERE id=104')
        resumed.event('published')
        resumed.finish()
        fixture.verify()
        (output/'result.json').write_text(json.dumps({'sql_oracle':'exact bag','memory_oracle':'independent partitions','null_group':True,'all_null':True,'retractions':True,'restart':'cold','window':window})+'\n')
        fixture.cleanup()
    except BaseException:
        fixture.preserve()
        raise


if __name__ == '__main__':
    output = pathlib.Path(sys.argv[1])
    command = sys.argv[2:]
    if command and command[0] == '--':
        command = command[1:]
    cases = [
        ('grouped', None), ('rows', ('2 PRECEDING AND 1 FOLLOWING', -2, 1, True)),
        ('unbounded', ('UNBOUNDED PRECEDING AND CURRENT ROW', None, 0, False)),
        ('following', ('CURRENT ROW AND UNBOUNDED FOLLOWING', 0, None, True)),
        ('empty', ('2 FOLLOWING AND 1 FOLLOWING', 2, 1, False)),
        ('preceding', ('2 PRECEDING AND 1 PRECEDING', -2, -1, False)),
        ('full', ('UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING', None, None, True)),
    ]
    for mode, frame in cases:
        directory = output/mode
        directory.mkdir()
        qualify(directory, command, frame is not None, frame)
