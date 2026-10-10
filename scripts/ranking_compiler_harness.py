#!/usr/bin/env python3
"""PostgreSQL peer/occurrence ranking with an independent raw-source oracle."""
import collections
import json
import pathlib
import sys
from sql_compiler_harness import SqlFixture


class RankingFixture(SqlFixture):
    oracle_name = 'independent peer/occurrence ranks'
    def __init__(self, output, mode):
        super().__init__(output, False)
        self.mode = mode
        self.sql(f'UPDATE {self.name}.bid SET price=CASE WHEN id%4=0 THEN NULL ELSE id%3 END,auction=CASE WHEN id%5=0 THEN NULL ELSE auction END')
        order = '' if mode == 'unordered' else ' ORDER BY b.price DESC NULLS FIRST'
        if mode == 'number':
            order += ',b.id'
            functions = f'row_number() OVER(PARTITION BY b.auction{order}) AS r'
            self.labels = ['id', 'auction', 'price', 'r']
        else:
            functions = f'rank() OVER(PARTITION BY b.auction{order}) AS r,dense_rank() OVER(PARTITION BY b.auction{order}) AS d'
            self.labels = ['id', 'auction', 'price', 'r', 'd']
        self.query = f'SELECT b.id,b.auction,b.price,{functions} FROM {self.name}.bid b'

    def verify(self):
        values = ','.join(self.labels)
        expected = f'SELECT jsonb_build_array(NULL,jsonb_build_array({values})) tuple,count(*)::bigint weight FROM ({self.query}) q GROUP BY 1'
        difference = self.sql(f'''WITH expected AS ({expected}), actual AS
            (SELECT tuple,weight FROM {self.name}.groups), difference AS
            ((SELECT * FROM expected EXCEPT ALL SELECT * FROM actual) UNION ALL
            (SELECT * FROM actual EXCEPT ALL SELECT * FROM expected)) SELECT count(*) FROM difference''')
        assert difference.strip() == '0', 'ranking differs from PostgreSQL'
        source = json.loads(self.sql(f"SELECT COALESCE(json_agg(b),'[]') FROM {self.name}.bid b"))
        groups = collections.defaultdict(list)
        for row in source:
            groups[row['auction']].append(row)
        memory = collections.Counter()
        for group in groups.values():
            group.sort(key=lambda row: (row['price'] is not None, -(row['price'] or 0), row['id']))
            distinct = []
            for index, row in enumerate(group):
                price = row['price']
                if price not in distinct:
                    distinct.append(price)
                prefix = (row['id'], row['auction'], price)
                if self.mode == 'number':
                    ranks = (index + 1,)
                elif self.mode == 'unordered':
                    ranks = (1, 1)
                else:
                    preceding = sum(other['price'] is None or (price is not None and other['price'] > price) for other in group if other['price'] != price)
                    ranks = (preceding + 1, len(distinct))
                memory[prefix + ranks] += 1
        actual = json.loads(self.sql(f"SELECT COALESCE(json_agg(g),'[]') FROM {self.name}.groups g"))
        assert {tuple(row['tuple'][1]): row['weight'] for row in actual} == dict(memory)
        assert self.sql(f'''SELECT count(*) FROM pg_replication_slots s JOIN
            {self.name}.pgderive_worker_registration r ON r.slot_name=s.slot_name JOIN
            {self.name}.pgderive_progress p ON true WHERE s.confirmed_flush_lsn>p.end_lsn''').strip() == '0'


def qualify(output, command, mode, fixture_class=RankingFixture):
    fixture = fixture_class(output, mode)
    try:
        fixture.rejected(command, 'unsupported', fixture.query + ' ORDER BY id')
        if mode == 'number':
            fixture.rejected(command, 'peer-order', fixture.query.replace(',b.id)', ')'))
        assert fixture.sql(f"SELECT to_regclass('{fixture.name}.pgderive_worker_registration') IS NULL").strip() == 't'
        worker = fixture.start(command, 'first')
        ready = worker.event('ready')
        fixture.verify()
        changes = [
            f'INSERT INTO {fixture.name}.bid VALUES(100,NULL,NULL),(101,1,2),(102,1,2)',
            f'UPDATE {fixture.name}.bid SET price=7,auction=1 WHERE id IN (100,101); UPDATE {fixture.name}.auction SET group_id=NULL WHERE id=1',
            f'UPDATE {fixture.name}.bid SET price=NULL WHERE auction=1',
            f'DELETE FROM {fixture.name}.bid WHERE auction=1; DELETE FROM {fixture.name}.bid WHERE id=100',
            f'INSERT INTO {fixture.name}.bid VALUES(103,100,NULL),(104,100,NULL)',
            f'UPDATE {fixture.name}.auction SET group_id=3 WHERE id=2',
        ]
        for index, change in enumerate(changes, 1):
            fixture.sql('BEGIN;' + change + ';COMMIT')
            worker.event('published', minimum_time=ready['time'] + index)
            fixture.verify()
        worker.abort()
        prior = fixture.sql(f'SELECT row_to_json(p) FROM {fixture.name}.pgderive_progress p')
        fixture.rejected(command, 'changed', fixture.query.replace('PARTITION BY b.auction', 'PARTITION BY b.price'))
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
        (output/'result.json').write_text(json.dumps({'sql_oracle':'exact bag','memory_oracle':fixture.oracle_name,'null_peers':True,'retractions':True,'restart':'cold','mode':mode})+'\n')
        fixture.cleanup()
    except BaseException:
        fixture.preserve()
        raise


if __name__ == '__main__':
    output = pathlib.Path(sys.argv[1])
    command = sys.argv[2:]
    if command and command[0] == '--':
        command = command[1:]
    for mode in ('peers', 'number', 'unordered'):
        directory = output/mode
        directory.mkdir()
        qualify(directory, command, mode)
