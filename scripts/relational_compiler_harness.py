#!/usr/bin/env python3
"""Owned sequential ungrouped join circuit qualification against two oracles."""
import collections
import json
import pathlib
import sys
from sql_compiler_harness import SqlFixture


class RelationalFixture(SqlFixture):
    def __init__(self, output, terminal):
        super().__init__(output, False)
        self.terminal = terminal
        self.sql(f"UPDATE {self.name}.auction SET group_id=NULL,category='same',enabled=true WHERE id IN (1,2); UPDATE {self.name}.bid SET price=-3 WHERE auction IN (1,2)")
        self.query = (f'SELECT a.group_id AS g,a.category AS c,b.price AS p,a.enabled AS e '
                      f'FROM {self.name}.auction a INNER JOIN {self.name}.bid b ON b.auction=a.id '
                      'WHERE a.enabled OR b.price IS NULL')
        if terminal:
            self.query = self.query.replace('a.category AS c,b.price AS p', 'length(a.category) AS c,abs(b.price) AS p')

    def verify(self):
        expected = f'SELECT jsonb_build_array(NULL,jsonb_build_array(g,c,p,e)) tuple,count(*)::bigint weight FROM ({self.query}) q GROUP BY 1'
        difference = self.sql(f'''WITH expected AS ({expected}), actual AS
            (SELECT tuple,weight FROM {self.name}.groups), difference AS
            ((SELECT * FROM expected EXCEPT ALL SELECT * FROM actual) UNION ALL
            (SELECT * FROM actual EXCEPT ALL SELECT * FROM expected)) SELECT count(*) FROM difference''')
        assert difference.strip() == '0', 'join projection differs from PostgreSQL'
        left = json.loads(self.sql(f"SELECT COALESCE(json_agg(a),'[]') FROM {self.name}.auction a"))
        right = json.loads(self.sql(f"SELECT COALESCE(json_agg(b),'[]') FROM {self.name}.bid b"))
        expected = collections.Counter()
        for a in left:
            for b in right:
                if b['auction'] is None or a['id'] != b['auction']:
                    continue
                if a['enabled'] is not True and b['price'] is not None:
                    continue
                category, price = a['category'], b['price']
                if self.terminal:
                    category = None if category is None else len(category)
                    price = None if price is None else abs(price)
                expected[(a['group_id'], category, price, a['enabled'])] += 1
        actual = json.loads(self.sql(f"SELECT COALESCE(json_agg(g),'[]') FROM {self.name}.groups g"))
        assert {tuple(row['tuple'][1]): row['weight'] for row in actual} == dict(expected)
        beyond = self.sql(f'''SELECT count(*) FROM pg_replication_slots s JOIN
            {self.name}.pgderive_worker_registration r ON r.slot_name=s.slot_name JOIN
            {self.name}.pgderive_progress p ON true WHERE s.confirmed_flush_lsn>p.end_lsn''')
        assert beyond.strip() == '0'


def qualify(output, command, terminal):
    fixture = RelationalFixture(output, terminal)
    try:
        for label, query in [
            ('ambiguous', fixture.query.replace('a.enabled OR', 'id > 0 OR')),
            ('unsupported', fixture.query + ' ORDER BY g'),
            ('text-key', fixture.query.replace('b.auction=a.id', 'a.category=a.category')),
        ]:
            fixture.rejected(command, label, query)
        assert fixture.sql(f"SELECT to_regclass('{fixture.name}.pgderive_worker_registration') IS NULL").strip() == 't'
        assert fixture.sql(f"SELECT count(*) FROM pg_replication_slots WHERE slot_name LIKE '{fixture.name}%'").strip() == '0'
        worker = fixture.start(command, 'first')
        ready = worker.event('ready')
        fixture.verify()
        assert int(fixture.sql(f'SELECT max(weight) FROM {fixture.name}.groups')) > 1
        changes = [
            f"INSERT INTO {fixture.name}.auction(id,category,group_id,enabled) VALUES(100,'é🙂',NULL,true); INSERT INTO {fixture.name}.bid VALUES(100,100,-7),(101,100,NULL),(102,NULL,4)",
            f"UPDATE {fixture.name}.auction SET category=NULL,enabled=NULL WHERE id=100; UPDATE {fixture.name}.bid SET price=9 WHERE id=100",
            f"UPDATE {fixture.name}.auction SET enabled=true,category='back',group_id=1 WHERE id=100; UPDATE {fixture.name}.bid SET auction=100,price=-2 WHERE id=2",
            f"DELETE FROM {fixture.name}.auction WHERE id=100; DELETE FROM {fixture.name}.bid WHERE id=101",
            f"INSERT INTO {fixture.name}.auction(id,category,group_id,enabled) VALUES(100,'new',2,false); UPDATE {fixture.name}.bid SET price=NULL WHERE id=100",
            f"DELETE FROM {fixture.name}.bid WHERE id=102",  # null key empty output tick
        ]
        for index, change in enumerate(changes, 1):
            fixture.sql('BEGIN;' + change + ';COMMIT')
            worker.event('published', minimum_time=ready['time'] + index)
            fixture.verify()
        worker.abort()
        prior = fixture.sql(f'SELECT row_to_json(p) FROM {fixture.name}.pgderive_progress p')
        fixture.rejected(command, 'changed', fixture.query.replace('a.group_id AS g', 'a.id AS g'))
        assert fixture.sql(f'SELECT row_to_json(p) FROM {fixture.name}.pgderive_progress p') == prior
        fixture.query = fixture.query.replace('a.', '"X".').replace('auction a ', 'auction AS "X" ').replace('INNER JOIN', 'JOIN')
        resumed = fixture.start(command, 'resumed', maximum=1)
        reopened = resumed.event('ready')
        assert reopened['slot'] == ready['slot'] and reopened['time'] == ready['time'] + len(changes)
        fixture.verify()
        fixture.sql(f'UPDATE {fixture.name}.bid SET price=NULL WHERE id=2')
        resumed.event('published')
        resumed.finish()
        fixture.verify()
        (output / 'result.json').write_text(json.dumps({'sql_oracle': 'exact weighted bag', 'memory_oracle': 'independent nested loop', 'simultaneous': 'insert/update/delete', 'nulls': 'keys/predicates/outputs', 'restart': 'cold same slot', 'changed': 'rejected without progress', 'terminal': terminal}) + '\n')
        fixture.cleanup()
    except BaseException:
        fixture.preserve()
        raise


if __name__ == '__main__':
    output = pathlib.Path(sys.argv[1])
    command = sys.argv[2:]
    if command and command[0] == '--':
        command = command[1:]
    for mode in ('plain', 'terminal'):
        directory = output / mode
        directory.mkdir()
        qualify(directory, command, mode == 'terminal')
