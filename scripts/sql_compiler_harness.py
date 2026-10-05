#!/usr/bin/env python3
"""Sequential SQL-compiled worker oracle/restart tests in an owned fixture."""
import json
import pathlib
import sys
from worker_harness import Fixture, Worker


class SqlFixture(Fixture):
    def __init__(self, output, null_only):
        super().__init__(output, 'sql', rows=32, auctions=8)
        self.null_only = null_only
        self.sql(f'ALTER TABLE {self.name}.auction ADD COLUMN group_id integer; UPDATE {self.name}.auction SET group_id=CASE WHEN id%3=0 THEN NULL ELSE id%2 END')
        self.query = (f'SELECT a.group_id, COUNT(*), SUM(b.price) FROM {self.name}.auction a '
                      f'JOIN {self.name}.bid b ON a.id=b.auction '
                      'WHERE a.category IS NOT NULL '
                      + ('AND b.price IS NULL ' if null_only else '') + 'GROUP BY a.group_id')

    def config(self, maximum=0):
        path = super().config(maximum)
        text = path.read_text()
        before, rest = text.split('[worker.query]')
        _, after = rest.split('[execution]')
        path.write_text(before + '[worker.query]\nsql=' + json.dumps(self.query)
                        + '\n[execution]' + after)
        return path

    def expected_query(self):
        self.sql(f"EXPLAIN {self.query}")
        return (f"SELECT COALESCE(to_jsonb(a.group_id::text),'null'::jsonb) group_key,count(*) row_count,sum(b.price) total "
                f"FROM {self.name}.auction a JOIN {self.name}.bid b ON a.id=b.auction "
                "WHERE a.category IS NOT NULL "
                + ("AND b.price IS NULL " if self.null_only else "") + "GROUP BY a.group_id")

    def verify(self):
        super().verify()
        # Independent raw-source nested-loop memory oracle, no compiled IR reuse.
        left = json.loads(self.sql(f'SELECT json_agg(a) FROM {self.name}.auction a'))
        right = json.loads(self.sql(f'SELECT json_agg(b) FROM {self.name}.bid b'))
        expected = {}
        for a in left:
            for b in right:
                if (a['category'] is None or b['auction'] is None or a['id'] != b['auction']
                        or (self.null_only and b['price'] is not None)):
                    continue
                group = expected.setdefault(None if a['group_id'] is None else str(a['group_id']), [0, None])
                group[0] += 1
                if b['price'] is not None:
                    group[1] = (group[1] or 0) + b['price']
        actual = json.loads(self.sql(f'SELECT COALESCE(json_agg(g),\'[]\') FROM {self.name}.groups g'))
        assert {g['group_key']: [g['row_count'], g['total']] for g in actual} == expected

    def rejected(self, command, label, query):
        original = self.query
        self.query = query
        directory = self.output / label
        directory.mkdir()
        config = self.config()
        config.write_text(config.read_text().replace('retry_attempts=8', 'retry_attempts=1'))
        worker = Worker(command, config, directory, self.environment)
        self.workers.append(worker)
        try:
            worker.process.wait(timeout=60)
            assert worker.process.returncode != 0, 'unsupported/changed SQL was accepted'
        finally:
            worker.abort()
            self.query = original


def qualify(output, command, null_only):
    fixture = SqlFixture(output, null_only)
    try:
        fixture.rejected(command, 'unsupported', fixture.query + ' HAVING COUNT(*)>1')
        assert fixture.sql(f"SELECT to_regclass('{fixture.name}.pgderive_worker_registration') IS NULL").strip() == 't'
        assert fixture.sql(f"SELECT count(*) FROM pg_replication_slots WHERE slot_name LIKE '{fixture.name}%'").strip() == '0'
        worker = fixture.start(command, 'first')
        ready = worker.event('ready')
        fixture.verify()
        changes = [
            f"INSERT INTO {fixture.name}.bid VALUES(100,1,NULL),(101,1,7)",
            f"UPDATE {fixture.name}.auction SET category=NULL WHERE id=1; UPDATE {fixture.name}.bid SET price=9,auction=1 WHERE id=2",
            f"UPDATE {fixture.name}.auction SET category='back',group_id=NULL WHERE id=1; UPDATE {fixture.name}.bid SET price=NULL WHERE auction=1",
            f"UPDATE {fixture.name}.bid SET price=0 WHERE id=100; DELETE FROM {fixture.name}.bid WHERE id=101",
            f"DELETE FROM {fixture.name}.bid WHERE auction=1",
        ]
        for index, change in enumerate(changes, 1):
            fixture.sql('BEGIN;' + change + ';COMMIT')
            worker.event('published', minimum_time=ready['time'] + index)
            fixture.verify()
        worker.finish(graceful=True)
        prior = fixture.sql(f'SELECT row_to_json(p) FROM {fixture.name}.pgderive_progress p')
        fixture.rejected(command, 'changed', fixture.query.replace('IS NOT NULL', 'IS NULL'))
        assert fixture.sql(f'SELECT row_to_json(p) FROM {fixture.name}.pgderive_progress p') == prior
        fixture.query = fixture.query.replace('a.group_id', 'a.U&"group\\005fid"').replace('a.category IS NOT NULL', '((a.category) IS NOT NULL)').replace('a.', '"X".').replace('auction a ', 'auction AS "X" ').replace('JOIN', 'INNER JOIN')
        resumed = fixture.start(command, 'resumed', maximum=1)
        reopened = resumed.event('ready')
        assert reopened['slot'] == ready['slot'] and reopened['time'] == ready['time'] + len(changes)
        price = 'NULL' if null_only else '11'
        fixture.sql(f'INSERT INTO {fixture.name}.bid VALUES(102,1,{price})')
        resumed.event('published')
        resumed.finish()
        fixture.verify()
        (output / 'result.json').write_text(json.dumps({'sql_oracle': 'exact', 'memory_oracle': 'exact',
            'transactions': len(changes) + 1, 'restart': 'normalized SQL reuses durable slot',
            'unsupported': 'before registration/slot', 'changed': 'rejected with unchanged progress'}) + '\n')
        fixture.cleanup()
    except BaseException:
        fixture.preserve()
        raise


if __name__ == '__main__':
    output = pathlib.Path(sys.argv[1])
    command = sys.argv[2:]
    if command and command[0] == '--':
        command = command[1:]
    for mode in ('regular', 'null-only'):
        directory = output / mode
        directory.mkdir()
        qualify(directory, command, mode == 'null-only')
