#!/usr/bin/env python3
"""Sequential owned PostgreSQL MV/memory/cold-restart ordered-extrema checks."""
from worker_harness import harness_arguments, History
import collections
import datetime
import json
from native_mv_oracle import expected
from sql_compiler_harness import SqlFixture


def temporal_key(value):
    if value == '-infinity':
        return (0,)
    if value == 'infinity':
        return (2,)
    parsed = datetime.datetime.fromisoformat(value)
    if parsed.tzinfo:
        parsed = parsed.astimezone(datetime.timezone.utc).replace(tzinfo=None)
    return (1, parsed)


class OrderedFixture(SqlFixture):
    def __init__(self, output, oid, joined):
        super().__init__(output, False)
        self.oid = oid
        self.joined = joined
        self.temporal = oid in ('timestamp', 'timestamptz')
        if self.temporal:
            self.sql(f"ALTER TABLE {self.name}.bid ALTER COLUMN price TYPE {oid} USING CASE WHEN price IS NULL THEN NULL WHEN id%5=0 THEN '-infinity'::{oid} WHEN id%7=0 THEN 'infinity'::{oid} ELSE '2000-01-01'::{oid}+id*interval '1 microsecond' END")
        else:
            self.sql(f'ALTER TABLE {self.name}.bid ALTER COLUMN price TYPE {oid}; UPDATE {self.name}.bid SET price=-id WHERE id%3=0')
        if joined:
            source = f'(SELECT a.group_id AS g,b.price AS price,b.auction AS auction FROM {self.name}.auction a JOIN {self.name}.bid b ON a.id=b.auction) b'
            group = 'b.g'
        else:
            source = f'{self.name}.bid b'
            group = 'b.auction'
        self.query = f'SELECT {group} AS g,MIN(b.price) AS lo,MAX(b.price) AS hi,MIN(b.price) FILTER(WHERE b.auction IS NULL) AS excluded FROM {source} GROUP BY {group}'
        self.labels = ['g', 'lo', 'hi', 'excluded']

    def value(self, value):
        if value is None:
            return 'NULL'
        if self.temporal:
            text = 'infinity' if value > 100 else ('-infinity' if value < 0 else f'2000-01-01 00:00:00.{value:06}')
            return f"'{text}'::{self.oid}"
        return str(value)

    def verify(self):
        native = expected(self, self.labels)
        difference = self.bag_difference(native)
        assert difference.strip() == '0', 'ordered extrema differs from native MV'
        bids = json.loads(self.sql(f"SELECT COALESCE(json_agg(b),'[]') FROM {self.name}.bid b"))
        auctions = json.loads(self.sql(f"SELECT json_agg(a) FROM {self.name}.auction a"))
        groups = collections.defaultdict(list)
        for bid in bids:
            if self.joined:
                for auction in auctions:
                    if bid['auction'] is not None and auction['id'] == bid['auction']:
                        groups[auction['group_id']].append(bid)
            else:
                groups[bid['auction']].append(bid)
        oracle = {}
        for group, rows in groups.items():
            values = [row['price'] for row in rows if row['price'] is not None]
            filtered = [row['price'] for row in rows if row['price'] is not None and row['auction'] is None]
            key = temporal_key if self.temporal else None
            normalize = lambda value: value.replace(' ', 'T') if self.temporal and value not in (None, 'infinity', '-infinity') else value
            oracle[(group, normalize(min(values, key=key)) if values else None, normalize(max(values, key=key)) if values else None, normalize(min(filtered, key=key)) if filtered else None)] = 1
        actual = json.loads(self.sql(f"SELECT COALESCE(json_agg(g),'[]') FROM {self.bag_relation()} g"))
        assert {tuple(row['tuple'][1]): row['weight'] for row in actual} == oracle, (actual, oracle)
        assert self.acknowledgement_ahead().strip() == '0'


def qualify(output, command, oid, joined):
    fixture = OrderedFixture(output, oid, joined)
    with fixture.managed():
        fixture.rejected(command, 'unsupported', fixture.query+' ORDER BY g')
        assert fixture.sql(f"SELECT to_regclass('{fixture.name}.pgderive_worker_registration') IS NULL").strip() == 't'
        changes = [
            f'INSERT INTO {fixture.name}.bid VALUES(100,NULL,NULL),(101,1,{fixture.value(-7)}),(102,1,{fixture.value(-7)})',
            f'UPDATE {fixture.name}.auction SET group_id=NULL WHERE id=1; UPDATE {fixture.name}.bid SET price={fixture.value(103)} WHERE id=101',
            f'DELETE FROM {fixture.name}.bid WHERE id=102; UPDATE {fixture.name}.bid SET auction=2 WHERE id=101',
            f'UPDATE {fixture.name}.bid SET price=NULL WHERE auction=2',
            f'DELETE FROM {fixture.name}.bid WHERE auction=2; DELETE FROM {fixture.name}.bid WHERE id=100',
            f'INSERT INTO {fixture.name}.bid VALUES(103,2,NULL),(104,2,NULL)',
            f'UPDATE {fixture.name}.bid SET price={fixture.value(3)} WHERE id=104; UPDATE {fixture.name}.auction SET group_id=9 WHERE id=2',
        ]
        fixture.run_history(command, History(
            changes=changes,
            final=f'DELETE FROM {fixture.name}.bid WHERE id=104',
            normalized=fixture.query,
            rejected=[('changed-semantics', fixture.query.replace('MIN(b.price)', 'MAX(b.price)', 1))],
        ))
        (output/'result.json').write_text(json.dumps({'native_mv':'identical SQL exact bag','memory':'independent source-row extrema','restart':'cold','null_group':True,'all_null':True,'filter_empty':True,'ties':True,'simultaneous_inputs':joined,'type':oid,'transactions':len(changes)+1})+'\n')


if __name__ == '__main__':
    root, command = harness_arguments()
    for oid in ('smallint', 'integer', 'bigint', 'timestamp', 'timestamptz'):
        directory = root/oid
        directory.mkdir()
        qualify(directory, command, oid, oid == 'bigint')
