#!/usr/bin/env python3
"""Scoped native Cartesian products against exact PostgreSQL MV and raw bags."""
from worker_harness import History
import collections
import json
import pathlib
import sys
from sql_compiler_harness import SqlFixture
from native_mv_oracle import expected as native_expected


class CrossFixture(SqlFixture):
    def __init__(self, output):
        super().__init__(output, False)
        self.pg.insert(1, '-q')
        self.query = f'''SELECT l.auction,r.group_id FROM
          (SELECT auction FROM {self.name}.bid WHERE price>=0 OR price IS NULL) l
          CROSS JOIN (SELECT group_id FROM {self.name}.auction WHERE category IS NOT NULL) r'''

    def verify(self):
        expected = native_expected(self, ['auction', 'group_id'])
        difference = self.bag_difference(expected)
        assert difference.strip() == '0', 'cross join differs from exact native MV'
        left = json.loads(self.sql(f"SELECT COALESCE(json_agg(b),'[]') FROM {self.name}.bid b"))
        right = json.loads(self.sql(f"SELECT COALESCE(json_agg(a),'[]') FROM {self.name}.auction a"))
        memory = collections.Counter((b['auction'], a['group_id']) for b in left for a in right
                                     if (b['price'] is None or b['price'] >= 0) and a['category'] is not None)
        actual = json.loads(self.sql(f"SELECT COALESCE(json_agg(g),'[]') FROM {self.name}.groups g"))
        assert {tuple(g['tuple'][1]): g['weight'] for g in actual} == dict(memory)
        assert self.acknowledgement_ahead().strip() == '0'


def qualify(output, command):
    fixture = CrossFixture(output)
    with fixture.managed():
        fixture.rejected(command, 'hidden-column', fixture.query.replace('l.auction,r.group_id', 'l.price,r.group_id'))
        assert fixture.sql(f"SELECT to_regclass('{fixture.name}.pgderive_worker_registration') IS NULL").strip() == 't'
        assert fixture.sql(f"SELECT count(*) FROM pg_replication_slots WHERE slot_name LIKE '{fixture.name}%'").strip() == '0'
        changes = [
            f"INSERT INTO {fixture.name}.bid VALUES(100,NULL,NULL),(101,NULL,7); UPDATE {fixture.name}.auction SET category='included',group_id=NULL WHERE id=1",
            f"DELETE FROM {fixture.name}.bid WHERE id=100; UPDATE {fixture.name}.auction SET category=NULL WHERE id=2",
            f"UPDATE {fixture.name}.bid SET id=102,auction=9,price=-1 WHERE id=101; UPDATE {fixture.name}.auction SET group_id=9 WHERE id=1",
            f"UPDATE {fixture.name}.bid SET price=NULL WHERE id=102; UPDATE {fixture.name}.auction SET group_id=NULL WHERE id=1",
            f"UPDATE {fixture.name}.auction SET category=NULL",
            f"UPDATE {fixture.name}.auction SET category='restored' WHERE id=1; INSERT INTO {fixture.name}.bid VALUES(103,NULL,NULL)",
        ]
        def initial():
            fixture.sql(f'BEGIN; DELETE FROM {fixture.name}.bid; ROLLBACK')
        fixture.run_history(command, History(
            changes=changes,
            final=f'DELETE FROM {fixture.name}.bid',
            normalized=fixture.query,
            rejected=[('changed-filter', fixture.query.replace('price>=0', 'price>=1'))],
        ), initial=initial)
        (output/'result.json').write_text(json.dumps({'postgres': 'exact native MV bag', 'memory': 'raw nested-loop bag', 'restart': 'cold same slot', 'simultaneous': 'both inputs; NULLs; collisions; retractions; empty input'})+'\n')


if __name__ == '__main__':
    command = sys.argv[2:]
    if command and command[0] == '--':
        command = command[1:]
    qualify(pathlib.Path(sys.argv[1]), command)
