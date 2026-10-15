#!/usr/bin/env python3
"""Owned DISTINCT/existence identical-MV/raw-row oracle qualification; run sequentially."""
from worker_harness import harness_arguments
import collections
import json
from native_mv_oracle import expected
from sql_compiler_harness import SqlFixture


class MembershipFixture(SqlFixture):
    def __init__(self, output, mode):
        super().__init__(output, False)
        self.mode = mode
        distinct = 'DISTINCT ' if mode.startswith('distinct') else ''
        anti = 'NOT ' if mode.endswith('anti') else ''
        clause = '' if mode == 'distinct' else f' WHERE {anti}EXISTS (SELECT 1 FROM {self.name}.bid b WHERE b.auction=a.id)'
        self.query = f'SELECT {distinct}a.category FROM {self.name}.auction a{clause}'
        self.labels = ['category']

    def memory(self):
        bids = json.loads(self.sql(f"SELECT COALESCE(json_agg(b),'[]') FROM {self.name}.bid b"))
        auctions = json.loads(self.sql(f"SELECT COALESCE(json_agg(a),'[]') FROM {self.name}.auction a"))
        bag = collections.Counter()
        for auction in auctions:
            present = any(bid['auction'] is not None and bid['auction'] == auction['id'] for bid in bids)
            if self.mode != 'distinct' and present == self.mode.endswith('anti'):
                continue
            bag[(auction['category'],)] += 1
        if self.mode.startswith('distinct'):
            bag = collections.Counter({key: 1 for key in bag})
        return bag

    def verify(self):
        native=expected(self,self.labels)
        assert self.bag_difference(native).strip()=='0','native MV mismatch'
        actual=json.loads(self.sql(f"SELECT COALESCE(json_agg(g),'[]') FROM {self.name}.groups g"))
        assert {tuple(row['tuple'][1]):row['weight'] for row in actual}==dict(self.memory())
        assert self.acknowledgement_ahead().strip()=='0'


def qualify(output,command,mode):
    fixture=MembershipFixture(output,mode)
    with fixture.managed():
        fixture.rejected(command,'unsupported',fixture.query+' ORDER BY 1')
        assert fixture.sql(f"SELECT to_regclass('{fixture.name}.pgderive_worker_registration') IS NULL").strip()=='t'
        worker=fixture.start(command,'first')
        ready=worker.event('ready')
        fixture.verify()
        name=fixture.name
        changes=[
            f'INSERT INTO {name}.bid VALUES(100,NULL,NULL),(101,1,7),(102,1,7)',
            f'UPDATE {name}.auction SET category=NULL WHERE id=1; UPDATE {name}.bid SET auction=1,price=NULL WHERE id=2',
            f'DELETE FROM {name}.bid WHERE auction=1',
            f"INSERT INTO {name}.bid VALUES(103,1,0),(104,NULL,-3); UPDATE {name}.auction SET category='new' WHERE id=1",
            f'UPDATE {name}.bid SET auction=2,price=-4 WHERE id=103; DELETE FROM {name}.bid WHERE id=100; UPDATE {name}.auction SET category=NULL WHERE id=2',
            f'DELETE FROM {name}.auction WHERE id=2; UPDATE {name}.bid SET auction=NULL WHERE id=104',
        ]
        for index,change in enumerate(changes,1):
            fixture.sql('BEGIN;'+change+';COMMIT')
            worker.event('published',minimum_time=ready['time']+index)
            fixture.verify()
        worker.abort()
        prior=fixture.sql(f'SELECT row_to_json(p) FROM {name}.pgderive_progress p')
        fixture.rejected(command,'changed',f'SELECT DISTINCT a.id FROM {name}.auction a')
        assert prior==fixture.sql(f'SELECT row_to_json(p) FROM {name}.pgderive_progress p')
        resumed=fixture.start(command,'resumed',maximum=1)
        reopened=resumed.event('ready')
        assert reopened['slot']==ready['slot'] and reopened['time']==ready['time']+len(changes)
        fixture.verify()
        fixture.sql(f'DELETE FROM {name}.bid WHERE id=103')
        resumed.event('published')
        resumed.finish()
        fixture.verify()
        (output/'result.json').write_text(json.dumps({'mode':mode,'native_mv':'identical SQL exact bag','memory':'independent nested-loop raw-row oracle','cold_restart':True,'simultaneous_changes':True,'transactions':len(changes)+1})+'\n')


if __name__=='__main__':
    root, command = harness_arguments()
    for mode in ['distinct', 'semi', 'anti', 'distinct-semi', 'distinct-anti']:
        directory=root/mode
        directory.mkdir()
        qualify(directory,command,mode)
