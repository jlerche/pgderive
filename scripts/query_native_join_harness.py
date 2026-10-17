#!/usr/bin/env python3
"""Owned native join-tree MV/raw-row oracle qualification; run sequentially."""
from worker_harness import harness_arguments, History
import collections
import json
from native_mv_oracle import expected
from sql_compiler_harness import SqlFixture


class JoinFixture(SqlFixture):
    def __init__(self,output,mode):
        super().__init__(output,False)
        self.mode=mode
        a=f'{self.name}.auction'
        b=f'{self.name}.bid'
        queries={
            'derived-left':(f'SELECT l.auction AS key,l.n,r.category FROM (SELECT auction,COUNT(*) AS n FROM {b} GROUP BY auction) l LEFT JOIN {a} r ON l.auction=r.id',['key','n','category']),
            'native-derived-left':(f'SELECT l.id AS aid,r.n FROM {a} l LEFT JOIN (SELECT auction,COUNT(*) AS n FROM {b} GROUP BY auction) r ON l.id=r.auction',['aid','n']),
            'left':(f'SELECT a.id AS aid,a.category,b.price FROM {a} a LEFT JOIN {b} b ON a.id=b.auction WHERE b.price IS NULL OR b.price>=0',['aid','category','price']),
            'compound':(f'SELECT a.id AS aid,b.id AS bid,b.price FROM {a} a LEFT JOIN {b} b ON a.id=b.auction AND a.id=b.id',['aid','bid','price']),
            'self':(f'SELECT l.id AS lid,r.id AS rid FROM {b} l JOIN {b} r ON l.auction=r.auction',['lid','rid']),
            'self-left':(f'SELECT l.id AS lid,r.id AS rid FROM {b} l LEFT JOIN {b} r ON l.auction=r.auction',['lid','rid']),
            'multiway':(f'SELECT l.id AS lid,r.id AS rid,a.category FROM {b} l JOIN {b} r ON l.auction=r.auction JOIN {a} a ON r.auction=a.id',['lid','rid','category']),
            'nested-left':(f'SELECT a.id AS aid,l.id AS lid,r.id AS rid FROM {a} a LEFT JOIN {b} l ON a.id=l.auction LEFT JOIN {b} r ON l.auction=r.auction',['aid','lid','rid']),
        }
        self.query,self.labels=queries[mode]

    def memory(self):
        bids=json.loads(self.sql(f"SELECT COALESCE(json_agg(b),'[]') FROM {self.name}.bid b"))
        auctions=json.loads(self.sql(f"SELECT COALESCE(json_agg(a),'[]') FROM {self.name}.auction a"))
        bag=collections.Counter()
        equal=lambda left,right:left is not None and right is not None and left==right
        if self.mode=='derived-left':
            groups=collections.Counter(b['auction'] for b in bids)
            for key,count in groups.items():
                matches=[a for a in auctions if equal(key,a['id'])]
                for a in matches or [None]:
                    bag[(key,count,a['category'] if a else None)]+=1
        elif self.mode=='native-derived-left':
            groups=collections.Counter(b['auction'] for b in bids)
            for a in auctions:
                bag[(a['id'],groups.get(a['id']))]+=1
        elif self.mode in ('left','compound','nested-left'):
            for a in auctions:
                matches=[b for b in bids if equal(a['id'],b['auction']) and (self.mode!='compound' or equal(a['id'],b['id']))]
                for b in matches or [None]:
                    if self.mode=='compound':
                        bag[(a['id'],b['id'] if b else None,b['price'] if b else None)]+=1
                    elif self.mode=='left':
                        price=b['price'] if b else None
                        if price is None or price>=0:
                            bag[(a['id'],a['category'],price)]+=1
                    else:
                        rights=[r for r in bids if b and equal(b['auction'],r['auction'])]
                        for r in rights or [None]:
                            bag[(a['id'],b['id'] if b else None,r['id'] if r else None)]+=1
        else:
            for left in bids:
                rights=[right for right in bids if equal(left['auction'],right['auction'])]
                for right in rights or ([None] if self.mode=='self-left' else []):
                    if self.mode!='multiway':
                        bag[(left['id'],right['id'] if right else None)]+=1
                    else:
                        for a in auctions:
                            if equal(right['auction'],a['id']):
                                bag[(left['id'],right['id'],a['category'])]+=1
        return bag

    def verify(self):
        native=expected(self,self.labels)
        assert self.bag_difference(native).strip()=='0','native MV mismatch'
        actual=json.loads(self.sql(f"SELECT COALESCE(json_agg(g),'[]') FROM {self.bag_relation()} g"))
        assert {tuple(row['tuple'][1]):row['weight'] for row in actual}==dict(self.memory())
        assert self.acknowledgement_ahead().strip()=='0'


def qualify(output,command,mode):
    fixture=JoinFixture(output,mode)
    with fixture.managed():
        fixture.rejected(command,'unsupported',fixture.query+' ORDER BY 1')
        assert fixture.sql(f"SELECT to_regclass('{fixture.name}.pgderive_worker_registration') IS NULL").strip()=='t'
        name = fixture.name
        changes=[
            f'INSERT INTO {name}.bid VALUES(100,NULL,NULL),(101,1,7),(102,1,7)',
            f'UPDATE {name}.auction SET category=NULL WHERE id=1; UPDATE {name}.bid SET auction=1,price=NULL WHERE id=2',
            f'DELETE FROM {name}.bid WHERE auction=1',
            f"INSERT INTO {name}.bid VALUES(103,1,0),(104,NULL,-3); UPDATE {name}.auction SET category='new' WHERE id=1",
            f'UPDATE {name}.bid SET auction=2,price=-4 WHERE id=103; DELETE FROM {name}.bid WHERE id=100; UPDATE {name}.auction SET category=NULL WHERE id=2',
            f'DELETE FROM {name}.auction WHERE id=2; UPDATE {name}.bid SET auction=NULL WHERE id=104',
        ]
        fixture.run_history(command, History(
            changes=changes,
            final=f'DELETE FROM {name}.bid WHERE id=103',
            normalized=fixture.query,
            rejected=[('changed', fixture.query + ' WHERE false' if ' WHERE ' not in fixture.query else fixture.query.replace('b.price>=0', 'b.price>0'))],
        ))
        (output/'result.json').write_text(json.dumps({'mode':mode,'native_mv':'identical SQL exact bag','memory':'independent nested-loop raw-row oracle','cold_restart':True,'simultaneous_changes':True,'transactions':len(changes)+1})+'\n')


if __name__=='__main__':
    root, command = harness_arguments()
    for mode in ['left','compound','self','self-left','multiway','nested-left','derived-left','native-derived-left']:
        directory=root/mode
        directory.mkdir()
        qualify(directory,command,mode)
