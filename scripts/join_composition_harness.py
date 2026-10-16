#!/usr/bin/env python3
"""Join-fed scopes against PostgreSQL and independent weighted recomputation."""
from worker_harness import harness_arguments, History
import collections
import json
from sql_compiler_harness import SqlFixture


class JoinedFixture(SqlFixture):
    def __init__(self, output, mode):
        super().__init__(output, False)
        self.mode = mode
        self.sql(f"UPDATE {self.name}.auction SET group_id=NULL,enabled=true WHERE id IN (1,2); UPDATE {self.name}.bid SET price=-3 WHERE auction IN (1,2)")
        inner = (f'SELECT a.id AS aid,b.id AS bid,a.group_id AS g,b.price AS p FROM '
                 f'{self.name}.auction a JOIN {self.name}.bid b ON b.auction=a.id '
                 'WHERE a.enabled OR b.price IS NULL')
        if mode == 'group':
            self.query = f'SELECT q.g,COUNT(*) AS n,MAX(q.p) AS m,COUNT(DISTINCT q.aid) AS d FROM({inner}) q WHERE q.p IS NULL OR q.p<0 GROUP BY q.g'
            self.labels = ['g','n','m','d']
        elif mode == 'winner':
            window = f'SELECT q.aid,q.bid,q.g,q.p,row_number() OVER(PARTITION BY q.aid ORDER BY q.p DESC NULLS LAST,q.bid,q.aid) AS r FROM({inner}) q'
            self.query = f'SELECT w.g,w.p FROM({window}) w WHERE w.r<=1'
            self.labels = ['g','p']
        else:
            self.query = f'SELECT q.g,q.p FROM({inner}) q WHERE q.p IS NULL OR q.p<0'
            self.labels = ['g','p']

    def verify(self):
        values = ','.join(self.labels)
        expected = f'SELECT jsonb_build_array(NULL,jsonb_build_array({values})) tuple,count(*)::bigint weight FROM ({self.query}) q GROUP BY 1'
        difference = self.bag_difference(expected)
        assert difference.strip() == '0', 'join-fed scope differs from PostgreSQL'
        auctions = json.loads(self.sql(f"SELECT COALESCE(json_agg(a),'[]') FROM {self.name}.auction a"))
        bids = json.loads(self.sql(f"SELECT COALESCE(json_agg(b),'[]') FROM {self.name}.bid b"))
        groups = collections.defaultdict(list)
        memory = collections.Counter()
        for a in auctions:
            for b in bids:
                if b['auction'] is None or a['id'] != b['auction']:
                    continue
                if a['enabled'] is not True and b['price'] is not None:
                    continue
                if self.mode != 'winner' and b['price'] is not None and b['price']>=0:
                    continue
                groups[a['id'] if self.mode == 'winner' else a['group_id']].append((a,b))
        for key, rows in groups.items():
            if self.mode == 'group':
                prices = [b['price'] for a,b in rows if b['price'] is not None]
                memory[(key,len(rows),max(prices) if prices else None,len({a['id'] for a,b in rows}))] += 1
            elif self.mode == 'winner':
                a,b = min(rows,key=lambda pair:(pair[1]['price'] is None,-(pair[1]['price'] or 0),pair[1]['id'],pair[0]['id']))
                memory[(a['group_id'],b['price'])] += 1
            else:
                memory.update((a['group_id'],b['price']) for a,b in rows)
        actual = json.loads(self.sql(f"SELECT COALESCE(json_agg(g),'[]') FROM {self.bag_relation()} g"))
        assert {tuple(row['tuple'][1]):row['weight'] for row in actual} == dict(memory), 'join-fed scope differs from memory oracle'
        assert self.acknowledgement_ahead().strip() == '0'


def qualify(output, command, mode):
    fixture = JoinedFixture(output, mode)
    with fixture.managed():
        fixture.rejected(command, 'unsupported', fixture.query + ' ORDER BY g')
        fixture.rejected(command, 'hidden', fixture.query.replace('q.g','q.enabled'))
        fixture.rejected(command, 'terminal', fixture.query.replace('b.price AS p','abs(b.price) AS p'))
        if mode == 'winner':
            fixture.rejected(command,'right-key',fixture.query.replace(',q.bid,q.aid)',',q.aid)'))
            # Auction identity is already fixed within each partition.
            fixture.query = fixture.query.replace(',q.bid,q.aid)',',q.bid)')
        assert fixture.sql(f"SELECT to_regclass('{fixture.name}.pgderive_worker_registration') IS NULL").strip() == 't'
        assert fixture.sql(f"SELECT count(*) FROM pg_replication_slots WHERE slot_name LIKE '{fixture.name}%' ").strip() == '0'
        changes = [
            f"INSERT INTO {fixture.name}.auction(id,category,group_id,enabled) VALUES(100,'new',NULL,true); INSERT INTO {fixture.name}.bid VALUES(100,100,-7),(101,100,NULL),(102,NULL,4)",
            f"UPDATE {fixture.name}.auction SET enabled=NULL WHERE id=100; UPDATE {fixture.name}.bid SET price=9 WHERE id=100",
            f"UPDATE {fixture.name}.auction SET enabled=true,group_id=1 WHERE id=100; UPDATE {fixture.name}.bid SET auction=100,price=-2 WHERE id=2",
            f"DELETE FROM {fixture.name}.auction WHERE id=100; DELETE FROM {fixture.name}.bid WHERE id=101",
            f"INSERT INTO {fixture.name}.auction(id,category,group_id,enabled) VALUES(100,'again',NULL,false); UPDATE {fixture.name}.bid SET price=NULL WHERE id=100",
            f"DELETE FROM {fixture.name}.bid WHERE id=102",
        ]
        def initial():
            fixture.sql(f'BEGIN; UPDATE {fixture.name}.bid SET price=999; ROLLBACK')
        fixture.run_history(command, History(
            changes=changes,
            final=f'BEGIN; DELETE FROM {fixture.name}.auction; DELETE FROM {fixture.name}.bid; COMMIT',
            normalized=fixture.query.replace('b.', '"B".').replace('bid b ', 'bid AS "B" '),
            rejected=[('changed', fixture.query.replace('a.enabled OR', 'a.enabled AND'))],
        ), initial=initial)
        assert fixture.sql(f'SELECT count(*) FROM {fixture.bag_relation()}').strip() == '0'
        (output/'result.json').write_text(json.dumps({'mode':mode,'postgres':'exact bag','memory':'independent nested loop and group/order recomputation','simultaneous':'both sources insert/update/delete','restart':'same slot cold recovery','changed':'rejected without progress'})+'\n')


if __name__ == '__main__':
    output, command = harness_arguments()
    for mode in ('projection','group','winner'):
        directory = output/mode
        directory.mkdir()
        qualify(directory,command,mode)
