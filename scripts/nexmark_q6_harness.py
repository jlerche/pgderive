#!/usr/bin/env python3
"""q6 closed-auction ranking driven by a persisted PostgreSQL clock relation."""
from worker_harness import History
import collections
from decimal import Decimal
import json
import pathlib
import sys
from nexmark_auction_harness import AuctionFixture, ROOT, time_key
from native_mv_oracle import expected as native_expected
from numeric_compiler_harness import average


class Q6Fixture(AuctionFixture):
    def __init__(self, output, instant):
        super().__init__(output, 'q4', instant)
        temporal = 'timestamptz' if instant else 'timestamp'
        self.sql(f'''CREATE TABLE {self.name}.global_clock(id integer PRIMARY KEY,recorded_time {temporal});
          ALTER TABLE {self.name}.global_clock REPLICA IDENTITY FULL;
          ALTER PUBLICATION {self.name} ADD TABLE {self.name}.global_clock;
          INSERT INTO {self.name}.global_clock VALUES(0,'2000-01-01 00:00:30'),(1,'infinity');
          INSERT INTO {self.name}.auction(id,category,seller,date_time,expires)
          SELECT 400+n,1,7,'2000-01-01'::{temporal},'2000-01-01'::{temporal}+INTERVAL '1 second'*(n/2+10) FROM generate_series(0,13) n;
          INSERT INTO {self.name}.bid(id,auction,price,date_time)
          SELECT 400+n,400+n,CASE WHEN n=13 THEN NULL ELSE n*11 END,'2000-01-01'::{temporal} FROM generate_series(0,13) n''')
        self.query = (ROOT/'queries/nexmark/q6.sql').read_text().replace('nexmark.', self.name+'.').strip().removesuffix(';')

    def verify(self):
        prefix = "SET DateStyle='ISO,MDY'; SET TimeZone='UTC'; "
        expected = native_expected(self, ['seller','avg_price'])
        difference = self.bag_difference(expected, context=prefix)
        assert difference.strip() == '0', 'q6 differs from exact native MV'
        auctions = json.loads(self.sql(prefix+f"SELECT COALESCE(json_agg(a),'[]') FROM {self.name}.auction a"))
        bids = json.loads(self.sql(prefix+f"SELECT COALESCE(json_agg(b),'[]') FROM {self.name}.bid b"))
        clocks = json.loads(self.sql(prefix+f"SELECT COALESCE(json_agg(c),'[]') FROM {self.name}.global_clock c WHERE id=0"))
        sellers = collections.defaultdict(list)
        for a in auctions:
            if not clocks or any(v is None for v in (a['expires'],a['date_time'],clocks[0]['recorded_time'])):
                continue
            if time_key(a['expires']) > time_key(clocks[0]['recorded_time']):
                continue
            eligible = [b for b in bids if b['auction']==a['id'] and b['date_time'] is not None
                        and time_key(a['date_time'])<=time_key(b['date_time'])<=time_key(a['expires'])]
            if eligible:
                prices = [b['price'] for b in eligible if b['price'] is not None]
                sellers[a['seller']].append((time_key(a['expires']),a['id'],max(prices) if prices else None))
        memory = {}
        for seller, auctions in sellers.items():
            recent = sorted(auctions,key=lambda row:(row[0],row[1]),reverse=True)[:10]
            memory[(seller,average([row[2] for row in recent if row[2] is not None]))] = 1
        actual = json.loads(self.sql(f"SELECT COALESCE(json_agg(g),'[]') FROM {self.bag_relation()} g"),parse_float=Decimal)
        assert {tuple(g['tuple'][1]):g['weight'] for g in actual} == memory, 'q6 differs from independent closed-auction oracle'
        assert self.acknowledgement_ahead().strip()=='0'


def qualify(output, command, instant):
    fixture = Q6Fixture(output, instant)
    with fixture.managed():
        fixture.rejected(command,'missing-identity',fixture.query.replace(',closed.auction_id DESC',''))
        assert fixture.sql(f"SELECT to_regclass('{fixture.name}.pgderive_worker_registration') IS NULL").strip()=='t'
        assert fixture.sql(f"SELECT count(*) FROM pg_replication_slots WHERE slot_name LIKE '{fixture.name}%'").strip()=='0'
        changes = [
            f"UPDATE {fixture.name}.global_clock SET recorded_time='2000-01-01 00:00:12' WHERE id=0; UPDATE {fixture.name}.auction SET seller=NULL WHERE id=410; UPDATE {fixture.name}.bid SET price=999 WHERE id=400",
            f"UPDATE {fixture.name}.global_clock SET recorded_time='2000-01-01 00:00:30' WHERE id=0; INSERT INTO {fixture.name}.bid(id,auction,price,date_time) VALUES(500,413,9223372036854775807,'2000-01-01')",
            f"DELETE FROM {fixture.name}.global_clock WHERE id=0; UPDATE {fixture.name}.bid SET price=-9223372036854775808 WHERE id=411",
            f"INSERT INTO {fixture.name}.global_clock VALUES(0,'2000-01-01 00:00:30'); UPDATE {fixture.name}.auction SET id=450 WHERE id=400; UPDATE {fixture.name}.bid SET auction=450,id=550 WHERE id=400",
            f"UPDATE {fixture.name}.auction SET expires=NULL WHERE id=412; DELETE FROM {fixture.name}.bid WHERE id=500; UPDATE {fixture.name}.global_clock SET recorded_time=NULL WHERE id=0",
            f"UPDATE {fixture.name}.global_clock SET recorded_time='infinity' WHERE id=0; UPDATE {fixture.name}.bid SET price=NULL WHERE auction=410",
            f"UPDATE {fixture.name}.global_clock SET recorded_time='-infinity' WHERE id=1",
        ]
        def initial():
            fixture.sql(f'BEGIN; DELETE FROM {fixture.name}.global_clock; ROLLBACK')
        fixture.run_history(command, History(
            changes=changes,
            final=f'DELETE FROM {fixture.name}.global_clock WHERE id=0',
            normalized=fixture.query,
            rejected=[('changed-clock', fixture.query.replace('WHERE id=0', 'WHERE id=1')),('changed-order', fixture.query.replace('closed.auction_id DESC', 'closed.auction_id ASC'))],
        ), initial=initial)
        (output/'result.json').write_text(json.dumps({'query':'q6','instant':instant,'postgres':'exact native MV','memory':'independent eligibility/max/latest ten/exact average','clock':'ordinary revisable CDC; simultaneous three inputs; NULL/delete/reinsert/backward','restart':'cold same slot'})+'\n')


if __name__=='__main__':
    command = sys.argv[2:]
    if command and command[0]=='--':
        command = command[1:]
    for instant in (False,True):
        output = pathlib.Path(sys.argv[1])/('instant' if instant else 'local')
        output.mkdir()
        qualify(output,command,instant)
