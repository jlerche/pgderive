#!/usr/bin/env python3
"""Sustained joined top-k qualification with exact native MV/raw-row oracles."""
import argparse
import collections
import json
import pathlib
import time
from native_mv_oracle import expected
from query_capacity_harness import inventory
from sql_compiler_harness import SqlFixture

class StorageFixture(SqlFixture):
    def __init__(self,output,rows):
        super().__init__(output,False)
        self.rows=rows
        self.sql(f'''INSERT INTO {self.name}.bid SELECT i,1,i%31 FROM generate_series(33,{rows}) i;
          UPDATE {self.name}.bid SET auction=CASE WHEN id%17=0 THEN NULL WHEN id%20=0 THEN 2+id%7 ELSE 1 END,
          price=CASE WHEN id%13=0 THEN NULL ELSE id%31 END''')
        joined=f'SELECT a.id AS aid,a.category,b.id AS bid,b.auction,b.price FROM {self.name}.auction a JOIN {self.name}.bid b ON a.id=b.auction'
        self.query=f'SELECT w.auction,w.price,w.category FROM (SELECT q.aid,q.bid,q.auction,q.price,q.category,ROW_NUMBER() OVER(PARTITION BY q.auction ORDER BY q.price DESC NULLS LAST,q.bid,q.aid) AS rn FROM ({joined}) q) w WHERE w.rn<=3'
        self.labels=['auction','price','category']
    def config(self,maximum=0):
        path=super().config(maximum)
        path.write_text(path.read_text().replace("[execution]","[execution]\nspill_runs=512\nscratch_bytes=134217728"))
        return path
    def verify(self):
        native=expected(self,self.labels)
        difference=self.bag_difference(native)
        assert difference.strip()=='0','sustained sink differs from exact native MV'
        bids=json.loads(self.sql(f"SELECT COALESCE(json_agg(b),'[]') FROM {self.name}.bid b"))
        auctions=json.loads(self.sql(f"SELECT json_agg(a) FROM {self.name}.auction a"))
        groups=collections.defaultdict(list)
        for bid in bids:
            for auction in auctions:
                if bid['auction'] is not None and bid['auction']==auction['id']:
                    groups[bid['auction']].append({**bid,'aid':auction['id'],'category':auction['category']})
        oracle=collections.Counter()
        for key,group in groups.items():
            ordered=sorted(group,key=lambda row:(row['price'] is None,0 if row['price'] is None else -row['price'],row['id'],row['aid']))
            for row in ordered[:3]:oracle[(key,row['price'],row['category'])]+=1
        actual=json.loads(self.sql(f"SELECT COALESCE(json_agg(g),'[]') FROM {self.bag_relation()} g"))
        assert {tuple(row['tuple'][1]):row['weight'] for row in actual}==dict(oracle),(actual,oracle)
        assert self.acknowledgement_ahead().strip()=='0'

def sample(fixture,worker,event,elapsed,ticks):
    status=pathlib.Path(f'/proc/{worker.process.pid}/status').read_text()
    rss=int(next(line.split()[1] for line in status.splitlines() if line.startswith('VmRSS:')))*1024
    catalog=json.loads(fixture.sql(f"SELECT json_build_object('roots',(SELECT count(*) FROM {fixture.name}.pgderive_objects),'ledger_rows',(SELECT count(*) FROM {fixture.name}.pgderive_protections),'active_uploads',(SELECT count(*) FROM {fixture.name}.pgderive_protections WHERE uploading AND active),'catalog_bytes',(SELECT pg_total_relation_size('{fixture.name}.pgderive_protections')) )"))
    return {'elapsed_seconds':elapsed,'source_transactions':ticks,'rss_bytes':rss,'cache':event.get('cache'),**catalog,'physical':inventory(fixture)}

def qualify(output,command,duration,transactions,rows):
    fixture=StorageFixture(output,rows)
    samples=[];latencies=[];restarts=[]
    try:
        worker=fixture.start(command,'first');ready=worker.event('ready');slot=ready['slot'];fixture.verify()
        started=time.monotonic();tick=0;next_sample=0
        while time.monotonic()-started<duration or tick<transactions:
            tick+=1
            ids=[1+(tick*7+offset)%rows for offset in range(8)]
            price='NULL' if tick%9==0 else str(tick%67-33)
            clauses=[f'UPDATE {fixture.name}.bid SET price={price},auction=CASE WHEN id%17=0 THEN NULL WHEN id%20=0 THEN 2+id%7 ELSE 1 END WHERE id IN({",".join(map(str,ids))})']
            if tick%11==0:
                identity=ids[0]
                clauses+=[f'DELETE FROM {fixture.name}.bid WHERE id={identity}',f'INSERT INTO {fixture.name}.bid VALUES({identity},2,{price})']
            if tick%100==0 or tick in (1,transactions):clauses+=[f"UPDATE {fixture.name}.auction SET category={'NULL' if tick%200==0 else chr(39)+'changed'+str(tick)+chr(39)} WHERE id=1"]
            sent=time.monotonic();fixture.sql('BEGIN;'+';'.join(clauses)+';COMMIT')
            published=worker.event('published',minimum_time=ready['time']+1)
            ready=published;latencies.append(time.monotonic()-sent);fixture.verify()
            elapsed=time.monotonic()-started
            if elapsed>=next_sample:
                value=sample(fixture,worker,published,elapsed,tick);samples.append(value)
                (output/'samples.json').write_text(json.dumps(samples,indent=2)+'\n')
                print(json.dumps({'elapsed_seconds':elapsed,'transactions':tick,'ledger_rows':value['ledger_rows'],'physical_objects':value['physical']['objects']}),flush=True)
                next_sample=elapsed+60
            if tick in (transactions//3,2*transactions//3):
                worker.finish(graceful=True);worker=fixture.start(command,f'restart-{tick}')
                reopened=worker.event('ready');assert reopened['slot']==slot and reopened['time']==ready['time'];fixture.verify();ready=reopened;restarts.append(tick)
            target=started+duration*min(tick,transactions)/transactions
            while time.monotonic()<target:time.sleep(min(1,target-time.monotonic()))
        worker.finish(graceful=True)
        final=inventory(fixture)
        ledger=int(fixture.sql(f'SELECT count(*) FROM {fixture.name}.pgderive_protections').strip())
        assert len(restarts)==2 and ledger<=128,(restarts,ledger)
        requests=[json.loads(line) for line in (output/'proxy.jsonl').read_text().splitlines() if line.startswith('{')]
        operations=[row for row in requests if 'status' in row and 'method' in row and not row.get('listing')]
        ordered=sorted(latencies)
        result={'duration_seconds':time.monotonic()-started,'source_transactions':tick,'source_rows':rows,'cold_restarts':restarts,'native_mv_and_independent_memory':True,'publication_latency_seconds':{'p50':ordered[len(ordered)//2],'p95':ordered[min(len(ordered)-1,int(len(ordered)*.95))],'max':max(ordered)},'final_ledger_rows':ledger,'final_physical_inventory':final,'samples':samples,'object_request_counts':{method:sum(row['method']==method for row in operations) for method in ['GET','PUT','DELETE','POST','HEAD']},'request_http_body_bytes':sum(row.get('request_body_bytes') or 0 for row in operations),'response_http_body_bytes':sum(row.get('response_body_bytes') or 0 for row in operations),'unaccounted_chunked_requests':sum(row.get('request_body_bytes') is None for row in operations),'scope':'owned local PostgreSQL/SeaweedFS/latency-proxy joined top-k fixture; interval samples can observe maintenance in flight; final inventory follows graceful stop; no universal capacity claim'}
        (output/'qualification.json').write_text(json.dumps(result,indent=2)+'\n');fixture.cleanup()
    except BaseException:fixture.preserve();raise

if __name__=='__main__':
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument('output',type=pathlib.Path);parser.add_argument('--duration',type=float,default=3600);parser.add_argument('--transactions',type=int,default=1000);parser.add_argument('--rows',type=int,default=10000);parser.add_argument('command',nargs=argparse.REMAINDER)
    args=parser.parse_args();assert args.duration>0 and args.transactions>=3 and args.rows>=32
    command=args.command
    if command and command[0]=='--':command=command[1:]
    qualify(args.output,command,args.duration,args.transactions,args.rows)
