from harness_control import start_recorded
#!/usr/bin/env python3
"""Deterministic sustained/skewed workload and supervised outage recovery."""
import json
import pathlib
import signal
import sys
import threading
import time
import urllib.parse
import urllib.request
import xml.etree.ElementTree as ET
from query_lifecycle_harness import configured, grouped
from query_health_harness import health
from query_rebuild_harness import command, verify
from worker_harness import Worker


class Samples:
    def __init__(self, worker, output):
        self.stop = threading.Event()
        self.values = []
        self.started = time.monotonic()
        self.worker = worker
        self.output = output
        self.thread = threading.Thread(target=self.sample,daemon=True)
        self.thread.start()

    def sample(self):
        while not self.stop.is_set():
            try:
                status = pathlib.Path(f'/proc/{self.worker.process.pid}/status').read_text()
                rss = int(next(line.split()[1] for line in status.splitlines() if line.startswith('VmRSS:')))*1024
                self.values.append({'elapsed_secs':time.monotonic()-self.started,'rss_bytes':rss})
            except (OSError,StopIteration):
                pass
            self.stop.wait(.1)

    def finish(self):
        self.stop.set(); self.thread.join(timeout=2)
        assert not self.thread.is_alive()
        self.output.write_text(json.dumps(self.values,indent=2)+'\n')
        return max((value['rss_bytes'] for value in self.values),default=0)


def inventory(fixture):
    endpoint = fixture.environment['PGDERIVE__OBJECT_STORE__ENDPOINT']
    prefix = fixture.name+'/'
    token = None
    seen = set()
    objects = []
    while True:
        query = {'list-type':'2','prefix':prefix,'max-keys':'1000'}
        if token is not None:
            query['continuation-token']=token
        with urllib.request.urlopen(endpoint+'/pgderive-mvp-tests?'+urllib.parse.urlencode(query),timeout=10) as response:
            data=response.read(16*1024*1024+1)
        assert len(data)<=16*1024*1024, 'owned-prefix inventory response too large'
        page=ET.fromstring(data)
        for item in page.findall('{*}Contents'):
            key=item.findtext('{*}Key')
            assert key and key.startswith(prefix) and key not in seen,key
            seen.add(key)
            objects.append({'key':key,'bytes':int(item.findtext('{*}Size'))})
        if page.findtext('{*}IsTruncated')!='true':
            break
        next_token=page.findtext('{*}NextContinuationToken')
        assert next_token and next_token!=token and len(objects)<100000
        token=next_token
    assert objects, 'owned object prefix unexpectedly empty'
    (fixture.output/'physical-object-inventory.json').write_text(json.dumps(objects,indent=2)+'\n')
    return {'objects':len(objects),'bytes':sum(item['bytes'] for item in objects),
            'scope':'all physical keys under owned prefix, including retained/unpublished objects'}


def qualify(output,binary):
    fixture = grouped(output)
    proxy = None
    samplers = []
    try:
        fixture.sql(f'''INSERT INTO {fixture.name}.auction SELECT i,'hot',true,NULL,i%4 FROM generate_series(9,64) i;
            INSERT INTO {fixture.name}.bid SELECT i,CASE WHEN i%20=0 THEN 1+i%64 ELSE 1 END,CASE WHEN i%5=0 THEN NULL ELSE i END FROM generate_series(33,10000) i;
            UPDATE {fixture.name}.bid SET auction=1 WHERE id%20<>0''')
        distribution = json.loads(fixture.sql(f"SELECT json_build_object('rows',count(*),'hot_rows',count(*) FILTER(WHERE auction=1),'null_prices',count(*) FILTER(WHERE price IS NULL)) FROM {fixture.name}.bid"))
        config = configured(fixture)
        text = config.read_text().replace('retry_attempts=8','retry_attempts=1').replace('[execution]','[execution]\nspill_runs=512')
        text = text.replace('[worker.query]','[worker.health]\nheartbeat_secs=1\nstale_secs=4\noperation_timeout_secs=5\n[worker.query]')
        config.write_text(text)
        proxy_log = output/'owned-storage-proxy.jsonl'
        proxy, ready = start_recorded([sys.executable, '-u', 'scripts/s3_latency_proxy.py',
                                       '--port', '0', '--scale', '0'], proxy_log)
        port = ready['port']
        fixture.environment['PGDERIVE__OBJECT_STORE__ENDPOINT']=f'http://127.0.0.1:{port}'
        labels = [column['name'] for column in command(fixture,binary,'validate',config)['columns']]
        def start(action,label):
            logs = output/label; logs.mkdir()
            worker = Worker(binary+['query',action],config,logs,fixture.environment)
            fixture.workers.append(worker)
            sampler = Samples(worker,logs/'rss.json'); samplers.append(sampler)
            return worker,sampler
        worker,sampler = start('create','sustained')
        ready = worker.event('ready')
        verify(fixture,labels)
        latencies = []
        began = time.monotonic()
        for ordinal in range(100):
            first = 1+(ordinal*32)%9900
            auction = f'UPDATE {fixture.name}.auction SET group_id={ordinal%7} WHERE id=1;' if ordinal%10==0 else ''
            started = time.monotonic()
            fixture.sql(f'BEGIN; {auction} UPDATE {fixture.name}.bid SET price=CASE WHEN id%5=0 THEN NULL ELSE price+1 END WHERE id BETWEEN {first} AND {first+31}; COMMIT')
            worker.event('published',minimum_time=ready['time']+ordinal+1)
            latencies.append(time.monotonic()-started)
            if ordinal%10==9:
                verify(fixture,labels)
        elapsed = time.monotonic()-began
        observed = health(fixture,binary,config)
        membership = json.loads(fixture.sql(f"SELECT json_build_object('objects',count(*),'bytes',coalesce(sum((reference->>'bytes')::bigint),0)) FROM {fixture.name}.pgderive_roots"))
        worker.finish(graceful=True)
        peak = sampler.finish()
        worker,sampler = start('restart','cold')
        worker.event('ready'); verify(fixture,labels)
        fixture.sql(f'UPDATE {fixture.name}.bid SET price=COALESCE(price,0)+1 WHERE id=1')
        worker.event('published', minimum_time=ready['time']+101)
        worker.event('maintenance')
        before = command(fixture,binary,'status',config)['progress']
        sink_sql = f"SELECT COALESCE(jsonb_agg(jsonb_build_array(tuple,weight) ORDER BY tuple),'[]') FROM {fixture.bag_relation()}"
        before_sink = fixture.sql(sink_sql)
        outage = time.monotonic(); proxy.send_signal(signal.SIGSTOP)
        fixture.sql(f'BEGIN; UPDATE {fixture.name}.auction SET group_id=19 WHERE id=1; UPDATE {fixture.name}.bid SET price=NULL WHERE id=2; COMMIT')
        worker.process.wait(timeout=30)
        assert worker.process.returncode != 0
        worker.abort(); peak=max(peak,sampler.finish())
        after = command(fixture,binary,'status',config)['progress']
        assert after['logical_time'] in (before['logical_time'],before['logical_time']+1)
        assert after['checkpoint_lsn']==before['checkpoint_lsn']
        assert after['checkpoint_time']==before['checkpoint_time']
        assert after['epoch']==before['epoch']
        if after['logical_time']==before['logical_time']:
            # A missing cold block can prevent preparation during an outage.
            assert after==before
            assert fixture.sql(sink_sql)==before_sink
        else:
            # Resident work may publish, but its replay WAL must remain retained.
            verify(fixture,labels)
        assert fixture.sql(f"SELECT confirmed_flush_lsn > '{before['checkpoint_lsn']}'::pg_lsn FROM pg_replication_slots WHERE slot_name='{ready['slot']}'").strip()=='f'
        bounded = text.replace('[worker.health]','startup_timeout_secs=5\n[worker.health]')
        config.write_text(bounded)
        retry,sampler = start('restart','supervisor-unavailable')
        retry.process.wait(timeout=20); assert retry.process.returncode != 0
        retry.abort(); peak=max(peak,sampler.finish())
        retried = command(fixture,binary,'status',config)['progress']
        assert {key:value for key,value in after.items() if key!='fence'} == {key:value for key,value in retried.items() if key!='fence'}
        assert fixture.sql(f"SELECT confirmed_flush_lsn > '{before['checkpoint_lsn']}'::pg_lsn FROM pg_replication_slots WHERE slot_name='{ready['slot']}'").strip()=='f'
        for ordinal in range(5):
            fixture.sql(f'UPDATE {fixture.name}.bid SET price={ordinal} WHERE id={ordinal+3}')
        while time.monotonic()-outage<60:
            time.sleep(max(0,min(1,60-(time.monotonic()-outage))))
        duration=time.monotonic()-outage
        proxy.send_signal(signal.SIGCONT); config.write_text(text)
        worker,sampler=start('restart','supervisor-recovered')
        worker.event('ready')
        worker.event('published',minimum_time=before['logical_time']+6)
        verify(fixture,labels); worker.finish(graceful=True)
        peak=max(peak,sampler.finish())
        physical_inventory=inventory(fixture)
        requests=[json.loads(line) for line in proxy_log.read_text().splitlines() if line.startswith('{')]
        assert peak>0, 'RSS sampler recorded no positive observation'
        (output/'qualification.json').write_text(json.dumps({'source_rows':10000,'auctions':64,'source_distribution':distribution,'hot_bid_fraction':distribution['hot_rows']/distribution['rows'],
            'source_transactions':100,'publication_latency_seconds':latencies,'workload_elapsed_seconds_including_oracle_checks':elapsed,
            'sampled_peak_rss_bytes':peak,'coarse_membership_roots':membership,'physical_prefix_inventory':physical_inventory,'health':observed,'outage_seconds':duration,
            'request_counts':{method:sum(row.get('method')==method for row in requests) for method in ['GET','PUT','POST','DELETE','HEAD']},
            'native_and_memory_equality':True,'cold_restart':True,'external_restart_policy_unavailable_and_recovered':True,
            'scope':'owned local fixture; measured envelope only'},indent=2)+'\n')
        command(fixture,binary,'drop',config)
        fixture.sql(f'DROP PUBLICATION {fixture.name}; DROP SCHEMA {fixture.name} CASCADE')
    except BaseException:
        fixture.preserve(); raise
    finally:
        for sampler in samplers: sampler.finish()
        if proxy is not None:
            proxy.send_signal(signal.SIGCONT); proxy.terminate(); proxy.wait(timeout=10)


if __name__=='__main__':
    output=pathlib.Path(sys.argv[1]); output.mkdir(parents=True,exist_ok=True)
    binary=sys.argv[2:]
    if binary and binary[0]=='--': binary=binary[1:]
    qualify(output,binary)
