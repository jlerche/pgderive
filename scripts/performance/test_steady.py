"""Qualification rejects growth, resource and measurement regressions independently."""
import gzip
import json
import pathlib
import sys
import tempfile
import types
import unittest

sys.path.insert(0,str(pathlib.Path(__file__).resolve().parent))
from steady import assess


class SteadyTests(unittest.TestCase):
    def fixture(self, directory):
        events=[];commits=[];observed=[];requests=[];samples=[]
        for second in range(600):
            events.append({'event':'published','_observed_at':1000+second+.1,
                           'elapsed_ms':2,'changes':1000,
                           'phases_us':{'prepare_including_object_io':500,'publication':100}})
            commits.append({'commit':1000+second+.05,'source_changes':1000})
            observed.append({'commit':1000+second+.05,'observed':1000+second+.055})
            if second%5==0:
                events.append({'event':'maintenance','_observed_at':1000+second+.2})
                requests.append({'ordinal':second,'completed_monotonic':1000+second+.2,
                                 'method':'PUT','listing':False,'status':200,
                                 'request_body_bytes':128,'response_body_bytes':0})
            if second%15==0:
                samples.append({'seconds':second,'activities':['steady'],'worker_generation':0,
                    'ack_safe':True,'cpu_seconds':second*.01,'source_changes':second*1000,
                    'rss_bytes':100*1024**2,'source_rows':12000+second*333,'groups':1000+second*333,
                    'pending_changes_after_marker':0})
        self.write(directory,'events-0.jsonl',events)
        self.write(directory,'transactions.jsonl.gz',commits)
        self.write(directory,'observations.jsonl.gz',observed)
        self.write(directory,'proxy.jsonl',requests)
        return samples,types.SimpleNamespace(duration=600,rate=1000,warmup_seconds=120,qualification='growth')

    def write(self, root, name, records):
        opener=gzip.open if name.endswith('.gz') else open
        with opener(root/name,'wt') as stream:
            for row in records:stream.write(json.dumps(row)+'\n')

    def test_constant_cost_with_growing_state_passes_and_warmup_is_explicit(self):
        with tempfile.TemporaryDirectory() as temporary:
            root=pathlib.Path(temporary);samples,args=self.fixture(root)
            result=assess(root,samples,args,1000)
            self.assertTrue(result['passed'],result['issues'])
            self.assertGreater(result['source_row_growth'],2)
            self.assertEqual(result['parts'][0]['interval'],[1120,1270])
            args.warmup_seconds=119
            with self.assertRaises(ValueError):assess(root,samples,args,1000)

    def test_resource_rate_and_restart_failures_are_independent(self):
        for fault in ['rss','cpu','backlog','restart','ack','rate','cardinality','missing']:
            with self.subTest(fault=fault),tempfile.TemporaryDirectory() as temporary:
                root=pathlib.Path(temporary);samples,args=self.fixture(root)
                if fault=='cardinality':
                    for row in samples:row['source_rows']=12000
                for row in samples:
                    if row['seconds']<450:continue
                    if fault=='rss':row['rss_bytes']*=2
                    if fault=='cpu':row['cpu_seconds']*=4
                    if fault=='backlog':row['pending_changes_after_marker']=10000
                    if fault=='restart':row['worker_generation']=1
                    if fault=='ack':row['ack_safe']=False
                if fault=='missing':samples=[row for row in samples if row['seconds']<450]
                if fault=='rate':self.write(root,'transactions.jsonl.gz',[{'commit':1000+i+.05,'source_changes':500} for i in range(600)])
                self.assertFalse(assess(root,samples,args,1000)['passed'])

    def test_latency_preparation_and_request_regressions_fail(self):
        for fault in ['latency','preparation','missing-phase','negative-phase','requests','transport','unknown','no-checkpoints']:
            with self.subTest(fault=fault),tempfile.TemporaryDirectory() as temporary:
                root=pathlib.Path(temporary);samples,args=self.fixture(root)
                events=[json.loads(line) for line in (root/'events-0.jsonl').read_text().splitlines()]
                requests=[json.loads(line) for line in (root/'proxy.jsonl').read_text().splitlines()]
                for row in events:
                    if row['_observed_at']<1450:continue
                    if fault=='latency' and row['event']=='published':row['elapsed_ms']=100
                    if fault=='preparation' and row['event']=='published':row['phases_us']['prepare_including_object_io']*=10
                    if fault=='missing-phase' and row['event']=='published':row['phases_us'].pop('publication')
                    if fault=='negative-phase' and row['event']=='published':row['phases_us']['publication']=-1
                if fault=='no-checkpoints':events=[row for row in events if row['event']!='maintenance']
                for row in requests:
                    if row['completed_monotonic']<1450:continue
                    if fault=='requests':row['request_body_bytes']*=10
                    if fault=='transport':row['error']='injected'
                    if fault=='unknown':row['request_body_bytes']=None
                self.write(root,'events-0.jsonl',events);self.write(root,'proxy.jsonl',requests)
                self.assertFalse(assess(root,samples,args,1000)['passed'])

    def test_churn_requires_constant_cardinality(self):
        with tempfile.TemporaryDirectory() as temporary:
            root=pathlib.Path(temporary);samples,args=self.fixture(root);args.qualification='churn'
            self.assertFalse(assess(root,samples,args,1000)['passed'])
            for row in samples:row['source_rows']=12000
            self.assertTrue(assess(root,samples,args,1000)['passed'])
