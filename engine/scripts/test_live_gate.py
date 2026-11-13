"""Every suite runs once; concurrent lanes own distinct PostgreSQL servers."""
import json
import os
import pathlib
import tempfile
import threading
import unittest
from unittest.mock import patch
import live_gate
from gate_build import manifest, verify_protocol, PROTOCOL_TEST
from verify_worker_core import LIVE_COUNTS


class LiveGateTests(unittest.TestCase):
    def test_inventory_keeps_every_live_boundary_and_builds_explicit_commands(self):
        names=[name for group in live_gate.GROUPS for name in group]
        self.assertEqual(len(names),len(set(names)))
        self.assertEqual(set(names),set(LIVE_COUNTS)|{'replication','tenant-storage','tenant-storage-commit','multi-query','query-rebuild'})
        binaries={'worker':'/worker','admin':'/admin'}
        for name in names:
            self.assertTrue(live_gate.command(name,pathlib.Path('/evidence'),binaries))
        self.assertIn('/admin',live_gate.command('query-rebuild',pathlib.Path('/evidence'),binaries))

    def test_parallelism_uses_distinct_servers_and_is_sequential_within_each(self):
        finished=[]; ports={}; lock=threading.Lock(); barrier=threading.Barrier(4)
        class Server:
            def __init__(self,output,label,**options):
                self.output=output;self.name=options['name'];self.port=output.name[-1]
            def finish(self,success):finished.append((self.port,success))
        class Commands:
            def run(self,argv,environment,log):
                port=environment['PGDERIVE_TEST_POSTGRES_PORT']
                with lock:
                    first=port not in ports
                    ports.setdefault(port,[]).append(str(log))
                if first:barrier.wait(timeout=5)
                self.assert_environment(environment,log.parent.name)
            @staticmethod
            def assert_environment(environment,name):
                if name=='replication':assert 'PGDERIVE__WORKER__RETRY_DELAY_MS' not in environment
                else:assert environment['PGDERIVE__WORKER__RETRY_DELAY_MS']=='1'
                assert 'PGDERIVE__POSTGRES__DATABASE' not in environment
            def cancel(self):raise AssertionError('unexpected cancellation')
        with tempfile.TemporaryDirectory() as directory, patch.object(live_gate,'prepare'), patch.object(live_gate,'Server',Server), patch.object(live_gate,'Commands',Commands), patch.object(live_gate,'verify_worker'), patch.dict(os.environ,{'PGDERIVE_GATE_REPLICATION':'1','PGDERIVE__POSTGRES__DATABASE':'foreign'}):
            root=pathlib.Path(directory)
            live_gate.run(root,{'worker':'/worker','admin':'/admin'})
            evidence=json.loads((root/'lanes.json').read_text())
        self.assertEqual(len(ports),4)
        self.assertEqual(sorted(len(v) for v in ports.values()),sorted(map(len,live_gate.GROUPS)))
        self.assertTrue(all(success for _,success in finished))
        self.assertTrue(evidence['passed'])

    def test_failed_lane_cancels_workers_and_preserves_servers(self):
        finished=[]; cancelled=[]; archived=[]
        class Server:
            def __init__(self,output,label,**options):self.output=output;self.name=options['name'];self.port=output.name[-1]
            def finish(self,success):finished.append(success)
        class Commands:
            def run(self,*args):raise RuntimeError('fault witness')
            def cancel(self):cancelled.append(True)
        with tempfile.TemporaryDirectory() as directory, patch.object(live_gate,'prepare'), patch.object(live_gate,'Server',Server), patch.object(live_gate,'Commands',Commands), patch.object(live_gate,'archive',side_effect=lambda server:archived.append(server.name)):
            with self.assertRaisesRegex(RuntimeError,'fault witness'):
                live_gate.run(pathlib.Path(directory),{'worker':'/worker','admin':'/admin'})
        self.assertTrue(cancelled)
        self.assertTrue(finished)
        self.assertTrue(all(not success for success in finished))
        self.assertEqual(len(archived),len(finished))

    def test_build_manifest_rejects_missing_or_ambiguous_artifacts(self):
        with tempfile.TemporaryDirectory() as directory:
            path=pathlib.Path(directory)/'binary';path.touch()
            def artifact(name,kind,test=False):
                return {'reason':'compiler-artifact','executable':str(path),'target':{'name':name,'kind':[kind]},'profile':{'test':test}}
            records=[artifact('pgderive','lib',True),*[artifact(name,'bin') for name in ('pgderive','pgderive_worker','replication_harness')]]
            self.assertEqual(set(manifest(records)),{'admin','worker','replication','protocol_tests'})
            for invalid in (records[:-1],records+records[:1]):
                with self.assertRaises(ValueError):manifest(invalid)
            path.unlink()
            with self.assertRaises(ValueError):manifest(records)

    def test_zero_filtered_protocol_tests_cannot_pass_the_gate(self):
        verify_protocol(f'test {PROTOCOL_TEST} ... fixture log\nok\ntest result: ok. 1 passed; 0 failed; 0 ignored;')
        for output in ('test result: ok. 0 passed; 0 failed;',
                       'test other ... ok\ntest result: ok. 1 passed; 0 failed;',
                       f'test {PROTOCOL_TEST} ... FAILED\ntest result: FAILED. 0 passed; 1 failed;'):
            with self.assertRaises(ValueError):verify_protocol(output)
