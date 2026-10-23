"""Qualification must fail closed on resource, liveness and cleanup errors."""
import json
import pathlib
import signal
import sys
import tempfile
import threading
import types
import unittest
from unittest import mock

sys.path.insert(0,str(pathlib.Path(__file__).resolve().parent))
from soak import Sampler, SoakFixture, cleanup_owned, interrupted, pending_boundary, process_observation, validate_sample


class SoakTests(unittest.TestCase):
    def test_each_resource_budget_and_ack_failure_is_independent(self):
        args=types.SimpleNamespace(max_rss_bytes=100,max_wal_bytes=100,
                                  max_state_bytes=100,max_pending_changes=100,
                                  max_steady_p50_ms=100,max_steady_p95_ms=250)
        sample={'rss_bytes':0,'retained_wal_bytes':0,'journal_rows':0,
                'protection_rows':0,'pending_changes_after_marker':0,
                'ack_safe':True,'physical':{'bytes':0},'activities':['steady']}
        validate_sample(sample,args)
        for name,value in [('rss_bytes',101),('retained_wal_bytes',101),
                           ('journal_rows',4097),('protection_rows',1025),
                           ('pending_changes_after_marker',101),('ack_safe',False),
                           ('physical',{'bytes':101})]:
            with self.subTest(name=name),self.assertRaises(RuntimeError):
                validate_sample({**sample,name:value},args)
        missing={**sample,'rss_bytes':None,'process_metrics_available':False,'expected_downtime':True}
        validate_sample(missing,args)
        with self.assertRaises(RuntimeError):
            validate_sample({**missing,'expected_downtime':False},args)

    def test_steady_latency_budgets_preserve_fault_windows_explicitly(self):
        args=types.SimpleNamespace(max_rss_bytes=100,max_wal_bytes=100,max_state_bytes=100,
                                  max_pending_changes=100,max_steady_p50_ms=100,max_steady_p95_ms=250)
        sample={'rss_bytes':1,'retained_wal_bytes':1,'journal_rows':1,'protection_rows':1,
                'pending_changes_after_marker':1,'ack_safe':True,'physical':{'bytes':1},
                'activities':['steady'],'sink_visibility_ms':{'n':20,'p50':101,'p95':200}}
        with self.assertRaises(RuntimeError): validate_sample(sample,args)
        with self.assertRaises(RuntimeError):
            validate_sample({**sample,'sink_visibility_ms':{'n':20,'p50':99,'p95':251}},args)
        validate_sample({**sample,'activities':['steady','checkpoint-crash']},args)
        with self.assertRaises(RuntimeError):
            validate_sample({**sample,'activities':['checkpoint-crash'],'retained_wal_bytes':101},args)

    def test_expected_zombie_and_generation_change_do_not_fabricate_zero_rss(self):
        worker=mock.Mock(pid=123); worker.poll.return_value=None
        fixture=types.SimpleNamespace(worker=worker,generation=1,expected_downtime=threading.Event())
        fixture.expected_downtime.set()
        with mock.patch('soak.process_stats',side_effect=StopIteration):
            observed=process_observation(fixture)
        self.assertIsNone(observed['rss_bytes'])
        self.assertFalse(observed['process_metrics_available'])
        fixture.expected_downtime.clear()
        with mock.patch('soak.process_stats',side_effect=FileNotFoundError),self.assertRaises(ChildProcessError):
            process_observation(fixture)
        def replaced(_pid):
            fixture.worker=mock.Mock(pid=456)
            raise FileNotFoundError
        with mock.patch('soak.process_stats',side_effect=replaced):
            observed=process_observation(fixture)
        self.assertEqual(observed['worker_pid'],123)
        self.assertEqual(observed['worker_generation'],1)
        self.assertTrue(observed['expected_downtime'])

    def test_cleanup_attempts_all_owned_resources_after_actor_and_preservation_failures(self):
        with tempfile.TemporaryDirectory() as directory:
            root=pathlib.Path(directory)
            fixture=mock.Mock(directory=root/'candidate',worker=None)
            fixture.close.return_value=[]
            traffic,sampler=mock.Mock(),mock.Mock()
            traffic.finish.side_effect=RuntimeError('actor failure')
            with mock.patch('soak.preserve',side_effect=RuntimeError('archive failure')):
                errors=cleanup_owned(fixture,traffic,sampler,False)
            self.assertEqual(len(errors),2)
            sampler.finish.assert_called_once()
            fixture.close.assert_called_once_with(success=False)
            self.assertEqual(json.loads((root/'cleanup.json').read_text())['errors'],errors)

    def test_cleanup_failure_cannot_leave_a_passed_qualification(self):
        with tempfile.TemporaryDirectory() as directory:
            root=pathlib.Path(directory)
            (root/'qualification.json').write_text('{"passed":true}')
            fixture=mock.Mock(directory=root/'candidate')
            fixture.close.return_value=['container removal failed']
            errors=cleanup_owned(fixture,None,None,True)
            result=json.loads((root/'qualification.json').read_text())
            self.assertFalse(result['passed'])
            self.assertEqual(result['cleanup_errors'],errors)

    def test_pending_boundary_uses_one_consistent_sql_observation(self):
        with mock.patch('soak.boundary',return_value={'published_time':2,'checkpoint_time':1}) as read:
            self.assertIsNotNone(pending_boundary(object()))
            read.assert_called_once()
        with mock.patch('soak.boundary',return_value={'published_time':2,'checkpoint_time':2}):
            self.assertIsNone(pending_boundary(object()))

    def test_phase_tags_and_latency_batch_share_an_atomic_boundary(self):
        with tempfile.TemporaryDirectory() as directory:
            fixture=SoakFixture.__new__(SoakFixture)
            fixture.metrics_lock=threading.RLock()
            fixture.activity='steady';fixture.activities={'steady'};fixture.timings={}
            fixture.s3_name='owned';fixture.objects=mock.Mock(return_value={'bytes':0,'objects':0})
            began,finished=threading.Event(),threading.Event()
            def phase():
                began.set();fixture.mark_activity('native-check');fixture.mark_activity('steady');finished.set()
            changing=threading.Thread(target=phase)
            def latency_snapshot():
                changing.start();self.assertTrue(began.wait(1))
                self.assertFalse(finished.wait(.02))
                return {}
            traffic=mock.Mock();traffic.snapshot.side_effect=latency_snapshot
            args=types.SimpleNamespace(output=pathlib.Path(directory))
            sampler=Sampler(fixture,traffic,args,0)
            with mock.patch('soak.process_observation',return_value={}),mock.patch('soak.boundary',return_value={}), \
                 mock.patch('soak.docker',return_value='123 /data'),mock.patch('soak.validate_sample'):
                sampler.capture()
            changing.join(timeout=1)
            self.assertTrue(finished.is_set())
            self.assertEqual(sampler.samples[0]['activities'],['steady'])
            self.assertEqual(fixture.take_timings()['activities'],['native-check','steady'])

    def test_sigterm_enters_exception_cleanup_path(self):
        with self.assertRaises(InterruptedError): interrupted(signal.SIGTERM,None)
