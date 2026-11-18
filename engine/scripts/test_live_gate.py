"""Every suite runs once; concurrent lanes own distinct PostgreSQL servers."""
import importlib
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
from worker_case_shard import select_cases


class LiveGateTests(unittest.TestCase):
    def test_inventory_keeps_every_live_boundary_and_builds_explicit_commands(self):
        names=[name for name, _ in live_gate.SUITES]
        self.assertEqual(len(names),len(set(names)))
        self.assertEqual(set(names),set(LIVE_COUNTS)|{*live_gate.full_live_gate.REPLICATION,'tenant-storage','tenant-storage-commit','multi-query','query-rebuild'})
        binaries={'worker':'/worker','admin':'/admin'}
        for name in names:
            self.assertTrue(live_gate.command(name,pathlib.Path('/evidence'),binaries))
        self.assertIn('/admin',live_gate.command('query-rebuild',pathlib.Path('/evidence'),binaries))

    def test_standalone_replication_inventory_includes_full_error_corpus(self):
        from live_gate import full_live_gate

        cases = [(name, 0, 1) for name in full_live_gate.REPLICATION]
        self.assertEqual(live_gate.tasks(False, replication_only=True), cases)
        count = full_live_gate.ERROR_ORDER_SHARDS
        self.assertEqual(live_gate.tasks(False, full=True, replication_only=True),
                         cases + [('error-order', index, count) for index in range(count)])

    def test_release_mode_shards_cover_each_original_history_once(self):
        from live_gate import full_live_gate

        tasks = live_gate.tasks(True, True)
        for suite, count in (full_live_gate.MODE_SHARDS | full_live_gate.CASE_SHARDS).items():
            argv = full_live_gate.command(suite, pathlib.Path('/evidence'),
                                          {'worker': '/worker', 'admin': '/admin'}, '5432')
            self.assertEqual(argv[1], 'scripts/worker_case_shard.py')
            module = importlib.import_module(argv[2])
            cases = list(getattr(module, argv[3]))
            self.assertEqual(count, len(cases))
            shards = []
            for name, index, total in tasks:
                if name == suite:
                    with patch.dict(os.environ, {'PGDERIVE_TEST_CASE_SHARD': f'{index}/{total}'}):
                        shard = list(select_cases(cases))
                    self.assertEqual(len(shard), 1)
                    shards.extend(shard)
            self.assertCountEqual(shards, cases)

    def test_full_composition_runs_each_history_and_inventory_separately(self):
        from composition_matrix_model import FAMILIES

        tasks = live_gate.tasks(True, True)
        self.assertEqual(sum(name == 'composition-matrix' for name, _, _ in tasks),
                         10 * len(FAMILIES))
        self.assertEqual(sum(name == 'composition-inventory' for name, _, _ in tasks), 1)

    def test_application_shards_keep_whole_histories_and_one_rejection_inventory(self):
        import application_portfolio_harness as application

        tasks = [task for task in live_gate.tasks(True, True) if task[0] == 'application']
        self.assertEqual(len(tasks), 6)
        with patch.dict(os.environ, {'PGDERIVE_CHECK_PROFILE': 'full'}, clear=True), \
             patch('sys.argv', ['application', '/evidence', '--', '/worker']), \
             patch.object(application, 'run_case') as run_case:
            application.main()
            unsharded = run_case.call_args_list
            self.assertEqual(len(unsharded), 12)
            sharded = []
            for _, index, count in tasks:
                run_case.reset_mock()
                with patch.dict(os.environ, {'PGDERIVE_TEST_CASE_SHARD': f'{index}/{count}'}):
                    application.main()
                self.assertEqual(len(run_case.call_args_list), 2)
                sharded.extend(run_case.call_args_list)

        self.assertCountEqual(sharded, unsharded)
        self.assertEqual(sum(call.kwargs['check_inventory'] for call in sharded), 1)

    def test_parallelism_uses_distinct_servers_and_is_sequential_within_each(self):
        finished=[]; ports={}; lock=threading.Lock(); barrier=threading.Barrier(4)
        class Server:
            def __init__(self,output,label,**options):
                self.output=output;self.name=options['name'];self.port=output.name.removeprefix('lane-')
            def finish(self,success):finished.append((self.port,success))
        class Commands:
            def __init__(self, timeout):
                pass
            def run(self,argv,environment,log):
                port=environment['PGDERIVE_TEST_POSTGRES_PORT']
                with lock:
                    first=port not in ports
                    ports.setdefault(port,[]).append(str(log))
                if first:barrier.wait(timeout=5)
                name, _, shard = str(log.parent.relative_to(root)).partition('/')
                self.assert_environment(environment, name)
                if shard:
                    count = dict(live_gate.SUITES)[name]
                    assert environment['PGDERIVE_TEST_CASE_SHARD'] == f'{int(shard.split("-")[1])-1}/{count}'
                else:
                    assert 'PGDERIVE_TEST_CASE_SHARD' not in environment
            @staticmethod
            def assert_environment(environment,name):
                if name in live_gate.full_live_gate.REPLICATION:assert 'PGDERIVE__WORKER__RETRY_DELAY_MS' not in environment
                else:assert environment['PGDERIVE__WORKER__RETRY_DELAY_MS']=='1'
                assert 'PGDERIVE__POSTGRES__DATABASE' not in environment
            def cancel(self, *, kill_active):raise AssertionError('unexpected cancellation')
        with tempfile.TemporaryDirectory() as directory, patch.object(live_gate,'prepare'), patch.object(live_gate,'Server',Server), patch.object(live_gate,'Commands',Commands), patch.object(live_gate,'verify_worker'), patch.dict(os.environ,{'PGDERIVE_GATE_REPLICATION':'1','PGDERIVE__POSTGRES__DATABASE':'foreign', 'PGDERIVE_GATE_LANES':'4'}):
            root=pathlib.Path(directory)
            live_gate.run(root,{'worker':'/worker','admin':'/admin'})
            evidence=json.loads((root/'lanes.json').read_text())
        self.assertEqual(len(ports),4)
        expected = {name if count == 1 else f'{name}/shard-{shard+1}'
                    for name, shard, count in live_gate.tasks(True)}
        completed = [task for lane in evidence['groups'] for task in lane]
        self.assertEqual(len(completed), len(expected))
        self.assertEqual(set(completed), expected)
        self.assertEqual(set(evidence['elapsed_seconds']), expected)
        self.assertTrue(all(success for _,success in finished))
        self.assertTrue(evidence['passed'])

    def test_failed_lane_cancels_workers_and_preserves_servers(self):
        finished=[]; cancelled=[]; archived=[]
        class Server:
            def __init__(self,output,label,**options):self.output=output;self.name=options['name'];self.port=output.name.removeprefix('lane-')
            def finish(self,success):finished.append(success)
        class Commands:
            def __init__(self, timeout):
                pass
            def run(self,*args):raise RuntimeError('fault witness')
            def cancel(self, *, kill_active):cancelled.append(kill_active)
        with tempfile.TemporaryDirectory() as directory, patch.object(live_gate,'prepare'), patch.object(live_gate,'Server',Server), patch.object(live_gate,'Commands',Commands), patch.object(live_gate,'archive',side_effect=lambda server:archived.append(server.name)):
            with self.assertRaisesRegex(RuntimeError,'fault witness'):
                live_gate.run(pathlib.Path(directory),{'worker':'/worker','admin':'/admin'})
        self.assertEqual(cancelled, [True])
        self.assertTrue(finished)
        self.assertTrue(all(not success for success in finished))
        self.assertEqual(len(archived),len(finished))

    def test_full_failure_lets_active_harness_finish_before_archiving(self):
        import sys
        import time

        commands = live_gate.Commands(timeout=None)
        with tempfile.TemporaryDirectory() as directory:
            root = pathlib.Path(directory)
            started = root / 'started'
            finished = root / 'finished'
            release = root / 'release'
            script = (
                'from pathlib import Path; import time; '
                f'Path({str(started)!r}).touch(); '
                f'exec("while not Path({str(release)!r}).exists(): time.sleep(.01)"); '
                f'Path({str(finished)!r}).touch()'
            )
            from concurrent.futures import ThreadPoolExecutor
            with ThreadPoolExecutor() as pool:
                job = pool.submit(commands.run, [sys.executable, '-c', script],
                                  os.environ, root / 'peer.log')
                deadline = time.monotonic() + 5
                try:
                    while not started.exists() and time.monotonic() < deadline:
                        time.sleep(.01)
                    self.assertTrue(started.exists())
                    commands.cancel(kill_active=False)
                    with self.assertRaisesRegex(RuntimeError, 'cancelled'):
                        commands.run([sys.executable, '-c', 'pass'], os.environ, root / 'pending.log')
                finally:
                    release.touch()
                job.result(timeout=5)
            self.assertTrue(finished.exists())

    def test_full_release_uses_isolated_lanes_and_package_dependencies(self):
        from live_gate import full_live_gate

        outputs = {}
        barrier = threading.Barrier(2)
        packaged = threading.Event()
        verified = []

        class Server:
            def __init__(self, output, label, **options):
                self.output = output
                self.name = options['name']
                self.port = output.name.removeprefix('lane-')

            def finish(self, success):
                assert success

        class Commands:
            def __init__(self, timeout):
                pass
            def run(self, argv, environment, log):
                if log.name == 'package.log':
                    barrier.wait(timeout=5)
                    packaged.set()
                    return
                if log.parent.name == 'replication-stream':
                    barrier.wait(timeout=5)
                if log.name in ('container-image.log', 'query-container.log') or log.parent.name == 'query-capacity':
                    assert packaged.is_set()
                outputs[log] = (argv, environment)

            def cancel(self, *, kill_active):
                raise AssertionError('unexpected cancellation')

        with tempfile.TemporaryDirectory() as directory, \
                patch.object(live_gate, 'prepare'), patch.object(live_gate, 'Server', Server), \
                patch.object(live_gate, 'Commands', Commands), \
                patch.object(full_live_gate, 'verify', side_effect=lambda *args: verified.append(args)), \
                patch.dict(os.environ, {'PGDERIVE_GATE_REPLICATION': '1', 'PGDERIVE_GATE_LANES': '2'}):
            root = pathlib.Path(directory)
            binaries = {'worker': '/worker', 'admin': '/admin'}
            live_gate.run(root, binaries, full=True)
            evidence = json.loads((root/'lanes.json').read_text())

        tasks = [task for lane in evidence['groups'] for task in lane]
        self.assertEqual(len(tasks), len(live_gate.tasks(True, True)))
        self.assertEqual(len(set(tasks)), len(tasks))
        self.assertEqual({port for _, env in outputs.values()
                          if (port := env.get('PGDERIVE_TEST_POSTGRES_PORT'))}, {'0', '1'})
        self.assertEqual(verified, [(root, binaries, '0', True)])
        self.assertEqual(outputs[root/'replication-stream/console.log'][1]['PGDERIVE_CHECK_PROFILE'], 'full')
        self.assertIn('--skip-inventory', outputs[root/'composition-matrix/shard-1/console.log'][0])
        self.assertIn('--inventory-only', outputs[root/'composition-inventory/console.log'][0])
        self.assertIn('scripts/worker_case_shard.py', outputs[root/'subqueries/shard-1/console.log'][0])
        self.assertIn('stream', outputs[root/'replication-stream/console.log'][0])
        self.assertTrue(outputs[root/'error-order/shard-1/console.log'][1]['PGDERIVE_ERROR_ORDER_EVIDENCE'])
        self.assertIn('/admin', outputs[root/'query-recovery/console.log'][0])
        self.assertIn(str(root/'package/bin/pgderive'), outputs[root/'query-capacity/console.log'][0])
        self.assertEqual(outputs[root/'query-container.log'][0][-2:], ['--transactions', '13'])
        self.assertEqual(outputs[root/'query-storage-soak/console.log'][0][-9:],
                         ['--duration', '1', '--transactions', '6', '--rows', '256',
                          str(root/'query-storage-soak'), '--', '/worker'])
        self.assertTrue(all('PGDERIVE__POSTGRES__PORT' not in env for _, env in outputs.values()))
        self.assertTrue(all('PGDERIVE__WORKER__RETRY_DELAY_MS' not in env
                            for _, env in outputs.values()))

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
