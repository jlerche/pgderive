#!/usr/bin/env python3
"""Sustained growing state with live ingestion across witnessed worker crashes."""
import argparse
from collections import deque
import gzip
import json
import math
import os
import pathlib
import signal
import shutil
import subprocess
import sys
import threading
import time
import uuid

from common import ROOT, QUERY, distribution, docker, process_stats, save, sha256, wait
from fixture import Fixture
from request_metrics import object_requests
from workload import execute_transaction, transaction_size


class SoakFixture(Fixture):
    def __init__(self, name, directory, args, binary):
        super().__init__(name, directory, args, binary)
        self.generation = 0
        self.expected_downtime = threading.Event()
        self.metrics_lock = threading.RLock()
        self.timings = {'processing_ms': [], 'received_to_sink_ms': [], 'checkpoint_wall_ms': []}
        self.latest = {}
        self.activity = 'initialization'
        self.activities = {'initialization'}
        self.gate = directory / 'checkpoint.release'

    def container(self, suffix, image, arguments, command):
        arguments = list(arguments)
        if suffix == 'pg':
            arguments[arguments.index('--tmpfs') + 1] = '/var/lib/postgresql/data:rw,size=6g'
        if suffix == 's3':
            arguments[arguments.index('--tmpfs') + 1] = '/data:rw,size=8g'
            arguments[arguments.index('--memory') + 1] = '10g'
        return super().container(suffix, image, arguments, command)

    def consume(self, process, filename, first=''):
        if filename != 'events.jsonl':
            return super().consume(process, filename, first)
        stream = (self.directory / f'events-{self.generation}.jsonl').open('w')
        def reader():
            try:
                for line in process.stdout:
                    event = json.loads(line)
                    event['_observed_at'] = time.monotonic()
                    stream.write(json.dumps(event) + '\n'); stream.flush()
                    self.events.append({'event': event.get('event'), 'time': event.get('time')})
                    with self.metrics_lock:
                        if event.get('event') == 'published':
                            self.timings['processing_ms'].append(event['elapsed_ms'])
                            self.latest = event
                        if event.get('event') == 'epoch':
                            self.timings['received_to_sink_ms'].append(event['received_to_sink_ms'])
                        if event.get('event') == 'maintenance':
                            self.timings['checkpoint_wall_ms'].append(event['elapsed_us'] / 1000)
            except BaseException as error:
                self.error = 'engine log reader: ' + repr(error)
            finally:
                stream.close()
        self.engine_reader = threading.Thread(target=reader, daemon=True)
        self.engine_reader.start(); self.readers.append(self.engine_reader)

    def start_pgderive(self):
        stderr = (self.directory / 'gate-proxy.stderr').open('w'); self.files.append(stderr)
        process = subprocess.Popen([sys.executable, '-u', str(ROOT/'scripts/s3_latency_proxy.py'),
            '--port', '0', '--scale', '0', '--gate', str(self.gate),
            '--upstream', f'http://127.0.0.1:{self.proxy_port}'],
            stdout=subprocess.PIPE, stderr=stderr, text=True)
        self.processes.append(process)
        line = process.stdout.readline()
        self.proxy_port = json.loads(line)['port']
        self.consume(process, 'gate-proxy.jsonl', line)
        super().start_pgderive()

    def restart(self):
        self.generation += 1
        first_event = len(self.events)
        stderr = (self.directory/f'engine-{self.generation}.stderr').open('w')
        self.files.append(stderr)
        self.worker = subprocess.Popen([str(self.binary), 'query', 'restart', str(self.directory/'pgderive.toml')],
            stdout=subprocess.PIPE, stderr=stderr, text=True,
            env={**{key:value for key,value in os.environ.items() if not key.startswith('PGDERIVE__')},
                 'PGDERIVE__POSTGRES__PASSWORD':'local', 'TMPDIR':str(self.directory/'scratch')})
        self.processes.append(self.worker); self.pid = self.worker.pid
        self.consume(self.worker, 'events.jsonl')
        def ready():
            if self.worker.poll() is not None:
                raise ChildProcessError('Restart failed; inspect engine stderr')
            return any(event['event'] == 'ready' for event in self.events[first_event:])
        wait(ready)

    def mark_activity(self, activity):
        with self.metrics_lock:
            self.activity=activity
            self.activities.add(activity)

    def take_timings(self):
        with self.metrics_lock:
            result = {key:distribution(value) for key,value in self.timings.items()}
            result['activities']=sorted(self.activities)
            self.activities={self.activity}
            for value in self.timings.values(): value.clear()
            return result


class Traffic:
    def __init__(self, fixture, args, started):
        self.fixture, self.args, self.started = fixture, args, started
        self.stop, self.done, self.pause, self.paused = [threading.Event() for _ in range(4)]
        self.lock = threading.Lock()
        self.pending = deque()
        self.rows, self.commits, self.visible = 0, 0, 0
        self.paused_seconds = 0
        self.latencies, self.errors = [], []
        self.threads = []

    def start(self):
        for target in [self.produce, self.observe]:
            thread = threading.Thread(target=self.guard, args=(target,), daemon=True)
            self.threads.append(thread); thread.start()

    def guard(self, target):
        try: target()
        except BaseException as error:
            self.errors.append(repr(error)); self.stop.set()

    def produce(self):
        pacing_start = self.started
        end = self.started + self.args.duration
        try:
            with self.fixture.connect() as connection, gzip.open(self.fixture.directory/'transactions.jsonl.gz', 'wt') as log:
                connection.execute("SET statement_timeout='15s'; SET transaction_timeout='15s'")
                while time.monotonic() < end and not self.stop.is_set():
                    if self.pause.is_set():
                        began = time.monotonic(); self.paused.set()
                        while self.pause.is_set() and not self.stop.wait(.02): pass
                        elapsed = time.monotonic() - began
                        pacing_start += elapsed
                        self.paused_seconds += max(0, min(end,time.monotonic())-min(end,began))
                        self.paused.clear()
                    if self.stop.is_set() or time.monotonic() >= end: break
                    if self.stop.wait(max(0, pacing_start + self.rows/self.args.rate - time.monotonic())): break
                    if time.monotonic() >= end: break
                    size = transaction_size(self.commits, 1000, self.args.transaction_mix)
                    record = execute_transaction(connection, self.args, self.rows+1, size)
                    with self.lock:
                        self.rows += size; self.commits += 1
                        if record['marker']:
                            self.pending.append((record['marker_value'], record['commit'], self.rows))
                    log.write(json.dumps(record)+'\n')
        finally:
            self.done.set(); self.paused.set()

    def observe(self):
        with self.fixture.connect() as connection:
            while not self.stop.is_set():
                value = connection.execute('SELECT units FROM pgd.groups WHERE book_id=1').fetchone()[0]
                after = time.monotonic()
                with self.lock:
                    while self.pending and self.pending[0][0] <= value:
                        _, commit, self.visible = self.pending.popleft()
                        self.latencies.append(max(0,(after-commit)*1000))
                self.stop.wait(.01)

    def snapshot(self):
        with self.lock:
            result = {'source_changes':self.rows,'source_transactions':self.commits,
                      'visible_marker_prefix_changes':self.visible,
                      'pending_changes_after_marker':self.rows-self.visible,
                      'sink_visibility_ms':distribution(self.latencies)}
            self.latencies.clear()
            return result

    def pending_count(self):
        with self.lock: return len(self.pending)

    def check(self):
        if self.errors or self.fixture.error:
            raise RuntimeError(self.errors or self.fixture.error)
        if self.fixture.worker.poll() is not None:
            raise ChildProcessError('Worker exited unexpectedly')

    def finish(self):
        self.stop.set(); self.pause.clear()
        for thread in self.threads: thread.join(timeout=20)
        if any(thread.is_alive() for thread in self.threads):
            raise TimeoutError('Traffic thread failed to stop')


def boundary(fixture):
    with fixture.connect() as connection:
        row = connection.execute('''SELECT q.logical_time,p.published_time,
            s.confirmed_flush_lsn<=p.checkpoint_lsn AND p.checkpoint_lsn<=p.end_lsn,
            pg_wal_lsn_diff(pg_current_wal_lsn(),s.restart_lsn)::bigint,
            (SELECT count(*) FROM pgd.pgderive_epoch_boundaries),
            (SELECT count(*) FROM pgd.pgderive_protections),
            (SELECT count(*) FROM pgd.pgderive_roots),s.slot_name,
            s.confirmed_flush_lsn::text,p.checkpoint_lsn::text,p.end_lsn::text
            FROM pgd.pgderive_queries q JOIN pgd.pgderive_progress p USING(query_id)
            JOIN pgd.pgderive_worker_registration r USING(query_id)
            JOIN pg_replication_slots s ON s.slot_name=r.slot_name''').fetchone()
    return dict(zip(['checkpoint_time','published_time','ack_safe','retained_wal_bytes',
                     'journal_rows','protection_rows','roots','slot','ack_lsn','checkpoint_lsn','end_lsn'],row))


def pending_boundary(fixture):
    value = boundary(fixture)
    return value if value['published_time'] > value['checkpoint_time'] else None


def native_equal(fixture):
    with fixture.connect() as connection:
        connection.execute("SET statement_timeout='120s'")
        return connection.execute('SELECT NOT EXISTS((('+QUERY+') EXCEPT ALL SELECT * FROM pgd.groups) '
            'UNION ALL ((SELECT * FROM pgd.groups) EXCEPT ALL ('+QUERY+')))').fetchone()[0]


def stable_audit(fixture):
    with fixture.connect() as connection:
        return connection.execute('''SELECT md5(string_agg(book_id::text||':'||ctid::text||':'||xmin::text,
            ',' ORDER BY book_id)) FROM pgd.groups WHERE book_id BETWEEN 2 AND %s''',
            (fixture.args.groups+1,)).fetchone()[0]


def verify(fixture, traffic):
    fixture.mark_activity('native-check')
    traffic.pause.set()
    try:
        wait(traffic.paused.is_set, 20)
        wait(lambda:native_equal(fixture), 180)
        wait(lambda:traffic.pending_count()==0,30)
        value = boundary(fixture)
        if not value['ack_safe']: raise AssertionError(value)
        with fixture.connect() as connection:
            rows,groups = connection.execute('SELECT count(*),count(DISTINCT book_id) FROM positions').fetchone()
        return {'native_equal':True,'source_rows':rows,'groups':groups,**value}
    finally:
        traffic.pause.clear()
        fixture.mark_activity('steady')


def crash(fixture, traffic, upload):
    fixture.mark_activity('checkpoint-crash' if upload else 'ingestion-crash')
    traffic.check()
    if traffic.rows <= 3*(fixture.args.groups+1):
        raise RuntimeError('Stable audit keys still overlap the growing producer')
    if traffic.done.is_set(): raise RuntimeError('Crash must occur during active ingestion')
    before = wait(lambda:pending_boundary(fixture),30)
    held = None
    if upload:
        fixture.gate.unlink(missing_ok=True)
        pathlib.Path(str(fixture.gate)+'.pending').unlink(missing_ok=True)
        pathlib.Path(str(fixture.gate)+'.arm').write_text('PUT')
        wait(lambda:pathlib.Path(str(fixture.gate)+'.pending').exists(), 60)
        held = boundary(fixture)
        wait(lambda:boundary(fixture)['published_time'] > held['published_time'], 30)
        advanced = boundary(fixture)
        if advanced['checkpoint_time'] != held['checkpoint_time'] or not advanced['ack_safe']:
            raise AssertionError('Held checkpoint advanced unexpectedly')
    audit = stable_audit(fixture)
    rows = traffic.rows
    fixture.expected_downtime.set()
    fixture.worker.kill(); fixture.worker.wait(timeout=10)
    fixture.engine_reader.join(timeout=10)
    fixture.gate.touch()
    started = time.monotonic()
    # Capture only after wait() confirmed death. Earlier commits cannot
    # satisfy this downtime witness.
    down_baseline = traffic.rows
    wait(lambda:traffic.rows > down_baseline,30)
    while_down = traffic.rows
    fixture.restart()
    ready_seconds=time.monotonic()-started
    recovery_target=traffic.rows
    fixture.expected_downtime.clear()
    if stable_audit(fixture) != audit:
        raise AssertionError('Replay rewrote unchanged result rows')
    wait(lambda:traffic.visible>=recovery_target,60)
    traffic.check()
    if boundary(fixture)['slot'] != before['slot']: raise AssertionError('Recovery replaced the source slot')
    fixture.mark_activity('steady')
    return {'kind':'during witnessed checkpoint PUT' if upload else 'during ingestion',
            'ready_seconds':ready_seconds,'caught_up_seconds':time.monotonic()-started,
            'source_changes_before':rows,
            'source_changes_at_confirmed_death':down_baseline,
            'source_changes_while_engine_down':while_down,'source_changes_after':traffic.rows,
            'before':before,'held':held,'after':boundary(fixture),
            'unchanged_sink_rows_preserved':True}


def validate_sample(sample, args):
    limits = {'rss_bytes':args.max_rss_bytes,'retained_wal_bytes':args.max_wal_bytes,
              'journal_rows':4096,'protection_rows':1024,
              'pending_changes_after_marker':args.max_pending_changes}
    for name,limit in limits.items():
        if name == 'rss_bytes' and sample[name] is None:
            if sample['process_metrics_available'] or not sample['expected_downtime']:
                raise RuntimeError('Unexplained missing process metrics')
            continue
        if sample[name] > limit: raise RuntimeError(f'{name} exceeded {limit}: {sample[name]}')
    if not sample['ack_safe']: raise RuntimeError('ACK exceeded durable checkpoint')
    if sample['physical']['bytes'] > args.max_state_bytes:
        raise RuntimeError('Physical state byte budget exceeded')
    latency=sample.get('sink_visibility_ms')
    if sample['activities']==['steady'] and latency is not None and latency['n']>=20:
        if latency['p50']>args.max_steady_p50_ms or latency['p95']>args.max_steady_p95_ms:
            raise RuntimeError('Steady source-commit-to-sink freshness budget exceeded')


def process_observation(fixture):
    worker, generation = fixture.worker, fixture.generation
    expected = fixture.expected_downtime.is_set()
    try:
        if worker.poll() is not None: raise ProcessLookupError('Worker exited')
        process = process_stats(worker.pid)
    except (OSError,StopIteration) as error:
        if not (expected or fixture.expected_downtime.is_set() or fixture.worker is not worker):
            raise ChildProcessError('Process metrics disappeared outside a witnessed restart') from error
        process = {'rss_bytes':None,'cpu_seconds':None}
        expected = True
    return {**process,'worker_pid':worker.pid,'worker_generation':generation,
            'process_metrics_available':process['rss_bytes'] is not None,
            'expected_downtime':expected}


class Sampler:
    """Keep observing resources during native checks, restart and final drain."""
    def __init__(self, fixture, traffic, args, started):
        self.fixture, self.traffic, self.args, self.started = fixture, traffic, args, started
        self.stop = threading.Event()
        self.samples = []
        self.thread = threading.Thread(target=lambda:traffic.guard(self.run),daemon=True)

    def capture(self):
        # Activity changes and observation windows share one boundary. Slow SQL
        # or object inventory below must not consume tags for a later batch.
        with self.fixture.metrics_lock:
            traffic=self.traffic.snapshot()
            timings=self.fixture.take_timings()
            observed=time.monotonic()
        process = process_observation(self.fixture)
        value={'seconds':observed-self.started,**process,**traffic,
               **boundary(self.fixture),'physical':self.fixture.objects(),
               'object_backend_bytes':int(docker('exec',self.fixture.s3_name,'du','-sb','/data').split()[0]),
               **timings}
        self.samples.append(value)
        with (self.args.output/'samples.jsonl').open('a') as stream:
            stream.write(json.dumps(value)+'\n')
        validate_sample(value,self.args)

    def run(self):
        while not self.stop.is_set():
            self.capture()
            self.stop.wait(self.args.sample_seconds)

    def finish(self):
        self.stop.set(); self.thread.join(timeout=30)
        if self.thread.is_alive(): raise TimeoutError('Resource sampler failed to stop')


def preserve(fixture):
    errors = []
    for name, command, filename in [
        (getattr(fixture,'pg_name',None), ['pg_dumpall','-U','postgres'], 'failed-database.sql.gz'),
        (getattr(fixture,'s3_name',None), ['tar','-czf','-','/data'], 'failed-object-store.tar.gz')]:
        if not name: continue
        try:
            with (fixture.directory/filename).open('wb') as target:
                if filename.endswith('.sql.gz'):
                    raw = fixture.directory/'failed-database.sql'
                    try:
                        with raw.open('wb') as dump:
                            subprocess.run(['docker','exec',name,*command],stdout=dump,check=True,timeout=180)
                    finally:
                        with raw.open('rb') as dump, gzip.GzipFile(fileobj=target,mode='wb') as compressed:
                            shutil.copyfileobj(dump,compressed)
                        raw.unlink()
                else:
                    subprocess.run(['docker','exec',name,*command],stdout=target,check=True,timeout=180)
        except BaseException as error: errors.append(repr(error))
    save(fixture.directory/'preservation.json',{'errors':errors})


def qualify(args):
    args.output.mkdir(parents=True)
    fixture = SoakFixture('pgd-soak-'+uuid.uuid4().hex[:12], args.output/'candidate', args, args.binary)
    success, traffic, sampler, verifications, crashes = False, None, None, [], []
    save(args.output/'manifest.json', {'git_head':subprocess.check_output(['git','rev-parse','HEAD'],text=True).strip(),
        'dependencies_sha256':{str(path.relative_to(ROOT)):sha256(path) for path in [*pathlib.Path(__file__).parent.glob('*.py'),ROOT/'scripts/s3_latency_proxy.py',ROOT/'worker.example.toml']},
        'binary_sha256':sha256(args.binary),
        'harness_sha256':sha256(pathlib.Path(__file__)), 'arguments':{key:str(value) if isinstance(value,pathlib.Path) else value for key,value in vars(args).items()},
        'scope':'One growing aggregate circuit. Source ingestion continues across two SIGKILL/restarts. Native checks pause only source writes; storage latency proxy remains enabled.'})
    try:
        fixture.start()
        started = time.monotonic(); end = started+args.duration
        traffic = Traffic(fixture,args,started); fixture.mark_activity('steady'); traffic.start()
        sampler = Sampler(fixture,traffic,args,started); sampler.thread.start()
        next_verify, next_crash = started+args.verify_seconds, 0
        while time.monotonic() < end:
            traffic.check(); now=time.monotonic()
            if next_crash < 2 and now >= started+args.duration*(next_crash+1)/3:
                crashes.append(crash(fixture,traffic,upload=next_crash==1)); next_crash+=1
                save(args.output/'crashes.json',crashes)
            if now >= next_verify:
                verifications.append({'seconds':now-started,**verify(fixture,traffic)})
                save(args.output/'verifications.json',verifications); next_verify=time.monotonic()+args.verify_seconds
            time.sleep(.05)
        wait(traffic.done.is_set,20)
        verifications.append({'seconds':time.monotonic()-started,**verify(fixture,traffic)})
        traffic.check(); traffic.finish()
        fixture.mark_activity('final-drain')
        fixture.expected_downtime.set()
        fixture.worker.send_signal(signal.SIGINT); fixture.worker.wait(timeout=180)
        fixture.engine_reader.join(timeout=10)
        if fixture.engine_reader.is_alive(): raise TimeoutError('Engine log drain timed out')
        sampler.finish(); sampler.capture()
        if traffic.errors or fixture.error: raise RuntimeError(traffic.errors or fixture.error)
        final=boundary(fixture)
        if fixture.worker.returncode != 0 or final['journal_rows'] != 0 or final['checkpoint_time'] != final['published_time']:
            raise AssertionError(final)
        if len(crashes) != 2: raise AssertionError('Both crash boundaries must be witnessed')
        active_seconds=args.duration-traffic.paused_seconds
        if traffic.rows < args.rate*active_seconds*.98:
            raise AssertionError('Offered source rate was not sustained')
        steady_windows=sum(row['activities']==['steady'] and row['sink_visibility_ms'] is not None and row['sink_visibility_ms']['n']>=20 for row in sampler.samples)
        if steady_windows<3: raise AssertionError('Need at least three steady visibility windows')
        result={'passed':True,'duration_seconds':time.monotonic()-started,
                'source_changes':traffic.rows,'source_transactions':traffic.commits,
                'paused_seconds':traffic.paused_seconds,'active_source_rows_per_second':traffic.rows/(args.duration-traffic.paused_seconds),
                'crashes':crashes,'verifications':verifications,'final':final,
                'request_counts':object_requests(fixture.directory),
                'steady_sample_windows':steady_windows,
                'resource_peaks':{key:max((row[key] for row in sampler.samples if row[key] is not None),default=None)
                    for key in ['rss_bytes','retained_wal_bytes','journal_rows','protection_rows','pending_changes_after_marker']},
                'object_backend_peak_bytes':max(row['object_backend_bytes'] for row in sampler.samples),
                'state_peak_bytes':max(row['physical']['bytes'] for row in sampler.samples),
                'state_peak_objects':max(row['physical']['objects'] for row in sampler.samples)}
        save(args.output/'qualification.json',result); success=True
    finally:
        errors=cleanup_owned(fixture,traffic,sampler,success)
        if errors and success: raise RuntimeError(errors)


def cleanup_owned(fixture, traffic, sampler, success):
    errors=[]
    for actor in [traffic,sampler]:
        if actor is not None:
            try: actor.finish()
            except BaseException as error: errors.append(repr(error))
    if not success:
        try:
            fixture.gate.touch()
            if fixture.worker is not None and fixture.worker.poll() is None:
                fixture.worker.send_signal(signal.SIGINT)
                try: fixture.worker.wait(timeout=30)
                except subprocess.TimeoutExpired:
                    fixture.worker.kill(); fixture.worker.wait(timeout=10)
        except BaseException as error: errors.append('stop worker: '+repr(error))
        try: preserve(fixture)
        except BaseException as error: errors.append('preservation: '+repr(error))
    try: errors.extend(fixture.close(success=success and not errors))
    except BaseException as error: errors.append('fixture cleanup: '+repr(error))
    output=fixture.directory.parent
    save(output/'cleanup.json',{'errors':errors})
    if errors and success:
        report=json.loads((output/'qualification.json').read_text())
        report['passed']=False; report['cleanup_errors']=errors
        save(output/'qualification.json',report)
    return errors


def arguments():
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--binary',type=pathlib.Path,required=True)
    parser.add_argument('--output',type=pathlib.Path,required=True)
    parser.add_argument('--duration',type=float,default=10800)
    parser.add_argument('--rate',type=float,default=1000)
    parser.add_argument('--state-rows',type=int,default=12000)
    parser.add_argument('--groups',type=int,default=1000)
    parser.add_argument('--transaction-mix',choices=['autocommit','varied'],default='varied')
    parser.add_argument('--proxy-scale',type=float,default=1)
    parser.add_argument('--verify-seconds',type=float,default=600)
    parser.add_argument('--sample-seconds',type=float,default=30)
    parser.add_argument('--postgres-image',default='postgres:17')
    parser.add_argument('--postgres-memory',default='8g')
    parser.add_argument('--max-rss-bytes',type=int,default=4*1024**3)
    parser.add_argument('--max-wal-bytes',type=int,default=512*1024**2)
    parser.add_argument('--max-state-bytes',type=int,default=4*1024**3)
    parser.add_argument('--max-pending-changes',type=int,default=60000)
    parser.add_argument('--max-steady-p50-ms',type=float,default=100)
    parser.add_argument('--max-steady-p95-ms',type=float,default=250)
    args=parser.parse_args(); args.workload='mixed'
    for key in ['duration','rate','verify_seconds','sample_seconds','max_rss_bytes','max_wal_bytes','max_state_bytes','max_pending_changes','max_steady_p50_ms','max_steady_p95_ms','groups']:
        if not math.isfinite(getattr(args,key)) or getattr(args,key)<=0: parser.error(key+' must be finite and positive')
    if args.duration<30 or args.state_rows<0 or not math.isfinite(args.proxy_scale) or args.proxy_scale<0:
        parser.error('duration>=30, state-rows>=0 and proxy-scale>=0 required')
    if args.duration*args.rate/3+args.state_rows+1000000002 >= 2147483647:
        parser.error('Requested source identities exceed integer range')
    args.binary=args.binary.resolve(); args.output=args.output.resolve()
    if not args.binary.is_file() or not os.access(args.binary,os.X_OK) or args.output.exists():
        parser.error('Executable and new output directory required')
    return args


def interrupted(_signal, _frame):
    raise InterruptedError('Soak terminated; preserve evidence and clean up owned fixtures')


if __name__=='__main__':
    signal.signal(signal.SIGTERM,interrupted)
    qualify(arguments())
