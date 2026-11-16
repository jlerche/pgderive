#!/usr/bin/env python3
"""Run unchanged live suites concurrently on independent owned PostgreSQL servers.

A lane never overlaps PostgreSQL-mutating harnesses. Different lanes never share
an instance. Failure retains every owned server and prevents new work.
"""
from concurrent.futures import ThreadPoolExecutor, as_completed
import json
import gzip
import tarfile
import uuid
import os
import pathlib
import queue
import signal
import subprocess
import sys
import threading
import time
import full_live_gate
from harness_control import Server
from run_with_s3_proxy import prepare
from verify_worker_core import main as verify_worker

# Start slow suites early. Each shard still runs complete histories and
# each lane owns one isolated PostgreSQL instance.
SUITES = (
    ('tenant-storage', 1), ('residual-subqueries', 24), ('quantified-subqueries', 18), ('text-ordering', 2), ('grouping-sets', 2),
    ('transactions', 1), ('crashes', 1), ('postgres-support', 1),
    ('query-rebuild', 1), ('projection-compiler', 1), ('memtable', 1),
    ('string-agg', 1), ('query-lifecycle', 1), ('checkpoint', 1),
    ('result-table', 1), ('multi-query', 1), ('tenant-storage-commit', 1),
    ('operation-budget', 1), ('replication', 1),
)


def tasks(with_replication, full=False):
    suites = SUITES
    if full:
        shards_by_name = dict(SUITES)
        names = full_live_gate.suites()
        prioritized = (
            'replication', 'error-order', 'residual-subqueries', 'quantified-subqueries',
            'subqueries', 'composition-matrix', 'outer-joins', 'join-predicates',
            'set-operations', 'temporal-arithmetic',
            'application', *full_live_gate.RELEASE, 'tenant-storage', 'text-ordering', 'grouping-sets',
        )
        names = (*prioritized, *(name for name in names if name not in prioritized))
        shards_by_name['error-order'] = full_live_gate.ERROR_ORDER_SHARDS
        shards_by_name.update(full_live_gate.MODE_SHARDS)
        shards_by_name['composition-matrix'] = full_live_gate.COMPOSITION_SHARDS
        suites = [(name, shards_by_name.get(name, 1)) for name in names]
    pending = [(name, index, shards) for name, shards in suites
               if with_replication or name not in ('replication', 'error-order')
               for index in range(shards)]
    if full:
        # Interleave the long histories with replication and the error corpus
        # so no single suite occupies the entire pool while peers are pending.
        first = {'replication', 'error-order', 'residual-subqueries', 'quantified-subqueries',
                 'composition-matrix', 'subqueries', 'outer-joins', 'join-predicates',
                 'set-operations', 'temporal-arithmetic'}
        pending.sort(key=lambda task: (task[0] not in first, task[1] if task[0] in first else 0))
    return pending


def workers(task_count):
    default = min(8, os.cpu_count() or 1)
    configured = int(os.environ.get('PGDERIVE_GATE_LANES', default))
    if configured < 1:
        raise ValueError('PGDERIVE_GATE_LANES must be positive')
    return min(configured, task_count)


def command(name, output, binaries):
    worker=[binaries['worker']]; admin=[binaries['admin']]
    if name=='replication':
        return ['bash','scripts/check_integration.sh']
    if name in ('memtable','checkpoint','query-lifecycle'):
        boundary='lifecycle' if name=='query-lifecycle' else name
        executable=admin if boundary=='lifecycle' else worker
        return [sys.executable,'scripts/core_worker_harness.py',boundary,str(output),'--',*executable]
    if name=='transactions':
        return [sys.executable,'scripts/worker_harness.py','--faults','--kill','--ticks','4','--burst','8',str(output),'--',*worker]
    script='worker_crash' if name=='crashes' else name.replace('-','_')
    options=['--counts','2','--seconds','2','--mode','shared'] if name=='multi-query' else []
    if name=='multi-query':script='multi_query_memory'
    executable=admin if name=='query-rebuild' else worker
    return [sys.executable,'scripts/'+script+'_harness.py',str(output),*options,'--',*executable]


class Commands:
    def __init__(self, timeout):
        self.timeout = timeout
        self.lock=threading.Lock()
        self.active=set()
        self.cancelled=False

    @staticmethod
    def kill(process):
        try:os.killpg(process.pid,signal.SIGKILL)
        except ProcessLookupError:pass

    def cancel(self, *, kill_active):
        with self.lock:
            self.cancelled=True
            processes = list(self.active) if kill_active else []
        for process in processes:self.kill(process)

    def run(self, argv, environment, log):
        with log.open('w') as stream:
            with self.lock:
                if self.cancelled:raise RuntimeError('Live gate cancelled')
                process=subprocess.Popen(argv,env=environment,stdout=stream,stderr=stream,start_new_session=True)
                self.active.add(process)
            try:
                code=process.wait(timeout=self.timeout)
                if code:raise RuntimeError(f'Live suite failed (exit {code}); see {log}')
            finally:
                # Kill only descendants in this owned process group, including
                # workers orphaned by a harness timeout. Never touch the server.
                self.kill(process)
                process.wait(timeout=10)
                with self.lock:self.active.discard(process)


def provision(index, output, servers):
    evidence=output/f'lane-{index}'; evidence.mkdir()
    server=Server(evidence,'postgres',name=f'pgderive-gate-{os.getpid()}-{index}-{uuid.uuid4().hex[:8]}',
                  flags=['max_replication_slots=96','max_wal_senders=96','max_connections=300'])
    servers.put(server)
    return server


def lane(index, pending, output, binaries, commands, server, completed, timings, package):
    full = package is not None
    environment = {
        **{key: value for key, value in os.environ.items()
           if not key.startswith('PGDERIVE__') and key != 'PGDERIVE_TEST_CASE_SHARD'},
        # Each lane can start several Rust processes. Keep their async pools
        # small so runnable threads scale with the lane count, not CPU count squared.
        'TOKIO_WORKER_THREADS': os.environ.get('TOKIO_WORKER_THREADS') or '2',
        'PGDERIVE_TEST_POSTGRES_PORT': str(server.port),
        'PGPASSWORD': 'postgres', 'PGDERIVE__POSTGRES__PASSWORD': 'postgres',
        'PGDERIVE_S3_LATENCY_SCALE': '0',
        'PGDERIVE_CHECK_PROFILE': 'full' if full else 'core',
    }
    if not full:
        environment['PGDERIVE__POSTGRES__PORT'] = str(server.port)
    while True:
        try:
            name, shard, shards = pending.get_nowait()
        except queue.Empty:
            return
        relative = pathlib.Path(name)
        task_environment = environment.copy()
        if shards > 1:
            relative /= f'shard-{shard + 1}'
            task_environment['PGDERIVE_TEST_CASE_SHARD'] = f'{shard}/{shards}'
        directory = output / relative
        if name in full_live_gate.RELEASE:
            package.result()
        started = time.monotonic()
        if name == 'query-container':
            full_live_gate.qualify_container(output, commands, task_environment)
        else:
            directory.mkdir(parents=True)
            if full:
                argv = full_live_gate.command(name, directory, binaries, environment['PGDERIVE_TEST_POSTGRES_PORT'])
                if name in full_live_gate.MATRIX:
                    task_environment['PGDERIVE_HARNESS_TIMEOUT_SECONDS'] = (
                        task_environment.get('PGDERIVE_HARNESS_TIMEOUT_SECONDS') or '1800')
                if name in ('replication', 'crashes'):
                    task_environment['PGDERIVE_S3_LATENCY_SCALE'] = (
                        os.environ.get('PGDERIVE_S3_LATENCY_SCALE') or '1')
            else:
                argv = command(name, directory, binaries)
                if name != 'replication':
                    # Retry count stays the same. Paused-time tests check timer scheduling.
                    task_environment['PGDERIVE__WORKER__RETRY_DELAY_MS'] = '1'
            if name == 'error-order':
                task_environment['PGDERIVE_ERROR_ORDER_EVIDENCE'] = str(directory / 'cases.json')
            elif name != 'replication':
                argv = [sys.executable, 'scripts/run_with_s3_proxy.py', str(directory), *argv]
            commands.run(argv, task_environment, directory / 'console.log')
        elapsed = time.monotonic() - started
        completed.append(str(relative))
        timings[str(relative)] = elapsed
        print(f'Lane {index}: {relative} passed in {elapsed:.1f}s', flush=True)


def run(output, binaries, full=False):
    output.mkdir(parents=True,exist_ok=True)
    commands = Commands(timeout=None if full else 1800)
    servers = queue.Queue()
    success = False
    with_replication = os.environ.get('PGDERIVE_GATE_REPLICATION') == '1'
    scheduled = tasks(with_replication, full)
    pending = queue.Queue()
    for task in scheduled:
        pending.put(task)
    lane_count = workers(len(scheduled))
    print(f'Live gate: {len(scheduled)} tasks across {lane_count} PostgreSQL lanes', flush=True)
    completed = [[] for _ in range(lane_count)]
    timings = [{} for _ in range(lane_count)]
    try:
        # Initialize the shared object service/bucket once, before any lane.
        prepare()
        with ThreadPoolExecutor(max_workers=lane_count + int(full)) as pool:
            try:
                package = None
                if full:
                    package = pool.submit(commands.run,
                                          ['bash', 'scripts/package_release.sh', str(output / 'package')],
                                          os.environ.copy(), output / 'package.log')
                prepared=[pool.submit(provision,index,output,servers) for index in range(lane_count)]
                owned=[job.result() for job in prepared]
                if len({server.port for server in owned})!=lane_count or len({server.name for server in owned})!=lane_count:
                    raise RuntimeError('Live lanes do not have distinct PostgreSQL servers')
                jobs = [pool.submit(lane, index, pending, output, binaries, commands, server,
                                    completed[index], timings[index], package) for index, server in enumerate(owned)]
                if package is not None:
                    jobs.append(package)
                for job in as_completed(jobs):
                    job.result()
                if full:
                    full_live_gate.verify(output, binaries, str(owned[0].port), with_replication)
                else:
                    verify_worker(output,[binaries['worker'],binaries['admin']])
                success=True
            except BaseException:
                # Full harnesses may own additional fixtures. Let active jobs
                # finish their cleanup so that their failure evidence survives.
                commands.cancel(kill_active=not full)
                raise
    finally:
        records=[]
        while not servers.empty():
            server=servers.get()
            records.append({'lane': server.output.name, 'container': server.name, 'port': server.port})
            if not success:
                try:archive(server)
                except Exception as error:print(f'Archive failed; server retained: {server.name}: {error}',file=sys.stderr)
            server.finish(success)
        records.sort(key=lambda record: int(record['lane'].split('-')[1]))
        evidence = {'passed': success, 'servers': records, 'groups': completed,
                    'elapsed_seconds': {task: seconds for lane_timings in timings
                                        for task, seconds in lane_timings.items()}}
        (output / 'lanes.json').write_text(json.dumps(evidence, indent=2) + '\n')


def archive(server):
    """Preserve the owned PG cluster even on an ephemeral CI runner."""
    loop='for task in /proc/[0-9]*; do IFS= read -r name < "$task/comm" || continue; if [ "$name" = postgres ]; then kill -SIGNAL "${task##*/}"; fi; done'
    path=server.output/'postgres-evidence.tar.gz'
    try:
        # Stop the postmaster first, then any already-forking backends.
        for _ in range(2):
            subprocess.run(['docker','exec',server.name,'sh','-c',loop.replace('SIGNAL','STOP')],check=True)
        with gzip.open(path,'wb',compresslevel=1) as target:
            source=subprocess.Popen(['docker','exec',server.name,'tar','-C','/var/lib/postgresql','-cf','-','data'],stdout=subprocess.PIPE)
            try:
                while chunk:=source.stdout.read(1024*1024):target.write(chunk)
            finally:
                source.stdout.close()
                if source.wait(timeout=30):raise RuntimeError('PG evidence archive failed')
    finally:
        subprocess.run(['docker','exec',server.name,'sh','-c',loop.replace('SIGNAL','CONT')],check=True)
    with tarfile.open(path,'r:gz') as saved:
        names=saved.getnames()
    assert 'data/global/pg_control' in names and any(n.startswith('data/pg_wal/') and len(n.rsplit('/',1)[-1])==24 for n in names)


def interrupted(_signal,_frame):
    raise KeyboardInterrupt('Live gate interrupted')


if __name__=='__main__':
    signal.signal(signal.SIGTERM,interrupted)
    run(pathlib.Path(sys.argv[1]), json.loads(pathlib.Path(sys.argv[2]).read_text()),
        full=sys.argv[3] == 'full')
