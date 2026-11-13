#!/usr/bin/env python3
"""Run unchanged live suites concurrently on independent owned PostgreSQL servers.

A lane never overlaps PostgreSQL-mutating harnesses. Different lanes never share
an instance. Failure retains every owned server and stops all harness processes.
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
from harness_control import Server
from run_with_s3_proxy import prepare
from verify_worker_core import main as verify_worker

GROUPS=(
    ('replication','result-table','operation-budget','multi-query','query-lifecycle'),
    ('tenant-storage','tenant-storage-commit','projection-compiler','quantified-subqueries'),
    ('memtable','postgres-support','checkpoint','query-rebuild','residual-subqueries'),
    ('transactions','crashes'),
)


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
    def __init__(self):
        self.lock=threading.Lock()
        self.active=set()
        self.cancelled=False

    @staticmethod
    def kill(process):
        try:os.killpg(process.pid,signal.SIGKILL)
        except ProcessLookupError:pass

    def cancel(self):
        with self.lock:
            self.cancelled=True
            processes=list(self.active)
        for process in processes:self.kill(process)

    def run(self, argv, environment, log):
        with log.open('w') as stream:
            with self.lock:
                if self.cancelled:raise RuntimeError('Live gate cancelled')
                process=subprocess.Popen(argv,env=environment,stdout=stream,stderr=stream,start_new_session=True)
                self.active.add(process)
            try:
                code=process.wait(timeout=1800)
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


def lane(index, names, output, binaries, commands, server):
    environment={**{k:v for k,v in os.environ.items() if not k.startswith('PGDERIVE__')},
                 'PGDERIVE_TEST_POSTGRES_PORT':str(server.port),
                 'PGDERIVE__POSTGRES__PORT':str(server.port),
                 'PGPASSWORD':'postgres','PGDERIVE__POSTGRES__PASSWORD':'postgres',
                 'PGDERIVE_S3_LATENCY_SCALE':'0'}
    for name in names:
        directory=output/name; directory.mkdir()
        argv=command(name,directory,binaries)
        if name!='replication':
            argv=[sys.executable,'scripts/run_with_s3_proxy.py',str(directory),*argv]
        task_environment=environment.copy()
        if name!='replication':
            # Same retry count; timer scheduling is checked with paused time.
            task_environment['PGDERIVE__WORKER__RETRY_DELAY_MS']='1'
        commands.run(argv,task_environment,directory/'console.log')
        print(f'Lane {index}: {name} passed',flush=True)


def run(output, binaries):
    output.mkdir(parents=True,exist_ok=True)
    commands=Commands(); servers=queue.Queue(); success=False
    with_replication=os.environ.get('PGDERIVE_GATE_REPLICATION')=='1'
    try:
        # Initialize the shared object service/bucket once, before any lane.
        prepare()
        with ThreadPoolExecutor(max_workers=len(GROUPS)) as pool:
            try:
                prepared=[pool.submit(provision,index,output,servers) for index in range(len(GROUPS))]
                owned=[job.result() for job in prepared]
                if len({server.port for server in owned})!=len(GROUPS) or len({server.name for server in owned})!=len(GROUPS):
                    raise RuntimeError('Live lanes do not have distinct PostgreSQL servers')
                jobs=[pool.submit(lane,index,[n for n in names if n!='replication' or with_replication],
                                  output,binaries,commands,owned[index]) for index,names in enumerate(GROUPS)]
                for job in as_completed(jobs):job.result()
                verify_worker(output,[binaries['worker'],binaries['admin']])
                success=True
            except BaseException:
                commands.cancel()
                raise
    finally:
        records=[]
        while not servers.empty():
            server=servers.get()
            records.append({'container':server.name,'port':server.port})
            if not success:
                try:archive(server)
                except Exception as error:print(f'Archive failed; server retained: {server.name}: {error}',file=sys.stderr)
            server.finish(success)
        (output/'lanes.json').write_text(json.dumps({'passed':success,'servers':records,
             'groups':[[n for n in names if n!='replication' or with_replication] for names in GROUPS]},indent=2)+'\n')


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
    run(pathlib.Path(sys.argv[1]),json.loads(pathlib.Path(sys.argv[2]).read_text()))
