"""Shared recorded CLI and proxy lifecycle; callers retain fault-specific assertions."""
import json
import os
import select
from contextlib import contextmanager
import subprocess
import sys
import time


def admin(fixture, binary, action, config=None, *, scope='query', success=True, timeout=240):
    result = subprocess.run(binary + [scope, action, str(config or fixture.config())],
                            env=fixture.environment, text=True, capture_output=True, timeout=timeout)
    with (fixture.output/'commands.jsonl').open('a') as evidence:
        evidence.write(json.dumps({'scope':scope, 'action':action, 'code':result.returncode,
                                   'stdout':result.stdout, 'stderr':result.stderr,
                                   'expected':'success' if success else 'rejection'})+'\n')
    assert (result.returncode == 0) == success, result.stderr or 'invalid command outcome'
    return json.loads(result.stdout.splitlines()[-1]) if success else result.stderr


def wait_records(path, predicate, *, process=None, timeout=20):
    deadline = time.monotonic()+timeout
    while time.monotonic() < deadline:
        # A concurrent writer may have flushed only part of its newest record.
        records = [json.loads(line) for line in path.read_text().splitlines(keepends=True)
                   if line.endswith('\n')]
        if predicate(records):
            return records
        if process is not None and process.poll() is not None:
            raise RuntimeError(f'process ended before its witness; see {path}')
        time.sleep(.05)
    raise AssertionError(f'fault witness missing: {path}')


def start_recorded(command, path, timeout=10):
    with path.open('w') as log:
        process = subprocess.Popen(command, stdout=log, stderr=log)
    try:
        records = wait_records(path, lambda rows: rows and rows[0].get('ready'),
                               process=process, timeout=timeout)
        return process, records[0]
    except BaseException:
        process.terminate()
        process.wait(timeout=10)
        raise


def commit_proxy(path, mode, *, once=False, marker=None, gate=None, upstream=None):
    command = [sys.executable, '-u', 'scripts/pg_commit_proxy.py', '--mode', mode]
    if once:
        command.append('--once')
    for flag, value in [('--arm-query', marker), ('--gate', gate), ('--upstream', upstream)]:
        if value is not None:
            command += [flag, str(value)]
    process, ready = start_recorded(command, path)
    return process, ready['port']


@contextmanager
def held_transaction(fixture, evidence, acquire):
    """Hold fixture SQL locks; acquisition must emit exactly one 'held' row."""
    with evidence.open('w') as errors:
        holder = subprocess.Popen(fixture.pg[:-1]+['-Atq'], env=fixture.environment,
            stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=errors, text=True)
        try:
            holder.stdin.write('BEGIN; '+acquire+';\n')
            holder.stdin.flush()
            assert select.select([holder.stdout], [], [], 10)[0], 'barrier acquisition timeout'
            assert holder.stdout.readline().strip() == 'held', 'barrier holder stopped'
            yield holder
        finally:
            if holder.poll() is None:
                holder.terminate()
                holder.wait(timeout=10)
            holder.stdout.close()
            if not holder.stdin.closed:
                holder.stdin.close()


def finish_transaction(holder, statement='COMMIT'):
    holder.stdin.write(statement+';\n')
    holder.stdin.close()
    assert holder.wait(timeout=10) == 0, 'barrier transaction failed'


def wait_sql(fixture, query, expected='1'):
    deadline = time.monotonic()+5
    while fixture.sql(query).strip() != expected:
        assert time.monotonic()<deadline, 'SQL lock witness missing: '+query
        time.sleep(.02)


class Server:
    def __init__(self, output, label, *, name=None, size="1g", flags=(), metadata=None, failure=None):
        self.output = output
        self.name = name or f'pgderive-recovery-{os.getpid()}-{label}'
        self.failure = failure or self.name+'-failure.log'
        subprocess.run(['docker','run','-d','--name',self.name,
            '--tmpfs',f'/var/lib/postgresql/data:rw,size={size}','-p','127.0.0.1::5432',
            '-e','POSTGRES_PASSWORD=postgres','-e','POSTGRES_DB=pgderive_dev',
            'postgres:17','-c','wal_level=logical', *[item for flag in flags for item in ('-c', flag)]],
            check=True,capture_output=True)
        try:
            info = json.loads(subprocess.check_output(['docker','inspect',self.name]))[0]
            assert info['Mounts']==[] and '/var/lib/postgresql/data' in info['HostConfig']['Tmpfs']
            self.port = info['NetworkSettings']['Ports']['5432/tcp'][0]['HostPort']
            (output/(metadata or f'{label}-postgres.json')).write_text(json.dumps({'container':self.name,
                'port':self.port,'tmpfs':info['HostConfig']['Tmpfs'],'mounts':info['Mounts']})+'\n')
            deadline=time.monotonic()+30
            while subprocess.run(['docker','exec',self.name,'pg_isready','-h','127.0.0.1','-U','postgres'],capture_output=True).returncode:
                assert time.monotonic()<deadline
                time.sleep(.1)
        except BaseException:
            self.finish(False)
            raise

    def finish(self, success):
        if success:
            subprocess.run(['docker','rm','-f',self.name],check=True,capture_output=True)
        else:
            with (self.output/self.failure).open('w') as log:
                subprocess.run(['docker','logs',self.name],stdout=log,stderr=log)
            print('Preserved failed owned tmpfs PostgreSQL:',self.name,file=sys.stderr)
