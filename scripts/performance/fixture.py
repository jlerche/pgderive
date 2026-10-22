"""Run-owned local fixtures. No existing container, database, or volume is reused."""
import json
import os
import pathlib
import signal
import re
import tomllib
import shutil
import socket
import subprocess
import sys
import threading
import urllib.request
import xml.etree.ElementTree as ET
from urllib.parse import urlencode

from common import QUERY, ROOT, RW_IMAGE, S3_IMAGE, docker, save, wait


def free_port():
    with socket.socket() as sock:
        sock.bind(('127.0.0.1', 0))
        return sock.getsockname()[1]


def compatible_worker_template(text):
    """Use candidate scheduler defaults without sending new fields to old binaries."""
    result, skipping_epoch = [], False
    for line in text.split('[worker.query]')[0].splitlines(keepends=True):
        header = line.split('#', 1)[0].strip()
        if header.startswith('['):
            skipping_epoch = header == '[worker.epoch]'
        if not skipping_epoch:
            result.append(line)
    return ''.join(result)


def effective_worker_config(text):
    """Stable executed settings without credentials or per-run connection locations."""
    config = tomllib.loads(text)
    dynamic = {'host', 'port', 'database', 'user', 'password', 'endpoint',
               'access_key_id', 'secret_access_key', 'credentials_file', 'ca_file'}
    def scrub(value):
        if isinstance(value, dict):
            return {key: scrub(item) for key, item in value.items() if key not in dynamic}
        return value
    return scrub(config)


def effective_proxy_profile(profile):
    return {key: value for key, value in profile.items() if key not in {'port', 'upstream'}}


def effective_risingwave_sql(statements):
    return [re.sub(r"\b(hostname|host|port|username|user|password)\s*=\s*'[^']*'",
                   r"\1='<connection>'", sql) for sql in statements]


class Fixture:
    def __init__(self, name, directory, args, binary=None):
        self.name, self.directory, self.args, self.binary = name, directory, args, binary
        directory.mkdir()
        self.containers, self.processes, self.files, self.readers = [], [], [], []
        self.events = []
        self.worker = None
        self.error = None

    def connect(self, native=False):
        import psycopg
        return psycopg.connect(host='127.0.0.1', port=self.rw_port if native else self.pg_port,
                               user='root' if native else 'postgres',
                               password=None if native else 'local',
                               dbname='dev' if native else 'postgres', autocommit=True,
                               connect_timeout=3, options='-c statement_timeout=10000 -c lock_timeout=3000')

    def container(self, suffix, image, arguments, command):
        name = self.name + '-' + suffix
        # Register only after successful creation; never remove a preexisting name.
        docker('create', '--name', name, '--label', 'pgderive.performance=' + self.name,
               *arguments, image, *command)
        self.containers.append(name)
        docker('start', name)
        return name

    def start(self):
        self.pg_name = self.container('pg', self.args.postgres_image,
            ['--memory', self.args.postgres_memory, '--tmpfs', '/var/lib/postgresql/data:rw,size=2g',
             '-e', 'POSTGRES_PASSWORD=local', '-p', '127.0.0.1::5432'],
            ['postgres', '-c', 'wal_level=logical', '-c', 'max_replication_slots=20', '-c', 'max_wal_senders=20'])
        self.pg_port = int(docker('port', self.pg_name, '5432/tcp').rsplit(':', 1)[1])
        self.s3_name = self.container('s3', S3_IMAGE,
            ['--memory', '2g', '--tmpfs', '/data:rw,size=1g', '-p', '127.0.0.1::8333'],
            ['server', '-s3', '-dir=/data', '-master.volumeSizeLimitMB=32'])
        self.s3_port = int(docker('port', self.s3_name, '8333/tcp').rsplit(':', 1)[1])
        import psycopg
        def postgres_ready():
            try:
                with self.connect() as conn:
                    return conn.execute('SELECT 1').fetchone()[0]
            except psycopg.Error:
                return False
        wait(postgres_ready)
        with self.connect() as conn:
            conn.execute('CREATE TABLE positions(id int PRIMARY KEY,book_id int NOT NULL,quantity int);'
                         'ALTER TABLE positions REPLICA IDENTITY FULL; CREATE SCHEMA pgd;'
                         'INSERT INTO positions VALUES(1,1,0);'
                         'CREATE PUBLICATION compare_pub FOR TABLE positions;'
                         'CREATE TABLE rw_sink(book_id int PRIMARY KEY,positions bigint,units bigint)')
            conn.execute('INSERT INTO positions SELECT i+1,2+(i-1)%% %s,NULL FROM generate_series(1,%s)i',
                         (self.args.groups, self.args.state_rows))
        wait(lambda: urllib.request.urlopen(f'http://127.0.0.1:{self.s3_port}', timeout=2).read())
        urllib.request.urlopen(urllib.request.Request(
            f'http://127.0.0.1:{self.s3_port}/pgderive-benchmark', method='PUT'), timeout=10).close()
        stderr = (self.directory / 'proxy.stderr').open('w'); self.files.append(stderr)
        proxy = subprocess.Popen([sys.executable, '-u', str(ROOT / 'scripts/s3_latency_proxy.py'),
            '--port', '0', '--upstream', f'http://127.0.0.1:{self.s3_port}',
            '--scale', str(self.args.proxy_scale)], stdout=subprocess.PIPE, stderr=stderr, text=True)
        self.processes.append(proxy)
        line = proxy.stdout.readline()
        profile = json.loads(line)
        self.proxy_port = profile['port']
        self.proxy_profile = effective_proxy_profile(profile)
        self.consume(proxy, 'proxy.jsonl', first=line)
        if self.binary:
            self.start_pgderive()
        else:
            self.start_risingwave()
        wait(self.equal)
        details = {name: json.loads(docker('inspect', name))[0] for name in self.containers}
        self.fingerprint = {
            'images': {name.rsplit('-', 1)[-1]: item['Image'] for name, item in details.items()},
            'resources': {name.rsplit('-', 1)[-1]: {key: item['HostConfig'].get(key) for key in
                ['Memory', 'MemorySwap', 'NanoCpus', 'CpuPeriod', 'CpuQuota', 'CpusetCpus', 'Tmpfs']}
                for name, item in details.items()},
            'worker_cpu_affinity': sorted(os.sched_getaffinity(self.pid)),
            'effective_engine_config': self.engine_config,
            'proxy_profile': self.proxy_profile,
            'postgres_settings': self.postgres_settings()}
        save(self.directory / 'fixture.json', {'postgres_port': self.pg_port,
             'proxy_port': self.proxy_port, 'containers': self.containers,
             'worker_pid': self.pid, 'fingerprint': self.fingerprint})

    def consume(self, process, filename, first=''):
        output = (self.directory / filename).open('w')
        output.write(first); output.flush()
        def read():
            import time
            for line in process.stdout:
                output.write(line); output.flush()
                if filename == 'events.jsonl':
                    try:
                        event = json.loads(line)
                        self.events.append({'event': event.get('event'), 'elapsed_ms': event.get('elapsed_ms'),
                                            '_observed_at': time.monotonic()})
                    except ValueError:
                        self.error = 'Unparseable engine event; inspect events.jsonl'
            output.close()
        reader = threading.Thread(target=read, daemon=True)
        reader.start(); self.readers.append(reader)

    def start_pgderive(self):
        template = compatible_worker_template((ROOT / 'worker.example.toml').read_text())
        replacements = {'port = 55434': f'port = {self.pg_port}',
            'database = "pgderive_dev"': 'database = "postgres"',
            'publication = "pgderive_worker_pub"': 'publication = "compare_pub"',
            'endpoint = "http://127.0.0.1:8333"': f'endpoint = "http://127.0.0.1:{self.proxy_port}"',
            'bucket = "pgderive-mvp-tests"': 'bucket = "pgderive-benchmark"',
            'catalog_schema = "pgderive_worker"': 'catalog_schema = "pgd"',
            'sink_table = "groups"': 'sink_table = "groups"\nsink_key = ["book_id"]',
            'phase_metrics = false': 'phase_metrics = true'}
        for before, after in replacements.items():
            if before not in template:
                raise ValueError('Configuration template changed: ' + before)
            template = template.replace(before, after)
        config = self.directory / 'pgderive.toml'
        config.write_text(template + '\n[worker.query]\nsql = ' + json.dumps(QUERY) + '\n')
        self.engine_config = effective_worker_config(config.read_text())
        scratch = self.directory / 'scratch'; scratch.mkdir()
        stderr = (self.directory / 'engine.stderr').open('w'); self.files.append(stderr)
        self.worker = subprocess.Popen([str(self.binary), 'query', 'create', str(config)],
            stdout=subprocess.PIPE, stderr=stderr, text=True,
            env={**{key: value for key, value in os.environ.items() if not key.startswith('PGDERIVE__')},
                 'PGDERIVE__POSTGRES__PASSWORD': 'local', 'TMPDIR': str(scratch)})
        self.processes.append(self.worker); self.pid = self.worker.pid
        self.consume(self.worker, 'events.jsonl')
        def ready():
            if self.worker.poll() is not None:
                raise ChildProcessError('Pgderive exited; inspect engine.stderr')
            return any(event.get('event') == 'ready' for event in self.events)
        wait(ready)

    def start_risingwave(self):
        self.rw_port, metrics_port = free_port(), free_port()
        config = self.directory / 'risingwave.toml'
        config.write_text('[server]\nmetrics_level="Debug"\ntelemetry_enabled=false\n[system]\n'
            f'barrier_interval_ms={self.args.rw_barrier_ms}\ncheckpoint_frequency=1\n'
            'data_directory="benchmark"\nsstable_size_mb=32\n')
        name = self.container('rw', RW_IMAGE, ['--network', 'host', '--memory', self.args.rw_memory,
            '--tmpfs', '/rwdata:rw,size=1g', '-v', str(config) + ':/pgderive-benchmark.toml:ro',
            '-e', 'ENABLE_TELEMETRY=false', '-e',
            f'RW_STATE_STORE=hummock+minio://local:local@127.0.0.1:{self.proxy_port}/pgderive-benchmark'],
            ['single_node', '--store-directory', '/rwdata', '--config-path', '/pgderive-benchmark.toml',
             '--total-memory-bytes', str(self.args.rw_memory_bytes), '--parallelism', '1',
             '--listen-addr', f'127.0.0.1:{self.rw_port}',
             '--prometheus-listener-addr', f'127.0.0.1:{metrics_port}'])
        self.pid = int(docker('inspect', '--format', '{{.State.Pid}}', name))
        self.metrics_port = metrics_port
        import psycopg
        def ready():
            try:
                with self.connect(native=True) as conn:
                    return conn.execute('SELECT 1').fetchone()[0]
            except psycopg.Error:
                return False
        wait(ready)
        statements = [f"CREATE SOURCE pg_input WITH(connector='postgres-cdc',hostname='127.0.0.1',port='{self.pg_port}',username='postgres',password='local',database.name='postgres',slot.name='rw_bench_slot',publication.name='compare_pub',publication.create.enable='false',transactional='true')",
                        "CREATE TABLE positions(id int PRIMARY KEY,book_id int,quantity int) FROM pg_input TABLE 'public.positions'",
                        'CREATE MATERIALIZED VIEW totals AS ' + QUERY,
                        f"CREATE SINK pg_output FROM totals WITH(connector='postgres',host='127.0.0.1',port='{self.pg_port}',user='postgres',password='local',database='postgres',table='rw_sink',type='upsert',primary_key='book_id')"]

        self.engine_config = {'toml': tomllib.loads(config.read_text()),
            'sql': effective_risingwave_sql(statements),
            'parallelism': 1, 'total_memory_bytes': self.args.rw_memory_bytes,
            'state_store_kind': 'hummock+minio', 'state_store_bucket': 'pgderive-benchmark'}
        with self.connect(native=True) as conn:
            for sql in statements:
                conn.execute(sql)

    def postgres_settings(self):
        names = ['server_version', 'wal_level', 'shared_buffers', 'work_mem',
                 'maintenance_work_mem', 'logical_decoding_work_mem', 'synchronous_commit',
                 'fsync', 'full_page_writes', 'wal_compression', 'max_replication_slots',
                 'max_wal_senders', 'max_connections', 'checkpoint_timeout', 'max_wal_size',
                 'max_slot_wal_keep_size', 'track_commit_timestamp', 'TimeZone',
                 'DateStyle', 'default_transaction_isolation', 'jit']
        with self.connect() as conn:
            return dict(conn.execute('SELECT name,setting FROM pg_settings WHERE name=ANY(%s) ORDER BY name',
                                     (names,)).fetchall())

    def equal(self):
        import psycopg
        try:
            with self.connect() as conn:
                expected = conn.execute(QUERY + ' ORDER BY book_id').fetchall()
                sink = conn.execute('SELECT * FROM ' + ('pgd.groups' if self.binary else 'rw_sink') +
                                    ' ORDER BY book_id').fetchall()
            if sink != expected:
                return False
            if not self.binary:
                with self.connect(native=True) as conn:
                    return conn.execute('SELECT * FROM totals ORDER BY book_id').fetchall() == expected
            return True
        except psycopg.Error:
            return False

    def objects(self):
        count, size, token = 0, 0, None
        while True:
            params = {'list-type': '2'}
            if token:
                params['continuation-token'] = token
            with urllib.request.urlopen(f'http://127.0.0.1:{self.s3_port}/pgderive-benchmark?' +
                                        urlencode(params), timeout=10) as response:
                page = ET.fromstring(response.read())
            # Strip namespace so S3 implementations with/without it both work.
            for node in page.iter():
                node.tag = node.tag.split('}')[-1]
            for item in page.findall('Contents'):
                count += 1; size += int(item.findtext('Size'))
            if page.findtext('IsTruncated') != 'true':
                return {'objects': count, 'bytes': size}
            token = page.findtext('NextContinuationToken')

    def close(self, success=False):
        errors = []
        def attempt(label, action):
            try:
                return action()
            except Exception as error:
                errors.append(label + ': ' + repr(error))
                return None
        for process in reversed(self.processes):
            if process.poll() is None:
                attempt('signal process', lambda p=process: p.send_signal(
                    signal.SIGINT if p is self.worker else signal.SIGTERM))
                try:
                    process.wait(timeout=120 if process is self.worker else 10)
                except subprocess.TimeoutExpired:
                    errors.append('Forced process kill')
                    attempt('kill process', process.kill)
                    attempt('wait killed process', lambda p=process: p.wait(timeout=10))
                except Exception as error:
                    errors.append('wait process: ' + repr(error))
        if self.worker and self.worker.returncode != 0:
            errors.append('Pgderive shutdown returned ' + str(self.worker.returncode))
        for reader in self.readers:
            attempt('join log reader', lambda r=reader: r.join(timeout=5))
        for stream in self.files:
            attempt('close log file', stream.close)
        removed, mounts = [], {}
        for name in reversed(self.containers):
            try:
                details = json.loads(docker('inspect', name))[0]
                mounts[name] = details['Mounts']
                if details['Config']['Labels'].get('pgderive.performance') != self.name:
                    raise RuntimeError('Ownership mismatch; refusing cleanup: ' + name)
                with (self.directory / (name + '.log')).open('w') as log:
                    subprocess.run(['docker', 'logs', name], stdout=log, stderr=subprocess.STDOUT, check=False)
                docker('rm', '-f', '-v', name); removed.append(name)
            except Exception as error:
                errors.append('container ' + name + ': ' + repr(error))
        scratch_removed = False
        scratch = self.directory / 'scratch'
        if success and not errors and scratch.exists():
            attempt('remove scratch', lambda: shutil.rmtree(scratch))
            scratch_removed = not scratch.exists()
        save(self.directory / 'cleanup.json', {'removed_containers': removed, 'mounts': mounts,
             'scratch_removed': scratch_removed,
             'docker_volumes_created': sum(m['Type'] == 'volume' for ms in mounts.values() for m in ms),
             'errors': errors})
        return errors
