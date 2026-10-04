#!/usr/bin/env python3
"""Sequential, isolated continuous-worker qualification; retain failures and metrics.

Run through run_with_s3_proxy.py for a recorded storage latency profile. The
command after -- is an instrumented or ordinary worker executable prefix.
"""
import argparse
import json
import os
import pathlib
import queue
import signal
import subprocess
import sys
import threading
import time


class Worker:
    def __init__(self, command, config, output, environment):
        self.events = queue.Queue()
        self.metrics = (output / 'metrics.jsonl').open('w')
        self.stderr = (output / 'stderr.log').open('w')
        self.process = subprocess.Popen(command + [str(config)], env=environment,
                                        stdout=subprocess.PIPE, stderr=self.stderr, text=True)
        self.reader = threading.Thread(target=self.read, daemon=True)
        self.reader.start()

    def read(self):
        for line in self.process.stdout:
            self.metrics.write(line)
            self.metrics.flush()
            self.events.put(json.loads(line))
        self.events.put(None)

    def event(self, kind, seconds=240, minimum_time=None):
        deadline = time.monotonic() + seconds
        while time.monotonic() < deadline:
            event = self.events.get(timeout=max(.01, deadline - time.monotonic()))
            if event is None:
                raise RuntimeError(f'worker ended before {kind}; see {self.stderr.name}')
            if minimum_time is not None:
                if event['event'] in ('published', 'ready') and event['time'] >= minimum_time:
                    return event
            elif event['event'] == kind:
                return event
        raise TimeoutError(f'worker did not emit {kind}')

    def finish(self, graceful=False):
        if graceful:
            self.process.send_signal(signal.SIGINT)
        self.process.wait(timeout=240)
        self.reader.join(timeout=10)
        assert not self.reader.is_alive(), 'stdout reader remained alive'
        self.metrics.close()
        self.stderr.close()
        assert self.process.returncode == 0, f'worker failed; see {self.stderr.name}'

    def abort(self):
        if self.process.poll() is None:
            self.process.kill()
            self.process.wait(timeout=10)
        self.reader.join(timeout=10)
        self.metrics.close()
        self.stderr.close()


class Fixture:
    def __init__(self, output, index, rows=256, auctions=32):
        self.output = output
        self.name = f'pgderive_worker_{os.getpid()}_{index}'
        self.environment = os.environ.copy()
        self.environment['PGPASSWORD'] = os.environ.get('PGPASSWORD', 'postgres')
        self.environment['PGDERIVE__POSTGRES__PASSWORD'] = self.environment['PGPASSWORD']
        self.pg = ['psql', '-h', '127.0.0.1', '-p', '55434', '-U', 'postgres',
                   '-d', 'pgderive_dev', '-v', 'ON_ERROR_STOP=1', '-Atc']
        self.workers = []
        self.rows = rows
        self.auctions = auctions
        self.sql(f'''CREATE SCHEMA {self.name};
          CREATE TABLE {self.name}.auction(id int PRIMARY KEY,category text);
          CREATE TABLE {self.name}.bid(id int PRIMARY KEY,auction int,price bigint);
          ALTER TABLE {self.name}.auction REPLICA IDENTITY FULL;
          ALTER TABLE {self.name}.bid REPLICA IDENTITY FULL;
          INSERT INTO {self.name}.auction SELECT i,CASE WHEN i%7=0 THEN NULL ELSE 'g'||(i%3) END FROM generate_series(1,{auctions}) i;
          INSERT INTO {self.name}.bid SELECT i,CASE WHEN i%11=0 THEN NULL ELSE 1+(i%{auctions}) END,CASE WHEN i%5=0 THEN NULL ELSE i END FROM generate_series(1,{rows}) i;
          CREATE PUBLICATION {self.name} FOR TABLE {self.name}.auction,{self.name}.bid''')

    def sql(self, text):
        return subprocess.check_output(self.pg + [text], env=self.environment, text=True)

    def config(self, maximum=0):
        path = self.output / 'config.toml'
        path.write_text(f'''[postgres]
host="127.0.0.1"
port=55434
database="pgderive_dev"
user="postgres"
tls="disable"
[replication]
slot="{self.name}"
publication="{self.name}"
[object_store]
endpoint="http://127.0.0.1:8333"
bucket="pgderive-mvp-tests"
access_key_id="local"
secret_access_key="local"
[worker]
catalog_schema="{self.name}"
query_id="grouped"
sink_table="groups"
object_prefix="{self.name}"
block_rows=64
maintenance_ticks=4
retry_attempts=8
retry_delay_ms=100
max_transactions={maximum}
[worker.query]
left_schema="{self.name}"
left_table="auction"
left_key="id"
group="category"
right_schema="{self.name}"
right_table="bid"
right_key="auction"
sum="price"
[execution]
resident_entries=128
resident_bytes=65536
record_bytes=16384
''')
        return path

    def start(self, command, label, maximum=0):
        output = self.output / label
        output.mkdir()
        worker = Worker(command, self.config(maximum), output, self.environment)
        self.workers.append(worker)
        return worker

    def verify(self):
        # Compare every weighted aggregate with an independent PostgreSQL query.
        difference = self.sql(f'''WITH expected AS (
          SELECT COALESCE(to_jsonb(a.category),'null'::jsonb) group_key,count(*) row_count,sum(b.price) total
          FROM {self.name}.auction a JOIN {self.name}.bid b ON a.id=b.auction GROUP BY a.category),
          actual AS (SELECT group_key,row_count,total FROM {self.name}.groups)
          SELECT count(*) FROM ((SELECT * FROM expected EXCEPT SELECT * FROM actual)
          UNION ALL (SELECT * FROM actual EXCEPT SELECT * FROM expected)) differences''').strip()
        assert difference == '0', 'worker sink differs from independent SQL'
        beyond = self.sql(f'''SELECT count(*) FROM pg_replication_slots s
          JOIN {self.name}.pgderive_worker_registration r ON r.slot_name=s.slot_name
          CROSS JOIN {self.name}.pgderive_progress p
          WHERE s.confirmed_flush_lsn>p.end_lsn''').strip()
        assert beyond == '0', 'slot acknowledged beyond durable publication'

    def cleanup(self):
        slots = self.sql(f'SELECT slot_name FROM {self.name}.pgderive_worker_registration').splitlines()
        for slot in slots:
            self.sql(f"SELECT pg_drop_replication_slot('{slot}')")
        self.sql(f'DROP PUBLICATION {self.name}; DROP SCHEMA {self.name} CASCADE')

    def preserve(self):
        for worker in self.workers:
            worker.abort()
        subprocess.run(['pg_dump', '-h', '127.0.0.1', '-p', '55434', '-U', 'postgres',
                        '-d', 'pgderive_dev', f'--schema={self.name}',
                        f'--file={self.output}/failure-fixture.sql'], env=self.environment, check=False)


def qualify(output, command, ticks, index=0, mode=None, target=None, kill=False, burst=0, rows=256, auctions=32):
    fixture = Fixture(output, index, rows=rows, auctions=auctions)
    proxy = None
    proxy_log = None
    if mode:
        proxy_log = (output / 'sql-fault.jsonl').open('w')
        marker = {'journal': 'pgderive_worker_registration(query_id,',
                  'activation': 'pgderive_progress(query_id,binding',
                  'publication': 'pgderive_progress SET commit_lsn'}[target]
        proxy = subprocess.Popen([sys.executable, '-u', 'scripts/pg_commit_proxy.py',
                                  '--mode', mode, '--once', '--arm-query', marker],
                                 stdout=proxy_log, stderr=proxy_log)
    try:
        if proxy:
            deadline = time.monotonic() + 10
            while time.monotonic() < deadline:
                lines = (output / 'sql-fault.jsonl').read_text().splitlines()
                if lines:
                    fixture.environment['PGDERIVE__POSTGRES__PORT'] = str(json.loads(lines[0])['port'])
                    break
                time.sleep(.05)
            else:
                raise RuntimeError('SQL fault proxy did not start')
        worker = fixture.start(command, 'first')
        ready = worker.event('ready')
        fixture.verify()
        for tick in range(ticks):
            # Skew toward one join key, NULL sums, and both inputs in one transaction.
            fixture.sql(f'''BEGIN;
              UPDATE {fixture.name}.auction SET category='shift{tick%2}' WHERE id=1;
              UPDATE {fixture.name}.bid SET auction=1,price=CASE WHEN id%5=0 THEN NULL ELSE price+1 END WHERE id BETWEEN 1 AND 16;
              INSERT INTO {fixture.name}.bid VALUES({1000000+tick},1,{tick});
              DELETE FROM {fixture.name}.bid WHERE id={1000000+tick}; COMMIT''')
            worker.event('published', minimum_time=ready['time'] + tick + 1)
            fixture.verify()
        # Let the producer run ahead, then drain all committed ticks. SQL comparison
        # is meaningful only after the complete burst has reached durable output.
        for tick in range(burst):
            fixture.sql(f'UPDATE {fixture.name}.bid SET price=price+1 WHERE id BETWEEN 1 AND 64')
        for tick in range(burst):
            worker.event('published', minimum_time=ready['time'] + ticks + tick + 1)
        fixture.verify()
        ticks += burst
        if kill:
            worker.abort()
        else:
            worker.finish(graceful=True)
        fixture.verify()
        resumed = fixture.start(command, 'resumed', maximum=1)
        reopened = resumed.event('ready')
        assert reopened['slot'] == ready['slot'], 'resume recreated physical slot'
        assert reopened['time'] == ready['time'] + ticks, 'resume lost or duplicated a transaction'
        fixture.sql(f'UPDATE {fixture.name}.bid SET price=price+1 WHERE id=1')
        resumed.event('published')
        resumed.finish()
        fixture.verify()
        (output / 'result.json').write_text(json.dumps({'ticks': ticks + 1,
            'slot': reopened['slot'], 'sql_oracle': 'exact', 'restart': 'same durable slot'}) + '\n')
        if proxy:
            faults = [json.loads(line) for line in (output / 'sql-fault.jsonl').read_text().splitlines()[1:]]
            assert len(faults) == 1 and faults[0]['server_committed'] == (mode == 'after'), faults
            events = [json.loads(line) for line in (output / 'first/metrics.jsonl').read_text().splitlines()]
            assert sum(event['event'] == 'retry' for event in events) == 1, events
        fixture.cleanup()
    except BaseException:
        fixture.preserve()
        raise
    finally:
        if proxy:
            proxy.terminate()
            proxy.wait(timeout=10)
            proxy_log.close()


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('output', type=pathlib.Path)
    parser.add_argument('--ticks', type=int, default=12)
    parser.add_argument('--faults', action='store_true')
    parser.add_argument('--kill', action='store_true')
    parser.add_argument('--burst', type=int, default=0)
    parser.add_argument('--rows', type=int, default=256)
    parser.add_argument('--auctions', type=int, default=32)
    parser.add_argument('command', nargs=argparse.REMAINDER)
    arguments = parser.parse_args()
    command = arguments.command
    if command and command[0] == '--':
        command = command[1:]
    if not command:
        parser.error('provide the worker command after --')
    qualify(arguments.output, command, arguments.ticks, kill=arguments.kill, burst=arguments.burst, rows=arguments.rows, auctions=arguments.auctions)
    if arguments.faults:
        index = 1
        for target in ('journal', 'activation', 'publication'):
            for mode in ('before', 'after'):
                output = arguments.output / f'{mode}-{target}'
                output.mkdir()
                qualify(output, command, 3, index=index, mode=mode, target=target)
                index += 1
