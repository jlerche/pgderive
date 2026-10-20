#!/usr/bin/env python3
"""Owned tmpfs local example: run, mutate, compare, restart, inspect stdout, clean up."""
import argparse
import json
import os
import pathlib
import queue
import shutil
import signal
import subprocess
import threading
import time
import urllib.request
import uuid

ROOT = pathlib.Path(__file__).resolve().parents[2]
HERE = pathlib.Path(__file__).resolve().parent
OBJECT_IMAGE = 'chrislusf/seaweedfs@sha256:4e61d15fd35994cb1e43e1e553dff106794841fd9a99ade2fc8c8bfce4d7872d'


class Worker:
    def __init__(self, binary, action, config, log, environment):
        self.log = log.open('w')
        self.messages = queue.Queue()
        self.process = subprocess.Popen([str(binary),'query',action,str(config)],env=environment,
                                        stdout=subprocess.PIPE,text=True)
        self.reader = threading.Thread(target=self.read,daemon=True)
        self.reader.start()

    def read(self):
        for line in self.process.stdout:
            self.log.write(line)
            self.log.flush()
            self.messages.put(json.loads(line))
        self.messages.put(None)

    def event(self, kind):
        deadline = time.monotonic()+120
        while time.monotonic()<deadline:
            event = self.messages.get(timeout=max(.01,deadline-time.monotonic()))
            if event is None: raise RuntimeError('worker ended before '+kind)
            if event['event']==kind: return event
        raise TimeoutError(kind)

    def stop(self):
        if self.process.poll() is None:
            self.process.send_signal(signal.SIGINT)
        self.process.wait(timeout=30)
        self.reader.join(timeout=5)
        self.log.close()
        assert self.process.returncode==0,'worker failed'


def command(*args, **kwargs):
    return subprocess.check_output(list(args),text=True,**kwargs).strip()


def ready(action):
    deadline = time.monotonic()+60
    while time.monotonic()<deadline:
        try:
            action()
            return
        except (subprocess.CalledProcessError,OSError):
            time.sleep(.2)
    raise TimeoutError('local service did not become ready')


def configuration(output, pg_port, object_port, diagnostic=False):
    path = output/('stdout.toml' if diagnostic else 'worker.toml')
    query = (HERE/'query.sql').read_text().strip()
    path.write_text(f'''[postgres]
host="127.0.0.1"
port={pg_port}
database="pgderive_demo"
user="postgres"
tls="disable"
[replication]
slot="demo_{'stdout' if diagnostic else 'table'}"
publication="demo_source"
[object_store]
endpoint="http://127.0.0.1:{object_port}"
bucket="pgderive-demo"
access_key_id="local"
secret_access_key="local"
[worker]
catalog_schema="demo_engine"
query_id="{'stdout' if diagnostic else 'totals'}"
sink_table="{'diagnostic' if diagnostic else 'totals'}"
sink_output="{'stdout' if diagnostic else 'postgres'}"
object_prefix="demo"
block_rows=64
maintenance_ticks=4
retry_attempts=2
retry_delay_ms=100
max_transactions=0
[worker.query]
sql={json.dumps(query)}
''')
    return path


def run(args):
    for executable in ('docker','psql','cargo'):
        if shutil.which(executable) is None: raise RuntimeError(executable+' is required')
    output = args.output.resolve()
    output.mkdir(parents=True,exist_ok=False)
    subprocess.run(['cargo','build','--locked','--bin','pgderive'],cwd=ROOT,check=True)
    binary = ROOT/'target/debug/pgderive'
    names = ['pgderive-example-'+uuid.uuid4().hex[:10]+suffix for suffix in ('-pg','-objects')]
    environment = {k:v for k,v in os.environ.items() if not k.startswith('PGDERIVE__')}
    environment.update(PGPASSWORD='postgres',PGDERIVE__POSTGRES__PASSWORD='postgres')
    workers = []
    try:
        command('docker','run','-d','--name',names[0],'--tmpfs','/var/lib/postgresql/data:rw',
                '-e','POSTGRES_PASSWORD=postgres','-e','POSTGRES_DB=pgderive_demo',
                '-p','127.0.0.1::5432','postgres:17','-c','wal_level=logical')
        command('docker','run','-d','--name',names[1],'--tmpfs','/data:rw',
                '-p','127.0.0.1::8333',OBJECT_IMAGE,'server','-s3','-dir=/data')
        pg_port = command('docker','port',names[0],'5432').rsplit(':',1)[1]
        object_port = command('docker','port',names[1],'8333').rsplit(':',1)[1]
        pg = ['psql','-h','127.0.0.1','-p',pg_port,'-U','postgres','-d','pgderive_demo','-v','ON_ERROR_STOP=1','-Atc']
        def sql(text): return command(*pg,text,env=environment)
        ready(lambda: command(*pg,'SELECT 1',env=environment,stderr=subprocess.DEVNULL))
        def bucket():
            with urllib.request.urlopen(urllib.request.Request(f'http://127.0.0.1:{object_port}/pgderive-demo',method='PUT'),timeout=2) as response:
                assert response.status in (200,204)
        ready(bucket)
        sql((HERE/'source.sql').read_text())
        config = configuration(output,pg_port,object_port)
        debug = configuration(output,pg_port,object_port,True)
        query = (HERE/'query.sql').read_text().strip()
        sql(f'CREATE MATERIALIZED VIEW demo.native_totals AS {query}')
        def verify():
            sql('REFRESH MATERIALIZED VIEW demo.native_totals')
            def rows(table): return f'SELECT array_send(ARRAY[int4send(auction_id),int8send(bid_count),numeric_send(total)]) FROM {table}'
            a,b = rows('demo.native_totals'),rows('demo_engine.totals')
            assert sql(f'SELECT count(*) FROM (({a} EXCEPT ALL {b}) UNION ALL ({b} EXCEPT ALL {a})) d')=='0'
            print('Native materialized-view comparison passed:',sql('SELECT row_to_json(t) FROM demo_engine.totals t ORDER BY auction_id'))
        def start(action,path,label):
            worker = Worker(binary,action,path,output/(label+'.jsonl'),environment)
            workers.append(worker)
            worker.event('ready')
            return worker
        with (output/'validation.json').open('w') as validation:
            subprocess.run([str(binary),'query','validate',str(config)],env=environment,check=True,stdout=validation)
        worker = start('create',config,'initial')
        verify()
        sql((HERE/'changes.sql').read_text())
        worker.event('published')
        verify()
        worker.stop()
        worker = start('restart',config,'restarted')
        sql('INSERT INTO demo.bid VALUES(5,2,NULL)')
        worker.event('published')
        verify()
        worker.stop()
        worker = start('create',debug,'stdout')
        worker.stop()
        records = [json.loads(line) for line in (output/'stdout.jsonl').read_text().splitlines()]
        batches = [record for record in records if record['event']=='sink_batch']
        assert batches and sql("SELECT to_regclass('demo_engine.diagnostic') IS NULL")=='t'
        print('Stdout diagnostic batch:',json.dumps(batches[0],ensure_ascii=False))
        for path in (() if args.keep else (debug,config)):
            subprocess.run([str(binary),'query','drop',str(path)],env=environment,check=True,stdout=subprocess.DEVNULL)
        result = {'native_comparison':True,'cdc':True,'cold_restart':True,'stdout':True,
                  'containers':names,'postgres_port':pg_port,'object_port':object_port,'kept':args.keep}
        (output/'result.json').write_text(json.dumps(result,indent=2)+'\n')
        print('Logs and configurations:',output)
        if args.keep: print('Cleanup command: docker rm -f '+' '.join(names))
    except BaseException:
        if 'pg_port' in locals():
            subprocess.run(['pg_dump','-h','127.0.0.1','-p',pg_port,'-U','postgres',
                            '-d','pgderive_demo','--file='+str(output/'failure-fixture.sql')],
                           env=environment,check=False)
        for index,name in enumerate(names):
            with (output/f'failure-container-{index}.log').open('w') as log:
                subprocess.run(['docker','logs',name],stdout=log,stderr=log,check=False)
        raise
    finally:
        for worker in workers:
            if worker.process.poll() is None:
                worker.process.kill()
                worker.process.wait(timeout=10)
        if not args.keep:
            subprocess.run(['docker','rm','-f',*names],check=False,stdout=subprocess.DEVNULL)


if __name__=='__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--output',type=pathlib.Path,default=ROOT/'artifacts/local-example'/uuid.uuid4().hex[:10])
    parser.add_argument('--keep',action='store_true',help='keep owned tmpfs services for manual experimentation')
    run(parser.parse_args())
