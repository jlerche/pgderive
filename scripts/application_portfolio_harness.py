#!/usr/bin/env python3
"""Seeded finite application grammar, source model, native MV, replay and reduction."""
import argparse
import collections
import decimal
import itertools
import json
import os
import pathlib
import subprocess
import sys
import time
from application_model import VERSION, apply, expected, generated, query, scalar, state, statements
from application_reduce import reduce_history
from application_trace import validate
from application_evidence import checked_phases, process, provenance, requests
from native_mv_oracle import expected as native_expected
from sql_compiler_harness import SqlFixture
from worker_harness import Fixture

SERIAL = itertools.count()
PORTFOLIO = pathlib.Path('queries/application/portfolio.json')


class DifferentialFailure(AssertionError):
    def __init__(self, kind, detail):
        super().__init__(kind+': '+str(detail))
        self.kind = kind


class ApplicationFixture(SqlFixture):
    def __init__(self, output, trace):
        self.initialize_sources(output, trace)
        self.query = self.query_specification['sql'].format(schema=self.name)
        self.labels = self.output_labels

    def create_sources(self):
        self.sql(f'ALTER TABLE {self.name}.auction RENAME TO account; ALTER TABLE {self.name}.account RENAME COLUMN category TO label; ALTER TABLE {self.name}.account ADD COLUMN cohort integer; ALTER TABLE {self.name}.bid RENAME TO entry; ALTER TABLE {self.name}.entry RENAME COLUMN auction TO account_id; ALTER TABLE {self.name}.entry RENAME COLUMN price TO amount')

    def initialize_sources(self, output, trace):
        Fixture.__init__(self, output, f'app_{next(SERIAL)}', rows=0, auctions=0)
        self.create_sources()
        self.trace = trace
        self.model = state(trace['initial'])
        for table, rows in trace['initial'].items():
            for start in range(0,len(rows),64):
                chunk = rows[start:start+64]
                values = ','.join('('+','.join(scalar(value) for value in row.values())+')' for row in chunk)
                self.sql(f"INSERT INTO {self.name}.{table}({','.join(chunk[0])}) VALUES {values}")

    @property
    def output_labels(self):
        return {'projection':['label','bucket'], 'group':['group_key','rows','present','unique_amounts','total'],
                       'join':['label','amount'], 'left':['label','amount'], 'derived':['group_key','rows'],
                       'window':['id','account_id','total']}[self.query_specification['recipe']]

    def cleanup(self):
        for worker in self.workers:
            worker.abort()
        table = f'{self.name}.pgderive_worker_registration'
        if self.sql(f"SELECT to_regclass('{table}') IS NOT NULL").strip() == 't':
            for slot in self.sql(f'SELECT slot_name FROM {table}').splitlines():
                self.sql(f"SELECT pg_drop_replication_slot(slot_name) FROM pg_replication_slots WHERE slot_name='{slot}'")
        self.sql(f'DROP PUBLICATION {self.name}; DROP SCHEMA {self.name} CASCADE')

    def config(self, maximum=0):
        path = super().config(maximum)
        path.write_text(path.read_text().replace('retry_attempts=8','retry_attempts=1').replace('maintenance_ticks=4','maintenance_ticks=4\nphase_metrics=true'))
        return path

    @property
    def query_specification(self):
        return self.trace['spec']

    def memory(self):
        return expected(self.trace['spec'],self.model)

    def verify(self):
        native = native_expected(self,self.labels)
        if self.bag_difference(native).strip() != '0':
            raise DifferentialFailure('native_bag',self.query)
        oracle = self.name+'.pgderive_native_oracle'
        public = self.name+'.groups'
        metadata = "SELECT json_agg(json_build_array(attname,atttypid,atttypmod,attcollation) ORDER BY attnum) FROM pg_attribute WHERE attrelid='%s'::regclass AND attnum>0 AND NOT attisdropped"
        if json.loads(self.sql(metadata % oracle)) != json.loads(self.sql(metadata % public)):
            raise DifferentialFailure('output_metadata',self.query)
        for table in self.model:
            actual = json.loads(self.sql(f"SELECT COALESCE(json_agg(t ORDER BY id),'[]') FROM {self.name}.{table} t"))
            if actual != sorted(self.model[table].values(),key=lambda row:row['id']):
                raise DifferentialFailure('source_model',table)
        actual = json.loads(self.sql(f"SELECT COALESCE(json_agg(g),'[]') FROM {self.bag_relation()} g"),parse_float=decimal.Decimal)
        bag = {tuple(row['tuple'][1]):row['weight'] for row in actual}
        if bag != dict(self.memory()):
            raise DifferentialFailure('memory_oracle',self.query)
        assert self.acknowledgement_ahead().strip() == '0'
        return collections.Counter(bag)


def admin(fixture, command, sql):
    original = fixture.query
    fixture.query = sql
    config = fixture.config()
    fixture.query = original
    binary = str(pathlib.Path(command[0]).with_name('pgderive'))
    return subprocess.run([binary,'query','validate',str(config)],env=fixture.environment,
                          capture_output=True,text=True,timeout=60)


def inventory(fixture, command):
    records = []
    for item in json.loads(PORTFOLIO.read_text())['rejected']:
        sql = item['sql'].format(schema=fixture.name)
        fixture.sql('EXPLAIN '+sql)  # Native SQL must be valid before engine rejection counts.
        result = admin(fixture,command,sql)
        records.append({**item,'native_valid':True,'returncode':result.returncode,'stderr':result.stderr})
        (fixture.output/'rejections.json').write_text(json.dumps(records,indent=2)+'\n')
        assert result.returncode != 0 and item['diagnostic'] in result.stderr, records[-1]
    assert fixture.sql(f"SELECT count(*) FROM pg_replication_slots WHERE slot_name LIKE '{fixture.name}%'").strip() == '0'


def replay(output, command, trace, check_inventory=False, *, fixture_factory=ApplicationFixture, validator=validate):
    validator(trace)
    output.mkdir(parents=True,exist_ok=False)
    (output/'replay.json').write_text(json.dumps(trace,ensure_ascii=False,indent=2)+'\n')
    fixture = fixture_factory(output,trace)
    with fixture.managed():
        if check_inventory:
            inventory(fixture,command)
        provenance(output,command,fixture)
        proxy = output.parent/'proxy.jsonl'
        offset = len(proxy.read_text().splitlines())
        launched = time.perf_counter()
        worker = fixture.start(command,'initial')
        ready = worker.event('ready')
        initial_startup_ms = (time.perf_counter()-launched)*1000
        assert isinstance(ready['open_recovery_us'],int) and ready['open_recovery_us']>=0
        initial_process = process(worker)
        bag = fixture.verify()
        records, processes = [], []
        committed = 0
        base_time = ready['time']
        generation = 'initial'
        restarted = False
        for ordinal,transaction in enumerate(trace['history']):
            if transaction.get('restart'):
                processes.append(process(worker))
                worker.abort()
                launched = time.perf_counter()
                worker = fixture.start(command,f'cold-{ordinal}')
                reopened = worker.event('ready')
                cold_startup_ms = (time.perf_counter()-launched)*1000
                assert reopened['slot']==ready['slot'] and reopened['time']==base_time+committed
                assert reopened['open_recovery_us']>=0
                fixture.verify()
                records.append({'restart':True,'open_recovery_us':reopened['open_recovery_us'], 'client_startup_ms':cold_startup_ms})
                generation = f'cold-{ordinal}'
                restarted = True
                continue
            prior = fixture.sql(f'SELECT to_jsonb(p) FROM {fixture.name}.pgderive_progress p') if transaction['rollback'] else None
            began = time.perf_counter()
            fixture.sql(statements(fixture.name,transaction))
            committed_at = time.perf_counter()
            apply(fixture.model,transaction)
            if transaction['rollback']:
                assert fixture.sql(f'SELECT to_jsonb(p) FROM {fixture.name}.pgderive_progress p') == prior
                fixture.verify()
                continue
            committed += 1
            event = worker.event('published',minimum_time=base_time+committed)
            observed = time.perf_counter()
            assert event['event']=='published', 'unexpected worker retry masked a failed tick'
            phases = checked_phases(event)
            after = fixture.verify()
            records.append({'ordinal':ordinal,'generation':generation,'client_to_published_ms':(observed-began)*1000,
                            'source_sql_call_ms':(committed_at-began)*1000,'await_published_ms':(observed-committed_at)*1000,
                            'phases_us':phases,'publication_us':event['publication_us'],
                            'expected_insert_occurrences':sum((after-bag).values()),
                            'expected_delete_occurrences':sum((bag-after).values()),'output_occurrences':sum(after.values())})
            bag = after
        processes.append(process(worker))
        worker.finish(graceful=True)
        metric_files = sorted(output.glob('*/metrics.jsonl'))
        maintenance = [event for path in metric_files for line in path.read_text().splitlines()
                       if (event:=json.loads(line)).get('event')=='maintenance']
        assert all('elapsed_us' in event and event['elapsed_us']>=0 for event in maintenance)
        summary = {'version':1,'id':trace['id'],'seed':trace['seed'],'query':fixture.query_specification,
                   'transactions':committed,'restart':restarted,'initial_open_recovery_us':ready['open_recovery_us'],
                   'initial_client_startup_ms':initial_startup_ms,'initial_ready_process':initial_process,'process_generations':processes,
                   'source_rows':{table:len(rows) for table,rows in trace['initial'].items()},
                   'initial_key_distribution':dict(collections.Counter(str(row['account_id']) for row in trace['initial']['entry'])),
                   'ticks':records,'maintenance':maintenance,'object_requests':requests(proxy,offset),
                   'oracles':'native MV exact bag + metadata and independent full-source model',
                   'scope':'seeded finite grammar; not exhaustive SQL or production capacity'}
        (output/'result.json').write_text(json.dumps(summary,indent=2)+'\n')
        return summary


def run_case(root, command, trace, label, check_inventory=False, **replay_options):
    output = root/label
    try:
        return replay(output,command,trace,check_inventory,**replay_options)
    except DifferentialFailure as original:
        (output/'failure.json').write_text(json.dumps({'kind':original.kind,'original':trace,'status':'reducing'},indent=2)+'\n')
        attempts = itertools.count()
        def fails(history):
            candidate = {**trace,'history':history}
            try:
                replay(root/f'{label}-reduction-{next(attempts)}',command,candidate,**replay_options)
            except DifferentialFailure as error:
                return error.kind == original.kind
            except (AssertionError,RuntimeError,subprocess.SubprocessError,OSError):
                return False
            return False
        minimized = reduce_history(trace['history'],fails)
        (output/'failure.json').write_text(json.dumps({'kind':original.kind,'original':trace,'reduction':minimized},indent=2)+'\n')
        (output/'minimized-replay.json').write_text(json.dumps({**trace,'history':minimized['history']},indent=2)+'\n')
        raise


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('output',type=pathlib.Path)
    parser.add_argument('--replay',type=pathlib.Path)
    if '--' not in sys.argv[1:]:
        parser.error('worker executable required after --')
    split = sys.argv.index('--')
    args = parser.parse_args(sys.argv[1:split])
    command = sys.argv[split+1:]
    assert command, 'worker executable required after --'
    if args.replay:
        trace = json.loads(args.replay.read_text())
        assert trace['version']==VERSION
        run_case(args.output,command,trace,'replay')
        return
    portfolio = json.loads(PORTFOLIO.read_text())['accepted']
    rounds = int(os.environ.get('PGDERIVE_PORTFOLIO_ROUNDS','1' if os.environ.get('PGDERIVE_CHECK_PROFILE')=='core' else '2'))
    transactions = int(os.environ.get('PGDERIVE_PORTFOLIO_TRANSACTIONS','14'))
    rows = int(os.environ.get('PGDERIVE_PORTFOLIO_ROWS','24'))
    auctions = int(os.environ.get('PGDERIVE_PORTFOLIO_ACCOUNTS','8'))
    assert 1<=rounds<=8 and 6<=transactions<=256 and 4<=rows<=4096 and 4<=auctions<=128
    for round_ in range(rounds):
        for index,item in enumerate(portfolio):
            seed = 1701+round_*10007+index*1009
            initial, history = generated(seed,rows,auctions,transactions)
            history.insert(len(history)//2,{'restart':True})
            trace = {'version':VERSION,'id':item['id'],'seed':seed,'spec':query(item['recipe'],seed),
                     'initial':initial,'history':history}
            run_case(args.output,command,trace,f"{item['id']}-{seed}",check_inventory=(round_==index==0))


if __name__=='__main__':
    main()
