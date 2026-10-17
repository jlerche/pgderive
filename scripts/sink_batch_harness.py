from harness_control import commit_proxy
#!/usr/bin/env python3
"""Batched physical occurrences, observer counts and atomic failure/recovery."""
import collections
import contextlib
import json
from result_table_harness import NumericFixture
from native_mv_oracle import expected as native_expected
from worker_harness import harness_arguments


class SinkFixture(NumericFixture):
    def __init__(self, output, rows=1800):
        super().__init__(output)
        self.sql(f"DELETE FROM {self.name}.bid; INSERT INTO {self.name}.bid SELECT id,CASE WHEN id%17=0 THEN NULL ELSE (id-1)/3 END,CASE WHEN id%3=0 THEN 1.00::numeric WHEN id%3=1 THEN 1.0::numeric ELSE NULL END FROM generate_series(1,{rows}) id")
        self.query=f'SELECT auction AS key,price AS number FROM {self.name}.bid'

    def config(self, maximum=0):
        path=super().config(maximum)
        text=path.read_text().split('[worker.query]')[0]
        path.write_text(text.replace('maintenance_ticks=4','maintenance_ticks=4\nphase_metrics=true')+'[worker.query]\nsql='+json.dumps(self.query)+'\n')
        return path

    def verify(self):
        native_expected(self,['key','number'])
        source=f'SELECT int4send(auction),numeric_send(price) FROM {self.name}.bid'
        result=f'SELECT int4send(key),numeric_send(number) FROM {self.name}.groups'
        assert self.sql(f'SELECT count(*) FROM (({source} EXCEPT ALL {result}) UNION ALL ({result} EXCEPT ALL {source})) d').strip()=='0'
        source=json.loads(self.sql(f"SELECT coalesce(json_agg(json_build_array(auction,price::text)),'[]') FROM {self.name}.bid"))
        result=json.loads(self.sql(f"SELECT coalesce(json_agg(json_build_array(key,number::text)),'[]') FROM {self.name}.groups"))
        assert collections.Counter(map(tuple,source))==collections.Counter(map(tuple,result))
        assert self.acknowledgement_ahead().strip()=='0'

    def boundary(self):
        return self.sql(f"SELECT jsonb_build_object('progress',(SELECT to_jsonb(p) FROM {self.name}.pgderive_progress p),'queries',(SELECT jsonb_agg(to_jsonb(q) ORDER BY query_id) FROM {self.name}.pgderive_queries q),'sink',(SELECT jsonb_agg(jsonb_build_array(key,number::text) ORDER BY key,number::text) FROM {self.name}.groups))")

    def index(self):
        oid=self.sql(f"SELECT '{self.name}.groups'::regclass::oid").strip()
        return f'{self.name}.pgderive_sink_lookup_{oid}'


@contextlib.contextmanager
def observing(fixture):
    path=fixture.output/'sink-observer.jsonl'
    proxy, port = commit_proxy(path, 'observe', upstream=fixture.port)
    prior=fixture.environment.get('PGDERIVE__POSTGRES__PORT')
    try:
        fixture.environment['PGDERIVE__POSTGRES__PORT']=str(port)
        yield path
    finally:
        for worker in fixture.workers: worker.abort()
        if prior is None: fixture.environment.pop('PGDERIVE__POSTGRES__PORT',None)
        else: fixture.environment['PGDERIVE__POSTGRES__PORT']=prior
        proxy.terminate(); proxy.wait(timeout=10)


def counts(path):
    return collections.Counter(record['sink_dml'] for line in path.read_text().splitlines()
                               if 'sink_dml' in (record:=json.loads(line)))


def failed(fixture,worker,label):
    worker.process.wait(timeout=60)
    assert worker.process.returncode!=0
    worker.abort()
    error=(fixture.output/label/'stderr.log').read_text()
    assert 'result occurrence missing' in error,error


def qualify(output,command):
    fixture=SinkFixture(output)
    with fixture.managed(),observing(fixture) as observed:
        worker=fixture.start(command,'initial'); ready=worker.event('ready')
        fixture.verify()
        initial=counts(observed)
        assert 1<initial['insert']<10,initial
        before=counts(observed)
        fixture.sql(f'UPDATE {fixture.name}.bid SET auction=coalesce(auction,-1)+10000')
        event=worker.event('published',minimum_time=ready['time']+1)
        fixture.verify()
        mutations=counts(observed)-before
        assert 1<mutations['delete']<10 and 1<mutations['insert']<10,mutations
        worker.finish(graceful=True)
        progress=fixture.sql(f'SELECT to_jsonb(p) FROM {fixture.name}.pgderive_progress p')
        fixture.sql(f'DROP INDEX {fixture.index()}')
        worker=fixture.start(command,'backfilled'); reopened=worker.event('ready')
        assert reopened['time']==event['time'] and reopened['slot']==ready['slot']
        fixture.verify()
        assert fixture.sql(f"SELECT to_regclass('{fixture.index()}') IS NOT NULL").strip()=='t'
        worker.finish(graceful=True)
        # Wrong definitions are never dropped/replaced automatically.
        fixture.sql(f'DROP INDEX {fixture.index()}; CREATE INDEX {fixture.index().split(".")[1]} ON {fixture.name}.groups ((1))')
        boundary=fixture.boundary()
        invalid=fixture.start(command,'wrong-index'); invalid.process.wait(timeout=60); invalid.abort()
        assert invalid.process.returncode!=0
        assert 'lookup index definition changed' in (output/'wrong-index/stderr.log').read_text()
        assert fixture.boundary()==boundary
        fixture.sql(f'DROP INDEX {fixture.index()}')
        worker=fixture.start(command,'restored'); worker.event('ready'); fixture.verify()
        before=counts(observed)
        fixture.sql(f'DELETE FROM {fixture.name}.bid')
        worker.event('published'); fixture.verify(); worker.finish(graceful=True)
        assert counts(observed)['delete']-before['delete']>1
        (output/'result.json').write_text(json.dumps({'ordinary_table':True,'initial_batches':initial,'mutation_batches':mutations,
            'sink_phase_us':event['publication_us']['sink_dml'],'binary_source_oracle':True,'native_mv':True,
            'backfill_same_boundary':True,'wrong_index_rejected':True,'final_drain':True,'initial_progress':progress})+'\n')


def faults(output,command):
    fixture=SinkFixture(output)
    with fixture.managed(),observing(fixture) as observed:
        worker=fixture.start(command,'initial'); ready=worker.event('ready'); fixture.verify()
        fixture.sql(f"CREATE FUNCTION {fixture.name}.suppress_last() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN IF OLD.key>400 THEN RETURN NULL; END IF; RETURN OLD; END $$; CREATE TRIGGER suppress_last BEFORE DELETE ON {fixture.name}.groups FOR EACH ROW EXECUTE FUNCTION {fixture.name}.suppress_last()")
        before=fixture.boundary(); mutations=counts(observed)
        acknowledged=fixture.sql(f"SELECT confirmed_flush_lsn FROM pg_replication_slots WHERE slot_name='{ready['slot']}'")
        fixture.sql(f'UPDATE {fixture.name}.bid SET auction=coalesce(auction,-1)+10000')
        failed(fixture,worker,'initial')
        assert counts(observed)['delete']-mutations['delete']>1,'failure did not occur after a completed chunk'
        assert fixture.boundary()==before
        assert fixture.sql(f"SELECT confirmed_flush_lsn FROM pg_replication_slots WHERE slot_name='{ready['slot']}'")==acknowledged
        fixture.sql(f'DROP TRIGGER suppress_last ON {fixture.name}.groups; DROP FUNCTION {fixture.name}.suppress_last()')
        worker=fixture.start(command,'recovered'); worker.event('ready'); worker.event('published',minimum_time=ready['time']+1); fixture.verify()
        # Remove one sink occurrence to force underflow after other chunks mutate.
        fixture.sql(f'DELETE FROM {fixture.name}.groups WHERE ctid IN (SELECT ctid FROM {fixture.name}.groups WHERE key=10599 LIMIT 1)')
        before=fixture.boundary()
        fixture.sql(f'DELETE FROM {fixture.name}.bid')
        failed(fixture,worker,'recovered')
        assert fixture.boundary()==before
        (output/'result.json').write_text(json.dumps({'suppressed_late_chunk':'entire sink/membership/progress rollback','ack_unchanged':True,
            'replay_after_repair':True,'missing_occurrence':'complete rollback','observer':dict(counts(observed))})+'\n')


if __name__=='__main__':
    output,command=harness_arguments()
    for mode,function in [('bulk',qualify),('faults',faults)]:
        path=output/mode; path.mkdir(); function(path,command)
