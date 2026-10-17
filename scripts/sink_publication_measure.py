#!/usr/bin/env python3
"""Serial release worker publication samples over identical owned source histories."""
import json
import pathlib
import sys
import time
from sink_batch_harness import SinkFixture,observing,counts


def probe(fixture, indexed):
    table=f'{fixture.name}.groups'
    fixture.sql(f'ANALYZE {table}')
    predicate="int4send(d.key) IS NOT DISTINCT FROM int4send(599) AND numeric_send(d.number) IS NOT DISTINCT FROM numeric_send('1.00'::numeric)"
    if indexed:
        predicate="hash_record_extended(ROW(d.key,d.number),0)=hash_record_extended(ROW(599::integer,'1.00'::numeric),0) AND array_send(ARRAY[int4send(d.key),numeric_send(d.number)])=array_send(ARRAY[int4send(599),numeric_send('1.00'::numeric)])"
    plan=json.loads(fixture.sql(f'EXPLAIN(ANALYZE,BUFFERS,FORMAT JSON) SELECT d.ctid FROM {table} d WHERE {predicate} LIMIT 1 FOR UPDATE'))
    (fixture.output/'lookup-plan.json').write_text(json.dumps(plan,indent=2)+'\n')
    return plan


def measure(output,command,indexed):
    fixture=SinkFixture(output)
    with fixture.managed(),observing(fixture) as observed:
        worker=fixture.start(command,'initial'); worker.event('ready'); fixture.verify()
        plan=probe(fixture,indexed)
        index_bytes=int(fixture.sql(f"SELECT pg_relation_size('{fixture.index()}')")) if indexed else 0
        table_bytes=int(fixture.sql(f"SELECT pg_total_relation_size('{fixture.name}.groups')"))
        ticks=[]
        for tick in range(3):
            before=counts(observed)
            started=time.perf_counter()
            fixture.sql(f'UPDATE {fixture.name}.bid SET auction=coalesce(auction,-1)+10000')
            event=worker.event('published',minimum_time=2+tick)
            elapsed=(time.perf_counter()-started)*1000
            fixture.verify()
            ticks.append({'client_to_published_ms':elapsed,'publication_us':event['publication_us'],
                          'phases_us':event['phases_us'],'statements':dict(counts(observed)-before)})
        worker.finish(graceful=True)
        summary={'indexed':indexed,'rows':1800,'ticks':ticks,'initial_index_bytes':index_bytes,
                 'initial_total_relation_bytes':table_bytes,'lookup_plan':plan,
                 'scope':'serial local worker with phase clocks; psql startup included, oracle verification excluded; no capacity claim'}
        (output/'measurement.json').write_text(json.dumps(summary,indent=2)+'\n')
        return summary


if __name__=='__main__':
    root=pathlib.Path(sys.argv[1])
    for label,binary in [('before',sys.argv[2]),('after',sys.argv[3])]:
        path=root/label; path.mkdir()
        measure(path,[binary],label=='after')
