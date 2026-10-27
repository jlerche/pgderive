#!/usr/bin/env python3
"""Paired keyed publication cost through production workers and owned SQL fixtures.

Requires a disposable PostgreSQL with pg_stat_statements preloaded and JSON
statement logging enabled. Never point this measurement at application data.
"""
import argparse
import json
import os
import pathlib
import statistics
import sys
import time
from decimal import Decimal
from keyed_sink_harness import KeyedFixture
from worker_harness import harness_arguments


class MeasuredFixture(KeyedFixture):
    def config(self, maximum=0):
        path=super().config(maximum)
        path.write_text(path.read_text().replace('maintenance_ticks=4','maintenance_ticks=4\nphase_metrics=true'))
        return path


def table_stats(fixture):
    return json.loads(fixture.sql(f"SELECT json_build_object('updates',n_tup_upd,'hot_updates',n_tup_hot_upd,'deletes',n_tup_del) FROM pg_stat_all_tables WHERE relid='{fixture.name}.groups'::regclass"))


def dml_stats(fixture):
    return json.loads(fixture.sql(f"""SELECT coalesce(json_agg(json_build_object('calls',calls,
      'execution_ms',total_exec_time,'rows',rows,'wal_bytes',wal_bytes,'query',query)),'[]')
      FROM pg_stat_statements WHERE query LIKE '%pgderive:sink-%'
      AND query LIKE '%{fixture.name}.groups%'"""))


def statements(fixture):
    path=fixture.sql("SELECT pg_current_logfile('jsonlog')").strip()
    if not path:
        raise RuntimeError('benchmark requires PostgreSQL JSON statement logging')
    result={}
    raw=fixture.sql('SELECT pg_read_file('+"'"+path.replace("'","''")+"')")
    (fixture.output/'postgres-statements.jsonl').write_text(raw)
    for line in raw.splitlines():
        if not line.strip():
            continue
        row=json.loads(line)
        message=row.get('message','')
        for mode in ['update','delete']:
            marker='/* pgderive:sink-'+mode+'-v1 */'
            offset=message.find(marker)
            if offset>=0 and f'{fixture.name}.groups' in message[offset:]:
                result[mode]=message[offset:]
    if set(result)!={'update','delete'}:
        raise RuntimeError('actual worker UPDATE/DELETE templates absent from statement log')
    return result


def plans(fixture,queries):
    values=json.loads(fixture.sql(f"SELECT json_build_array(book::text,positions::text,units::text) FROM {fixture.name}.groups WHERE book=1"))
    old=[None,values]
    new=[None,[*values[:2],str(Decimal(values[2])+1)]]
    result={}
    for mode,payload in [('update',[[[old,new],1]]),('delete',[[old,1]])]:
        encoded=json.dumps(payload).replace("'","''")
        sql=queries[mode].replace('$1',"'"+encoded+"'::jsonb")
        raw=fixture.sql('BEGIN; EXPLAIN(ANALYZE,BUFFERS,WAL,FORMAT JSON) '+sql+'; ROLLBACK')
        (fixture.output/('plan-'+mode+'.txt')).write_text(raw)
        lines=raw.splitlines()
        assert lines[0]=='BEGIN' and lines[-1]=='ROLLBACK'
        result[mode]=json.loads('\n'.join(lines[1:-1]))
        assert result[mode][0]['Plan']['Plans'][-1]['Actual Rows']==1
    return result


def worker_cpu(worker):
    fields=pathlib.Path(f'/proc/{worker.process.pid}/stat').read_text().rpartition(')')[2].split()
    return sum(int(fields[index]) for index in [11,12])*1000/os.sysconf('SC_CLK_TCK')


def measure(output,command,groups):
    fixture=MeasuredFixture(output,rows=groups*3,auctions=groups)
    with fixture.managed():
        fixture.sql('CREATE EXTENSION IF NOT EXISTS pg_stat_statements')
        worker=fixture.start(command,'worker');ready=worker.event('ready');fixture.verify()
        # Equal page-space headroom isolates index eligibility, not default-page packing.
        fixture.sql(f'ALTER TABLE {fixture.name}.groups SET(fillfactor=60)')
        fixture.sql(f'VACUUM(FULL,ANALYZE) {fixture.name}.groups')
        fixture.sql('SELECT pg_stat_statements_reset()')
        before=table_stats(fixture);cpu_before=worker_cpu(worker)
        ticks=[];logical=ready['time'];upper=groups
        for mode in ['update','delete']:
            for size in [1,32,600]:
                for repeat in range(3):
                    if mode=='update':
                        change=f'UPDATE {fixture.name}.bid SET price=coalesce(price,0)+1 WHERE auction BETWEEN 1 AND {size}'
                    else:
                        change=f'DELETE FROM {fixture.name}.bid WHERE auction BETWEEN {upper-size+1} AND {upper}'
                        upper-=size
                    started=time.perf_counter();fixture.sql(change);logical+=1
                    event=worker.event('published',minimum_time=logical)
                    elapsed=(time.perf_counter()-started)*1000
                    fixture.verify()
                    ticks.append({'mode':mode,'keys':size,'repeat':repeat,
                        'client_to_published_ms':elapsed,'phases_us':event['phases_us'],
                        'publication_us':event['publication_us'],'changes':event['changes']})
        cpu_ms=worker_cpu(worker)-cpu_before
        (output/'ticks.json').write_text(json.dumps(ticks,indent=2)+'\n')
        worker.finish(graceful=True)
        after=table_stats(fixture);sql_stats=dml_stats(fixture);queries=statements(fixture)
        physical={key:after[key]-before[key] for key in before}
        report={'initial_groups':groups,'worker_cpu_ms':cpu_ms,'ticks':ticks,'physical':physical,'postgres':sql_stats,
                'index_bytes':int(fixture.sql(f"SELECT pg_indexes_size('{fixture.name}.groups')")),
                'index_count':int(fixture.sql(f"SELECT count(*) FROM pg_index WHERE indrelid='{fixture.name}.groups'::regclass")),
                'plans':plans(fixture,queries),'actual_worker_sql':queries,
                'scope':'local production worker; three samples per mode/size; native binary oracle and safe ACK; matched fillfactor60; not a throughput or production freshness SLO'}
        fixture.verify()
        (output/'measurement.json').write_text(json.dumps(report,indent=2)+'\n')
        return report


def main():
    output,commands=harness_arguments()
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--candidate-first',action='store_true')
    parser.add_argument('--groups',type=int,default=2000)
    parser.add_argument('baseline')
    parser.add_argument('candidate')
    args=parser.parse_args(commands)
    if not 2000<=args.groups<=100000:
        raise ValueError('groups must be between 2000 and 100000')
    output.mkdir()
    summary={}
    pairs=[('baseline',args.baseline),('candidate',args.candidate)]
    if args.candidate_first:
        pairs.reverse()
    for name,binary in pairs:
        path=output/name;path.mkdir()
        report=measure(path,[binary],args.groups);summary[name]={key:report[key] for key in ['physical','index_count','index_bytes','worker_cpu_ms']}
        summary[name]['postgres_execution_ms']=sum(row['execution_ms'] for row in report['postgres'])
        summary[name]['wal_bytes']=sum(row['wal_bytes'] for row in report['postgres'])
        summary[name]['publication_p50_ms']=statistics.median(t['phases_us']['publication']/1000 for t in report['ticks'])
        (output/'summary.json').write_text(json.dumps(summary,indent=2)+'\n')
        print(json.dumps({name:summary[name]}))


if __name__=='__main__':
    main()
