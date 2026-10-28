#!/usr/bin/env python3
"""Paired keyed publication cost through production workers and owned SQL fixtures.

Requires a disposable PostgreSQL with pg_stat_statements preloaded and JSON
statement logging enabled. Never point this measurement at application data.
"""
import argparse
import json
import os
import pathlib
import re
import statistics
import sys
import time
from decimal import Decimal
from keyed_sink_harness import KeyedFixture
from worker_harness import harness_arguments


class MeasuredFixture(KeyedFixture):
    def config(self, maximum=0):
        path=super().config(maximum)
        path.write_text(path.read_text().replace('maintenance_ticks=4',f'maintenance_ticks={self.maintenance_ticks}\nphase_metrics=true'))
        return path


def table_stats(fixture):
    return json.loads(fixture.sql(f"SELECT json_build_object('updates',n_tup_upd,'hot_updates',n_tup_hot_upd,'deletes',n_tup_del) FROM pg_stat_all_tables WHERE relid='{fixture.name}.groups'::regclass"))


def dml_stats(fixture):
    return json.loads(fixture.sql(f"""SELECT coalesce(json_agg(json_build_object('calls',calls,
      'execution_ms',total_exec_time,'rows',rows,'wal_bytes',wal_bytes,'query',query)),'[]')
      FROM pg_stat_statements WHERE query LIKE '%pgderive:sink-%'
      AND query LIKE '%{fixture.name}.groups%'"""))


def statements(fixture):
    path,offset=fixture.statement_log
    assert path==fixture.sql("SELECT pg_current_logfile('jsonlog')").strip(), 'statement log rotated during measurement'
    if not path:
        raise RuntimeError('benchmark requires PostgreSQL JSON statement logging')
    result={}
    executions={}
    raw=fixture.sql('SELECT pg_read_file('+"'"+path.replace("'","''")+"',"+str(offset)+",(pg_stat_file('"+path.replace("'","''")+"')).size-"+str(offset)+")")
    (fixture.output/'postgres-statements.jsonl').write_text(raw)
    for line in raw.splitlines():
        if not line.strip():
            continue
        row=json.loads(line)
        message=row.get('message','')
        handle=re.match(r'execute ([^:]+): ',message)
        if handle and f'{fixture.name}.groups' in message and '/* pgderive:sink-' in message:
            marker=re.search(r'pgderive:sink-(\w+)-v1',message).group(1)
            names=executions.setdefault(marker,{})
            names[handle.group(1)]=names.get(handle.group(1),0)+1
        for mode in ['update','delete']:
            marker='/* pgderive:sink-'+mode+'-v1 */'
            offset=message.find(marker)
            if offset>=0 and f'{fixture.name}.groups' in message[offset:]:
                result[mode]=message[offset:]
    if set(result)!={'update','delete'}:
        raise RuntimeError('actual worker UPDATE/DELETE templates absent from statement log')
    (fixture.output/'statement-executions.json').write_text(json.dumps(executions,indent=2)+'\n')
    return result


def plan_nodes(plan):
    yield plan
    for child in plan.get('Plans',[]):
        yield from plan_nodes(child)


def require_keyed_access(plans):
    for name,record in plans.items():
        nodes=list(plan_nodes(record['explain'][0]['Plan']))
        targets=[node for node in nodes if node.get('Relation Name')=='groups']
        assert targets, name+' has no sink access'
        assert not any(node['Node Type']=='Seq Scan' for node in targets), name+' scans the full sink'
        assert any(node['Node Type']=='Tid Scan' for node in targets), name+' has no bounded modifying scan'
        assert any(node['Node Type'] in ['Index Scan','Index Only Scan'] for node in targets), name+' has no key probe'
        probes=[node for node in targets if node['Node Type'] in ['Tid Scan','Index Scan','Index Only Scan']]
        assert sum(node['Actual Rows']*node['Actual Loops'] for node in probes)<=2*record['rows'], name+' revisits sink rows'
        batches=[node for node in nodes if node['Node Type']=='CTE Scan']
        assert sum(node['Actual Rows']*node['Actual Loops'] for node in batches)<=4*record['rows'], name+' rescans the batch'
        assert record['explain'][0]['Plan']['Plans'][-1]['Actual Rows']==record['rows'], name


def plans(fixture,queries):
    result={}
    for size in [1,32,512]:
        rows=json.loads(fixture.sql(f"SELECT json_agg(r.values) FROM (SELECT json_build_array(book::text,positions::text,units::text) AS values FROM {fixture.name}.groups ORDER BY book LIMIT {size})r"))
        old=[[None,values] for values in rows]
        pairs=[[previous,[None,[*previous[1][:2],str(Decimal(previous[1][2] or 0)+1)]]] for previous in old]
        for mode in ['force_generic_plan','force_custom_plan']:
            for action,tuples in [('update',pairs),('delete',old)]:
                name=f'{mode}-{action}-{size}'
                payload=json.dumps([[value,1] for value in tuples]).replace("'","''")
                raw=fixture.sql('BEGIN; SET LOCAL plan_cache_mode='+mode+
                    '; PREPARE sink_plan_probe(jsonb) AS '+queries[action]+
                    "; EXPLAIN(ANALYZE,BUFFERS,WAL,FORMAT JSON) EXECUTE sink_plan_probe('"+payload+
                    "'::jsonb); DEALLOCATE sink_plan_probe; ROLLBACK")
                (fixture.output/('plan-'+name+'.txt')).write_text(raw)
                explained=json.loads(raw[raw.index('['):raw.rindex(']')+1])
                assert explained[0]['Plan']['Plans'][-1]['Actual Rows']==len(rows), name
                result[name]={'rows':len(rows),'explain':explained}
    return result


def worker_cpu(worker):
    fields=pathlib.Path(f'/proc/{worker.process.pid}/stat').read_text().rpartition(')')[2].split()
    return sum(int(fields[index]) for index in [11,12])*1000/os.sysconf('SC_CLK_TCK')


def measure(output,command,groups,repetitions,maintenance_ticks):
    fixture=MeasuredFixture(output,rows=groups*3,auctions=groups)
    fixture.maintenance_ticks=maintenance_ticks
    with fixture.managed():
        fixture.sql('CREATE EXTENSION IF NOT EXISTS pg_stat_statements')
        log=fixture.sql("SELECT pg_current_logfile('jsonlog')").strip()
        fixture.statement_log=(log,int(fixture.sql("SELECT (pg_stat_file('"+log.replace("'","''")+"')).size")))
        storage_started=time.monotonic()
        worker=fixture.start(command,'worker');ready=worker.event('ready');fixture.verify()
        # Equal page-space headroom isolates index eligibility, not default-page packing.
        fixture.sql(f'ALTER TABLE {fixture.name}.groups SET(fillfactor=60)')
        fixture.sql(f'VACUUM(FULL,ANALYZE) {fixture.name}.groups')
        fixture.sql('CHECKPOINT')
        fixture.sql('SELECT pg_stat_statements_reset()')
        before=table_stats(fixture);cpu_before=worker_cpu(worker)
        ticks=[];logical=ready['time'];upper=groups
        for mode in ['update','delete']:
            for size in [1,32,600]:
                for repeat in range(repetitions):
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
        records=map(json.loads,pathlib.Path(worker.metrics.name).read_text().splitlines())
        maintenance=[row for row in records if row.get('event')=='maintenance']
        after=table_stats(fixture);sql_stats=dml_stats(fixture);queries=statements(fixture)
        physical={key:after[key]-before[key] for key in before}
        report={'initial_groups':groups,'maintenance_ticks':maintenance_ticks,'worker_cpu_ms':cpu_ms,'maintenance':maintenance,'ticks':ticks,'physical':physical,'postgres':sql_stats,
                'index_bytes':int(fixture.sql(f"SELECT pg_indexes_size('{fixture.name}.groups')")),
                'index_count':int(fixture.sql(f"SELECT count(*) FROM pg_index WHERE indrelid='{fixture.name}.groups'::regclass")),
                'plans':plans(fixture,queries),'actual_worker_sql':queries,
                'scope':'local production worker; repeated small and chunk-crossing batches; native binary oracle and safe ACK; matched fillfactor60; not a throughput or production freshness SLO'}
        fixture.verify()
        report['storage_interval_monotonic']=[storage_started,time.monotonic()]
        (output/'measurement.json').write_text(json.dumps(report,indent=2)+'\n')
        return report


def main():
    output,commands=harness_arguments()
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--candidate-first',action='store_true')
    parser.add_argument('--groups',type=int,default=2000)
    parser.add_argument('--repetitions',type=int,default=3)
    parser.add_argument('--maintenance-ticks',type=int,default=16)
    parser.add_argument('baseline')
    parser.add_argument('candidate')
    args=parser.parse_args(commands)
    if not 2000<=args.groups<=100000:
        raise ValueError('groups must be between 2000 and 100000')
    if not 1<=args.repetitions<=20 or args.repetitions*633>=args.groups:
        raise ValueError('repetitions must leave at least one undeleted group')
    if not 1<=args.maintenance_ticks<=4096:
        raise ValueError('maintenance ticks must be between1 and4096')
    output.mkdir()
    summary={}
    pairs=[('baseline',args.baseline),('candidate',args.candidate)]
    if args.candidate_first:
        pairs.reverse()
    for name,binary in pairs:
        path=output/name;path.mkdir()
        report=measure(path,[binary],args.groups,args.repetitions,args.maintenance_ticks);summary[name]={key:report[key] for key in ['physical','index_count','index_bytes','worker_cpu_ms']}
        if name=='candidate':
            require_keyed_access(report['plans'])
        summary[name]['maintenance_p50_ms']=statistics.median(row['elapsed_us']/1000 for row in report['maintenance'])
        summary[name]['maintenance_samples']=len(report['maintenance'])
        summary[name]['postgres_execution_ms']=sum(row['execution_ms'] for row in report['postgres'])
        summary[name]['wal_bytes']=sum(row['wal_bytes'] for row in report['postgres'])
        summary[name]['publication_p50_ms']=statistics.median(t['phases_us']['publication']/1000 for t in report['ticks'])
        summary[name]['publication_phases_p50_ms']={key:statistics.median(t['publication_us'][key]/1000 for t in report['ticks']) for key in report['ticks'][0]['publication_us']}
        (output/'summary.json').write_text(json.dumps(summary,indent=2)+'\n')
        print(json.dumps({name:summary[name]}))


if __name__=='__main__':
    main()
