#!/usr/bin/env python3
"""Actual image, restricted TLS PostgreSQL, mounted secrets and long multiquery soak."""
import argparse
import collections
import hashlib
import json
import os
import pathlib
import subprocess
import time
from container_runtime import ContainerWorker,cli
from query_lifecycle_harness import configured,grouped
from query_postgres_tls_harness import certificates
from query_storage_tls_harness import proxy
from query_rebuild_harness import verify
from native_mv_oracle import expected as native_expected
from query_capacity_harness import inventory


def ownership(path):
    # Only this newly created owned fixture path is mounted into the helper.
    subprocess.run(['docker','run','--rm','--network=none','--read-only','--user=0',
        '--mount',f'type=bind,src={path.resolve()},dst=/owned',
        'ubuntu@sha256:534baea6a22c03a63003dbc8dbe78fe34bc0d7e595d9a9dc9834884ff530eb55',
        'sh','-c','chown 10001:1000 /owned && if test -d /owned; then chmod 0750 /owned; else chmod 0600 /owned; fi'],check=True,capture_output=True)


def metrics(fixture,worker,keeper):
    state=worker.inspect()['State']; pid=state['Pid']
    status=pathlib.Path(f'/proc/{pid}/status').read_text()
    rss=int(next(line.split()[1] for line in status.splitlines() if line.startswith('VmRSS:')))*1024
    metadata=json.loads(fixture.sql(f"SELECT json_build_object('roots',count(*),'root_bytes',coalesce(sum((reference->>'bytes')::bigint),0)) FROM {fixture.name}.pgderive_roots"))
    wal=json.loads(fixture.sql("SELECT coalesce(json_agg(json_build_object('slot',slot_name,'backlog_bytes',pg_wal_lsn_diff(pg_current_wal_lsn(),restart_lsn),'wal_status',wal_status)),'[]') FROM pg_replication_slots"))
    ledger=int(fixture.sql(f'SELECT count(*) FROM {fixture.name}.pgderive_protections').strip())
    return {'cache':worker.latest_cache,'protection_ledger_rows':ledger,'rss_bytes':rss,'scratch_bytes':int(subprocess.check_output(['docker','exec',keeper,'du','-sb','/scratch'],text=True).split()[0]),'coarse_metadata':metadata,'slots':wal}


def qualifier_inputs(manifest):
    # Bind all Python harnesses, including subprocess proxy helpers and their
    # imports, rather than guessing a narrower transitive import list.
    hashes={str(path):hashlib.sha256(path.read_bytes()).hexdigest()
            for path in sorted(pathlib.Path('scripts').rglob('*.py'))}
    expected=manifest['package']['source_sha256']
    for path,value in hashes.items():
        assert expected.get(path)==value,f'qualifier source differs from image package: {path}'
    return hashes


def qualify(output,image,image_manifest,duration,interval):
    output.mkdir(mode=0o700,parents=True)
    manifest_bytes=image_manifest.read_bytes(); manifest=json.loads(manifest_bytes)
    image=json.loads(subprocess.check_output(['docker','image','inspect',image]))[0]['Id']
    assert image==manifest['image_id'],'requested image differs from manifest'
    inputs=qualifier_inputs(manifest)
    (output/'inputs.json').write_text(json.dumps({'image_id':image,'image_manifest_sha256':hashlib.sha256(manifest_bytes).hexdigest(),'qualifier_sha256':inputs},indent=2)+'\n')
    certificates(output)
    name=f'pgderive-container-pg-{os.getpid()}'
    old_port=os.environ.get('PGDERIVE_TEST_POSTGRES_PORT')
    completed=False; fixtures=[]; workers=[]; proxies=[]; names=[]; volumes=[]; keepers=[]
    subprocess.run(['docker','run','--detach','--name',name,'--tmpfs','/var/lib/postgresql/data:rw,size=2g',
        '--publish','127.0.0.1::5432','-e','POSTGRES_PASSWORD=postgres','-e','POSTGRES_DB=pgderive_dev',
        'postgres:17','-c','wal_level=logical','-c','max_slot_wal_keep_size=256MB'],check=True,capture_output=True)
    try:
        pg=json.loads(subprocess.check_output(['docker','inspect',name]))[0]
        assert pg['Mounts']==[] and '/var/lib/postgresql/data' in pg['HostConfig']['Tmpfs']
        port=pg['NetworkSettings']['Ports']['5432/tcp'][0]['HostPort']; os.environ['PGDERIVE_TEST_POSTGRES_PORT']=port
        (output/'postgres.json').write_text(json.dumps({'name':name,'image':pg['Image'],'tmpfs':pg['HostConfig']['Tmpfs'],'mounts':pg['Mounts'],'port':port},indent=2)+'\n')
        deadline=time.monotonic()+30
        while subprocess.run(['docker','exec',name,'pg_isready','-h','127.0.0.1','-U','postgres'],capture_output=True).returncode:
            assert time.monotonic()<deadline; time.sleep(.1)
        for label in ('server.pem','server.key'):
            subprocess.run(['docker','cp',str(output/label),name+':/tmp/'+label],check=True,capture_output=True)
        subprocess.run(['docker','exec',name,'chown','postgres:postgres','/tmp/server.pem','/tmp/server.key'],check=True)
        secret=output/'secrets'; secret.mkdir(); secret.chmod(0o755)
        (secret/'ca.pem').write_bytes((output/'ca.pem').read_bytes())
        credential=secret/'s3.json'; credential.write_text(json.dumps({'access_key_id':'local','secret_access_key':'local','session_token':'container-fixture'})+'\n')
        credential.chmod(0o600); ownership(credential)
        proxy_dir=output/'storage'; proxy_dir.mkdir()
        service,s3_port,proxy_log=proxy(proxy_dir,output/'server.pem',output/'server.key','127.0.0.1'); proxies.append(service)
        cases=[]
        for index in range(3):
            directory=output/f'query-{index}'; directory.mkdir(); directory.chmod(0o755)
            fixture=grouped(directory)
            original_name=fixture.name; unique_name=original_name+f'_container_{index}'
            fixture.sql(f'ALTER SCHEMA {original_name} RENAME TO {unique_name}; ALTER PUBLICATION {original_name} RENAME TO {unique_name}')
            fixture.name=unique_name; fixture.query=fixture.query.replace(original_name,unique_name)
            fixtures.append(fixture)
            fixture.sql("ALTER SYSTEM SET ssl='on'"); fixture.sql("ALTER SYSTEM SET ssl_cert_file='/tmp/server.pem'"); fixture.sql("ALTER SYSTEM SET ssl_key_file='/tmp/server.key'"); fixture.sql('SELECT pg_reload_conf()')
            fixture.sql(f"INSERT INTO {fixture.name}.bid SELECT i,CASE WHEN i%17=0 THEN 2 ELSE 1 END,CASE WHEN i%5=0 THEN NULL ELSE i END FROM generate_series(33,2000) i")
            if index==1:
                fixture.query=f'SELECT auction, COUNT(*) AS row_count, SUM(price) AS total, AVG(price) AS mean, COUNT(DISTINCT price) AS distinct_prices FROM {fixture.name}.bid GROUP BY auction'
            if index==2:
                fixture.query=f'SELECT auction, MIN(price) AS low, MAX(price) AS high FROM {fixture.name}.bid GROUP BY auction'
            role=fixture.name+'_worker'
            fixture.sql(f"CREATE ROLE {role} LOGIN REPLICATION NOSUPERUSER NOCREATEDB NOCREATEROLE NOBYPASSRLS PASSWORD 'owned-container-password'; GRANT CONNECT ON DATABASE pgderive_dev TO {role}; GRANT USAGE,CREATE ON SCHEMA {fixture.name} TO {role}; GRANT SELECT ON {fixture.name}.auction,{fixture.name}.bid TO {role}; GRANT EXECUTE ON FUNCTION pg_control_system(),pg_control_checkpoint() TO {role}")
            subprocess.run(['docker','exec',name,'sh','-c',f"sed -i '1i hostnossl all {role} 0.0.0.0/0 reject\\nhostnossl all {role} ::/0 reject' /var/lib/postgresql/data/pg_hba.conf"],check=True)
            fixture.sql('SELECT pg_reload_conf()')
            config=configured(fixture)
            text=config.read_text().replace('user="postgres"',f'user="{role}"').replace('host="127.0.0.1"','host="localhost"').replace('tls="disable"','tls="verify-full"\nca_file="/run/secrets/ca.pem"').replace('[execution]','[execution]\nspill_runs=512\nscratch_bytes=134217728')
            before,rest=text.split('[object_store]'); _,after=rest.split('[worker]',1)
            config.write_text(before+f'[object_store]\nendpoint="https://localhost:{s3_port}"\nbucket="pgderive-mvp-tests"\nregion="fixture-region"\nca_file="/run/secrets/ca.pem"\ncredentials_file="/run/secrets/s3.json"\n[worker]'+after)
            config.chmod(0o644)
            scratch=f'pgderive-scratch-{os.getpid()}-{index}'; volumes.append(scratch)
            subprocess.run(['docker','volume','create','--driver=local','--opt','type=tmpfs','--opt','device=tmpfs','--opt','o=size=256m,uid=10001,gid=10001,mode=0700',scratch],check=True,capture_output=True)
            keeper=scratch+'-keeper'; keepers.append(keeper)
            subprocess.run(['docker','run','--detach','--name',keeper,'--network=none','--read-only','--mount',f'type=volume,src={scratch},dst=/scratch',
                'ubuntu@sha256:534baea6a22c03a63003dbc8dbe78fe34bc0d7e595d9a9dc9834884ff530eb55','sleep','infinity'],check=True,capture_output=True)
            subprocess.run(['docker','exec',keeper,'sh','-c','printf retained > /scratch/restart-marker; chown 10001:10001 /scratch/restart-marker'],check=True)
            mount_info=subprocess.check_output(['docker','exec',keeper,'sh','-c','df -B1 /scratch; grep " /scratch " /proc/mounts'],text=True)
            assert 'tmpfs' in mount_info and '268435456' in mount_info,mount_info
            (directory/'scratch-mount.txt').write_text(mount_info)
            labels=[c['name'] for c in cli(image,config,secret,'owned-container-password','validate')['columns']]
            container=f'pgderive-container-{os.getpid()}-{index}-create'; names.append(container)
            worker=ContainerWorker(image,container,config,secret,scratch,directory/'create','create','owned-container-password'); workers.append(worker)
            ready=worker.event('ready'); worker.inspect()
            def check(fixture=fixture,labels=labels,index=index):
                if index==0: verify(fixture,labels); return
                native_expected(fixture,labels)
                rows=json.loads(fixture.sql(f"SELECT coalesce(json_agg(b),'[]') FROM {fixture.name}.bid b"))
                groups={}
                for row in rows:
                    g=groups.setdefault(row['auction'],[]); g.append(row['price'])
                expected=[]
                for key,values in groups.items():
                    nonnull=[v for v in values if v is not None]
                    if index==2:
                        expected.append((key,min(nonnull) if nonnull else None,max(nonnull) if nonnull else None)); continue
                    expected.append((key,len(values),sum(nonnull) if nonnull else None, sum(nonnull)/len(nonnull) if nonnull else None,len(set(nonnull))))
                actual=json.loads(fixture.sql(f"SELECT coalesce(json_agg(r),'[]') FROM {fixture.name}.groups r"))
                # AVG numeric may be decimal-rounded; compare using the native
                # oracle for exact decimals, plus independently calculated count,
                # sum and distinct fields and tolerance for memory mean.
                assert len(actual)==len(expected)
                bykey={r[labels[0]]:r for r in actual}
                if index==2:
                    assert collections.Counter(tuple(row[label] for label in labels) for row in actual)==collections.Counter(expected)
                    assert fixture.sql(f"SELECT count(*) FROM ((SELECT * FROM {fixture.name}.pgderive_native_oracle EXCEPT ALL SELECT * FROM {fixture.name}.groups) UNION ALL (SELECT * FROM {fixture.name}.groups EXCEPT ALL SELECT * FROM {fixture.name}.pgderive_native_oracle)) d").strip()=='0'
                    return
                for key,count,total,average,distinct in expected:
                    row=bykey[key]; assert (row[labels[1]],row[labels[2]],row[labels[4]])==(count,total,distinct)
                    assert row[labels[3]] is None if average is None else abs(row[labels[3]]-average)<1e-8
                assert fixture.sql(f"SELECT count(*) FROM ((SELECT * FROM {fixture.name}.pgderive_native_oracle EXCEPT ALL SELECT * FROM {fixture.name}.groups) UNION ALL (SELECT * FROM {fixture.name}.groups EXCEPT ALL SELECT * FROM {fixture.name}.pgderive_native_oracle)) d").strip()=='0'
            check(); worker.finish()
            container=f'pgderive-container-{os.getpid()}-{index}-restart'; names.append(container)
            worker=ContainerWorker(image,container,config,secret,scratch,directory/'restart','restart','owned-container-password'); workers.append(worker)
            ready=worker.event('ready'); check()
            cases.append({'fixture':fixture,'config':config,'scratch':scratch,'keeper':keeper,'worker':worker,'check':check,'tick':ready['time'],'container':container})
        began=time.monotonic(); ordinal=0; samples=[]
        while time.monotonic()-began<duration:
            for case in cases:
                f=case['fixture']; first=1+(ordinal*13)%1900
                f.sql(f"BEGIN; UPDATE {f.name}.auction SET group_id=CASE WHEN {ordinal}%7=0 THEN NULL ELSE {ordinal}%9 END WHERE id=1; UPDATE {f.name}.bid SET price=CASE WHEN id%5=0 THEN NULL ELSE {ordinal}+id END WHERE id BETWEEN {first} AND {first+15}; DELETE FROM {f.name}.bid WHERE id=2001; INSERT INTO {f.name}.bid VALUES(2001,CASE WHEN {ordinal}%3=0 THEN NULL ELSE 1 END,CASE WHEN {ordinal}%4=0 THEN NULL ELSE {ordinal} END); COMMIT")
                case['tick']+=1; case['worker'].event('published',minimum_time=case['tick']); case['check']()
                samples.append({'elapsed_seconds':time.monotonic()-began,'query':f.name,'ordinal':ordinal,**metrics(f,case['worker'],case['keeper'])})
            ordinal+=1
            (output/'samples.json').write_text(json.dumps(samples,indent=2)+'\n')
            if ordinal%30==0:
                for case in cases:
                    case['worker'].finish(graceful=False)
                    worker=ContainerWorker(image,case['container'],case['config'],secret,case['scratch'],case['fixture'].output/f'cold-{ordinal}','restart','owned-container-password',existing=True)
                    workers.append(worker); case['worker']=worker; worker.event('ready'); case['check']()
            time.sleep(min(interval,max(0,duration-(time.monotonic()-began))))
        elapsed=time.monotonic()-began
        for case in cases:
            case['worker'].finish(graceful=False)
            worker=ContainerWorker(image,case['container'],case['config'],secret,case['scratch'],case['fixture'].output/'final-cold','restart','owned-container-password',existing=True)
            workers.append(worker); worker.event('ready'); case['check'](); worker.finish()
            assert subprocess.check_output(['docker','exec',case['keeper'],'cat','/scratch/restart-marker'],text=True)=='retained'
            case['fixture'].environment['PGDERIVE__OBJECT_STORE__ENDPOINT']='http://127.0.0.1:8333'
            case['physical_inventory']=inventory(case['fixture'])
            cli(image,case['config'],secret,'owned-container-password','drop')
            case['fixture'].sql(f"DROP PUBLICATION {case['fixture'].name}; DROP SCHEMA {case['fixture'].name} CASCADE")
        requests=[json.loads(line) for line in proxy_log.read_text().splitlines() if line.startswith('{')]
        puts=[r for r in requests if r.get('request_method')=='PUT']
        maintenance=[json.loads(line) for path in output.rglob('metrics.jsonl') for line in path.read_text().splitlines() if line.startswith('{') and json.loads(line).get('event')=='maintenance']
        if ordinal>=4: assert maintenance,'no witnessed maintenance'
        assert puts and all(r['signed'] and r['session_token_present'] and r['create_condition']=='*' for r in puts)
        (output/'samples.json').write_text(json.dumps(samples,indent=2)+'\n')
        assert qualifier_inputs(manifest)==inputs,'qualifier source changed during run'
        assert image_manifest.read_bytes()==manifest_bytes,'image manifest changed during run'
        (output/'qualification.json').write_text(json.dumps({'image_id':image,'requested_seconds':duration,'elapsed_seconds':elapsed,'transactions_per_query':ordinal,'query_count':3,'native_and_memory_oracles':True,'actual_container_restart':True,'forced_cold_restarts_per_query':1+ordinal//30,'physical_prefix_inventories':[case['physical_inventory'] for case in cases],'restricted_postgres_tls':True,'mounted_private_s3_secret_tls':True,'read_only_nonroot_pid1_sigint':True,'maintenance_events':len(maintenance),'delete_requests':sum(r.get('request_method')=='DELETE' for r in requests),'signed_conditional_puts':len(puts),'scope':'owned tmpfs PostgreSQL and local SeaweedFS TLS proxy; no cloud or orchestrator claim'},indent=2)+'\n')
        completed=True
    except BaseException:
        for fixture in fixtures: fixture.preserve()
        raise
    finally:
        for worker in workers: worker.abort()
        for keeper in keepers:
            with (output/(keeper+'.tar')).open('wb') as archive:
                subprocess.run(['docker','exec',keeper,'tar','-C','/scratch','-cf','-','.'],stdout=archive,check=True)
        for service in proxies: service.terminate(); service.wait(timeout=10)
        if old_port is None: os.environ.pop('PGDERIVE_TEST_POSTGRES_PORT',None)
        else: os.environ['PGDERIVE_TEST_POSTGRES_PORT']=old_port
        if completed:
            for owned in names+keepers+[name]: subprocess.run(['docker','rm','--force',owned],capture_output=True,check=True)
            for volume in volumes: subprocess.run(['docker','volume','rm',volume],capture_output=True,check=True)
        else: print('Preserved container failure evidence and owned tmpfs PostgreSQL: '+name)


if __name__=='__main__':
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument('output',type=pathlib.Path); parser.add_argument('--image',required=True)
    parser.add_argument('--image-manifest',type=pathlib.Path,required=True)
    parser.add_argument('--duration-seconds',type=float,default=1800); parser.add_argument('--interval-seconds',type=float,default=5)
    args=parser.parse_args(); assert args.duration_seconds>0 and args.interval_seconds>0
    qualify(args.output,args.image,args.image_manifest,args.duration_seconds,args.interval_seconds)
