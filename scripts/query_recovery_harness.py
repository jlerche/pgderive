#!/usr/bin/env python3
"""Owned logical backup/restore, immutable-object copy and fresh-query cutover drill."""
import collections
import hashlib
import json
import os
import pathlib
import subprocess
import sys
import time
import urllib.request

from native_mv_oracle import expected as native_expected
from query_capacity_harness import inventory
from query_lifecycle_harness import configured, grouped, rejected, verify_packed_root
from query_rebuild_harness import command, pin_roots, verify
from worker_harness import Worker
from harness_control import Server


def archive_objects(fixture, roots):
    inventory(fixture)
    objects=json.loads((fixture.output/'physical-object-inventory.json').read_text())
    saved=fixture.output/'object-backup'
    saved.mkdir()
    endpoint=fixture.environment.get('PGDERIVE__OBJECT_STORE__ENDPOINT','http://127.0.0.1:8333')
    records={}
    for ordinal,item in enumerate(objects):
        key=item['key']
        with urllib.request.urlopen(endpoint+'/pgderive-mvp-tests/'+key,timeout=10) as response:
            content=response.read()
        assert len(content)==item['bytes']
        file=saved/str(ordinal)
        file.write_bytes(content)
        records[key]={'file':str(file),'sha256':hashlib.sha256(content).hexdigest(),'bytes':len(content)}
    # Verify closure from PG membership and reader roots, not merely a prefix count.
    for root in roots:
        verify_packed_root(root, lambda path: pathlib.Path(records[fixture.name+'/'+path]['file']).read_bytes())
    (fixture.output/'object-backup-manifest.json').write_text(json.dumps(records,indent=2)+'\n')
    return records


def restore_objects(fixture, records):
    endpoint=fixture.environment.get('PGDERIVE__OBJECT_STORE__ENDPOINT','http://127.0.0.1:8333')
    mirror=fixture.name+'_restore_copy'
    for key,item in records.items():
        content=pathlib.Path(item['file']).read_bytes()
        assert hashlib.sha256(content).hexdigest()==item['sha256']
        target=mirror+'/'+key.removeprefix(fixture.name+'/')
        url=endpoint+'/pgderive-mvp-tests/'+target
        request=urllib.request.Request(url,data=content,method='PUT',headers={'If-None-Match':'*'})
        with urllib.request.urlopen(request,timeout=10) as response:
            assert response.status in (200,201)
        with urllib.request.urlopen(url,timeout=10) as response:
            assert hashlib.sha256(response.read()).hexdigest()==item['sha256']
    return mirror


def source_state(fixture):
    return [fixture.sql(f'SELECT jsonb_agg(t ORDER BY id) FROM {fixture.name}.{table} t').strip()
            for table in ('auction','bid')]


def verify_fresh(fixture, result, labels):
    native_expected(fixture,labels)
    oracle=fixture.name+'.pgderive_native_oracle'
    assert fixture.sql(f'SELECT count(*) FROM ((SELECT * FROM {oracle} EXCEPT ALL SELECT * FROM {result}) '
                       f'UNION ALL (SELECT * FROM {result} EXCEPT ALL SELECT * FROM {oracle})) d').strip()=='0'
    left=json.loads(fixture.sql(f'SELECT json_agg(a) FROM {fixture.name}.auction a'))
    right=json.loads(fixture.sql(f'SELECT json_agg(b) FROM {fixture.name}.bid b'))
    groups={}
    for a in left:
        for b in right:
            if a['category'] is None or a['id']!=b['auction']:
                continue
            value=groups.setdefault(a['group_id'],[0,None])
            value[0]+=1
            if b['price'] is not None:
                value[1]=(value[1] or 0)+b['price']
    expected=collections.Counter((key,*value) for key,value in groups.items())
    rows=json.loads(fixture.sql(f"SELECT coalesce(json_agg(r),'[]'::json) FROM {result} r"))
    assert collections.Counter(tuple(row[label] for label in labels) for row in rows)==expected


def qualify(output, binary):
    servers=[]
    completed=False
    old_port=os.environ.get('PGDERIVE_TEST_POSTGRES_PORT')
    fixture=None
    try:
        source=Server(output,'source', flags=['max_slot_wal_keep_size=64MB']);servers.append(source)
        os.environ['PGDERIVE_TEST_POSTGRES_PORT']=source.port
        fixture=grouped(output)
        config=configured(fixture)
        labels=[column['name'] for column in command(fixture,binary,'validate',config)['columns']]
        logs=output/'initial';logs.mkdir()
        worker=Worker(binary+['query','create'],config,logs,fixture.environment)
        fixture.workers.append(worker);ready=worker.event('ready')
        fixture.sql(f'BEGIN; UPDATE {fixture.name}.auction SET group_id=NULL WHERE id=1; '
                    f'UPDATE {fixture.name}.bid SET price=NULL WHERE id=1; '
                    f'DELETE FROM {fixture.name}.bid WHERE id=2; '
                    f'INSERT INTO {fixture.name}.bid VALUES(100,1,7),(101,1,7); COMMIT')
        worker.event('published',minimum_time=ready['time']+1)
        verify(fixture,labels);worker.finish(graceful=True)
        assert fixture.sql(f"SELECT count(*) FROM pg_replication_slots WHERE slot_name='{ready['slot']}' AND active").strip()=='0'
        fixture.sql(f'CREATE VIEW {fixture.name}.consumer AS SELECT * FROM {fixture.name}.groups')
        roots=pin_roots(fixture)
        before=command(fixture,binary,'status',config)
        sources=source_state(fixture)
        (output/'backup-boundary.json').write_text(json.dumps({'status':before,
            'sources':sources,'roots':roots},indent=2)+'\n')
        objects=archive_objects(fixture,roots)
        dump=output/'database.dump'
        with dump.open('wb') as file, (output/'dump.log').open('w') as log:
            subprocess.run(['docker','exec',source.name,'pg_dump','-U','postgres','-d','pgderive_dev','-Fc'],stdout=file,stderr=log,check=True)
        dump_hash=hashlib.sha256(dump.read_bytes()).hexdigest()
        subprocess.run(['docker','stop',source.name],check=True,capture_output=True)
        target=Server(output,'restored', flags=['max_slot_wal_keep_size=64MB']);servers.append(target)
        began=time.monotonic()
        with dump.open('rb') as file, (output/'restore.log').open('w') as log:
            subprocess.run(['docker','exec','-i',target.name,'pg_restore','-U','postgres',
                '-d','pgderive_dev','--exit-on-error','--no-owner'],stdin=file,stdout=log,stderr=log,check=True)
        restore_seconds=time.monotonic()-began
        fixture.port=target.port;fixture.pg[fixture.pg.index('-p')+1]=target.port
        config.write_text(config.read_text().replace('port='+source.port,'port='+target.port).replace('retry_attempts=8','retry_attempts=1'))
        assert source_state(fixture)==sources
        assert command(fixture,binary,'status',config)['progress']==before['progress']
        assert fixture.sql('SELECT count(*) FROM pg_replication_slots').strip()=='0'
        mirror=restore_objects(fixture,objects)
        error=rejected(binary,fixture,'restart',config)
        assert 'worker registration source identity changed' in error,error
        assert command(fixture,binary,'status',config)['progress']==before['progress']
        new=fixture.name+'_fresh'
        fixture.sql(f'CREATE SCHEMA {new}')
        text=config.read_text().replace(f'catalog_schema="{fixture.name}"',f'catalog_schema="{new}"').replace(
            'query_id="grouped"','query_id="recovered"').replace(f'slot="{fixture.name}"',f'slot="{new}"').replace(
            f'object_prefix="{fixture.name}"',f'object_prefix="{new}"')
        fresh=output/'fresh.toml';fresh.write_text(text)
        # Change restored sources before bootstrap: cutover must reflect the current database.
        fixture.sql(f'BEGIN; UPDATE {fixture.name}.auction SET group_id=19 WHERE id=1; '
                    f'UPDATE {fixture.name}.bid SET price=11 WHERE id=3; COMMIT')
        logs=output/'fresh';logs.mkdir();began=time.monotonic()
        worker=Worker(binary+['query','create'],fresh,logs,fixture.environment)
        fixture.workers.append(worker);active=worker.event('ready')
        bootstrap_seconds=time.monotonic()-began
        assert active['slot']!=ready['slot']
        verify_fresh(fixture,new+'.groups',labels)
        consumer_oid=fixture.sql(f"SELECT '{fixture.name}.consumer'::regclass::oid").strip()
        assert fixture.sql(f"SELECT pg_get_viewdef('{fixture.name}.consumer'::regclass,true)").find(new+'.groups')<0
        fixture.sql(f'BEGIN; CREATE OR REPLACE VIEW {fixture.name}.consumer AS SELECT * FROM {new}.groups; COMMIT')
        verify_fresh(fixture,fixture.name+'.consumer',labels)
        assert fixture.sql(f"SELECT '{fixture.name}.consumer'::regclass::oid").strip()==consumer_oid
        fixture.sql(f'BEGIN; UPDATE {fixture.name}.auction SET group_id=20 WHERE id=1; '
                    f'UPDATE {fixture.name}.bid SET price=NULL WHERE id=100; '
                    f'DELETE FROM {fixture.name}.bid WHERE id=101; COMMIT')
        worker.event('published',minimum_time=active['time']+1)
        verify_fresh(fixture,fixture.name+'.consumer',labels);worker.finish(graceful=True)
        logs=output/'cold';logs.mkdir()
        worker=Worker(binary+['query','restart'],fresh,logs,fixture.environment)
        fixture.workers.append(worker);reopened=worker.event('ready')
        assert reopened['slot']==active['slot']
        verify_fresh(fixture,fixture.name+'.consumer',labels);worker.finish(graceful=True)
        (output/'qualification.json').write_text(json.dumps({'logical_dump_sha256':dump_hash,
            'restore_seconds':restore_seconds,'fresh_bootstrap_seconds':bootstrap_seconds,
            'copied_objects':len(objects),'verified_membership_and_reader_roots':len(roots),
            'object_restore_prefix':mirror,'source_preserved':True,'copied_identity_restart_rejected':True,
            'fresh_generation':True,'application_cutover':True,'native_and_memory_equality':True,
            'post_restore_simultaneous_changes':True,'cold_restart':True,
            'scope':'owned PostgreSQL17 logical dump/restore and local SeaweedFS object copy; no physical/PITR/promotion qualification'},indent=2)+'\n')
        completed=True
    except BaseException:
        if fixture is not None:fixture.preserve()
        raise
    finally:
        if old_port is None:os.environ.pop('PGDERIVE_TEST_POSTGRES_PORT',None)
        else:os.environ['PGDERIVE_TEST_POSTGRES_PORT']=old_port
        for server in servers:server.finish(completed)


if __name__=='__main__':
    output=pathlib.Path(sys.argv[1]);output.mkdir(parents=True,exist_ok=True)
    binary=sys.argv[2:]
    if binary and binary[0]=='--':binary=binary[1:]
    qualify(output,binary)
