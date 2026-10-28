#!/usr/bin/env python3
"""Actual Rust S3 client over owned private-network TLS; no cloud-provider claim."""
import hashlib
import json
import pathlib
import subprocess
import sys

from harness_control import start_recorded
from query_lifecycle_harness import configured, grouped, rejected
from query_rebuild_harness import command, verify
from worker_harness import Worker


def certificates(output, host):
    def run(args):
        with (output/'certificate-generation.log').open('a') as log:
            subprocess.run(['openssl']+args,check=True,stdout=log,stderr=log)
    run(['req','-x509','-newkey','rsa:2048','-nodes','-days','1','-subj','/CN=Pgderive test CA',
         '-keyout',str(output/'ca.key'),'-out',str(output/'ca.pem')])
    run(['req','-new','-newkey','rsa:2048','-nodes','-subj','/CN=Owned S3 fixture',
         '-keyout',str(output/'server.key'),'-out',str(output/'server.csr')])
    extensions = output/'extensions.txt'
    extensions.write_text(f'subjectAltName=IP:{host}\nextendedKeyUsage=serverAuth\n')
    run(['x509','-req','-in',str(output/'server.csr'),'-CA',str(output/'ca.pem'),
         '-CAkey',str(output/'ca.key'),'-CAcreateserial','-days','1','-extfile',str(extensions),
         '-out',str(output/'server.pem')])
    run(['req','-x509','-newkey','rsa:2048','-nodes','-days','1','-subj','/CN=Untrusted test CA',
         '-keyout',str(output/'wrong-ca.key'),'-out',str(output/'wrong-ca.pem')])
    for key in output.glob('*.key'):
        key.chmod(0o600)


def proxy(output, certificate, key, host):
    log_path = output/'proxy.jsonl'
    process, ready = start_recorded([sys.executable,'-u','scripts/s3_tls_proxy.py','--host',host,
        '--certificate',str(certificate),'--key',str(key)], log_path)
    return process, ready['port'], log_path


def qualify(output, binary):
    addresses = json.loads(subprocess.check_output(['ip','-4','-j','addr','show','docker0']))
    host = next(item['local'] for item in addresses[0]['addr_info'] if item['family']=='inet')
    certificates(output,host)
    fixture = grouped(output)
    processes = []
    try:
        for listener in [host,'127.0.0.1']:
            directory = output/('remote' if listener==host else 'wrong-name')
            directory.mkdir()
            process, port, log = proxy(directory,output/'server.pem',output/'server.key',listener)
            processes.append(process)
            if listener==host:
                remote_port, remote_log = port, log
            else:
                wrong_name_port = port
        config = configured(fixture)
        credentials = output/'credentials.json'
        credentials.write_text(json.dumps({'access_key_id':'local','secret_access_key':'local',
            'session_token':'fixture-session-token'})+'\n')
        credentials.chmod(0o600)
        original = config.read_text().replace('retry_attempts=8','retry_attempts=1')
        section = ('[object_store]\nendpoint='+json.dumps(f'https://{host}:{remote_port}')+
            '\nbucket="pgderive-mvp-tests"\nregion="fixture-region"\nca_file='+json.dumps(str((output/'ca.pem').resolve()))+
            '\ncredentials_file='+json.dumps(str(credentials.resolve()))+'\n')
        before, rest = original.split('[object_store]')
        _, after = rest.split('[worker]',1)
        healthy = before+section+'[worker]'+after
        fixture.environment.pop('PGDERIVE__OBJECT_STORE__ENDPOINT',None)
        for bad in [healthy.replace(str((output/'ca.pem').resolve()),str((output/'wrong-ca.pem').resolve())),
                    healthy.replace(f'https://{host}:{remote_port}',f'https://127.0.0.1:{wrong_name_port}')]:
            config.write_text(bad)
            error = rejected(binary,fixture,'create',config)
            assert 'preflight' in error,error
            assert fixture.sql(f"SELECT to_regclass('{fixture.name}.pgderive_worker_registration') IS NULL").strip()=='t'
            assert fixture.sql(f"SELECT count(*) FROM pg_replication_slots WHERE slot_name LIKE '{fixture.name}%'").strip()=='0'
        config.write_text(healthy)
        labels = [column['name'] for column in command(fixture,binary,'validate',config)['columns']]
        logs = output/'initial'
        logs.mkdir()
        worker = Worker(binary+['query','create'],config,logs,fixture.environment)
        fixture.workers.append(worker)
        ready = worker.event('ready')
        fixture.sql(f'BEGIN; UPDATE {fixture.name}.auction SET group_id=13 WHERE id=1; UPDATE {fixture.name}.bid SET price=NULL WHERE id=1; COMMIT')
        worker.event('published',minimum_time=ready['time']+1)
        verify(fixture,labels)
        worker.finish(graceful=True)
        logs = output/'restarted'
        logs.mkdir()
        worker = Worker(binary+['query','restart'],config,logs,fixture.environment)
        fixture.workers.append(worker)
        worker.event('ready')
        verify(fixture,labels)
        worker.finish(graceful=True)
        command(fixture,binary,'drop',config)
        requests = [json.loads(line) for line in remote_log.read_text().splitlines() if line.startswith('{')]
        puts = [row for row in requests if row.get('request_method')=='PUT']
        assert puts and all(row['create_condition']=='*' and row['signed'] and row['session_token_present'] for row in puts),puts
        (output/'qualification.json').write_text(json.dumps({'endpoint':f'https://{host}:{remote_port}',
            'scope':'owned private-network TLS proxy to SeaweedFS; no cloud provider qualified',
            'ca_sha256':hashlib.sha256((output/'ca.pem').read_bytes()).hexdigest(),
            'wrong_ca_and_hostname_rejected_before_registration':True,'conditional_signed_puts':len(puts),
            'native_and_memory_equality':True,'cold_restart':True},indent=2)+'\n')
        fixture.sql(f'DROP PUBLICATION {fixture.name}; DROP SCHEMA {fixture.name} CASCADE')
    except BaseException:
        fixture.preserve()
        raise
    finally:
        for process in processes:
            process.terminate()
            process.wait(timeout=10)


if __name__ == '__main__':
    output = pathlib.Path(sys.argv[1])
    output.mkdir(parents=True,exist_ok=True)
    binary = sys.argv[2:]
    if binary and binary[0]=='--':
        binary=binary[1:]
    qualify(output,binary)
