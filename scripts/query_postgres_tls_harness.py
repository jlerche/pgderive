#!/usr/bin/env python3
"""Owned tmpfs PostgreSQL TLS and restricted-role qualification."""
import hashlib
import json
import os
import pathlib
import socket
import subprocess
import sys
from query_lifecycle_harness import configured, grouped, rejected
from query_rebuild_harness import command, verify
from worker_harness import Worker
from harness_control import Server


def certificates(output):
    def run(args):
        with (output/'certificate-generation.log').open('a') as log:
            subprocess.run(['openssl']+args, check=True, stdout=log, stderr=log)
    for label in ['ca','wrong-ca']:
        run(['req','-x509','-newkey','rsa:2048','-nodes','-days','1','-subj',f'/CN={label}',
             '-keyout',str(output/f'{label}.key'),'-out',str(output/f'{label}.pem')])
    run(['req','-new','-newkey','rsa:2048','-nodes','-subj','/CN=localhost',
         '-keyout',str(output/'server.key'),'-out',str(output/'server.csr')])
    (output/'extensions.txt').write_text('subjectAltName=DNS:localhost\nextendedKeyUsage=serverAuth\n')
    run(['x509','-req','-in',str(output/'server.csr'),'-CA',str(output/'ca.pem'),
         '-CAkey',str(output/'ca.key'),'-CAcreateserial','-days','1',
         '-extfile',str(output/'extensions.txt'),'-out',str(output/'server.pem')])
    for path in output.glob('*.key'):
        path.chmod(0o600)


def qualify(output, binary):
    assert any(address[4][0]=='127.0.0.1' for address in socket.getaddrinfo('localhost.localdomain',None,socket.AF_INET)), 'fixture requires localhost.localdomain loopback DNS alias'
    certificates(output)
    name = f'pgderive-postgres-tls-{os.getpid()}'
    old_port = os.environ.get('PGDERIVE_TEST_POSTGRES_PORT')
    completed = False
    server = Server(output, 'postgres', name=name, size='512m', flags=[],
                    metadata='postgres.json', failure='postgres-failure.log')
    try:
        port = server.port
        os.environ['PGDERIVE_TEST_POSTGRES_PORT'] = port
        for label in ['server.pem','server.key']:
            subprocess.run(['docker','cp',str(output/label),name+':/tmp/'+label],check=True,capture_output=True)
        subprocess.run(['docker','exec',name,'chown','postgres:postgres','/tmp/server.pem','/tmp/server.key'],check=True)
        fixture = grouped(output)
        try:
            fixture.sql("ALTER SYSTEM SET ssl='on'")
            fixture.sql("ALTER SYSTEM SET ssl_cert_file='/tmp/server.pem'")
            fixture.sql("ALTER SYSTEM SET ssl_key_file='/tmp/server.key'")
            fixture.sql('SELECT pg_reload_conf()')
            role = fixture.name+'_worker'
            fixture.sql(f'''CREATE ROLE {role} LOGIN REPLICATION NOSUPERUSER NOCREATEDB NOCREATEROLE NOBYPASSRLS PASSWORD 'owned-fixture-password';
                GRANT CONNECT ON DATABASE pgderive_dev TO {role};
                GRANT USAGE,CREATE ON SCHEMA {fixture.name} TO {role};
                GRANT SELECT ON {fixture.name}.auction,{fixture.name}.bid TO {role};
                GRANT EXECUTE ON FUNCTION pg_control_system(),pg_control_checkpoint() TO {role}''')
            subprocess.run(['docker','exec',name,'sh','-c',
                f"sed -i '1i hostnossl all {role} 0.0.0.0/0 reject\\nhostnossl all {role} ::/0 reject' /var/lib/postgresql/data/pg_hba.conf"],check=True)
            fixture.sql('SELECT pg_reload_conf()')
            config = configured(fixture)
            fixture.environment['PGDERIVE__POSTGRES__PASSWORD']='owned-fixture-password'
            for key in ['HOST','PORT','USER','TLS','CA_FILE']:
                fixture.environment.pop('PGDERIVE__POSTGRES__'+key,None)
            healthy = config.read_text().replace('user="postgres"',f'user="{role}"').replace(
                'host="127.0.0.1"','host="localhost"').replace('tls="disable"',
                'tls="verify-full"\nca_file='+json.dumps(str((output/'ca.pem').resolve()))).replace('retry_attempts=8','retry_attempts=1')
            assert f'user="{role}"' in healthy and 'tls="verify-full"' in healthy
            control_probe = [str(pathlib.Path(binary[0]).with_name('postgres_tls_control_probe'))]
            connector_rejections = []
            for bad in [healthy.replace(str((output/'ca.pem').resolve()),str((output/'wrong-ca.pem').resolve())),
                        healthy.replace('host="localhost"','host="localhost.localdomain"')]:
                config.write_text(bad)
                sql_error = rejected(binary,fixture,'validate',config)
                assert 'certificate' in sql_error.lower(),sql_error
                connector_rejections.append({'connector':'sql','stderr':sql_error})
                for connector, invocation in [('control',control_probe+[str(config)]),
                                              ('stream',binary+['listen',str(config)])]:
                    probe = subprocess.run(invocation,env=fixture.environment,text=True,capture_output=True,timeout=20)
                    assert probe.returncode != 0 and any(word in probe.stderr.lower() for word in ['certificate','unknownissuer','notvalidforname','hostname mismatch']),probe.stderr
                    connector_rejections.append({'connector':connector,'stderr':probe.stderr})
                assert fixture.sql(f"SELECT to_regclass('{fixture.name}.pgderive_worker_registration') IS NULL").strip()=='t'
                assert fixture.sql(f"SELECT count(*) FROM pg_replication_slots WHERE slot_name LIKE '{fixture.name}%'").strip()=='0'
            config.write_text(healthy)
            restricted_pg = ['psql','-h','localhost','-p',port,'-U',role,'-d','pgderive_dev',
                '-X','-v','ON_ERROR_STOP=1','-c']
            restricted_env = {**fixture.environment,'PGPASSWORD':'owned-fixture-password',
                'PGSSLMODE':'verify-full','PGSSLROOTCERT':str((output/'ca.pem').resolve())}
            denials = []
            for statement in [f'INSERT INTO {fixture.name}.bid VALUES (999999,1,10)',
                              f'UPDATE {fixture.name}.auction SET group_id=99',
                              f'DELETE FROM {fixture.name}.bid',
                              f'ALTER TABLE {fixture.name}.auction ADD COLUMN forbidden integer']:
                result = subprocess.run(restricted_pg+[statement],env=restricted_env,text=True,capture_output=True)
                assert result.returncode != 0 and any(reason in result.stderr.lower() for reason in ['permission denied','must be owner']),result.stderr
                denials.append({'operation':statement.split()[0],'stderr':result.stderr})
            no_tls = subprocess.run(restricted_pg+['SELECT 1'],env={**restricted_env,'PGSSLMODE':'disable'},text=True,capture_output=True)
            assert no_tls.returncode != 0 and 'pg_hba.conf rejects' in no_tls.stderr, no_tls.stderr
            labels = [column['name'] for column in command(fixture,binary,'validate',config)['columns']]
            logs = output/'initial'; logs.mkdir()
            worker = Worker(binary+['query','create'],config,logs,fixture.environment)
            fixture.workers.append(worker)
            ready = worker.event('ready')
            encrypted = json.loads(fixture.sql(f"SELECT json_agg(json_build_object('backend_type',a.backend_type,'ssl',s.ssl)) FROM pg_stat_activity a JOIN pg_stat_ssl s USING(pid) WHERE a.usename='{role}'"))
            assert encrypted and all(row['ssl'] for row in encrypted),encrypted
            assert any(row['backend_type']=='walsender' for row in encrypted),encrypted
            fixture.sql(f'BEGIN; UPDATE {fixture.name}.auction SET group_id=13 WHERE id=1; UPDATE {fixture.name}.bid SET price=NULL WHERE id=1; COMMIT')
            worker.event('published',minimum_time=ready['time']+1)
            verify(fixture,labels)
            worker.finish(graceful=True)
            logs = output/'restart'; logs.mkdir()
            worker = Worker(binary+['query','restart'],config,logs,fixture.environment)
            fixture.workers.append(worker); worker.event('ready')
            verify(fixture,labels); worker.finish(graceful=True)
            command(fixture,binary,'rebuild',config); verify(fixture,labels)
            command(fixture,binary,'drop',config)
            attributes = json.loads(fixture.sql(f"SELECT row_to_json(r) FROM (SELECT rolname,rolsuper,rolreplication,rolcreatedb,rolcreaterole,rolbypassrls FROM pg_roles WHERE rolname='{role}') r"))
            assert attributes['rolreplication'] and not any(attributes[key] for key in ['rolsuper','rolcreatedb','rolcreaterole','rolbypassrls'])
            (output/'qualification.json').write_text(json.dumps({'role':attributes,'source_write_denials':denials,'cleartext_rejected':True,'encrypted_connections':encrypted,
                'wrong_ca_and_hostname_rejected':True,'connector_rejections':connector_rejections,'native_and_memory_equality':True,'cold_restart':True,'rebuild_drop':True,
                'ca_sha256':hashlib.sha256((output/'ca.pem').read_bytes()).hexdigest()},indent=2)+'\n')
            completed = True
        except BaseException:
            fixture.preserve(); raise
    finally:
        if old_port is None:
            os.environ.pop('PGDERIVE_TEST_POSTGRES_PORT',None)
        else:
            os.environ['PGDERIVE_TEST_POSTGRES_PORT']=old_port
        server.finish(completed)


if __name__=='__main__':
    output = pathlib.Path(sys.argv[1]); output.mkdir(parents=True,exist_ok=True)
    binary = sys.argv[2:]
    if binary and binary[0]=='--': binary=binary[1:]
    qualify(output,binary)
