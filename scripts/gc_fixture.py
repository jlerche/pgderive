"""Owned loopback object fixtures for the production physical collector."""
import hashlib
import json
import pathlib
import subprocess
import tomllib
import urllib.error
import urllib.parse
import urllib.request
import xml.etree.ElementTree as ET

def object_url(fixture, suffix):
    endpoint = fixture.environment.get('PGDERIVE__OBJECT_STORE__ENDPOINT', 'http://127.0.0.1:8333')
    parsed = urllib.parse.urlparse(endpoint)
    assert parsed.scheme == 'http' and parsed.hostname in ('127.0.0.1', 'localhost'), 'GC fixture requires loopback S3'
    return endpoint.rstrip('/')+'/pgderive-mvp-tests'+('/'+suffix if suffix else '')


def paged_garbage(fixture):
    object_url(fixture, '')
    namespace = 'pgderive/upload-v1/admin-cleanup-'+fixture.name.replace('_','-')
    fixture.sql(f"INSERT INTO {fixture.name}.pgderive_protections(token,namespace,query_id,owner_fence,uploading,roots) SELECT 'gc-admin','{namespace}',query_id,fence,true,'[]'::jsonb FROM {fixture.name}.pgderive_progress LIMIT 1")
    assert fixture.sql(f"SELECT count(*) FROM {fixture.name}.pgderive_protections WHERE token='gc-admin' AND active AND uploading").strip() == '1', 'orphan fixture did not reserve an upload'
    payload = b'x'*1101
    owner = manifest_sql(fixture, f"INSERT INTO {authority(fixture)['schema']}.owners(tenant,active) VALUES('{authority(fixture)['tenant']}',false) RETURNING id::text").splitlines()[0]
    pack = manifest_sql(fixture, 'SELECT gen_random_uuid()::text').strip()
    path = f"owners/{owner}/{pack}-{hashlib.sha256(payload).hexdigest()}"
    with urllib.request.urlopen(urllib.request.Request(object_url(fixture, physical_prefix(fixture)+'/'+path), data=payload, method='PUT'), timeout=10) as response:
        response.read()
    control = authority(fixture)
    schema, tenant, catalog = control['schema'], control['tenant'], control['catalog']
    manifest_sql(fixture, f"""BEGIN;
      INSERT INTO {schema}.packs(id,tenant,owner,path,length,checksum,state)
      VALUES('{pack}','{tenant}','{owner}','{path}',1101,'{hashlib.sha256(payload).hexdigest()}','ready');
      INSERT INTO {schema}.namespaces(tenant,catalog,namespace) VALUES('{tenant}','{catalog}','{namespace}');
      INSERT INTO {schema}.chunks(tenant,catalog,name,namespace,pack,offset_bytes,length,checksum)
      SELECT '{tenant}','{catalog}','{namespace}/pgderive/pack/'||lpad(to_hex(i),64,'0'),
        '{namespace}','{pack}',i,1,'{hashlib.sha256(b'x').hexdigest()}' FROM generate_series(0,1100) i;
      COMMIT""")
    fixture.sql(f"UPDATE {fixture.name}.pgderive_protections SET active=false,quiescent=true WHERE token='gc-admin'")
    return 1101


def empty_inventory(fixture, prefix):
    prefix = physical_prefix(fixture)
    # Logical drop and physical reclamation have separate authorities. Drain
    # bounded physical turns after all logical roots/protections are gone.
    worker = fixture.workers[0]
    binary = str(pathlib.Path(worker.process.args[0]).with_name('pgderive'))
    for _ in range(128):
        result = subprocess.run([binary, 'storage', 'collect', str(fixture.output/'config.toml')], env=fixture.environment, capture_output=True, text=True, timeout=60)
        assert result.returncode == 0, result.stderr
        remaining = manifest_sql(fixture, f"SELECT count(*) FROM {authority(fixture)['schema']}.packs WHERE tenant='{authority(fixture)['tenant']}' AND state NOT IN ('deleted')").strip()
        if remaining == '0':
            break
    query = urllib.parse.urlencode({'list-type':2, 'prefix':prefix+'/', 'max-keys':1})
    with urllib.request.urlopen(object_url(fixture, '')+'?'+query, timeout=10) as response:
        body = response.read()
    (fixture.output/'drop-inventory.xml').write_bytes(body)
    root = ET.fromstring(body)
    assert not root.findall('{*}Contents') and root.findtext('{*}IsTruncated') == 'false', 'drop left physical objects after its last worker exited'


def authority(fixture):
    if not hasattr(fixture, '_storage_authority'):
        config = tomllib.loads((fixture.output/'config.toml').read_text())
        control = config['tenant_storage'].copy()
        control['tenant'] = config['worker']['tenant_id']
        control['catalog'] = fixture.sql(f'SELECT catalog_id::text FROM {fixture.name}.pgderive_tenant_owner').strip()
        fixture._storage_authority = control
    return fixture._storage_authority


def manifest_sql(fixture, statement):
    pg = authority(fixture)['postgres']
    args = fixture.pg.copy()
    args[args.index('-p')+1] = str(pg['port'])
    args[args.index('-d')+1] = pg['database']
    return subprocess.check_output(args+[statement], env=fixture.environment, text=True)


def physical_prefix(fixture):
    control = authority(fixture)
    identity = manifest_sql(fixture, f"SELECT identity::text FROM {control['schema']}.tenants WHERE tenant='{control['tenant']}'").strip()
    return control['object_prefix']+'/'+identity


def logical_get(fixture, name):
    control = authority(fixture)
    escaped = name.replace("'", "''")
    mapping = manifest_sql(fixture, f"SELECT json_build_array(p.path,c.offset_bytes,c.length,c.checksum) FROM {control['schema']}.chunks c JOIN {control['schema']}.packs p ON p.id=c.pack WHERE c.tenant='{control['tenant']}' AND c.catalog='{control['catalog']}' AND c.name='{escaped}'").strip()
    if not mapping:
        raise urllib.error.HTTPError(name,404,'logical object absent',None,None)
    path, offset, length, checksum = json.loads(mapping)
    request = urllib.request.Request(object_url(fixture,physical_prefix(fixture)+'/'+path), headers={'Range':f'bytes={offset}-{offset+length-1}'})
    with urllib.request.urlopen(request, timeout=10) as response:
        content = response.read()
    assert len(content) == length and hashlib.sha256(content).hexdigest() == checksum
    return content
