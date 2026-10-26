"""Owned loopback object fixtures for the production physical collector."""
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
    namespace = 'pgderive/upload-v1/admin-cleanup-'+fixture.name
    fixture.sql(f"INSERT INTO {fixture.name}.pgderive_protections(token,namespace,query_id,owner_fence,uploading,roots) SELECT 'gc-admin','{namespace}',query_id,fence,true,'[]'::jsonb FROM {fixture.name}.pgderive_progress LIMIT 1")
    assert fixture.sql(f"SELECT count(*) FROM {fixture.name}.pgderive_protections WHERE token='gc-admin' AND active AND uploading").strip() == '1', 'orphan fixture did not reserve an upload'
    for index in range(1101):
        path = fixture.name+'/'+namespace+'/pgderive/pack/'+f'{index:064x}'
        with urllib.request.urlopen(urllib.request.Request(object_url(fixture, path), data=b'x', method='PUT'), timeout=10) as response:
            response.read()
    fixture.sql(f"UPDATE {fixture.name}.pgderive_protections SET active=false,quiescent=true WHERE token='gc-admin'")
    return 1101


def empty_inventory(fixture, prefix):
    query = urllib.parse.urlencode({'list-type':2, 'prefix':prefix+'/', 'max-keys':1})
    with urllib.request.urlopen(object_url(fixture, '')+'?'+query, timeout=10) as response:
        body = response.read()
    (fixture.output/'drop-inventory.xml').write_bytes(body)
    root = ET.fromstring(body)
    assert not root.findall('{*}Contents') and root.findtext('{*}IsTruncated') == 'false', 'drop left physical objects after its last worker exited'
