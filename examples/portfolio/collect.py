#!/usr/bin/env python3
"""Publish only aggregate demo measurements. No browser credentials or DB writes."""
import datetime
import hashlib
import hmac
import json
import math
import os
import pathlib
import runpy
import subprocess
import time
import tomllib
import urllib.parse
import urllib.request
import xml.etree.ElementTree as ET
from history import publish, CACHE_FIELDS
from queries import QUERIES

ROOT = pathlib.Path(os.environ.get('DEMO_ROOT', pathlib.Path(__file__).resolve().parents[1]))
PRIVATE = ROOT / 'metrics/sample.json'
PUBLIC = ROOT / 'stack-r2/web/metrics.json'
def inventory(config):
    host = urllib.parse.urlsplit(config['object_store']['endpoint']).netloc
    bucket = config['object_store']['bucket']
    prefix = config['worker']['object_prefix']
    credentials = json.loads((ROOT / 'stack-r2/r2-credentials.json').read_text())
    count = size = 0
    params = {'list-type': '2', 'prefix': prefix.rstrip('/')+'/'}
    while True:
        now = datetime.datetime.now(datetime.timezone.utc)
        stamp, day = now.strftime('%Y%m%dT%H%M%SZ'), now.strftime('%Y%m%d')
        query = urllib.parse.urlencode(sorted(params.items()), quote_via=urllib.parse.quote, safe='~')
        empty = hashlib.sha256(b'').hexdigest()
        headers = f'host:{host}\nx-amz-content-sha256:{empty}\nx-amz-date:{stamp}\n'
        signed = 'host;x-amz-content-sha256;x-amz-date'
        canonical = f'GET\n/{bucket}\n{query}\n{headers}\n{signed}\n{empty}'
        scope = f'{day}/auto/s3/aws4_request'
        message = f'AWS4-HMAC-SHA256\n{stamp}\n{scope}\n{hashlib.sha256(canonical.encode()).hexdigest()}'
        key = ('AWS4' + credentials['secret_access_key']).encode()
        for part in [day, 'auto', 's3', 'aws4_request']:
            key = hmac.new(key, part.encode(), hashlib.sha256).digest()
        signature = hmac.new(key, message.encode(), hashlib.sha256).hexdigest()
        authorization = f"AWS4-HMAC-SHA256 Credential={credentials['access_key_id']}/{scope}, SignedHeaders={signed}, Signature={signature}"
        request = urllib.request.Request(f'https://{host}/{bucket}?{query}', headers={
            'x-amz-date': stamp, 'x-amz-content-sha256': empty, 'Authorization': authorization})
        with urllib.request.urlopen(request, timeout=10) as response:
            page = ET.fromstring(response.read())
        ns = {'s': 'http://s3.amazonaws.com/doc/2006-03-01/'}
        entries = page.findall('s:Contents', ns)
        count += len(entries)
        size += sum(int(item.findtext('s:Size', namespaces=ns)) for item in entries)
        if page.findtext('s:IsTruncated', namespaces=ns) != 'true':
            return {'observed_at': time.time(), 'objects': count, 'bytes': size}
        params['continuation-token'] = page.findtext('s:NextContinuationToken', namespaces=ns)

def log_samples(output, event, field='elapsed_ms'):
    values = []
    for line in output.splitlines():
        try:
            row = json.loads(line)
        except ValueError:
            continue
        value = row.get(field)
        if row.get('event') == event and isinstance(value, (int, float)) and math.isfinite(value) and value >= 0:
            values.append(value)
    values.sort()
    def percentile(p):
        return values[max(0, math.ceil(len(values)*p)-1)] if values else None
    return {'samples': len(values), 'p50_ms': percentile(.5), 'p95_ms': percentile(.95), 'p99_ms': percentile(.99)}

def latency(container, observer):
    output = subprocess.check_output(['docker','logs','--tail','10000','--since','60s',
        container], text=True, stderr=subprocess.PIPE, timeout=10)
    if len(output.splitlines()) >= 10000:
        raise RuntimeError('latency log window exceeded bounded tail')
    now = time.time()
    status = observer_status(observer, now)
    valid = status == 'healthy'
    observer = observer or {}
    events = []
    for line in output.splitlines():
        try:
            row = json.loads(line)
            if isinstance(row, dict):
                events.append(row)
        except ValueError:
            pass
    phases = {}
    for name in ['inputs', 'prepare_including_object_io', 'publication']:
        synthetic = '\n'.join(json.dumps({'event':'phase', 'elapsed_ms':row['phases_us'][name]/1000})
            for row in events if row.get('event') == 'published' and name in row.get('phases_us', {}))
        phases[name] = log_samples(synthetic, 'phase')
    checkpoint = log_samples(output, 'maintenance', 'elapsed_us')
    for key in ['p50_ms', 'p95_ms', 'p99_ms']:
        if checkpoint.get(key) is not None:
            checkpoint[key] /= 1000
    return {'observed_at': time.time(), 'window_seconds': 60, 'observer_healthy': valid, 'observer_status': status,
            'published_epochs': sum(row.get('event') == 'published' for row in events),
            'published_source_transactions': sum(row.get('source_transactions', 0) for row in events if row.get('event') == 'published'), 'phases': phases,
            'checkpoint': checkpoint,
            'write_to_sink': log_samples(output, 'epoch', 'received_to_sink_ms'),
            'source_commit_to_sink': observer.get('write_to_sink') if valid else {'samples':None,'p50_ms':None,'p95_ms':None},
            'source_commit_to_sink_by_source_changes': observer.get('by_source_changes', {}) if valid else {},
            'observer': {key: observer.get(key) for key in ['observed_at','pending','dropped','expired','generator_samples_skipped','poll_interval_ms','endpoint']},
            'engine': log_samples(output, 'published')}

def atomic(path, value, mode):
    temporary = path.with_suffix('.tmp')
    temporary.write_text(json.dumps(value, indent=2) + '\n')
    temporary.chmod(mode)
    os.replace(temporary, path)

def observer_status(observer, now):
    if observer is None:
        return 'unconfigured'
    stamp = observer.get('observed_at', 0)
    if not isinstance(stamp, (int, float)) or not 0 <= now-stamp < 5:
        return 'stale'
    poll = observer.get('last_progress_poll_at')
    return 'healthy' if isinstance(poll, (int, float)) and 0 <= now-poll < 5 else 'unavailable'


def flow_sql(spec):
    schema, table = spec['schema'], spec['table']
    return f"""SELECT json_build_object('observed_at',extract(epoch FROM clock_timestamp()),
      'inserts',s.n_tup_ins,'updates',s.n_tup_upd,'deletes',s.n_tup_del,
      'stats_reset',d.stats_reset,'engine_epochs',GREATEST(p.published_time-1,0),
      'end_lsn',p.end_lsn::text,'checkpoint_lsn',p.checkpoint_lsn::text,
      'source_bytes',pg_total_relation_size('public.demo_positions'),
      'sink_bytes',pg_total_relation_size('{schema}.{table}'),
      'replay_bytes',pg_wal_lsn_diff(p.end_lsn,p.checkpoint_lsn),
      'retained_wal_bytes',(SELECT pg_wal_lsn_diff(pg_current_wal_lsn(),s.restart_lsn) FROM pg_replication_slots s JOIN {schema}.pgderive_worker_registration r ON r.slot_name=s.slot_name WHERE r.query_id=p.query_id),
      'unconfirmed_wal_bytes',(SELECT pg_wal_lsn_diff(pg_current_wal_lsn(),s.confirmed_flush_lsn) FROM pg_replication_slots s JOIN {schema}.pgderive_worker_registration r ON r.slot_name=s.slot_name WHERE r.query_id=p.query_id),
      'books',totals.books,'positions',totals.positions,'units',totals.units)
      FROM pg_stat_user_tables s CROSS JOIN pg_stat_database d CROSS JOIN {schema}.pgderive_progress p
      CROSS JOIN (SELECT count(*) AS books,coalesce(sum(positions),0) AS positions,coalesce(sum(units),0)::text AS units FROM {schema}.{table}) totals
      WHERE s.schemaname='public' AND s.relname='demo_positions'
      AND d.datname=current_database() AND p.sink_table='{table}'"""


def health_sql(spec):
    schema, table = spec['schema'], spec['table']
    scope = f"pgderive-worker:{schema}:{table}"
    return f"""SELECT json_build_object('observed_at',extract(epoch FROM h.heartbeat_at),
      'owner_live',EXISTS(SELECT 1 FROM pg_locks WHERE locktype='advisory' AND pid=h.owner_pid
        AND database=(SELECT oid FROM pg_database WHERE datname=current_database()) AND objsubid=1
        AND classid=((hashtextextended('{scope}',0)>>32)&4294967295)::oid
        AND objid=(hashtextextended('{scope}',0)&4294967295)::oid AND granted),
      'fence_current',h.fence=p.fence,'diagnostics',h.diagnostics)
      FROM {schema}.pgderive_worker_health h JOIN {schema}.pgderive_progress p USING(query_id)
      WHERE h.query_id='{table}'"""


def telemetry(health):
    # Explicit allowlists keep private catalog fields and tokens out of public JSON.
    if not health['owner_live'] or not health['fence_current']:
        raise RuntimeError('worker health owner unavailable')
    diagnostics = health['diagnostics']
    result = {key: diagnostics.get(key) for key in ['phase', 'logical_time', 'memtable_bytes', 'memtable_limit', 'runs']}
    result['observed_at'] = health['observed_at']
    cache = diagnostics.get('cache') or {}
    result['cache'] = {key: cache[key] for key in CACHE_FIELDS if key in cache}
    for scope in ['foreground', 'scan']:
        result[scope] = {key: value for key, value in cache.get('io', {}).get(scope, {}).items()
                         if key in ['gets', 'fetched_bytes', 'requested_bytes']}
    result['object_service'] = {scope: {key: value for key, value in values.items()
        if key in ['requests', 'read_requested_bytes', 'read_bytes', 'write_requested_bytes', 'written_bytes']}
        for scope, values in (diagnostics.get('object_service') or {}).items()
        if scope in ['foreground', 'maintenance']}
    return result


def worker_memory(container):
    pid = int(subprocess.check_output(['docker','inspect','--format','{{.State.Pid}}',container], text=True))
    if pid <= 0:
        raise RuntimeError('worker stopped')
    rss = next(line.split()[1] for line in pathlib.Path(f'/proc/{pid}/status').read_text().splitlines() if line.startswith('VmRSS:'))
    return {'observed_at': time.time(), 'rss_bytes': int(rss)*1024}


def source_rates(flow, old, generator):
    flow.update(transactions=generator['transactions'], generated_source_changes=generator['source_changes'], transaction_counter_version=2)
    seconds = float(flow['observed_at']) - float(old.get('observed_at', flow['observed_at']))
    changes = sum(flow[key] for key in ['inserts', 'updates', 'deletes'])
    prior = sum(old.get(key, 0) for key in ['inserts', 'updates', 'deletes'])
    valid = seconds > 0 and old.get('stats_reset') == flow['stats_reset'] and changes >= prior
    flow['changes_per_second'] = (changes-prior)/seconds if valid else None
    delta = flow['transactions']-old.get('transactions',flow['transactions'])
    flow['transactions_per_second'] = delta/seconds if seconds > 0 and delta >= 0 and old.get('transaction_counter_version') == 2 else None
    return flow


def collect_query(spec, previous, sql, observer, generator, config):
    result = {'errors': []}
    def capture(key, action):
        try:
            result[key] = action()
        except Exception as error:
            result[key] = previous.get(key)
            result['errors'].append(key+' unavailable: '+type(error).__name__)
    capture('flow', lambda: source_rates(json.loads(sql(flow_sql(spec))), previous.get('flow') or {}, generator))
    capture('engine', lambda: telemetry(json.loads(sql(health_sql(spec)))))
    capture('worker', lambda: worker_memory(spec['container']))
    capture('latency', lambda: latency(spec['container'], observer))
    if result.get('latency') and not result['latency']['observer_healthy']:
        result['errors'].append('source-to-sink observer '+result['latency'].get('observer_status', 'unavailable'))
    result['state'] = previous.get('state')
    if not result['state'] or time.time()-result['state']['observed_at'] >= 60:
        capture('state', lambda: inventory(config))
    return result


def main():
    previous = json.loads(PRIVATE.read_text()) if PRIVATE.exists() else {}
    helper = runpy.run_path(str(ROOT / 'verify-remote-memtable.py'))
    generator = json.loads((ROOT / 'generator/activity-state.json').read_text())
    result = {'collected_at': time.time(), 'errors': [], 'queries': {}}
    for name, spec in QUERIES.items():
        config = tomllib.loads((ROOT/'stack-r2'/spec['config']).read_text())
        # Refresh after the peer's potentially slow object inventory.
        try:
            observer = json.loads((ROOT/'metrics/latency-observer.json').read_text())['queries'].get(name)
        except (FileNotFoundError, ValueError, KeyError):
            observer = {}
        query = collect_query(spec, previous.get('queries', {}).get(name) or {}, helper['sql'],
                              observer, generator, config)
        result['queries'][name] = query
        result['errors'].extend(name+': '+error for error in query['errors'])
    # The byte-preserved original page reads its totals measurements at the root.
    result.update({key: result['queries']['totals'].get(key) for key in ['flow','engine','worker','latency','state']})
    try:
        publish(ROOT, result)
    except Exception as error:
        result['errors'].append('history unavailable: ' + type(error).__name__)
    atomic(PRIVATE, result, 0o600)
    atomic(PUBLIC, result, 0o644)
    history = ROOT / 'metrics/history.jsonl'
    if history.exists() and history.stat().st_size > 5*1024*1024:
        os.replace(history, history.with_name('history.previous.jsonl'))
    with history.open('a') as stream:
        stream.write(json.dumps(result)+'\n')
    print(json.dumps(result))

if __name__ == '__main__':
    main()
