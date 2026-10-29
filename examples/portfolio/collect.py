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

ROOT = pathlib.Path(os.environ.get('DEMO_ROOT', pathlib.Path(__file__).resolve().parents[1]))
PRIVATE = ROOT / 'metrics/sample.json'
PUBLIC = ROOT / 'stack-r2/web/metrics.json'
CONFIG = tomllib.loads((ROOT/'stack-r2/query.toml').read_text())
HOST = urllib.parse.urlsplit(CONFIG['object_store']['endpoint']).netloc
BUCKET = CONFIG['object_store']['bucket']
PREFIX = CONFIG['worker']['object_prefix']

def inventory(prefix=PREFIX):
    credentials = json.loads((ROOT / 'stack-r2/r2-credentials.json').read_text())
    count = size = 0
    params = {'list-type': '2', 'prefix': prefix.rstrip('/')+'/'}
    while True:
        now = datetime.datetime.now(datetime.timezone.utc)
        stamp, day = now.strftime('%Y%m%dT%H%M%SZ'), now.strftime('%Y%m%d')
        query = urllib.parse.urlencode(sorted(params.items()), quote_via=urllib.parse.quote, safe='~')
        empty = hashlib.sha256(b'').hexdigest()
        headers = f'host:{HOST}\nx-amz-content-sha256:{empty}\nx-amz-date:{stamp}\n'
        signed = 'host;x-amz-content-sha256;x-amz-date'
        canonical = f'GET\n/{BUCKET}\n{query}\n{headers}\n{signed}\n{empty}'
        scope = f'{day}/auto/s3/aws4_request'
        message = f'AWS4-HMAC-SHA256\n{stamp}\n{scope}\n{hashlib.sha256(canonical.encode()).hexdigest()}'
        key = ('AWS4' + credentials['secret_access_key']).encode()
        for part in [day, 'auto', 's3', 'aws4_request']:
            key = hmac.new(key, part.encode(), hashlib.sha256).digest()
        signature = hmac.new(key, message.encode(), hashlib.sha256).hexdigest()
        authorization = f"AWS4-HMAC-SHA256 Credential={credentials['access_key_id']}/{scope}, SignedHeaders={signed}, Signature={signature}"
        request = urllib.request.Request(f'https://{HOST}/{BUCKET}?{query}', headers={
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

def latency(container='pgderive-demo-engine-1', observer_file='latency-observer.json'):
    output = subprocess.check_output(['docker','logs','--tail','10000','--since','60s',
        container], text=True, stderr=subprocess.PIPE, timeout=10)
    if len(output.splitlines()) >= 10000:
        raise RuntimeError('latency log window exceeded bounded tail')
    observer_path = ROOT / 'metrics' / observer_file if observer_file else None
    observer = json.loads(observer_path.read_text()) if observer_path and observer_path.exists() else {}
    valid = time.time()-observer.get('observed_at', 0) < 5
    events = []
    for line in output.splitlines():
        try:
            row = json.loads(line)
            if isinstance(row, dict):
                events.append(row)
        except ValueError:
            pass
    last = next((row for row in reversed(events) if row.get('event') == 'published'), {})
    cache = last.get('cache') or {}
    telemetry = {'observed_at': time.time(), 'cache': {key: cache[key] for key in CACHE_FIELDS if key in cache},
                 'memtable_bytes': last.get('memtable_bytes')}
    for scope in ['foreground', 'scan']:
        telemetry[scope] = {key: value for key, value in cache.get('io', {}).get(scope, {}).items()
                            if key in ['gets', 'fetched_bytes', 'requested_bytes']}
    phases = {}
    for name in ['inputs', 'prepare_including_object_io', 'publication']:
        synthetic = '\n'.join(json.dumps({'event':'phase', 'elapsed_ms':row['phases_us'][name]/1000})
            for row in events if row.get('event') == 'published' and name in row.get('phases_us', {}))
        phases[name] = log_samples(synthetic, 'phase')
    checkpoint = log_samples(output, 'maintenance', 'elapsed_us')
    for key in ['p50_ms', 'p95_ms', 'p99_ms']:
        if checkpoint.get(key) is not None:
            checkpoint[key] /= 1000
    return {'observed_at': time.time(), 'window_seconds': 60, 'observer_healthy': valid,
            'telemetry': telemetry, 'phases': phases,
            'checkpoint': checkpoint,
            'write_to_sink': log_samples(output, 'epoch', 'received_to_sink_ms'),
            'source_commit_to_sink': observer.get('write_to_sink') if valid else {'samples':0,'p50_ms':None,'p95_ms':None},
            'source_commit_to_sink_by_source_changes': observer.get('by_source_changes', {}) if valid else {},
            'observer': {key: observer.get(key) for key in ['observed_at','pending','dropped','expired','generator_samples_skipped','poll_interval_ms','endpoint']},
            'engine': log_samples(output, 'published')}

def atomic(path, value, mode):
    temporary = path.with_suffix('.tmp')
    temporary.write_text(json.dumps(value, indent=2) + '\n')
    temporary.chmod(mode)
    os.replace(temporary, path)

def main():
    previous = json.loads(PRIVATE.read_text()) if PRIVATE.exists() else {}
    result = {'collected_at': time.time(), 'errors': []}
    try:
        helper = runpy.run_path(str(ROOT / 'verify-remote-memtable.py'))
        query = """SELECT json_build_object('observed_at',extract(epoch FROM clock_timestamp()),
          'inserts',s.n_tup_ins,'updates',s.n_tup_upd,'deletes',s.n_tup_del,
          'stats_reset',d.stats_reset,'engine_epochs',GREATEST(p.published_time-1,0),
          'source_bytes',pg_total_relation_size('public.demo_positions'),
          'sink_bytes',pg_total_relation_size('demo_ivm.book_totals_keyed'),
          'replay_bytes',pg_wal_lsn_diff(p.end_lsn,p.checkpoint_lsn),
          'retained_wal_bytes',(SELECT pg_wal_lsn_diff(pg_current_wal_lsn(),s.restart_lsn) FROM pg_replication_slots s JOIN demo_ivm.pgderive_worker_registration r ON r.slot_name=s.slot_name WHERE r.query_id=p.query_id),
          'unconfirmed_wal_bytes',(SELECT pg_wal_lsn_diff(pg_current_wal_lsn(),s.confirmed_flush_lsn) FROM pg_replication_slots s JOIN demo_ivm.pgderive_worker_registration r ON r.slot_name=s.slot_name WHERE r.query_id=p.query_id),
          'books',totals.books,'positions',totals.positions,'units',totals.units)
          FROM pg_stat_user_tables s CROSS JOIN pg_stat_database d
          CROSS JOIN demo_ivm.pgderive_progress p
          CROSS JOIN (SELECT count(*) AS books,coalesce(sum(positions),0) AS positions,coalesce(sum(units),0)::text AS units FROM demo_ivm.book_totals_keyed) totals
          WHERE s.schemaname='public' AND s.relname='demo_positions'
          AND d.datname=current_database() AND p.sink_table='book_totals_keyed'"""
        flow = json.loads(helper['sql'](query))
        generator = json.loads((ROOT / 'generator/activity-state.json').read_text())
        flow['transactions'] = generator['transactions']
        flow['generated_source_changes'] = generator['source_changes']
        flow['transaction_counter_version'] = 2
        old = previous.get('flow') or {}
        seconds = float(flow['observed_at']) - float(old.get('observed_at', flow['observed_at']))
        changes = sum(flow[k] for k in ['inserts', 'updates', 'deletes'])
        prior_changes = sum(old.get(k, 0) for k in ['inserts', 'updates', 'deletes'])
        valid = seconds > 0 and old.get('stats_reset') == flow['stats_reset'] and changes >= prior_changes
        flow['changes_per_second'] = (changes-prior_changes)/seconds if valid else None
        tx_delta = flow['transactions']-old.get('transactions',flow['transactions'])
        flow['transactions_per_second'] = tx_delta/seconds if seconds > 0 and tx_delta >= 0 and old.get('transaction_counter_version') == 2 else None
        result['flow'] = flow
    except Exception as error:
        result['flow'] = previous.get('flow')
        result['errors'].append('flow unavailable: ' + type(error).__name__)
    try:
        pid = int(subprocess.check_output(['docker','inspect','--format','{{.State.Pid}}','pgderive-demo-engine-1'], text=True))
        if pid <= 0:
            raise RuntimeError('worker stopped')
        rss = next(line.split()[1] for line in pathlib.Path(f'/proc/{pid}/status').read_text().splitlines() if line.startswith('VmRSS:'))
        result['worker'] = {'observed_at': time.time(), 'rss_bytes': int(rss)*1024}
    except Exception as error:
        result['worker'] = previous.get('worker')
        result['errors'].append('worker unavailable: ' + type(error).__name__)
    try:
        result['latency'] = latency()
        if not result['latency']['observer_healthy']:
            result['errors'].append('source-to-sink observer is unavailable or stale')
    except Exception as error:
        result['latency'] = previous.get('latency')
        result['errors'].append('latency unavailable: ' + type(error).__name__)
    if result.get('latency'):
        result['engine'] = result['latency'].pop('telemetry', previous.get('engine'))
    result['state'] = previous.get('state')
    if not result['state'] or time.time()-result['state']['observed_at'] >= 60:
        try:
            result['state'] = inventory()
        except Exception as error:
            result['errors'].append('state unavailable: ' + type(error).__name__)
    result['queries'] = {'totals': {key: result.get(key) for key in ['flow', 'worker', 'latency', 'engine', 'state']}}
    try:
        top_flow = json.loads(helper['sql'](query.replace('demo_ivm', 'demo_topk').replace('book_totals_keyed', 'book_top10')))
        top_latency = latency('pgderive-demo-engine-top10-1', None)
        pid = int(subprocess.check_output(['docker','inspect','--format','{{.State.Pid}}','pgderive-demo-engine-top10-1'], text=True))
        if pid <= 0:
            raise RuntimeError('top ten worker stopped')
        rss = next(line.split()[1] for line in pathlib.Path(f'/proc/{pid}/status').read_text().splitlines() if line.startswith('VmRSS:'))
        prior_top = previous.get('queries', {}).get('top10', {})
        top_state = prior_top.get('state')
        if not top_state or time.time()-top_state['observed_at'] >= 60:
            top_config = tomllib.loads((ROOT/'stack-r2/top10.toml').read_text())
            top_state = inventory(top_config['worker']['object_prefix'])
        result['queries']['top10'] = {'flow': top_flow, 'latency': top_latency,
            'engine': top_latency.pop('telemetry'), 'state': top_state,
            'worker': {'observed_at': time.time(), 'rss_bytes': int(rss)*1024}}
    except Exception as error:
        result['queries']['top10'] = previous.get('queries', {}).get('top10')
        result['errors'].append('top ten unavailable: ' + type(error).__name__)
    try:
        publish(ROOT, result)
    except Exception as error:
        result['errors'].append('history unavailable: ' + type(error).__name__)
    atomic(PRIVATE, result, 0o600)
    atomic(PUBLIC, result, 0o644)
    # Retain measurements without credentials or object names.
    history = ROOT / 'metrics/history.jsonl'
    if history.exists() and history.stat().st_size > 5*1024*1024:
        os.replace(history, history.with_name('history.previous.jsonl'))
    with history.open('a') as stream:
        stream.write(json.dumps(result)+'\n')
    print(json.dumps(result))

if __name__ == '__main__':
    main()
