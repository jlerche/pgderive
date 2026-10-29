"""Bounded public history of explicitly public aggregate measurements."""
import json
import math

DAY = 86400
ROOTS = {'flow', 'worker', 'latency', 'state', 'engine'}
CACHE_FIELDS = ['bytes','live_bytes','entries','filter_bytes','filter_entries','point_bytes',
                'point_entries','index_bytes','index_entries','index_decoded_bytes',
                'index_encoded_bytes','index_encoded_entries','index_reads','index_read_bytes',
                'index_requested_bytes','filter_rejections','filter_eviction_steps',
                'misses','hits','point_hits','decoded_rows','decoded_bytes','pins']
METRICS = {'worker.rss_bytes', 'state.bytes', 'state.objects', 'engine.memtable_bytes',
           'engine.memtable_limit', 'engine.runs', 'engine.logical_time',
           'latency.observer_healthy', 'latency.published_epochs', 'latency.published_source_transactions'}
METRICS.update('engine.object_service.'+scope+'.'+key for scope in ['foreground','maintenance']
    for key in ['requests','read_requested_bytes','read_bytes','write_requested_bytes','written_bytes'])
METRICS.update('engine.cache.' + key for key in CACHE_FIELDS)
METRICS.update('flow.' + key for key in ['inserts','updates','deletes','engine_epochs','products',
    'order_lines','net_units','net_revenue_cents','transactions','generated_source_changes','changes_per_second',
    'transactions_per_second','source_bytes','sink_bytes','replay_bytes','retained_wal_bytes',
    'unconfirmed_wal_bytes'])
METRICS.update('engine.' + scope + '.' + key for scope in ['foreground','scan']
    for key in ['gets','fetched_bytes','requested_bytes'])
METRICS.update('latency.observer.' + key for key in ['pending','dropped','expired','generator_samples_skipped'])
DISTRIBUTIONS = ['engine','write_to_sink','source_commit_to_sink','checkpoint',
    'phases.inputs','phases.prepare_including_object_io','phases.publication']
DISTRIBUTIONS += ['source_commit_to_sink_by_source_changes.' + size
    for size in ['1','2-31','32-127','128-511','512-1000']]
METRICS.update('latency.' + name + '.' + field for name in DISTRIBUTIONS
    for field in ['p50_ms','p95_ms','p99_ms','samples'])
SKIP = {'observed_at', 'window_seconds', 'transaction_counter_version', 'poll_interval_ms'}


def flatten(sample):
    result = {'health.errors': len(sample.get('errors', []))}
    now = sample['collected_at']
    def visit(value, path):
        if isinstance(value, dict):
            if 'observed_at' in value:
                stamp = value['observed_at']
                if not isinstance(stamp, (int, float)) or not math.isfinite(stamp):
                    return
                if now - stamp > (150 if path == 'state' else 75) or stamp - now > 60:
                    return
            for key, child in value.items():
                if key not in SKIP:
                    visit(child, path + '.' + key)
        elif path in METRICS and isinstance(value, (int, float)) and math.isfinite(value):
            result[path] = int(value) if isinstance(value, bool) else value
        elif path in ['flow.net_units','flow.net_revenue_cents']:
            try:
                number = float(value)
                if math.isfinite(number):
                    result[path] = number
            except (ValueError, TypeError):
                pass
    for key in ROOTS:
        visit(sample.get(key), key)
    return result


def update(document, samples, now):
    # At most one last-observed sample per minute, never invented interpolation.
    cutoff = now - DAY
    columns = list(document.get('columns', []))
    records = {int(row[0] // 60): (row[0], dict(zip(columns, row[1:])))
               for row in document.get('rows', []) if cutoff <= row[0] <= now}
    for sample in samples:
        if not isinstance(sample, dict):
            continue
        stamp = sample.get('collected_at', 0)
        if not isinstance(stamp, (int, float)) or not math.isfinite(stamp) or not cutoff <= stamp <= now:
            continue
        minute = int(stamp // 60)
        if minute not in records or stamp >= records[minute][0]:
            records[minute] = (stamp, flatten(sample))
    records = sorted(records.values())[-1441:]
    columns = sorted({key for _, values in records for key in values})
    return {'version': 1, 'updated_at': now, 'retention_seconds': DAY,
            'columns': columns,
            'rows': [[stamp, *(values.get(key) for key in columns)] for stamp, values in records]}


def publish_query(root, sample, name):
    path = root / 'stack-r2/web' / ('metrics-history.json' if name == 'totals' else 'metrics-history-'+name+'.json')
    now = sample['collected_at']
    samples = [sample]
    if path.exists():
        document = json.loads(path.read_text())
    else:
        document = {}
        # Reuse existing retained observations on first installation only.
        for filename in ['history.previous.jsonl', 'history.jsonl']:
            source = root / 'metrics' / filename
            if source.exists():
                with source.open() as stream:
                    for line in stream:
                        try:
                            record = json.loads(line)
                            query = record.get('queries', {}).get(name)
                            if query:
                                samples.append(query | {'collected_at': record['collected_at']})
                            elif name == 'totals':
                                samples.append(record)
                        except (ValueError, TypeError):
                            continue
    document = update(document, samples, now)
    temporary = path.with_suffix('.tmp')
    temporary.write_text(json.dumps(document, separators=(',', ':'), allow_nan=False) + '\n')
    temporary.chmod(0o644)
    temporary.replace(path)


def publish(root, sample):
    for name, query in sample['queries'].items():
        publish_query(root, query | {'collected_at': sample['collected_at']}, name)
