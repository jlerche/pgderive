"""Canonical input-window and growth measurements from retained raw evidence."""
import gzip
import json
import math

from common import distribution
from request_metrics import object_requests


def number(value):
    if not isinstance(value, (int, float)) or isinstance(value, bool) or not math.isfinite(value) or value < 0:
        raise ValueError('Expected a finite nonnegative measurement: ' + repr(value))
    return value



def lsn(value):
    if not isinstance(value, str):
        raise ValueError('WAL fences must be PostgreSQL LSN text')
    parts = value.split('/')
    if len(parts) != 2 or any(not part or len(part) > 8 or any(character not in '0123456789abcdefABCDEF' for character in part) for part in parts):
        raise ValueError('Invalid PostgreSQL WAL fence: ' + repr(value))
    return (int(parts[0], 16) << 32) + int(parts[1], 16)

def percentile(values, name):
    return distribution([number(value) for value in values])[name] if values else None


def aggregate(records, arrivals, events, samples, requests, begin, end):
    """Quantiles use raw observations, never quantiles of window quantiles."""
    committed = [row for row in records if begin <= row['commit'] < end]
    changes = sum(number(row['source_changes']) for row in committed)
    visible = [max(0, (arrivals[str(row['tick'])] - row['commit']) * 1000)
               for row in committed if row['marker']]
    selected = [event for event in events if begin <= event['_observed_at'] < end]
    processing = [number(event['elapsed_ms']) for event in selected if event['event'] == 'published']
    receipt = [number(event['received_to_sink_ms']) for event in selected if event['event'] == 'epoch']
    checkpoints = [number(event['elapsed_us']) / 1000 for event in selected if event['event'] == 'maintenance']
    resources = [sample for at, sample in samples if begin <= at < end]
    classes = requests['estimated_r2_classes']
    scalars = {'sink_visibility_ms_' + name: percentile(visible, name) for name in ['p50', 'p95', 'p99']}
    for label, values in [('processing_ms', processing), ('received_to_sink_ms', receipt)]:
        scalars.update({label + '_' + name: percentile(values, name) for name in ['p50', 'p95']})
    scalars['checkpoint_wall_ms_p95'] = percentile(checkpoints, 'p95')
    scalars['checkpoint_wall_ms_per_change'] = sum(checkpoints) / changes if changes else None
    for label, field in [('peak_rss_bytes', lambda row: row['rss_bytes']),
                         ('peak_state_bytes', lambda row: row['state']['bytes']),
                         ('peak_retained_wal_bytes', lambda row: row['retained_wal_bytes']),
                         ('peak_pending_changes', lambda row: row['pending_source_changes_after_marker'])]:
        values = [number(field(row)) for row in resources]
        scalars[label] = max(values) if values else None
    for label, count in [('class_a', classes.get('class_a', 0)),
                         ('class_b', classes.get('class_b', 0)),
                         ('object_attempts', requests['attempts']),
                         ('uploaded_bytes', requests['request_body_bytes'])]:
        scalars[label + '_per_change'] = number(count) / changes if changes else None
    return {'scalars': scalars, 'observations': {'source_changes': changes, 'markers': len(visible),
        'processing': len(processing), 'receipt': len(receipt), 'checkpoints': len(checkpoints),
        'resources': len(resources)}, 'requests': requests}


def extract_engine(directory, engine, parameters):
    with gzip.open(directory / 'timeline.json.gz', 'rt') as stream:
        timeline = json.load(stream)
    events = [json.loads(line) for line in (directory / 'events.jsonl').read_text().splitlines()]
    start = number(timeline['start_monotonic'])
    duration, width = parameters['duration'], parameters['window_seconds']
    warmup = parameters['warmup_windows']
    expected = list(range(0, duration, width))
    actual = [window['start_seconds'] for window in engine['windows']]
    if actual != expected:
        raise ValueError('Missing, unordered or duplicate measurement windows')
    samples = [(start + number(row['seconds']), row) for row in engine['samples']]
    if any(left[0] >= right[0] for left, right in zip(samples, samples[1:])):
        raise ValueError('Resource samples must have strictly increasing timestamps')
    for event in events:
        number(event['_observed_at'])
    for at in timeline['sink_arrivals'].values():
        number(at)
    for row in timeline['transactions']:
        number(row['commit'])
        if not isinstance(row['source_changes'], int) or isinstance(row['source_changes'], bool) or row['source_changes'] <= 0:
            raise ValueError('Source transaction change counts must be positive integers')
    if len(timeline['transactions']) != engine['transactions'] or sum(number(row['source_changes']) for row in timeline['transactions']) != engine['source_changes']:
        raise ValueError('Raw transactions and reported source changes differ')
    fences = [lsn(row['source_wal_fence']) for row in timeline['transactions']]
    if not fences or any(left >= right for left, right in zip(fences, fences[1:])):
        raise ValueError('Source WAL fences must increase with complete transactions')
    progress = engine['final_progress']
    if progress['source_wal_fence'] != timeline['transactions'][-1]['source_wal_fence'] or lsn(progress['published_end_lsn']) < fences[-1]:
        raise ValueError('Final publication does not cover the last source transaction WAL fence')
    accepted = sum(number(row['source_changes']) for row in timeline['transactions']
                   if start <= row['commit'] < start + duration)
    if accepted != sum(number(window['source_changes']) for window in engine['windows']):
        raise ValueError('Raw timeline and reported source progress differ')
    args = (timeline['transactions'], timeline['sink_arrivals'], events, samples)
    whole = aggregate(*args, object_requests(directory, (start, start + duration)), start, start + duration)
    scalars, issues = whole['scalars'], []
    counts = whole['observations']
    for label, minimum in [('markers', 20), ('processing', 20), ('receipt', 20), ('checkpoints', 3), ('resources', 3)]:
        if counts[label] < minimum:
            issues.append(f'{label}: {counts[label]} observations, need {minimum}')
    if not samples or samples[0][0] > start + parameters['sample_seconds'] or samples[-1][0] < start + duration - parameters['sample_seconds'] - .5:
        issues.append('Resource samples do not cover the input interval')
    if whole['requests']['unknown_request_body_sizes']:
        issues.append('Unknown upload sizes prevent request-byte qualification')
    if whole['requests']['transport_errors']:
        issues.append('Transport errors occurred in the input interval')
    for label, field in [('peak_rss_bytes', lambda row: row['rss_bytes']),
                         ('peak_state_bytes', lambda row: row['state']['bytes']),
                         ('peak_retained_wal_bytes', lambda row: row['retained_wal_bytes']),
                         ('peak_pending_changes', lambda row: row['pending_source_changes_after_marker'])]:
        scalars['measurement_and_drain_' + label] = max(number(field(row)) for _, row in samples) if samples else None
        drain = [number(field(row)) for at, row in samples if at >= start + duration]
        scalars['drain_' + label] = max(drain) if drain else None
    # Exact final queue drain additionally requires the last DML WAL fence to be
    # covered by the published source commit boundary. Bag equality alone cannot
    # prove consumption of trailing unmarked transactions with net-zero output.
    markers = [row for row in timeline['transactions'] if row['marker']]
    if engine['ack_safe'] is not True or not engine['oracle_equal'] or any(str(row['tick']) not in timeline['sink_arrivals'] for row in markers):
        raise ValueError('Final native equality or marker drain missing')
    scalars['final_pending_changes'] = 0
    scalars['cpu_ms_per_source_change'] = number(engine['process_cpu_seconds']) * 1000 / max(1, number(engine['source_changes']))
    trends = {}
    full = duration // width
    steady = full - warmup
    third = steady // 3
    if duration % width or warmup < 1 or third < 2:
        issues.append('Growth qualification needs warmup plus at least six complete steady windows')
    else:
        ranges = [(start + warmup * width, start + (warmup + third) * width),
                  (start + (full - third) * width, start + full * width)]
        parts = [aggregate(*args, object_requests(directory, bounds), *bounds) for bounds in ranges]
        trend_names = ['sink_visibility_ms_p95', 'processing_ms_p95', 'received_to_sink_ms_p95',
                       'checkpoint_wall_ms_p95', 'peak_pending_changes', 'peak_rss_bytes',
                       'peak_state_bytes', 'peak_retained_wal_bytes', 'class_a_per_change',
                       'class_b_per_change', 'object_attempts_per_change', 'uploaded_bytes_per_change']
        for index, part in enumerate(parts):
            for label, minimum in [('markers', 20), ('processing', 20), ('receipt', 20), ('checkpoints', 1), ('resources', 2)]:
                if part['observations'][label] < minimum:
                    issues.append(f'Growth part {index}: insufficient {label} observations')
        for name in trend_names:
            early, late = (part['scalars'][name] for part in parts)
            if early is None or late is None:
                issues.append('Missing growth metric: ' + name)
            else:
                trends[name] = {'early': early, 'late': late, 'delta': late - early}
    if any(value is None for value in scalars.values()):
        issues.append('Missing canonical scalar measurements')
    return {'scalars': scalars, 'trends': trends, 'qualification_issues': issues,
            'observations': counts, 'input_requests': whole['requests'],
            'lifecycle_requests': object_requests(directory),
            'scope': 'Input-window quantiles and attempts; resource peaks separately include measurement and bounded drain; CPU includes bounded drain. Checkpoint wall time is background work, not foreground stall time. Queue uses marker-prefix upper bounds. Trends compare post-warmup first and last thirds.'}


def execution_issues(manifest, reports):
    """Fingerprint equality alone cannot establish isolation or workload equality."""
    issues = []
    if manifest['parameters']['execution_mode'] != 'sequential':
        issues.append('Concurrent execution is diagnostic, not a qualifying paired comparison')
    intervals = manifest['execution_intervals']
    order = manifest['execution_order']
    expected = ['baseline', 'candidate'] if manifest['parameters']['pair_order'] == 'baseline-first' else ['candidate', 'baseline']
    if 'risingwave' in reports:
        expected.append('risingwave')
    if order != expected:
        issues.append('Actual execution order differs from the declared pair order')
    if [row['engine'] for row in intervals] != order or set(order) != set(reports):
        issues.append('Execution order/interval evidence differs from measured engines')
    for row in intervals:
        if number(row['closed_at']) <= number(row['started_at']):
            issues.append('Invalid execution interval')
    for left, right in zip(intervals, intervals[1:]):
        if left['closed_at'] > right['started_at']:
            issues.append('Paired fixture lifetimes overlap')
    maximum_load = number(manifest['parameters']['maximum_host_load_per_cpu'])
    observations = [row[field] for row in intervals for field in ['host_before', 'host_after']]
    observations.extend(sample['host'] for engine in reports.values() for sample in engine['samples'])
    if not observations or any(number(row['load_per_cpu']) > maximum_load
                               or row['external_performance_containers'] for row in observations):
        issues.append('Sampled host load or competing performance fixtures violate quiet-host controls')
    if reports['baseline']['source_digest'] != reports['candidate']['source_digest']:
        issues.append('Final source state differs between paired engines')
    return issues
