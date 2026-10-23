#!/usr/bin/env python3
"""Read-only compact summaries of saved benchmark reports and marker timelines."""
import argparse
import collections
import gzip
import json
import pathlib

from common import distribution, SCHEMA_VERSION


def marker_progress(timeline, duration, arrivals, workload):
    """Marker observations distinguish source acceptance from consumer progress.

    Only client-acknowledged commits inside the measurement window count as
    accepted. Marker visibility can race the client's acknowledgement; retain
    the observed prefix separately rather than inventing a server commit time.
    Mixed inserts/deletes between marker updates have no individual timestamps.
    """
    start = timeline['start_monotonic']
    end = start + duration
    records = timeline['transactions']
    accepted = sum(start <= record['commit'] < end for record in records)
    markers = {record['tick'] for record in records if record['marker']}
    observed = {int(tick): at for tick, at in arrivals.items() if int(tick) in markers}
    visible = max((tick for tick, at in observed.items() if at <= end), default=0)
    last = max(markers, default=None)
    return {'source_commits_in_input_window': accepted,
            'source_accepted_rows_per_second': accepted / duration,
            'visible_marker_prefix_transactions_at_input_end': visible,
            'visible_marker_prefix_transactions_per_second': visible / duration,
            'prefix_is_lower_bound': workload == 'mixed',
            'marker_samples': len(observed),
            'last_source_marker_tick': last,
            'last_marker_observed': last in observed if last is not None else False,
            'last_marker_drain_ms': max(0, (observed[last] - end) * 1000)
                if last is not None and last in observed else None,
            'scope': 'Marker progress does not prove multi-key external-sink atomicity. '
                     'Mixed latency/progress samples marker updates; trailing non-marker DML requires final oracle.'}


def read_events(directory):
    path = directory / 'events.jsonl'
    if not path.exists():
        return None
    events = [json.loads(line) for line in path.read_text().splitlines()]
    publications = [event for event in events if event.get('event') == 'published']
    maintenance = [event for event in events if event.get('event') == 'maintenance']
    return {'publication_epochs': len(publications),
            'source_transactions_per_epoch': distribution([event.get('source_transactions', 1) for event in publications]),
            'source_changes_per_epoch': distribution([event['changes'] for event in publications]),
            'processing_ms': distribution([event['elapsed_ms'] for event in publications]),
            'maintenance_ms': distribution([event['elapsed_us'] / 1000 for event in maintenance]),
            'event_scope': 'All saved engine events, including post-input drain and shutdown maintenance.'}


def object_requests(directory):
    """Count completed proxy attempt records, including failures and retries."""
    methods, classes, statuses = collections.Counter(), collections.Counter(), collections.Counter()
    uploaded = downloaded = unknown_upload_sizes = transport_errors = 0
    with (directory / 'proxy.jsonl').open() as stream:
        for line in stream:
            row = json.loads(line)
            if 'error' in row:
                transport_errors += 1
            if 'ordinal' not in row:
                continue
            method = row['method']
            category = ('class_a' if method == 'PUT' or (method == 'GET' and row['listing']) else
                        'class_b' if method in {'GET', 'HEAD'} else
                        'free' if method == 'DELETE' else 'unclassified')
            methods[method] += 1
            classes[category] += 1
            statuses[str(row['status'])] += 1
            if row['request_body_bytes'] is None:
                unknown_upload_sizes += 1
            else:
                uploaded += row['request_body_bytes']
            downloaded += row['response_body_bytes']
    return {'attempts': sum(methods.values()), 'methods': dict(methods),
            'estimated_r2_classes': dict(classes), 'status_codes': dict(statuses),
            'transport_errors': transport_errors, 'request_body_bytes': uploaded,
            'response_body_bytes': downloaded, 'unknown_request_body_sizes': unknown_upload_sizes,
            'scope': 'All completed proxy attempts, including bootstrap, drain, GC and retries. '
                     'Class estimates use HTTP method/listing flags; POST is unclassified because '
                     'multipart versus bulk deletion is not recorded. Not a billing invoice.'}


def summarize_report(path):
    report = json.loads(path.read_text())
    manifest = json.loads(path.with_name('manifest.json').read_text())
    parameters = manifest['parameters']
    result = {'schema_version': SCHEMA_VERSION, 'run_id': report['run_id'],
              'passed': report['passed'], 'parameters': parameters,
              'binaries': manifest['binaries'], 'engines': {}}
    for label, engine in report['engines'].items():
        directory = path.parent / label
        with gzip.open(directory / 'timeline.json.gz', 'rt') as stream:
            timeline = json.load(stream)
        samples = engine['samples']
        summary = {'source_transactions_total': engine['transactions'],
                   'source_acceptance_rate_including_late_commits': engine['actual_rows_per_second'],
                   'sink_progress': marker_progress(timeline, parameters['duration'],
                                                   timeline['sink_arrivals'], parameters['workload']),
                   'sink_visibility_ms': engine['sink_visibility_ms'],
                   'native_visibility_ms': engine.get('native_visibility_ms'),
                   'cpu_ms_per_source_transaction': engine['process_cpu_seconds'] * 1000 / max(1, engine['transactions']),
                   'peak_rss_bytes': max((s['rss_bytes'] for s in samples), default=None),
                   'peak_state_bytes': max((s['state']['bytes'] for s in samples), default=None),
                   'peak_retained_wal_bytes': max((s['retained_wal_bytes'] for s in samples), default=None),
                   'native_equal': engine['oracle_equal'], 'ack_safe': engine.get('ack_safe'),
                   'execution': read_events(directory),
                   'object_requests': object_requests(directory)}
        if timeline['native_arrivals']:
            summary['native_progress'] = marker_progress(timeline, parameters['duration'],
                                                        timeline['native_arrivals'], parameters['workload'])
        result['engines'][label] = summary
    result['limitations'] = ['Source acceptance rate is not engine consumption throughput.',
        'CPU measurement includes drain work and is divided by source transaction count; it covers the measured engine process only.',
        'Resource peaks use periodic samples; raw event distributions cover the complete saved run.',
        'Missing engine entries in a failed report remain absent; inspect raw logs and failure evidence.']
    return result


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('reports', nargs='+', type=pathlib.Path)
    args = parser.parse_args()
    print(json.dumps({'schema_version': SCHEMA_VERSION,
                      'runs': [summarize_report(path) for path in args.reports]}, indent=2))


if __name__ == '__main__':
    main()
