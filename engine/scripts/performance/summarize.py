#!/usr/bin/env python3
"""Read-only compact summaries of saved benchmark reports and marker timelines."""
import argparse
import gzip
import json
import pathlib

from common import SCHEMA_VERSION
from metrics import extract_engine
from request_metrics import object_requests, request_windows


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
    accepted_changes = sum(record['source_changes'] for record in records if start <= record['commit'] < end)
    markers = {record['tick'] for record in records if record['marker']}
    observed = {int(tick): at for tick, at in arrivals.items() if int(tick) in markers}
    visible = max((tick for tick, at in observed.items() if at <= end), default=0)
    last = max(markers, default=None)
    return {'source_commits_in_input_window': accepted,
            'source_accepted_rows_per_second': accepted_changes / duration,
            'source_changes_in_input_window': accepted_changes,
            'visible_marker_prefix_transactions_at_input_end': visible,
            'visible_marker_prefix_transactions_per_second': visible / duration,
            'visible_marker_prefix_rows_per_second': sum(record['source_changes'] for record in records if record['tick'] <= visible) / duration,
            'prefix_is_lower_bound': workload == 'mixed',
            'marker_samples': len(observed),
            'last_source_marker_tick': last,
            'last_marker_observed': last in observed if last is not None else False,
            'last_marker_drain_ms': max(0, (observed[last] - end) * 1000)
                if last is not None and last in observed else None,
            'scope': 'Marker progress does not prove multi-key external-sink atomicity. '
                     'Mixed latency/progress samples marker updates; trailing non-marker DML requires final oracle.'}


def summarize_report(path):
    report = json.loads(path.read_text())
    manifest = json.loads(path.with_name('manifest.json').read_text())
    if report.get('schema_version') != SCHEMA_VERSION or manifest.get('schema_version') != SCHEMA_VERSION:
        raise ValueError('Only current performance evidence schema is accepted; rerun old experiments')
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
                   'source_changes_total': engine['source_changes'],
                   'transaction_sizes': engine['transaction_sizes'],
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
                   'execution': extract_engine(directory, engine, parameters) if label != 'risingwave' else None,
                   'object_requests': object_requests(directory),
                   'object_request_windows': request_windows(directory, timeline['start_monotonic'],
                                                            parameters['duration'], parameters['window_seconds'])}
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
