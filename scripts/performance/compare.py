#!/usr/bin/env python3
"""Compare repeat runs using run-level medians and explicit practical/noise margins.

A nonzero exit always signals correctness/cleanup failures. --gate additionally
requires controlled repeat runs, matched throughput and no measured regressions.
"""
import argparse
import json
import math
import pathlib
import statistics


def verdict(baseline, candidate, relative=.10, absolute=2):
    if not baseline or not candidate or any(
            not isinstance(value, (int, float)) or isinstance(value, bool)
            or not math.isfinite(value) or value < 0 for value in [*baseline, *candidate]):
        raise ValueError('Metric samples must be nonempty, finite and nonnegative')
    old, new = statistics.median(baseline), statistics.median(candidate)
    # MAD across independent runs, not pseudo-replication of per-row samples.
    noise = 3 * max(statistics.median(abs(v - old) for v in baseline),
                    statistics.median(abs(v - new) for v in candidate))
    threshold = max(abs(old) * relative, absolute, noise)
    status = 'inconclusive: repeat runs required' if min(len(baseline), len(candidate)) < 3 else (
        'possible regression' if new - old > threshold else
        'improvement' if old - new > threshold else 'within noise/practical margin')
    return {'baseline_median': old, 'candidate_median': new, 'delta': new - old,
            'threshold': threshold, 'status': status,
            'baseline_runs': len(baseline), 'candidate_runs': len(candidate)}


def environment_mismatches(manifests):
    """Require measured image/resource identities, not mutable configured tags."""
    mismatches = []
    for index, manifest in enumerate(manifests):
        if not manifest.get('fixtures') or not manifest.get('host'):
            mismatches.append(f'run {index + 1}: missing measured fixture/host fingerprint')
    if mismatches:
        return mismatches
    for field in ['host', 'fixtures']:
        if any(manifest[field] != manifests[0][field] for manifest in manifests[1:]):
            mismatches.append(field + ' fingerprint differs across runs')
    return mismatches


def throughput_check(report, manifest, tolerance=.02):
    """Latency comparisons require matched source acceptance in every measurement window."""
    parameters = manifest['parameters']
    rate, duration = parameters['rate'], parameters['duration']
    windows = parameters['window_seconds']
    expected_total = int(rate * duration)
    engines, issues = {}, []
    for label in ['baseline', 'candidate']:
        engine = report['engines'][label]
        actual_windows = {window['start_seconds']: window['transactions'] for window in engine['windows']}
        total = sum(actual_windows.values())
        engines[label] = {'commits_in_measurement_window': total,
                         'source_accepted_rows_per_second': total / duration,
                         'offered_rows_per_second': rate,
                         'offered_fraction': total / max(1, expected_total)}
        for start in range(0, duration, windows):
            expected = int(rate * min(windows, duration - start))
            actual = actual_windows.get(start, 0)
            if actual < expected - max(1, expected * tolerance):
                issues.append(f'{label} window {start}: {actual}/{expected} offered commits')
    old, new = (engines[label]['commits_in_measurement_window'] for label in ['baseline', 'candidate'])
    if abs(old - new) > max(1, max(old, new) * tolerance):
        issues.append('baseline/candidate achieved commit counts differ')
    return {'valid_for_latency_comparison': not issues, 'engines': engines, 'issues': issues,
            'tolerance_fraction': tolerance}


def latency_verdict(baseline, candidate, throughput_valid, relative=.10, absolute=2):
    result = verdict(baseline, candidate, relative, absolute)
    if not throughput_valid:
        result['status'] = 'invalid latency comparison: offered/achieved throughput mismatch'
    return result


def gate_failures(result):
    """Fail closed when controlled evidence cannot establish a passing comparison."""
    failures = []
    if result['exploratory']:
        failures.append('exploratory environment cannot qualify a release')
    if any(not run['valid_for_latency_comparison'] for run in result['throughput']['runs']):
        failures.append('offered source load was not sustained comparably')
    for name, value in result.items():
        if not isinstance(value, dict) or 'baseline_runs' not in value:
            continue
        if min(value['baseline_runs'], value['candidate_runs']) < 3:
            failures.append(name + ': at least three controlled repeats required')
        elif value['status'] not in {'improvement', 'within noise/practical margin'}:
            failures.append(name + ': ' + value['status'])
    return failures


def validate_runs(paths, reports, manifests):
    identities = [report.get('run_id') for report in reports]
    if any(not isinstance(identity, str) or not identity for identity in identities) or len(set(identities)) != len(identities):
        raise ValueError('Gate requires distinct nonempty run identities')
    if len({pathlib.Path(path).resolve() for path in paths}) != len(paths):
        raise ValueError('Gate requires distinct report paths')
    if any(report['run_id'] != manifest.get('run_id') for report, manifest in zip(reports, manifests)):
        raise ValueError('Report and manifest run identities must match')


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('reports', nargs='+')
    parser.add_argument('--relative-margin', type=float, default=.10)
    parser.add_argument('--absolute-ms', type=float, default=2)
    parser.add_argument('--throughput-tolerance', type=float, default=.02)
    parser.add_argument('--exploratory', action='store_true', help='Allow unmatched/missing environment fingerprints; mark results exploratory')
    parser.add_argument('--gate', action='store_true', help='Fail on regressions, invalid load, exploratory evidence or fewer than three repeats')
    args = parser.parse_args()
    reports = [json.loads(pathlib.Path(path).read_text()) for path in args.reports]
    manifests = [json.loads(pathlib.Path(path).with_name('manifest.json').read_text()) for path in args.reports]
    if args.gate:
        validate_runs(args.reports, reports, manifests)
    ignored = {'output', 'baseline', 'candidate'}
    signatures = [{key: value for key, value in manifest['parameters'].items() if key not in ignored}
                  for manifest in manifests]
    if any(signature != signatures[0] for signature in signatures):
        raise SystemExit('Scenario parameters differ; compare only matched repeats')
    if any(manifest['binaries'] != manifests[0]['binaries'] for manifest in manifests):
        raise SystemExit('Binary identities/paths differ across repeats')
    if not all(math.isfinite(value) and value >= 0 for value in [args.relative_margin, args.absolute_ms, args.throughput_tolerance]) or args.throughput_tolerance >= 1:
        parser.error('Margins must be finite/nonnegative and throughput tolerance below one')
    mismatches = environment_mismatches(manifests)
    if mismatches and not args.exploratory:
        raise SystemExit('; '.join(mismatches) + '; --exploratory permits diagnostic comparisons')
    if not all(r['passed'] for r in reports):
        raise SystemExit('Correctness/cleanup failure in supplied report')
    checks = [throughput_check(report, manifest, args.throughput_tolerance) for report, manifest in zip(reports, manifests)]
    throughput_valid = all(check['valid_for_latency_comparison'] for check in checks)
    result = {'throughput': {'status': 'matched source offered load' if throughput_valid else 'source offered load not sustained or accepted rates differ',
                             'runs': checks}, 'environment_mismatches': mismatches,
              'exploratory': bool(mismatches)}
    for metric in ['p50', 'p95', 'p99']:
        result['sink_visibility_ms_' + metric] = latency_verdict(
            [r['engines']['baseline']['sink_visibility_ms'][metric] for r in reports],
            [r['engines']['candidate']['sink_visibility_ms'][metric] for r in reports],
            throughput_valid, args.relative_margin, args.absolute_ms)
    for metric, extract in [
        ('cpu_ms_per_transaction', lambda e: e['process_cpu_seconds'] * 1000 / max(1, e['transactions'])),
        ('peak_rss_bytes', lambda e: max(s['rss_bytes'] for s in e['samples'])),
        ('peak_state_bytes', lambda e: max(s['state']['bytes'] for s in e['samples'])),
        ('peak_retained_wal_bytes', lambda e: max(s['retained_wal_bytes'] for s in e['samples']))]:
        result[metric] = verdict([extract(r['engines']['baseline']) for r in reports],
                                 [extract(r['engines']['candidate']) for r in reports],
                                 args.relative_margin, 0)
    if mismatches:
        for value in result.values():
            if isinstance(value, dict) and 'status' in value:
                value['status'] = 'exploratory: ' + value['status']
    result['notes'] = ['Match scenario, hardware, image IDs and proxy settings before comparing.',
                       'Inspect achieved throughput and per-window trends; fast results with dropped offered load are not wins.',
                       'Gate mode requires three controlled repeats and rejects possible regressions.']
    if args.gate:
        result['gate'] = {'failures': gate_failures(result)}
        result['gate']['passed'] = not result['gate']['failures']
    print(json.dumps(result, indent=2))
    if args.gate and not result['gate']['passed']:
        raise SystemExit(1)


if __name__ == '__main__':
    main()
