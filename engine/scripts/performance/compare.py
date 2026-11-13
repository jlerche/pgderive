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
from common import SCHEMA_VERSION
from metrics import execution_issues, extract_engine
from workload import VARIED_SIZES, offered_changes


def verdict(baseline, candidate, relative=.10, absolute=2, *, signed=False):
    if not baseline or not candidate or any(
            not isinstance(value, (int, float)) or isinstance(value, bool)
            or not math.isfinite(value) or (value < 0 and not signed) for value in [*baseline, *candidate]):
        raise ValueError('Metric samples must be nonempty, finite and nonnegative unless explicitly signed')
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



def metric_margin(name, absolute_ms, quantum):
    name = name.removeprefix('measurement_and_drain_').removeprefix('drain_')
    if name == 'cpu_ms_per_source_change':
        return .001
    if name.endswith('_per_change'):
        return .1 if 'bytes' in name else .0001 if 'class_' in name or 'attempts' in name else .005
    if name == 'peak_rss_bytes':
        return 1024 * 1024
    if name == 'peak_state_bytes':
        return 65536
    if name == 'peak_retained_wal_bytes':
        return 16 * 1024 * 1024
    if 'pending_changes' in name:
        return quantum
    return absolute_ms


def compare_measurements(measurements, relative, absolute_ms, quantum, *, throughput_valid):
    result = {}
    for section in ['scalars', 'trends']:
        names = set().union(*(engine[section] for run in measurements for engine in run.values()))
        for name in sorted(names):
            values = [[run[label][section].get(name) for run in measurements]
                      for label in ['baseline', 'candidate']]
            if section == 'trends':
                values = [[value['delta'] if value is not None else None for value in group] for group in values]
            key = ('trend_' if section == 'trends' else '') + name
            if any(value is None for group in values for value in group):
                result[key] = {'status': 'unavailable: insufficient measurement evidence',
                               'baseline_runs': len(values[0]), 'candidate_runs': len(values[1])}
            else:
                result[key] = verdict(*values, relative, metric_margin(name, absolute_ms, quantum),
                                      signed=section == 'trends')
    if not throughput_valid:
        for value in result.values():
            value['status'] = 'invalid comparison: source offered load was not sustained comparably'
    return result

def environment_mismatches(manifests):
    """Require measured image/resource identities, not mutable configured tags."""
    mismatches = []
    for index, manifest in enumerate(manifests):
        if not manifest.get('fixtures') or not manifest.get('host') or not manifest.get('harness_sha256'):
            mismatches.append(f'run {index + 1}: missing measured fixture/host/harness fingerprint')
    if mismatches:
        return mismatches
    for field in ['host', 'fixtures', 'harness_sha256']:
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
        actual_windows = {window['start_seconds']: window['source_changes'] for window in engine['windows']}
        if any(not isinstance(value,int) or isinstance(value,bool) or value < 0 for value in actual_windows.values()):
            raise ValueError('Source change counts must be nonnegative integers')
        total = sum(actual_windows.values())
        engines[label] = {'source_changes_in_measurement_window': total,
                         'source_accepted_rows_per_second': total / duration,
                         'offered_rows_per_second': rate,
                         'offered_fraction': total / max(1, expected_total)}
        if total < expected_total - max(1, expected_total * tolerance):
            issues.append(f'{label}: total {total}/{expected_total} offered row changes')
        for start in range(0, duration, windows):
            expected = offered_changes(start, min(start + windows, duration), rate, duration,
                                       parameters['burst_seconds'])
            actual = actual_windows.get(start, 0)
            quantum = max(VARIED_SIZES) if parameters['transaction_mix'] == 'varied' else 1
            if actual < expected - max(quantum, expected * tolerance):
                issues.append(f'{label} window {start}: {actual}/{expected} offered row changes')
    old, new = (engines[label]['source_changes_in_measurement_window'] for label in ['baseline', 'candidate'])
    if abs(old - new) > max(1, max(old, new) * tolerance):
        issues.append('baseline/candidate achieved source change counts differ')
    return {'valid_for_latency_comparison': not issues, 'engines': engines, 'issues': issues,
            'tolerance_fraction': tolerance}


def capacity_check(report, manifest, minimum_fraction, maximum_drain_ms):
    candidate = report['engines']['candidate']['consumer_progress']
    offered = manifest['parameters']['rate']*manifest['parameters']['duration']
    prefix = candidate['visible_prefix_source_changes_at_input_end']
    if not isinstance(prefix,int) or isinstance(prefix,bool) or prefix < 0:
        raise ValueError('Consumer prefix must be a nonnegative integer row count')
    fraction = prefix/offered
    drain = candidate['last_marker_drain_ms']
    issues = []
    if fraction < minimum_fraction:
        issues.append('candidate consumed prefix falls below required offered-load fraction')
    if not isinstance(drain,(int,float)) or isinstance(drain,bool) or not math.isfinite(drain) or drain < 0 or drain > maximum_drain_ms:
        issues.append('candidate last-marker drain is missing or exceeds the capacity bound')
    return {'passed':not issues,'offered_prefix_fraction':fraction,'last_marker_drain_ms':drain,
            'minimum_fraction':minimum_fraction,'maximum_drain_ms':maximum_drain_ms,'issues':issues}


def gate_failures(result):
    """Fail closed when controlled evidence cannot establish a passing comparison."""
    failures = []
    if any(run['issues'] for run in result['qualification']):
        failures.append('Canonical metrics, paired execution or source equivalence did not qualify')
    if set(result['pair_orders']) != {'baseline-first', 'candidate-first'}:
        failures.append('Both paired execution orders are required')
    if any(not run['passed'] for run in result['capacity']):
        failures.append('candidate consumer capacity bound failed')
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
    parser.add_argument('--minimum-consumed-fraction', type=float, default=.98)
    parser.add_argument('--maximum-drain-ms', type=float, default=1000)
    parser.add_argument('--gate', action='store_true', help='Fail on regressions, invalid load, exploratory evidence or fewer than three repeats')
    args = parser.parse_args()
    if not math.isfinite(args.minimum_consumed_fraction) or not 0 < args.minimum_consumed_fraction <= 1 or not math.isfinite(args.maximum_drain_ms) or args.maximum_drain_ms < 0:
        parser.error('Consumed fraction must be in (0,1]; drain bound must be finite and nonnegative')
    reports = [json.loads(pathlib.Path(path).read_text()) for path in args.reports]
    manifests = [json.loads(pathlib.Path(path).with_name('manifest.json').read_text()) for path in args.reports]
    if any(row.get('schema_version') != SCHEMA_VERSION for row in [*reports, *manifests]):
        parser.error('Only current performance evidence schema is accepted; rerun old experiments')
    if args.gate:
        validate_runs(args.reports, reports, manifests)
    ignored = {'output', 'baseline', 'candidate', 'pair_order'}
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
    capacity = [capacity_check(report, manifest, args.minimum_consumed_fraction, args.maximum_drain_ms)
                for report,manifest in zip(reports,manifests)]
    result = {'capacity':capacity, 'throughput': {'status': 'matched source offered load' if throughput_valid else 'source offered load not sustained or accepted rates differ',
                             'runs': checks}, 'environment_mismatches': mismatches,
              'exploratory': bool(mismatches)}
    measurements, qualifications = [], []
    for path, report, manifest in zip(args.reports, reports, manifests):
        directory = pathlib.Path(path).parent
        measured = {label: extract_engine(directory / label, report['engines'][label], manifest['parameters'])
                    for label in ['baseline', 'candidate']}
        measurements.append(measured)
        issues = execution_issues(manifest, report['engines'])
        for label, engine in measured.items():
            issues.extend(label + ': ' + issue for issue in engine['qualification_issues'])
        qualifications.append({'run_id': report['run_id'], 'issues': issues})
    quantum = max(VARIED_SIZES) if manifests[0]['parameters']['transaction_mix'] == 'varied' else 1
    result.update(compare_measurements(measurements, args.relative_margin, args.absolute_ms, quantum, throughput_valid=throughput_valid))
    result['qualification'] = qualifications
    result['pair_orders'] = [manifest['parameters']['pair_order'] for manifest in manifests]
    result['measurements'] = measurements
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
