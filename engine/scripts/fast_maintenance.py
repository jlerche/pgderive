"""Short, deliberately strict maintenance acceptance through existing production paths."""
import json
import pathlib
from fast_cycles import boundary_indices, interval, trend_checks
from fast_endurance import assess_report
from fast_replay import LIFECYCLE, endurance
from performance.common import sha256

COLD_TEST = 'engine::trace::window_tests::report_cold_root_scaling'
REVISION = 'maintenance-v1'
SIZES = (100, 1000, 10000)


def request_budget():
    return json.loads((pathlib.Path(__file__).parent/'performance/baselines/maintenance-v1.json').read_text())


def validate_workload(path):
    digest = sha256(path)
    if digest != request_budget()['trace_sha256']:
        raise ValueError('Maintenance workload changed without a new profile/baseline revision')
    return digest


def compiler_processes():
    names = {'rustc', 'clippy-driver', 'cc1', 'cc1plus', 'rust-lld', 'ld.lld', 'lto1'}
    result = []
    for process in pathlib.Path('/proc').iterdir():
        if not process.name.isdigit():
            continue
        try:
            name = (process/'comm').read_text().strip()
        except OSError:
            continue
        if name in names:
            result.append(process.name + ':' + name)
    return result


def suite():
    # Small cache deliberately exposes the same cache/state boundary quickly.
    # It does not predict absolute production latency or memory requirements.
    cases = [endurance(rows, 512, batch_rows=64, growing=growing, group_width=1,
                       oracle_interval=128, initial_random_amounts=True)
             for rows, growing in ((512, False), (8192, False), (8192, True))]
    return dict(version=2, limits=dict(cache_bytes=128*1024, cache_entries=64),
                lifecycle=dict(LIFECYCLE, checkpoint_interval_ms=128), cases=cases)


def exact_work(samples):
    histogram = [0] * 64
    for sample in samples:
        rows = sample.get('merge_input_rows')
        if (not isinstance(rows, list) or len(rows) > 4096
                or any(type(n) is not int or n < 0 for n in rows)):
            raise ValueError('Missing or invalid exact merge input rows')
        for count in rows:
            tier = max(1, count).bit_length() - 1
            if tier >= len(histogram):
                raise ValueError('Merge input rows exceed histogram domain')
            histogram[tier] += 1
        if histogram != sample['cache']['merges']:
            raise ValueError('Exact merge work differs from production histogram')


def comparable_cycles(report):
    """Retain all intervening work; comparable witnesses differ by at most 25%."""
    samples = report['lifecycle']['samples']
    exact_work(samples)
    boundaries = boundary_indices(samples)
    if len(boundaries) < 5:
        raise ValueError('Insufficient installed maintenance boundaries')
    start = boundaries[4]
    jobs = [n for s in samples[start+1:] for n in s['merge_input_rows']]
    peak = max(jobs + report['lifecycle']['final_merge_input_rows'], default=0)
    witnesses = []
    previous = start
    for index in boundaries[5:]:
        work = [n for s in samples[previous+1:index+1] for n in s['merge_input_rows']
                if n > 0 and n * 5 >= peak * 4]
        if work:
            witnesses.append(dict(epoch=index, input_rows=work))
        previous = index
    windows = None
    checks = [dict(metric='comparable_large_merge_coverage', minimum=4,
                   actual=len(witnesses), passed=len(witnesses) >= 4)]
    if len(witnesses) >= 4:
        first = interval(samples, witnesses[0]['epoch'], witnesses[1]['epoch'])
        last = interval(samples, witnesses[-2]['epoch'], witnesses[-1]['epoch'])
        checks.extend(trend_checks(first, last, 'comparable_'))
        windows = dict(early=first, late=last)
    return dict(peak_input_rows=peak, comparable_ratio_limit=1.25,
                witnesses=witnesses, windows=windows, checks=checks,
                scope='Completed installed boundaries only. All work between witnesses '
                      'is charged, including smaller and unmatched merges; no forced compaction.')


def cold_scaling(path):
    rows = json.loads(path.read_text())
    identities = [(r.get('layout'), r.get('cold_rows_per_side_per_cohort')) for r in rows]
    expected = {(layout, size) for layout in ('partitioned', 'broad') for size in SIZES}
    fields = ('selected_durable_rows', 'selected_cold_rows', 'reads', 'read_bytes', 'puts',
              'put_bytes', 'flush_ns', 'maximum_runs', 'final_runs',
              'source_row_changes', 'natural_merges', 'external_reloads')
    if len(rows) != len(expected) or set(identities) != expected:
        raise ValueError('Cold-root layout/state-size coverage differs')
    for row in rows:
        if (any(type(row.get(k)) is not int or row[k] < 0 for k in fields)
                or row.get('source_row_changes') != 2 or row.get('natural_merges') != 1
                or row.get('cold_oracle') is not True or row.get('pending_upload') is not False
                or row.get('external_reloads') != 0
                or not isinstance(row.get('merge_sizes'), list)
                or len(row['merge_sizes']) != 64
                or any(type(n) is not int or n < 0 for n in row['merge_sizes'])
                or sum(row['merge_sizes']) != 1
                or any(type(n) is not int or n <= 0 for n in row['before_cohorts'] + row['after_cohorts'])
                or len(row['before_objects']) != sum(row['before_cohorts'])
                or len(row['after_objects']) != sum(row['after_cohorts'])
                or len(row['after_objects']) != row['final_runs']):
            raise ValueError('Invalid installed cold-root merge evidence')
    checks = []
    for layout in ('partitioned', 'broad'):
        cases = sorted((r for r in rows if r['layout'] == layout),
                       key=lambda r: r['cold_rows_per_side_per_cohort'])
        first, last = cases[0], cases[-1]
        for key, slack in (('reads', 4), ('read_bytes', 16384), ('put_bytes', 16384)):
            limit = first[key] * 1.25 + slack
            checks.append(dict(layout=layout, metric=key, early=first[key], late=last[key],
                               limit=limit, passed=last[key] <= limit))
        # Root/index/data PUTs have no percentage slack: do not buy locality with
        # a second durable delta upload or more frequent requests.
        checks.append(dict(layout=layout, metric='puts', early=first['puts'], late=last['puts'],
                           limit=first['puts'], passed=last['puts'] <= first['puts']))
    return dict(rows=rows, checks=checks, qualified=all(c['passed'] for c in checks),
                scope='One naturally selected merge per independent preload; fixed hot change, '
                      'increasing cold state. Independent cases are not endurance cycles. '
                      'Timing is reported but deterministic I/O/PUT bounds decide acceptance.')


def replay_costs(report, budget):
    life = report['lifecycle']
    if life['restarts'] or not life['samples'] or life['maintenance_quiescent'] is not True:
        raise ValueError('Maintenance acceptance requires uninterrupted, fully installed state')
    base = assess_report(report, cache_bytes=128*1024, cache_entries=64)
    cycles = base['cycles']
    comparable = comparable_cycles(report)
    samples = life['samples']
    checks = base['checks'] + comparable['checks']
    # A transaction may cross the 128-row deadline by one <=64-row batch.
    for key, limit in (('runs', 32), ('checkpoint_lag_rows', 192), ('checkpoint_age_ms', 128)):
        maximum = max(s[key] for s in samples)
        checks.append(dict(metric=key, maximum=maximum, limit=limit, passed=maximum <= limit))
    # Pending checkpoints are cleared, not merely moved outside the last sample.
    final = life['final_maintenance']
    total_bytes = samples[-1]['put_bytes'] + final['put_bytes']
    if report['object_puts'] != samples[-1]['puts'] + final['puts']:
        raise ValueError('Total PUT accounting omits observed or final-drain work')
    for key, actual in (('puts', report['object_puts']), ('put_bytes', total_bytes)):
        limit = budget[key]
        checks.append(dict(metric='total_' + key, actual=actual, limit=limit,
                           passed=actual <= limit))
    for key in ('gets', 'read_bytes', 'puts', 'put_bytes'):
        maximum = max(b[key] - a[key] for a, b in zip(samples, samples[1:]))
        # A drain can install the pending checkpoint and a final fresh capture.
        limit = maximum * 2
        checks.append(dict(metric='final_drain_' + key, actual=final[key], limit=limit,
                           passed=final[key] <= limit))
    checks.extend([dict(metric='final_checkpoint_lag_rows', maximum=final['checkpoint_lag_rows'],
                        limit=0, passed=final['checkpoint_lag_rows'] == 0),
                   dict(metric='cold_reopen_equal', passed=report['cold_reopen_equal'] is True)])
    return dict(case=report['case'], executed_source_rows=life['source_rows'],
                logical_ms=life['logical_ms'], cycles=cycles, comparable=comparable,
                final_maintenance=final, final_roots=life['durable_roots'],
                final_cohorts=life['durable_cohorts'], stored_bytes=report['stored_bytes'],
                stored_objects=report['stored_objects'], total_puts=report['object_puts'],
                total_put_bytes=total_bytes,
                checks=checks, qualified=all(c['passed'] for c in checks))


def assess(cold_path, replay_path):
    cold = cold_scaling(cold_path)
    baseline = request_budget()
    reports = json.loads(replay_path.read_text())
    if (baseline['revision'] != REVISION or len(reports) != len(baseline['cases'])
            or {r['case'] for r in reports} != set(baseline['cases'])):
        raise ValueError('Fixed-profile PUT baseline or requested case coverage differs')
    cases = [replay_costs(row, baseline['cases'][row['case']]) for row in reports]
    return dict(revision=REVISION, cold_roots=cold, cases=cases,
                request_budget_source=baseline['source_commit'],
                qualified=cold['qualified'] and bool(cases) and all(c['qualified'] for c in cases),
                full_worker_steady_state_proven=False,
                scope='Scaled component acceptance, not a production throughput prediction. '
                      'Reuses compiled SQL, bounded ingestion, checkpoint/rebase, GC and cold oracle. '
                      'Logical time covers only actually executed changes at 1000/s, no idle-day jumps. '
                      'Excludes PostgreSQL CDC/publication and real object-store latency. '
                      'Quiescence clears checkpoint uploads, not all retained consolidation debt; '
                      'root/cohort counts, repeated merges and final drain costs remain visible.')
