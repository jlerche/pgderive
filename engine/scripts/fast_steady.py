"""Deterministic cold-probe work qualification, separate from worker freshness."""
import json

SIZES = (32000, 64000, 128000, 262144, 524288)
TEST = 'engine::reader::cache::filter::tests::growth::growing_statistics_compactions'


def assess(path):
    rows = [json.loads(line) for line in path.read_text().splitlines() if line.startswith('{')]
    expected = {(size, cycle, coalesced) for size in SIZES for cycle in range(2) for coalesced in (False, True)}
    identities = [(r.get('rows'), r.get('cycle'), r.get('coalesced')) for r in rows]
    counters = ('changes', 'reads', 'read_bytes', 'decoded_bytes', 'virtual_io_us', 'wall_us', 'filter_bytes', 'filter_entries', 'compactions', 'compaction_reads', 'compaction_read_bytes', 'compaction_wall_us')
    if (set(identities) != expected or len(rows) != len(expected)
            or any(type(r.get('rows')) is not int or type(r.get('cycle')) is not int for r in rows)
            or any(type(r.get('coalesced')) is not bool or r.get('changes') != 4096 or r.get('compactions', 0) < 1 for r in rows)
            or any(type(r.get(k)) is not int or r[k] < 0 for r in rows for k in counters)):
        raise ValueError('Missing or invalid multi-compaction evidence')
    totals = {(size, coalesced): {key: (max if key in ('filter_bytes', 'filter_entries') else sum)(
                r[key] for r in rows if r['rows'] == size and r['coalesced'] == coalesced)
                for key in counters} for size in SIZES for coalesced in (False, True)}
    comparisons = []
    for size in SIZES:
        before, after = totals[size, False], totals[size, True]
        comparisons.append({'rows': size, 'control': before, 'candidate': after,
                            'reads_not_worse': after['reads'] <= before['reads'],
                            'bytes_not_worse': after['read_bytes'] <= before['read_bytes'],
                            'decode_not_worse': after['decoded_bytes'] <= before['decoded_bytes'] * 1.05,
                            'virtual_io_not_worse': after['virtual_io_us'] <= before['virtual_io_us'] * 1.05})
    # Predeclared fixed-work bounds on the final two state sizes. These are
    # component work bounds, not an assertion about CPU, RSS or worker freshness.
    checks = []
    for metric in ('reads', 'read_bytes', 'decoded_bytes', 'virtual_io_us'):
        early, late = (totals[size, True][metric] / totals[size, True]['changes'] for size in SIZES[-2:])
        limit = early * 1.25
        checks.append({'metric': metric + '_per_change', 'early': early, 'late': late,
                       'limit': limit, 'passed': late <= limit})
    return {'scope': 'Unpaced production statistics, validation, flush and compaction with paused Tokio I/O; cold reopen and independent weighted-bag oracle after every cycle. Excludes source CDC, sink publication, GC, CPU scheduling and real freshness.',
            'state_sizes': list(SIZES), 'cycles_per_size': 2, 'comparisons': comparisons,
            'checks': checks, 'component_plateau': all(c['passed'] for c in checks),
            'non_regression': all(c['reads_not_worse'] and c['bytes_not_worse'] and c['decode_not_worse'] and c['virtual_io_not_worse'] for c in comparisons),
            'full_worker_steady_state_proven': False}
