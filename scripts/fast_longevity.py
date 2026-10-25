"""Qualify repeated state transitions; virtual idle days are not executed traffic."""
import json

TEST = 'engine::reader::cache::filter::tests::growth::long_lived_statistics_compactions'
COLD_TEST = 'engine::reader::cache::filter::tests::growth::cold_statistics_compactions'
SIZES = (524288, 1048576, 2097152, 4194304)
CYCLES = 28
WARMUP = 8


def assess(path, *, cold=False):
    sizes = SIZES[-2:] if cold else SIZES
    cycles = 4 if cold else WARMUP + CYCLES
    rows = [json.loads(line) for line in path.read_text().splitlines() if line.startswith('{')]
    expected = {(size, cycle) for size in sizes for cycle in range(cycles)}
    counters = ('rows', 'cycle', 'changes', 'reads', 'read_bytes', 'decoded_bytes',
                'virtual_io_us', 'wall_us', 'filter_bytes', 'filter_entries',
                'compactions', 'compaction_reads', 'compaction_read_bytes',
                'compaction_wall_us', 'cache_bytes', 'cache_entries', 'live_bytes',
                'pins', 'runs', 'state_rows', 'logical_elapsed_seconds',
                'index_bytes', 'index_entries', 'resident_root_bytes',
                'index_decoded_bytes', 'index_encoded_bytes', 'index_encoded_entries',
                'index_reads', 'index_read_bytes', 'index_requested_bytes',
                'foreground_read_peak', 'data_reads', 'data_read_bytes',
                'compaction_virtual_io_us', 'puts', 'put_bytes',
                'compaction_puts', 'compaction_put_bytes')
    retirement = ('gets', 'read_bytes', 'cache_bytes', 'deleted_objects', 'state_objects',
                  'state_bytes', 'rss_bytes', 'peak_rss_bytes', 'virtual_io_us', 'wall_us')
    if (len(rows) != len(expected)
            or {(r.get('rows'), r.get('cycle')) for r in rows} != expected
            or any(type(r.get(key)) is not int or r[key] < 0 for r in rows for key in counters)
            or any(not isinstance(r.get('retirement'), dict) for r in rows)
            or any(type(r['retirement'].get(key)) is not int or r['retirement'][key] < 0
                   for r in rows for key in retirement)
            or any(r.get('changes') != 4096 or r.get('compactions', 0) < 1
                   or r.get('state_rows', 0) < r.get('rows', 0)
                   or not r['retirement']['rss_bytes'] or not r['retirement']['state_objects']
                   or r.get('coalesced') is not True
                   or type(r.get('warmup')) is not bool
                   or r['warmup'] != (not cold and r['cycle'] < WARMUP) for r in rows)):
        raise ValueError('Missing or invalid long-lived compaction evidence')
    if any(r['index_bytes'] != r['index_decoded_bytes'] + r['index_encoded_bytes']
           or r['data_reads'] + r['index_reads'] > r['reads']
           or r['data_read_bytes'] + r['index_read_bytes'] > r['read_bytes'] for r in rows):
        raise ValueError('Inconsistent cache or object-read accounting')
    for ordinal, row in enumerate(rows, 1):
        if ((row['rows'], row['cycle']) != (sizes[(ordinal - 1) // cycles], (ordinal - 1) % cycles)
                or row['logical_elapsed_seconds'] < ordinal * (0 if cold else 43200)):
            raise ValueError('Logical clock or ordered cycle coverage differs')
    bounds = {
        'cache_bytes': 16 * 1024**2, 'cache_entries': 512,
        # Live decoded payload includes an output workspace and pinned cursor
        # heads; these are charges, not allocator/RSS measurements.
        'live_bytes': 32 * 1024**2, 'pins': 512,
        'filter_bytes': 4 * 1024**2, 'filter_entries': 8192, 'runs': 256,
        'index_bytes': 4 * 1024**2, 'index_entries': 512,
        'index_decoded_bytes': 1 * 1024**2, 'index_encoded_bytes': 3 * 1024**2,
        'index_encoded_entries': 3072, 'foreground_read_peak': 32,
        'resident_root_bytes': 8 * 1024**2,
    }
    checks = [{'metric': key, 'maximum': max(r[key] for r in rows), 'limit': limit,
               'passed': all(r[key] <= limit for r in rows)} for key, limit in bounds.items()]
    for key, limit in (('cache_bytes', 16 * 1024**2), ('rss_bytes', 3 * 1024**3), ('peak_rss_bytes', 3 * 1024**3)):
        maximum = max(r['retirement'][key] for r in rows)
        checks.append({'metric': 'retirement_' + key, 'maximum': maximum,
                       'limit': limit, 'passed': maximum <= limit})
    range_bounds = {'active_batches': 0, 'pinned_bytes': 0, 'pinned_windows': 0,
                    'resident_bytes': 4 * 1024**2, 'resident_charge': 4 * 1024**2,
                    'resident_windows': 1024}
    for key, limit in range_bounds.items():
        if any(not isinstance(r.get('ranges'), dict) or type(r['ranges'].get(key)) is not int
               or r['ranges'][key] < 0 for r in rows):
            raise ValueError('Missing or invalid compressed-cache/prefetch bounds')
        maximum = max(r['ranges'][key] for r in rows)
        checks.append({'metric': 'ranges_' + key, 'maximum': maximum,
                       'limit': limit, 'passed': maximum <= limit})
    transition_checks = []
    for size in sizes:
        cases = [r for r in rows if r['rows'] == size]
        service = max(r['virtual_io_us'] for r in cases)
        # This necessary foreground-I/O bound excludes CPU and sink work. Forced
        # full compaction every 4096 changes is a stress cadence, not production
        # checkpoint scheduling; its work is checked per retained row above.
        checks.append({'rows': size, 'metric': 'maximum_foreground_virtual_io_us',
                       'maximum': service, 'limit': 4096000, 'passed': service <= 4096000})
        if cold:
            continue
        # Retain the broad comparison, including declared warmup, as a separate
        # result. A cold-regime pass does not turn a cache-fit transition into a
        # broad plateau. Warmup duration and both cohort windows are fixed.
        transition_checks.extend(trends(cases, size))
        checks.extend(trends([r for r in cases if not r['warmup']], size))
    return {
        'workload_revision': 'stratified-256-v1',
        'scope': 'Production statistics, signed validation, flush, compaction and GC marking on one live fixture; independent per-step oracle and cold reopen after each state size. Owned in-memory object retirement and process RSS include the oracle. No active-state restarts. Excludes CDC, sink, SQL GC liveness/protection barriers, real freshness and worker deadlines.',
        'executed_changes': sum(r['changes'] for r in rows),
        'measured_changes': sum(r['changes'] for r in rows if not r['warmup']),
        'state_sizes': list(sizes), 'cycles_per_size': cycles, 'cold_screen_only': cold,
        'warmup_cycles_per_size': 0 if cold else WARMUP,
        'measured_cycles_per_size': 4 if cold else CYCLES,
        'warmup_policy': 'Eight declared warmup cycles per size; chosen after prior failed observations, fixed before this revised run. Absolute resource and service bounds apply to every cycle.',
        'compactions': sum(r['compactions'] for r in rows),
        'logical_elapsed_seconds': rows[-1]['logical_elapsed_seconds'],
        'clock_schedule': 'No idle jumps; short capacity screen.' if cold else '12 hours of virtual idle time after each 4096-change burst; this is not continuous 1000 changes/s for those days.',
        'continuous_1000_changes_per_second_equivalent_seconds': sum(r['changes'] for r in rows) / 1000,
        'checks': checks, 'bounded_cold_regime': not cold and all(c['passed'] for c in checks),
        'broad_transition_checks': transition_checks,
        'broad_transition_plateau_passed': None if cold else all(c['passed'] for c in transition_checks),
        'cold_capacity_passed': cold and all(c['passed'] for c in checks),
        'full_worker_steady_state_proven': False,
    }


def trends(cases, size):
    checks = []
    for key in ('reads', 'read_bytes', 'decoded_bytes', 'virtual_io_us',
                'index_reads', 'index_read_bytes', 'data_reads', 'data_read_bytes',
                'puts', 'put_bytes'):
        early = sum(r[key] for r in cases[2:9]) / 7
        late = sum(r[key] for r in cases[-7:]) / 7
        limit = early * 1.25
        checks.append({'rows': size, 'metric': key, 'early': early, 'late': late,
                       'limit': limit, 'passed': late <= limit})
    for key in ('compaction_reads', 'compaction_read_bytes', 'compaction_puts', 'compaction_put_bytes'):
        # A full compaction scans retained rows by definition. Normalize by
        # actual cardinality, including newly inserted gap keys.
        early = sum(r[key] / r['state_rows'] for r in cases[2:9]) / 7
        late = sum(r[key] / r['state_rows'] for r in cases[-7:]) / 7
        limit = early * 1.25
        checks.append({'rows': size, 'metric': key + '_per_state_row',
                       'early': early, 'late': late, 'limit': limit,
                       'passed': late <= limit})
    for key in ('gets', 'read_bytes', 'state_bytes', 'rss_bytes'):
        early = sum(r['retirement'][key] / r['state_rows'] for r in cases[2:9]) / 7
        late = sum(r['retirement'][key] / r['state_rows'] for r in cases[-7:]) / 7
        limit = early * 1.25
        checks.append({'rows': size, 'metric': 'retirement_' + key + '_per_state_row',
                       'early': early, 'late': late, 'limit': limit, 'passed': late <= limit})
    return checks
