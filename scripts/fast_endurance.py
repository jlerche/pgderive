"""Bounded uninterrupted SQL replay; idle virtual days are qualified separately."""
import json
from fast_cycles import assess_cycles


def assess_report(report, *, cache_bytes, cache_entries):
    if min(cache_bytes, cache_entries) < 1:
        raise ValueError('Positive cache bounds required')
    life = report['lifecycle']
    samples = life['samples']
    if len(samples) < 128 or life['restarts'] or life['checkpoints'] < 16:
        raise ValueError('Insufficient uninterrupted checkpoint coverage')
    cycles = assess_cycles(report)
    late = cycles['late']['metrics']
    checks = list(cycles['checks'])
    checks.append({'metric':'largest_tier_repeat_coverage',
                   'passed':cycles['largest_tier_repeat_coverage']})
    caps = {'bytes':cache_bytes, 'live_bytes':2*cache_bytes,
            'index_bytes':cache_bytes//4, 'filter_bytes':cache_bytes//4,
            'point_bytes':cache_bytes//4, 'point_entries':cache_bytes//4//512,
            'entries':cache_entries, 'pins':512}
    checks.extend({'metric':key, 'maximum':max(s['cache'][key] for s in samples),
                   'limit':limit, 'passed':all(s['cache'][key] <= limit for s in samples)}
                  for key, limit in caps.items())
    bounds = {'maximum_runs':32, 'maximum_memtable_bytes':64*1024**2}
    checks.extend({'metric':key, 'maximum':life[key], 'limit':limit,
                   'passed':life[key] <= limit} for key, limit in bounds.items())
    rss = max(r['rss_bytes'] for r in life['retirement'])
    checks.append({'metric':'fixture_rss_bytes','maximum':rss,'limit':3*1024**3,
                   'passed':rss <= 3*1024**3})
    checks.append({'metric':'late_component_service_ns_per_row',
                   'maximum':late['prepare_ns_per_row'] + late['maintenance_ns_per_row'],
                   'limit':1_000_000,
                   'passed':late['prepare_ns_per_row'] + late['maintenance_ns_per_row'] <= 1_000_000})
    compactions = sum(samples[-1]['cache']['merges'])
    checks.append({'metric':'compactions','minimum':4,'actual':compactions,'passed':compactions>=4})
    checks.append({'metric':'cold_reopen_equal','passed':report['cold_reopen_equal'] is True})
    checks.append({'metric':'rebased_uploads','minimum':1,'actual':life['rebased_uploads'],'passed':life['rebased_uploads']>=1})
    return {'case':report['case'],'executed_source_rows':life['source_rows'],
                  'logical_ms':life['logical_ms'],'checkpoints':life['checkpoints'],
                  'rebased_uploads':life['rebased_uploads'],'cold_reopen_equal':report['cold_reopen_equal'],
                  'cycles':cycles,'checks':checks,'qualified':all(c['passed'] for c in checks)}


def assess(path, *, cache_bytes, cache_entries):
    if min(cache_bytes, cache_entries) < 1:
        raise ValueError('Positive cache bounds required')
    results = json.loads(path.read_text())
    cases = [assess_report(report, cache_bytes=cache_bytes, cache_entries=cache_entries)
             for report in results]
    return {'scope':'Production SQL, bounded ingestion, freeze/upload/rebase and owned-store GC marking. Virtual time paces actually executed source rows at 1000/s; no active-state restart. Includes maintenance I/O. Excludes CDC, PG publication, SQL GC barriers and cloud timings. Finite qualification, not arbitrary-size weekly production proof.',
            'cases':cases,'qualified':bool(cases) and all(c['qualified'] for c in cases)}
