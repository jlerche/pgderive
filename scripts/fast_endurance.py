"""Bounded uninterrupted SQL replay; idle virtual days are qualified separately."""
import json


def window(samples):
    first, last = samples[0], samples[-1]
    work = last['source_rows'] - first['source_rows']
    if work <= 0:
        raise ValueError('Endurance window lacks executed source work')
    result = {key + '_per_row': (last[key] - first[key]) / work
              for key in ('gets', 'read_bytes', 'puts', 'put_bytes', 'maintenance_ns')}
    result['prepare_ns_per_row'] = sum(s['prepare_ns'] for s in samples[1:]) / work
    return result


def assess(path):
    results = json.loads(path.read_text())
    cases = []
    for report in results:
        life = report['lifecycle']
        samples = life['samples']
        if len(samples) < 128 or life['restarts'] or life['checkpoints'] < 16:
            raise ValueError('Insufficient uninterrupted checkpoint coverage')
        quarter = len(samples)//4
        early, late = window(samples[quarter:2*quarter]), window(samples[-quarter:])
        checks = [{'metric': key, 'early': value, 'late': late[key],
                   'limit': value*1.25, 'passed': late[key] <= value*1.25}
                  for key, value in early.items()]
        caps = {'bytes':64*1024**2, 'live_bytes':128*1024**2,
                'index_bytes':16*1024**2, 'filter_bytes':16*1024**2, 'pins':512}
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
        cases.append({'case':report['case'],'executed_source_rows':life['source_rows'],
                      'logical_ms':life['logical_ms'],'checkpoints':life['checkpoints'],
                      'rebased_uploads':life['rebased_uploads'],'cold_reopen_equal':report['cold_reopen_equal'],
                      'checks':checks,'qualified':all(c['passed'] for c in checks)})
    return {'scope':'Production SQL, bounded ingestion, freeze/upload/rebase and owned-store GC marking. Virtual time paces actually executed source rows at 1000/s; no active-state restart. Includes maintenance I/O. Excludes CDC, PG publication, SQL GC barriers and cloud timings. Finite qualification, not arbitrary-size weekly production proof.',
            'cases':cases,'qualified':bool(cases) and all(c['qualified'] for c in cases)}
