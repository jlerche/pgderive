"""Compare installed natural-compaction intervals, retaining strict trend failures."""
import os


def window(samples):
    first, last = samples[0], samples[-1]
    work = last['source_rows'] - first['source_rows']
    if work <= 0:
        raise ValueError('Endurance window lacks executed source work')
    result = {key + '_per_row': (last[key] - first[key]) / work
              for key in ('gets', 'read_bytes', 'puts', 'put_bytes', 'maintenance_ns')}
    result['prepare_ns_per_row'] = sum(s['prepare_ns'] for s in samples[1:]) / work
    return result


def boundary_indices(samples):
    boundaries = []
    installed = 0
    merges = 0
    previous_installed = 0
    previous_merges = 0
    for index, sample in enumerate(samples):
        count = sample['installed_checkpoints']
        total = sum(sample['cache']['merges'])
        if count < previous_installed or total < previous_merges:
            raise ValueError('Checkpoint or merge counter regressed')
        previous_installed, previous_merges = count, total
        if count > installed and not sample['upload_pending']:
            if not boundaries or total > merges:
                boundaries.append(index)
                merges = total
            installed = count
    return boundaries


def interval(samples, first, last):
    selected = samples[first:last+1]
    result = window(selected)
    before, after = selected[0], selected[-1]
    work = after['source_rows'] - before['source_rows']
    result['cpu_ns_per_row'] = ((after['cpu_ticks'] - before['cpu_ticks'])
                                * 1e9 / os.sysconf('SC_CLK_TCK') / work)
    for key in ('misses', 'hits', 'decoded_bytes', 'decoded_rows', 'index_reads',
                'index_read_bytes', 'index_requested_bytes'):
        result[key + '_per_row'] = (after['cache'][key] - before['cache'][key]) / work
    foreground = after['cache']['io']['foreground']
    prior = before['cache']['io']['foreground']
    result['foreground_gets_per_row'] = (foreground['gets'] - prior['gets']) / work
    result['foreground_bytes_per_row'] = (foreground['fetched_bytes'] - prior['fetched_bytes']) / work
    hist = [b-a for a, b in zip(before['cache']['merges'], after['cache']['merges'], strict=True)]
    return dict(metrics=result, affected_rows=work, first_epoch=first, last_epoch=last,
                installed_checkpoints=after['installed_checkpoints']-before['installed_checkpoints'],
                merge_size_histogram=hist, natural_merges=sum(hist))


def trend_checks(early, late, prefix=''):
    return [dict(metric=prefix+key, early=value, late=late['metrics'][key], limit=value*1.25,
                 passed=late['metrics'][key] <= value*1.25)
            for key, value in early['metrics'].items() if key != 'hits_per_row']


def assess_cycles(report, *, warmup=4, width=4):
    samples = report['lifecycle']['samples']
    boundaries = boundary_indices(samples)
    if len(boundaries) < warmup + 2*width + 1:
        raise ValueError('Insufficient installed natural-compaction intervals')
    early = interval(samples, boundaries[warmup], boundaries[warmup+width])
    late = interval(samples, boundaries[-width-1], boundaries[-1])
    checks = trend_checks(early, late)
    tier = max(index for index, count in enumerate(samples[-1]['cache']['merges']) if count)
    largest = []
    previous = 0
    for index in boundaries:
        count = samples[index]['cache']['merges'][tier]
        if count > previous:
            if index >= boundaries[warmup]:
                largest.append(index)
            previous = count
    largest_windows = None
    if len(largest) >= 4:
        first = interval(samples, largest[0], largest[1])
        last = interval(samples, largest[-2], largest[-1])
        checks.extend(trend_checks(first, last, 'largest_tier_'))
        largest_windows = dict(early=first, late=last)
    return dict(case=report['case'], warmup_intervals=warmup, comparison_intervals=width,
                complete_intervals=len(boundaries)-1, early=early, late=late,
                checks=checks, trends_passed=all(c['passed'] for c in checks),
                pending_upload_at_last_boundary=samples[boundaries[-1]]['upload_pending'],
                boundary_epochs=boundaries,
                largest_merge_tier=tier, largest_tier_boundary_epochs=largest,
                largest_tier_repeat_coverage=len(largest)>=4,
                largest_tier_windows=largest_windows,
                scope='Intervals end after checkpoint installation with no pending upload; '
                      'natural merges across all arrangements, not forced full compaction. '
                      'Merge-size histograms retain unequal work across tiers. CPU covers preparation, '
                      'output integration and maintenance, excluding explicit oracle comparisons. '
                      'Preload and final drain/cold verification are outside comparison windows. '
                      'Finite component evidence excludes CDC, PostgreSQL publication and real storage latency.')
