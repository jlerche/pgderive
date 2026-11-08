#!/usr/bin/env python3
"""Measure existing production-path workloads in isolated, immutable source copies."""
import argparse
import hashlib
import io
import json
import os
import pathlib
import random
import re
import shutil
import subprocess
import tarfile
import time
import statistics
import tomllib

ROOT = pathlib.Path(__file__).resolve().parents[1]
WORKLOADS = {
    'reader': ('src/engine/reader/tests/performance.rs', 'reader_execution_measurement',
               'engine::reader::performance::reader_execution_measurement', True),
    'replay': ('src/worker/tests/fast_replay.rs', 'fast_replay',
               'worker::tests::fast_replay::fast_replay', False),
    'spill': ('src/engine/execution/tests.rs', 'spill_boundaries_never_finalize_intermediate_weights',
              'engine::execution::tests::spill_boundaries_never_finalize_intermediate_weights', False),
    'numeric': ('src/numeric.rs', 'exact_numeric_keys_order_scale_and_limits',
                'numeric::tests::exact_numeric_keys_order_scale_and_limits', False),
    'spill_strings': ('src/engine/execution/tests.rs',
                      'canonical_sizes_match_after_cross_run_cancellation',
                      'engine::execution::tests::canonical_sizes_match_after_cross_run_cancellation', False),
    'topk': ('src/engine/dataflow/tests/topk.rs',
             'top_k_counts_occurrences_clips_cutoffs_and_reopens',
             'engine::dataflow::topk_tests::top_k_counts_occurrences_clips_cutoffs_and_reopens', False),
    'group': ('src/engine/dataflow/tests/group.rs',
              'cursor_overlay_matches_weighted_recomputation_and_cold_restore',
              'engine::dataflow::group_tests::cursor_overlay_matches_weighted_recomputation_and_cold_restore', False),
    'processing': ('src/engine/plan/tests/typed.rs',
                   'chained_join_statistics_emit_snapshot_differences_and_restore',
                   'engine::plan::tests::typed::chained_join_statistics_emit_snapshot_differences_and_restore', False),
    'join': ('src/engine/dataflow/tests/join.rs',
             'simultaneous_signed_inputs_match_independent_oracle',
             'engine::dataflow::join_tests::simultaneous_signed_inputs_match_independent_oracle', False),
    'statistics': ('src/engine/dataflow/statistics/tests.rs',
                   'ordered_and_spilled_statistics_match_independent_group_state',
                   'engine::dataflow::statistics::tests::ordered_and_spilled_statistics_match_independent_group_state', False),
    'batch': ('src/engine/batch/tests.rs', 'exact_merge_trees_preserve_identity_and_physical_grouping',
              'engine::batch::tests::exact_merge_trees_preserve_identity_and_physical_grouping', False),
}
PROBE = '''//! Experiment-only library allocator; production dependencies remain unchanged.
#[global_allocator]
static ALLOCATOR: dhat::Alloc = dhat::Alloc;
pub(crate) fn begin() -> anyhow::Result<dhat::Profiler> {
    let path = std::env::var_os("PGDERIVE_ALLOCATION_PROFILE")
        .ok_or_else(|| anyhow::anyhow!("missing allocation profile destination"))?;
    Ok(dhat::Profiler::builder().file_name(std::path::PathBuf::from(path))
        .trim_backtraces(Some(16)).build())
}
'''


def digest(path):
    return hashlib.sha256(path.read_bytes()).hexdigest()


def save(path, value):
    temporary = path.with_name(path.name + '.pending')
    temporary.write_text(json.dumps(value, indent=2) + '\n')
    temporary.replace(path)


def execute(command, cwd, output, env):
    started = time.monotonic()
    with output.open('x') as log:
        result = subprocess.run(command, cwd=cwd, env=env, stdout=log, stderr=subprocess.STDOUT)
    if result.returncode:
        raise RuntimeError(f'{command[0]} failed ({result.returncode}); evidence: {output}')
    return time.monotonic() - started


def snapshot(revision, destination):
    destination.mkdir()
    head = subprocess.check_output(['git', 'rev-parse', 'HEAD' if revision == 'working-tree' else revision], cwd=ROOT, text=True).strip()
    if revision == 'working-tree':
        paths = subprocess.check_output(['git', 'ls-files', '-z', '--cached', '--others', '--exclude-standard'], cwd=ROOT).decode().split('\0')
        for name in filter(None, paths):
            source = ROOT / name
            if source.is_file():
                target = destination / name
                target.parent.mkdir(parents=True, exist_ok=True)
                shutil.copy2(source, target)
        patch = subprocess.check_output(['git', 'diff', 'HEAD', '--binary'], cwd=ROOT)
        (destination.parent / 'working-tree.patch').write_bytes(patch)
    else:
        archive = subprocess.check_output(['git', 'archive', head], cwd=ROOT)
        with tarfile.open(fileobj=io.BytesIO(archive)) as files:
            files.extractall(destination, filter='data')
    hashes = {str(path.relative_to(destination)): digest(path)
              for path in sorted(destination.rglob('*')) if path.is_file()}
    save(destination.parent / 'source-hashes.json', hashes)
    return {'revision': revision, 'head': head, 'source_sha256': hashlib.sha256(json.dumps(hashes, sort_keys=True).encode()).hexdigest()}


def build(source, output, env):
    log = output.with_suffix('.build.log')
    execute(['cargo', 'test', '--locked', '--release', '--lib', '--no-run', '--message-format=json'], source, log, env)
    executables = []
    for line in log.read_text().splitlines():
        if not line.startswith('{'):
            continue
        record = json.loads(line)
        if (record.get('reason') == 'compiler-artifact' and record.get('executable')
                and record.get('target', {}).get('kind') == ['lib']
                and record.get('profile', {}).get('test') is True):
            executables.append(pathlib.Path(record['executable']))
    if len(executables) != 1:
        raise RuntimeError(f'Expected exactly one library test executable: {log}')
    shutil.copy2(executables[0], output)
    return digest(output)


def instrument(source, workloads):
    manifest = source / 'Cargo.toml'
    text = manifest.read_text()
    if text.count('[dev-dependencies]') != 1 or 'dhat' in tomllib.loads(text).get('dev-dependencies', {}):
        raise RuntimeError('Expected one uninstrumented dev-dependencies table')
    manifest.write_text(text.replace('[dev-dependencies]', '[dev-dependencies]\ndhat = "=0.3.3"', 1))
    library = source / 'src/lib.rs'
    library.write_text(library.read_text() + '\n#[cfg(test)]\nmod allocation_probe;\n')
    (source / 'src/allocation_probe.rs').write_text(PROBE)
    for name in workloads:
        relative, function, _, _ = WORKLOADS[name]
        path = source / relative
        text = path.read_text()
        marker = f'fn {function}() -> Result<()> {{'
        if text.count(marker) != 1:
            raise RuntimeError(f'Workload entry point changed: {path}:{function}')
        path.write_text(text.replace(marker, marker + '\n    let _allocation_probe = crate::allocation_probe::begin()?;', 1))


def validate_selection(binary, source, log, test, env):
    execute([str(binary), test, '--exact', '--list', '--format=terse'], source, log, env)
    if log.read_text().splitlines() != [f'{test}: test']:
        raise RuntimeError(f'Expected exactly one selected workload: {log}')


def validate_run(log, test):
    text = log.read_text()
    summaries = re.findall(
        r'^test result: ok\. (\d+) passed; (\d+) failed; (\d+) ignored; '
        r'(\d+) measured; (\d+) filtered out; finished in [0-9.]+s$', text, re.MULTILINE)
    entries = re.findall(r'^test ([^ ]+) \.\.\.', text, re.MULTILINE)
    if len(summaries) != 1 or summaries[0][:4] != ('1', '0', '0', '0') or entries != [test]:
        raise RuntimeError(f'Expected exactly one passing workload: {log}')


def validate_instrumented_lock(original, instrumented, package):
    def packages(path):
        data = tomllib.loads(path.read_text())
        return {(entry['name'], entry['version'], entry.get('source')): entry for entry in data['package']}
    before, after = packages(original), packages(instrumented)
    if not any(key[:2] == ('dhat', '0.3.3') for key in after):
        raise RuntimeError('Instrumented lockfile lacks pinned dhat 0.3.3')
    for key, entry in before.items():
        actual = after.get(key)
        if entry['name'] == package and key[2] is None and actual is not None:
            actual = dict(actual)
            actual['dependencies'] = [value for value in actual.get('dependencies', [])
                                      if value not in ('dhat', 'dhat 0.3.3')]
        if actual != entry:
            raise RuntimeError(f'Instrumentation changed existing locked package: {key}')


def preserve_original_lock(original, instrumented, package):
    # Cargo's additive resolution may retarget permissive transitive edges even
    # when all old versions remain locked. Preserve their original records,
    # then ask --locked metadata to validate the full augmented dependency graph.
    def blocks(path):
        return re.split(r'(?m)(?=^\[\[package\]\]\s*$)', path.read_text())
    def identity(block):
        entries = tomllib.loads(block).get('package')
        if not entries:
            return None
        entry = entries[0]
        return entry['name'], entry['version'], entry.get('source')
    before = {identity(block): block for block in blocks(original) if identity(block)}
    resolved = blocks(instrumented)
    keys = {identity(block) for block in resolved}
    if not before.keys() <= keys:
        raise RuntimeError('Instrumentation removed or upgraded original locked packages')
    restored = []
    for block in resolved:
        key = identity(block)
        root = key is not None and key[0] == package and key[2] is None
        restored.append(before.get(key, block) if not root else block)
    instrumented.write_text(''.join(restored))
    validate_instrumented_lock(original, instrumented, package)


def heap_totals(path):
    data = json.loads(path.read_text())
    if (not isinstance(data, dict) or data.get('dhatFileVersion') != 2 or data.get('mode') != 'rust-heap'
            or not isinstance(data.get('pps'), list) or not data['pps']):
        raise RuntimeError(f'Missing heap measurements: {path}')
    names = {'total_bytes': 'tb', 'total_blocks': 'tbk', 'peak_bytes': 'gb',
             'peak_blocks': 'gbk', 'end_bytes': 'eb', 'end_blocks': 'ebk'}
    for point in data['pps']:
        if (not isinstance(point, dict) or any(type(point.get(field)) is not int or point[field] < 0
                                              for field in names.values())):
            raise RuntimeError(f'Invalid heap counters: {path}')
        if point['gb'] > point['tb'] or point['eb'] > point['tb'] or point['gbk'] > point['tbk'] or point['ebk'] > point['tbk']:
            raise RuntimeError(f'Inconsistent heap counters: {path}')
    totals = {name: sum(point[field] for point in data['pps']) for name, field in names.items()}
    if totals['end_bytes'] > totals['peak_bytes']:
        raise RuntimeError(f'Heap end exceeds global byte peak: {path}')
    return totals


def summarize(samples, workloads, repeats):
    expected = {(name, profiled, repeat) for name in workloads
                for profiled in (False, True) for repeat in range(repeats)}
    actual = [(sample['workload'], sample['profiled'], sample['repeat']) for sample in samples]
    if len(actual) != len(expected) or set(actual) != expected:
        raise RuntimeError('Missing or duplicate allocation measurement samples')
    result = {}
    for name in workloads:
        result[name] = {}
        for profiled in (False, True):
            selected = [sample for sample in samples if sample['workload'] == name and sample['profiled'] == profiled]
            fields = set(selected[0]) - {'workload', 'profiled', 'repeat'}
            if any(set(sample) - {'workload', 'profiled', 'repeat'} != fields for sample in selected):
                raise RuntimeError('Inconsistent measurement fields')
            result[name]['heap' if profiled else 'plain'] = {
                field: {'median': statistics.median(sample[field] for sample in selected),
                        'minimum': min(sample[field] for sample in selected),
                        'maximum': max(sample[field] for sample in selected)}
                for field in sorted(fields)}
    return result


def clean_environment(environment):
    env = environment.copy()
    for key in list(env):
        if key.startswith(('PGDERIVE_', 'CARGO_PROFILE_', 'CARGO_BUILD_', 'CARGO_TARGET_',
                           'RUSTFLAGS', 'RUSTDOCFLAGS', 'LLVM_PROFILE_FILE')) or key in {
                'CARGO_ENCODED_RUSTFLAGS', 'CARGO_ENCODED_RUSTDOCFLAGS', 'CARGO_INCREMENTAL',
                'RUSTC_WRAPPER', 'RUSTC_WORKSPACE_WRAPPER', 'RUSTC', 'RUSTDOC', 'RUSTUP_TOOLCHAIN'}:
            del env[key]
    env.update({'CARGO_PROFILE_RELEASE_DEBUG': '1', 'LC_ALL': 'C', 'TZ': 'UTC'})
    return env


def run_workload(binary, source, directory, name, env, profiled):
    _, _, test, ignored = WORKLOADS[name]
    command = [str(binary), test, '--exact', '--nocapture', '--test-threads=1']
    if ignored:
        command.append('--ignored')
    validate_selection(binary, source, directory.with_suffix('.selection.log'), test, env)
    log = directory.with_suffix('.log')
    child_env = env.copy()
    replay_path = directory.with_suffix('.replay.json')
    if name == 'replay':
        child_env['PGDERIVE_FAST_REPORT'] = str(replay_path)
    if profiled:
        heap = directory.with_suffix('.dhat.json')
        child_env['PGDERIVE_ALLOCATION_PROFILE'] = str(heap)
    else:
        timing = directory.with_suffix('.time.txt')
        command = ['/usr/bin/time', '-v', '-o', str(timing), *command]
    elapsed = execute(command, source, log, child_env)
    validate_run(log, test)
    operations = object_metrics(name, log, replay_path)
    if profiled:
        return {'wall_seconds_instrumented': elapsed, **heap_totals(heap), **operations}
    text = timing.read_text()
    def field(label):
        match = re.search(re.escape(label) + r':\s*([0-9.]+)', text)
        if not match:
            raise RuntimeError(f'Missing GNU time field {label}: {timing}')
        return float(match[1])
    return {'wall_seconds': elapsed, 'user_seconds': field('User time (seconds)'),
            'system_seconds': field('System time (seconds)'),
            'maximum_rss_kib': int(field('Maximum resident set size (kbytes)')), **operations}


MEASUREMENT_PROBE_VERSION = 1


def prepare_measurement(source, workloads):
    if 'reader' in workloads:
        path = source / WORKLOADS['reader'][0]
        text = path.read_text()
        old = 'use object_store::{ObjectStore, memory::InMemory};'
        start = '    let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());'
        end = '    Ok(())\n}\n\nasync fn measure_merged('
        if text.count(old) != 1 or text.count(start) != 1 or text.count(end) != 1:
            raise RuntimeError('Reader operation-accounting probe no longer matches fixture')
        text = text.replace(old, 'use object_store::ObjectStore;')
        text = text.replace(start, '    let measured_store = Arc::new(crate::engine::tests::store::Store::unpaced());\n    let store: Arc<dyn ObjectStore> = measured_store.clone();')
        fields = {'object_puts': 'puts', 'object_put_bytes': 'put_bytes', 'object_reads': 'reads',
                  'object_range_bytes': 'range_bytes', 'object_delete_batches': 'deletes'}
        loads = ','.join(f'"{label}":measured_store.{field}.load(std::sync::atomic::Ordering::SeqCst)'
                         for label, field in fields.items())
        emitted = '    println!("allocation_object_io={{}}", serde_json::json!({{{loads}}}));\n'.format(loads=loads)
        text = text.replace(end, emitted + end)
        path.write_text(text)
    if 'replay' in workloads:
        path = source / WORKLOADS['replay'][0]
        text = path.read_text()
        marker = '"object_puts":store.puts.load(Ordering::SeqCst),"stored_objects":objects.len(),'
        if text.count(marker) != 1:
            raise RuntimeError('Replay operation-accounting probe no longer matches fixture')
        extra = ','.join(f'"{label}":store.{field}.load(Ordering::SeqCst)'
                         for label, field in {'object_put_bytes': 'put_bytes', 'object_reads': 'reads',
                                              'object_range_bytes': 'range_bytes', 'object_delete_batches': 'deletes'}.items())
        path.write_text(text.replace(marker, marker + extra + ','))


def fixture_body(text, function):
    match = re.search(r'^( *)(?:async )?fn ' + re.escape(function) + r'\b[^\{]*\{ *$', text, re.MULTILINE)
    if not match:
        raise RuntimeError(f'Workload fixture changed: {function}')
    end = re.search(r'^' + re.escape(match[1]) + r'\}$', text[match.end():], re.MULTILINE)
    if not end:
        raise RuntimeError(f'Workload fixture end missing: {function}')
    return text[match.start():match.end() + end.end()]


def object_metrics(name, log, replay_path):
    if name == 'reader':
        records = re.findall(r'^allocation_object_io=(.+)$', log.read_text(), re.MULTILINE)
        if len(records) != 1:
            raise RuntimeError(f'Missing reader operation counters: {log}')
        result = json.loads(records[0])
    elif name == 'replay':
        cases = json.loads(replay_path.read_text())
        if (not isinstance(cases, list) or not cases or any(not isinstance(case, dict)
                or case.get('cold_reopen_equal') is not True or not case.get('case')
                or not isinstance(case.get('lifecycle'), dict)
                or not isinstance(case['lifecycle'].get('service'), list)
                or not case['lifecycle']['service']
                or any(not isinstance(service, dict) for service in case['lifecycle']['service'])
                for case in cases)):
            raise RuntimeError(f'Invalid replay operation evidence: {replay_path}')
        if len({case['case'] for case in cases}) != len(cases):
            raise RuntimeError(f'Duplicate replay cases: {replay_path}')
        fields = ('object_puts', 'object_put_bytes', 'object_reads', 'object_range_bytes',
                  'object_delete_batches', 'stored_objects', 'stored_bytes',
                  'measured_object_reads', 'measured_object_puts')
        result = {}
        for field in fields:
            values = [case.get(field) for case in cases]
            if any(type(value) is not int or value < 0 for value in values):
                raise RuntimeError(f'Missing replay counter {field}: {replay_path}')
            result[field] = sum(values)
        for field in ('requests', 'read_requested_bytes', 'read_bytes', 'write_requested_bytes', 'written_bytes'):
            values = [service.get(field) for case in cases for service in case['lifecycle']['service']]
            if any(type(value) is not int or value < 0 for value in values):
                raise RuntimeError(f'Invalid replay lifecycle counter {field}: {replay_path}')
            result[f'lifecycle_{field}'] = sum(values)
    else:
        return {}
    required = {'object_puts', 'object_put_bytes', 'object_reads', 'object_range_bytes', 'object_delete_batches'}
    if (not isinstance(result, dict) or not required.issubset(result)
            or any(type(value) is not int or value < 0 for value in result.values())):
        raise RuntimeError(f'Invalid operation counters: {log}')
    return result


def validate_object_cost(samples, workloads, repeats):
    comparisons = []
    for name in workloads:
        if name not in ('reader', 'replay'):
            continue
        for profiled in (False, True):
            for repeat in range(repeats):
                pair = {sample['side']: sample for sample in samples if sample['workload'] == name
                        and sample['profiled'] == profiled and sample['repeat'] == repeat}
                if set(pair) != {'baseline', 'candidate'}:
                    raise RuntimeError('Incomplete object-cost comparison')
                for field in ('object_puts', 'object_put_bytes'):
                    baseline, candidate = (pair[side][field] for side in ('baseline', 'candidate'))
                    comparisons.append({'workload': name, 'profiled': profiled, 'repeat': repeat,
                                        'counter': field, 'baseline': baseline, 'candidate': candidate})
                    if candidate > baseline:
                        raise RuntimeError(f'Candidate object-store cost increased: {name} {field} {baseline} -> {candidate}')
    return comparisons


def fixture_hashes(source, workloads):
    result = {}
    for name in workloads:
        relative, function, _, _ = WORKLOADS[name]
        text = (source / relative).read_text()
        if name not in ('reader', 'replay'):
            text = fixture_body(text, function)
        hashes = {relative: hashlib.sha256(text.encode()).hexdigest()}
        if name in ('reader', 'replay'):
            relative_store = 'src/engine/tests/store.rs'
            hashes[relative_store] = digest(source / relative_store)
        if name in ('spill', 'spill_strings'):
            helper = fixture_body((source / relative).read_text(), 'limits')
            hashes[relative + ':limits'] = hashlib.sha256(helper.encode()).hexdigest()
        helpers = {
            'processing': ('plan', 'operators', 'expected'),
            'join': ('graph', 'tick', 'oracle'),
            'statistics': ('aggregate',),
            'group': ('drain',),
        }
        for function in helpers.get(name, ()):
            body = fixture_body((source / relative).read_text(), function)
            hashes[relative + ':' + function] = hashlib.sha256(body.encode()).hexdigest()
        if name == 'processing':
            for helper_path in ('src/engine/plan/tests/contract.rs', 'src/engine/tests/mod.rs'):
                hashes[helper_path] = digest(source / helper_path)
        if name == 'replay':
            relative_setup = 'src/worker/tests.rs'
            hashes[relative_setup] = digest(source / relative_setup)
            for path in sorted((source / 'src/worker/tests/fast_replay').rglob('*.rs')):
                hashes[str(path.relative_to(source))] = digest(path)
            for path in sorted((source / 'tests/fixtures').rglob('*')):
                if path.is_file():
                    hashes[str(path.relative_to(source))] = digest(path)
        result[name] = hashes
    return result


def verify_source(source, manifest, expected):
    recorded = json.loads(manifest.read_text())
    actual = {str(path.relative_to(source)): digest(path)
              for path in sorted(source.rglob('*')) if path.is_file()}
    if (recorded != actual
            or hashlib.sha256(json.dumps(actual, sort_keys=True).encode()).hexdigest() != expected):
        raise RuntimeError(f'Frozen source changed: {source}')


def comparison_inputs(paths):
    sides = {}
    for side, path in zip(('baseline', 'candidate'), paths):
        path = path.resolve()
        report = json.loads(path.read_text())
        if report.get('version') != 2 or not report.get('complete'):
            raise RuntimeError(f'Comparison requires a completed version 2 report: {path}')
        source = path.parent / 'source'
        profiled_source = path.parent / 'profiled-source'
        verify_source(source, path.parent / 'source-hashes.json', report['source_sha256'])
        verify_source(profiled_source, path.parent / 'instrumented-source-hashes.json', report['instrumented_source_sha256'])
        for label in ('plain', 'profiled'):
            binary = path.parent / f'{label}-tests'
            if digest(binary) != report[f'{label}_binary_sha256']:
                raise RuntimeError(f'Frozen executable changed: {binary}')
        if fixture_hashes(source, report['workloads']) != report['fixture_hashes']:
            raise RuntimeError(f'Frozen workload sources changed: {source}')
        sides[side] = {'path': path, 'report': report, 'source': source,
                       'profiled_source': profiled_source}
    baseline, candidate = (sides[side]['report'] for side in ('baseline', 'candidate'))
    for field in ('workloads', 'dhat', 'fixture_hashes', 'rustc', 'measurement_probe_version',
                  'original_lock_sha256', 'instrumented_lock_sha256'):
        if baseline[field] != candidate[field]:
            raise RuntimeError(f'Comparison inputs differ: {field}')
    return sides


def compare(paths, output, repeats, seed):
    sides = comparison_inputs(paths)
    workloads = sides['baseline']['report']['workloads']
    report = {'version': 2, 'kind': 'paired-comparison', 'complete': False,
              'script_sha256': digest(pathlib.Path(__file__)), 'repeats': repeats,
              'seed': seed, 'workloads': workloads, 'samples': [],
              'scope': 'Serialized paired process runs of frozen executables. GNU time is uninstrumented; CPU timings are diagnostics, not a regression qualification.',
              'inputs': {side: {'report': str(value['path']), 'report_sha256': digest(value['path']),
                                'plain_binary_sha256': value['report']['plain_binary_sha256'],
                                'profiled_binary_sha256': value['report']['profiled_binary_sha256'],
                                'source_sha256': value['report']['source_sha256'],
                                'fixture_hashes': value['report']['fixture_hashes']}
                         for side, value in sides.items()}}
    save(output / 'report.json', report)
    env = clean_environment(os.environ)
    order = []
    for name in workloads:
        for profiled in (False, True):
            first = random.Random(f'{seed}:{name}:{profiled}').randrange(2)
            for repeat in range(repeats):
                pair = ('baseline', 'candidate') if (repeat + first) % 2 == 0 else ('candidate', 'baseline')
                order.extend((side, name, profiled, repeat) for side in pair)
    report['order'] = order
    try:
        for side, name, profiled, repeat in order:
            selected = sides[side]
            label = 'profiled' if profiled else 'plain'
            binary = selected['path'].parent / f'{label}-tests'
            source = selected['profiled_source'] if profiled else selected['source']
            if digest(binary) != selected['report'][f'{label}_binary_sha256']:
                raise RuntimeError(f'Frozen executable changed: {binary}')
            values = run_workload(binary, source, output / f'{side}-{name}-{label}-{repeat}', name, env, profiled)
            if digest(binary) != selected['report'][f'{label}_binary_sha256']:
                raise RuntimeError(f'Frozen executable changed during run: {binary}')
            report['samples'].append({'side': side, 'workload': name, 'profiled': profiled,
                                      'repeat': repeat, **values})
            save(output / 'report.json', report)
            print(side, name, label, repeat, json.dumps(values), flush=True)
        report['summary'] = {side: summarize([{key: value for key, value in sample.items() if key != 'side'}
                                             for sample in report['samples'] if sample['side'] == side], workloads, repeats)
                             for side in sides}
        report['object_cost_gate'] = validate_object_cost(report['samples'], workloads, repeats)
        report['complete'] = True
        save(output / 'report.json', report)
    except BaseException as error:
        report['failure'] = f'{type(error).__name__}: {error}'
        save(output / 'report.json', report)
        raise


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--compare', nargs=2, type=pathlib.Path, metavar=('BASE_REPORT', 'CAND_REPORT'), help='Run paired frozen executables without building')
    parser.add_argument('--revision', default='working-tree', help='Git revision, or working-tree (includes untracked files)')
    parser.add_argument('--output', required=True, type=pathlib.Path)
    parser.add_argument('--target-dir', type=pathlib.Path, help='Separate reusable build cache; never shared with live coverage')
    parser.add_argument('--workloads', nargs='+', choices=WORKLOADS, default=list(WORKLOADS))
    parser.add_argument('--repeats', type=int, default=3)
    parser.add_argument('--seed', type=int, default=714)
    args = parser.parse_args()
    if args.repeats < 3:
        parser.error('At least three repeats are required')
    if len(set(args.workloads)) != len(args.workloads):
        parser.error('Workloads must be unique')
    output = args.output.resolve()
    output.mkdir(parents=True, exist_ok=False)
    if args.compare:
        try:
            compare(args.compare, output, args.repeats, args.seed)
        except BaseException as error:
            path = output / 'report.json'
            report = json.loads(path.read_text()) if path.exists() else {'version': 2, 'complete': False}
            report['failure'] = f'{type(error).__name__}: {error}'
            save(path, report)
            raise
        return
    env = clean_environment(os.environ)
    target = (args.target_dir or output / 'target').resolve()
    if target == ROOT or target.is_relative_to(ROOT / 'target'):
        parser.error('Build cache must be separate from the checkout and its target directory')
    plain_source = output / 'source'
    report = {'version': 2, 'complete': False, 'samples': []}
    save(output / 'report.json', report)
    try:
        report.update(snapshot(args.revision, plain_source))
        report['original_source_sha256'] = report['source_sha256']
        (output / 'source-hashes.json').rename(output / 'original-source-hashes.json')
        prepare_measurement(plain_source, args.workloads)
        hashes = {str(path.relative_to(plain_source)): digest(path)
                  for path in sorted(plain_source.rglob('*')) if path.is_file()}
        save(output / 'source-hashes.json', hashes)
        report['source_sha256'] = hashlib.sha256(json.dumps(hashes, sort_keys=True).encode()).hexdigest()
        report['measurement_probe_version'] = MEASUREMENT_PROBE_VERSION
        report.update({'script_sha256': digest(pathlib.Path(__file__)), 'dhat': '0.3.3',
                       'seed': args.seed, 'repeats': args.repeats, 'workloads': args.workloads,
                       'scope': 'Existing test entry through fixture destruction, after Tokio runtime setup; includes production work and existing oracles. Alloc/realloc requests and requested bytes are not copy traffic or RSS.',
                       'rustc': subprocess.check_output(['rustc', '-Vv'], cwd=plain_source, env=env, text=True).strip(),
                       'original_lock_sha256': digest(plain_source / 'Cargo.lock'),
                       'fixture_hashes': fixture_hashes(plain_source, args.workloads),
                       'plain_target': str(target / 'plain'), 'profiled_target': str(target / 'profiled')})
        save(output / 'report.json', report)
        plain_env = {**env, 'CARGO_TARGET_DIR': str(target / 'plain')}
        profiled_env = {**env, 'CARGO_TARGET_DIR': str(target / 'profiled')}
        report['plain_binary_sha256'] = build(plain_source, output / 'plain-tests', plain_env)
        save(output / 'report.json', report)
        profiled_source = output / 'profiled-source'
        shutil.copytree(plain_source, profiled_source)
        instrument(profiled_source, args.workloads)
        execute(['cargo', 'update', '--offline', '--workspace'], profiled_source, output / 'instrument-lock.log', profiled_env)
        package = tomllib.loads((plain_source / 'Cargo.toml').read_text())['package']['name']
        shutil.copy2(profiled_source / 'Cargo.lock', output / 'instrument-resolved.Cargo.lock')
        preserve_original_lock(plain_source / 'Cargo.lock', profiled_source / 'Cargo.lock', package)
        execute(['cargo', 'metadata', '--locked', '--offline', '--format-version=1'],
                profiled_source, output / 'instrument-metadata.log', profiled_env)
        validate_instrumented_lock(plain_source / 'Cargo.lock', profiled_source / 'Cargo.lock', package)
        report['instrumented_lock_sha256'] = digest(profiled_source / 'Cargo.lock')
        hashes = {str(path.relative_to(profiled_source)): digest(path)
                  for path in sorted(profiled_source.rglob('*')) if path.is_file()}
        save(output / 'instrumented-source-hashes.json', hashes)
        report['instrumented_source_sha256'] = hashlib.sha256(json.dumps(hashes, sort_keys=True).encode()).hexdigest()
        save(output / 'report.json', report)
        report['profiled_binary_sha256'] = build(profiled_source, output / 'profiled-tests', profiled_env)
        order = [(name, profiled, repeat) for name in args.workloads
                 for profiled in (False, True) for repeat in range(args.repeats)]
        random.Random(args.seed).shuffle(order)
        report['order'] = order
        save(output / 'report.json', report)
        for name, profiled, repeat in order:
            label = f'{name}-{"heap" if profiled else "plain"}-{repeat}'
            binary = output / ('profiled-tests' if profiled else 'plain-tests')
            source = profiled_source if profiled else plain_source
            values = run_workload(binary, source, output / label, name, env, profiled)
            report['samples'].append({'workload': name, 'profiled': profiled, 'repeat': repeat, **values})
            save(output / 'report.json', report)
            print(label, json.dumps(values), flush=True)
        report['summary'] = summarize(report['samples'], args.workloads, args.repeats)
        report['complete'] = True
        save(output / 'report.json', report)
    except BaseException as error:
        report['failure'] = f'{type(error).__name__}: {error}'
        save(output / 'report.json', report)
        raise


if __name__ == '__main__':
    main()
