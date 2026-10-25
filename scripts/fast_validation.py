#!/usr/bin/env python3
"""Fast production-path checks; optional unpaced state-size measurements or live checks."""
import argparse
import json
import os
import re
import pathlib
import signal
import shutil
import subprocess
import sys
import time
from fast_replay import export, preloaded, traces
from fast_steady import TEST as STEADY_TEST, assess as assess_steady
from fast_longevity import COLD_TEST, TEST as LONGEVITY_TEST, assess as assess_longevity
from performance.common import ROOT, save, sha256

STORAGE_GROWTH = 'engine::trace::growth_tests::fixed_change_checkpoint_growth'
REPLAY = 'worker::tests::fast_replay::fast_replay'


VIRTUAL_TESTS = {
    'worker::checkpoint::tests::virtual_checkpoint_timeout_retains_unresolved_upload_and_aborts_on_drop',
    'worker::epoch::collect::tests::virtual_time_coalesces_paced_commits_and_preserves_order',
    'worker::epoch::collect::tests::virtual_time_idle_deadline_shutdown_and_held_input',
    'worker::epoch::collect::tests::virtual_idle_uses_production_predicted_arrival_deadline',
}


def validate_tests(path, expected):
    text = path.read_text()
    passed = set(re.findall(r'^test (\S+) \.\.\. ok$', text, re.MULTILINE))
    if not expected <= passed:
        raise RuntimeError('Expected tests did not run successfully: ' + str(sorted(expected - passed)))


def validate_report(path, cases):
    report = json.loads(path.read_text())
    if not isinstance(report, list) or len(report) != len(cases):
        raise RuntimeError('Engine report lacks requested case coverage')
    expected = {case['name']: case for case in cases}
    if len(expected) != len(cases) or {row['case'] for row in report} != set(expected):
        raise RuntimeError('Engine case identities differ from requested cases')
    for row in report:
        steps = expected[row['case']]['steps']
        measured = [step for step in steps if step['measure']]
        times = row['prepare_commit_ns']
        counts = {'bootstrap_rows':len(steps[0]['updates']), 'oracle_checks': sum(step['expected'] is not None for step in steps),
                  'logical_time': sum(not step['restart'] for step in steps),
                  'measured_weighted_updates': sum(len(step['updates']) for step in measured),
                  'output_groups': len(steps[-1]['expected'])}
        if (len(times) != len(measured) or any(type(t) is not int or t < 0 for t in times)
                or any(type(row[key]) is not int or row[key] != count for key, count in counts.items())
                or any(type(row[key]) is not int or row[key] < 0 for key in [
                    'bootstrap_ns', 'memtable_bytes', 'measured_cache_misses', 'measured_decoded_bytes', 'measured_decoded_rows', 'measured_object_reads',
                    'measured_object_puts', 'object_puts', 'stored_objects', 'stored_bytes'])
                or row['measured_object_puts'] != 0):
            raise RuntimeError('Engine measurement/oracle coverage differs: ' + row['case'])


def validate_storage_growth(path):
    rows = json.loads(path.read_text())
    if not isinstance(rows, list) or [row['initial_rows'] for row in rows] != [2000, 8000, 32000, 128000]:
        raise RuntimeError('Storage growth sizes differ from qualification')
    for row in rows:
        times = row['flush_ns']
        if (row['changes'] != 256 or row['weighted_bag_equal'] is not True
                or len(times) != 256 or any(type(t) is not int or t < 0 for t in times)
                or any(type(row[key]) is not int or row[key] < 0 for key in ['reads','read_bytes','puts','put_bytes'])
                or row['reads'] > 256 or row['read_bytes'] > 64*1024 or row['put_bytes'] > 256*1024 or row['puts'] > 550
                or not 0 < row['final_roots'] <= row['max_roots'] < 32):
            raise RuntimeError('Fixed-change checkpoint work grew with retained state: ' + str(row['initial_rows']))
    for key in ['reads', 'read_bytes', 'put_bytes']:
        values = [row[key] for row in rows]
        if max(values) > 2 * min(values):
            raise RuntimeError('Checkpoint work scales with retained state: ' + key)


def validate_sparse_growth(path):
    rows=[json.loads(line) for line in path.read_text().splitlines() if line.startswith('{')]
    if any(type(row.get(key)) is not int or row[key]<0 for row in rows for key in ['rows','iteration','ns','decoded_bytes','decoded_rows','reads','read_bytes','filter_rejections','filter_bytes']):
        raise RuntimeError('Invalid sparse growth counters')
    if any(type(row.get('filtered')) is not bool for row in rows):raise RuntimeError('Invalid sparse growth configuration')
    expected={(size,enabled,index) for size in [2000,32000,128000,262144] for enabled in [False,True] for index in range(4)}
    if {(row['rows'],row['filtered'],row['iteration']) for row in rows}!=expected or len(rows)!=len(expected):
        raise RuntimeError('Sparse growth case coverage missing')
    for size in [2000,32000,128000,262144]:
        totals={enabled:{key:sum(row[key] for row in rows if row['rows']==size and row['filtered']==enabled)
                         for key in ['decoded_bytes','reads','read_bytes']} for enabled in [False,True]}
        before,after=totals[False],totals[True]
        for key in ['reads','read_bytes']:
            if after[key]>before[key]*1.25+1:raise RuntimeError('Sparse synopsis increased storage work: '+str(size))
        if size>=32000 and after['decoded_bytes']>before['decoded_bytes']*.7:
            raise RuntimeError('Sparse decode amplification did not improve: '+str(size))


def execute(command, output, name, environment=None):
    started = time.monotonic()
    with (output / (name + '.log')).open('w') as log:
        child = subprocess.Popen(command, cwd=ROOT, env=environment, stdout=log, stderr=subprocess.STDOUT, start_new_session=True)
        try:
            code = child.wait()
        except BaseException:
            if child.poll() is None:
                try:
                    os.killpg(child.pid, signal.SIGINT)
                except ProcessLookupError:
                    pass
            try:
                child.wait(timeout=30)
            except subprocess.TimeoutExpired:
                try:
                    os.killpg(child.pid, signal.SIGKILL)
                except ProcessLookupError:
                    pass
                child.wait()
            raise
    result = {'command': list(map(str, command)), 'seconds': time.monotonic() - started, 'exit_code': code}
    save(output / (name + '.json'), result)
    if code:
        raise RuntimeError(f'{name} failed; see {output / (name + ".log")}')
    return result


def test_binary(output, release):
    command = ['cargo', 'test', '--locked', '--lib', '--no-run', '--message-format=json']
    if release:
        command.append('--release')
    execute(command, output, 'build')
    artifacts = [json.loads(line) for line in (output / 'build.log').read_text().splitlines() if line.startswith('{')]
    binaries = [item['executable'] for item in artifacts if item.get('reason') == 'compiler-artifact'
                and item.get('target', {}).get('name') == 'pgderive' and item.get('executable')]
    if len(binaries) != 1:
        raise RuntimeError('Expected exactly one library test executable')
    return freeze_binary(binaries[0], output)


def freeze_binary(binary, output):
    # Cargo can reuse one executable path across source checkouts. Freeze the
    # built inode before another build replaces it, then hash and run this copy.
    frozen = output / 'pgderive-tests'
    shutil.copy2(binary, frozen)
    return str(frozen)


def growth_limits(rows):
    # Preloading wide aggregate statistics can emit many identities per group.
    # Scale fixture output and total scratch guards for bounded bootstrap workspaces.
    # Resident/cache/per-update budgets stay unchanged.
    return {'output_entries': max(250_000, rows * 16),
            'output_bytes': max(64 * 1024 * 1024, rows * 4096),
            'scratch_bytes': max(1024**3, rows * 128 * 1024)}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--output', type=pathlib.Path, required=True)
    parser.add_argument('--mode', choices=['correctness', 'growth', 'storage-growth', 'steady', 'cold', 'longevity', 'integration'], default='correctness')
    parser.add_argument('--seeds', type=int, default=3)
    parser.add_argument('--state-rows', type=int, nargs='+', default=[1000, 8000, 32000],
                        help='Growth replay sizes; steady mode uses its fixed 32k through 524k matrix')
    parser.add_argument('--repeats', type=int, default=3)
    parser.add_argument('--warmup', type=int, default=8, help='Growth warmup updates after cold restore')
    parser.add_argument('--updates', type=int, default=32, help='Growth measured updates per case')
    parser.add_argument('--release', action='store_true')
    parser.add_argument('--replay', type=pathlib.Path)
    parser.add_argument('--worker', type=pathlib.Path)
    args = parser.parse_args()
    if args.seeds < 1 or args.repeats < 1 or any(not 1 <= rows <= 262144 for rows in args.state_rows):
        parser.error('Positive seeds/repeats and state rows in [1,262144] required')
    if args.warmup < 0 or args.updates < 1:
        parser.error('Nonnegative warmup and positive measured updates required')
    output = args.output.resolve()
    if output.exists():
        parser.error('Output must be new; previous evidence is retained')
    if args.replay and args.mode != 'correctness':
        parser.error('--replay is only for correctness mode')
    if args.mode == 'integration' and sys.flags.optimize:
        parser.error('Live harness assertions require Python optimization disabled')
    if args.mode == 'integration' and (args.worker is None or not args.worker.is_file()):
        parser.error('Integration mode requires --worker PATH to the built worker')
    output.mkdir(parents=True)
    report = {'mode': args.mode, 'passed': False, 'steps': [], 'purpose': 'Component correctness and unpaced preparation measurements; not real freshness qualification'}
    try:
        if args.mode == 'integration':
            report['worker_binary_sha256'] = sha256(args.worker)
            # The existing runners own their isolated schemas and fault-controlled workers.
            directory = output / 'checkpoint-events'
            directory.mkdir()
            env = dict(os.environ, PGDERIVE_S3_LATENCY_SCALE='0')
            report['steps'].append(execute([sys.executable, 'scripts/run_with_s3_proxy.py', str(directory),
                sys.executable, 'scripts/async_checkpoint_harness.py', str(directory), '--', str(args.worker.resolve())], output, 'checkpoint-events', env))
            for name in ['during-upload', 'after-commit', 'pressure', 'shutdown', 'timeout']:
                if json.loads((directory/name/'result.json').read_text()).get('passed') is not True:
                    raise RuntimeError('Live checkpoint case did not pass: ' + name)
        elif args.mode in ('steady', 'cold', 'longevity'):
            binary = test_binary(output, args.release)
            report['steps'].append(json.loads((output / 'build.json').read_text()))
            report['test_binary_sha256'] = sha256(pathlib.Path(binary))
            test = STEADY_TEST if args.mode == 'steady' else COLD_TEST if args.mode == 'cold' else LONGEVITY_TEST
            report['steps'].append(execute([binary, test, '--exact', '--ignored', '--nocapture'], output, 'steady-work'))
            validate_tests(output/'steady-work.log', {test})
            assessment = (assess_steady(output/'steady-work.log') if args.mode == 'steady'
                          else assess_longevity(output/'steady-work.log', cold=args.mode == 'cold'))
            save(output/'assessment.json', assessment)
            passed = (assessment['non_regression'] and assessment['component_plateau']
                      if args.mode == 'steady' else assessment['cold_capacity_passed']
                      if args.mode == 'cold' else assessment['bounded_cold_regime'])
            if not passed:
                raise RuntimeError('Component work has not qualified; see assessment.json. Do not infer worker steady state.')
        elif args.mode == 'storage-growth':
            binary = test_binary(output, args.release)
            report['steps'].append(json.loads((output / 'build.json').read_text()))
            report['test_binary_sha256'] = sha256(pathlib.Path(binary))
            report['steps'].append(execute([binary, 'engine::trace::tier_tests::', '--nocapture'], output, 'storage-correctness'))
            validate_tests(output/'storage-correctness.log', {
                'engine::trace::tier_tests::growing_tiny_flushes_never_rewrite_large_base',
                'engine::trace::tier_tests::multipack_cohort_survives_tiny_flushes_and_restart',
                'engine::trace::tier_tests::tier_subset_overflow_consolidates_only_complete_state',
                'engine::trace::tier_tests::empty_cohorts_survive_growth_cancellation_and_cold_restore'})
            env = dict(os.environ, PGDERIVE_STORAGE_GROWTH_REPORT=str(output / 'storage.json'))
            report['steps'].append(execute([binary, STORAGE_GROWTH, '--exact', '--ignored', '--nocapture'], output, 'storage-growth', env))
            validate_tests(output/'storage-growth.log', {STORAGE_GROWTH})
            validate_storage_growth(output/'storage.json')
        else:
            binary = test_binary(output, args.release)
            report['steps'].append(json.loads((output / 'build.json').read_text()))
            report['test_binary_sha256'] = sha256(pathlib.Path(binary))
            report['steps'].append(execute([binary, 'virtual_', '--nocapture'], output, 'virtual-time'))
            validate_tests(output/'virtual-time.log', VIRTUAL_TESTS)
            report['steps'].append(execute([binary,'engine::reader::cache::','--nocapture'],output,'cache-correctness'))
            validate_tests(output/'cache-correctness.log', {
                'engine::reader::cache::filter::tests::rejected_page_metadata_cannot_seed_or_reuse_an_exclusion',
                'engine::reader::cache::filter::tests::exclusions_survive_page_eviction_without_changing_weighted_identity',
                'engine::reader::cache::filter::tests::clock_eviction_work_is_amortized_and_replacements_remain_bounded',
                'engine::reader::cache::filter::tests::held_probe_heads_reference_synopses_before_clock_pressure',
                'engine::reader::cache::filter::tests::cold_source_validation_overlaps_pages_without_extra_gets',
                'engine::reader::cache::filter::tests::cold_statistics_overlap_keys_without_extra_gets',
                'engine::reader::cache::filter::tests::cancelled_statistics_never_prefetch_unreadable_prior',
                'engine::reader::cache::filter::tests::growth::stratified_keys_cover_the_full_relation_and_reject_overflow',
                'engine::reader::cache::filter::tests::growth::compaction_range_kinds_use_authenticated_identity_not_shared_pack_prefix',
                'engine::reader::cache::filter::tests::failed_and_cancelled_prefetch_preserve_heads_and_retry',
                'engine::reader::cache::range::tests::sparse_pressure_reduces_unused_prefetch_without_increasing_gets',
                'engine::reader::cache::range::tests::dense_pressure_retains_coalescing_and_read_origin_accounting',
                'engine::reader::cache::range::tests::changing_width_never_retains_duplicate_compressed_covers',
                'engine::reader::cache::range::tests::charged_windows_fit_small_cache_budgets',
                'engine::reader::cache::range::prefetch::tests::reservations_are_atomic_charge_tails_and_reuse_old_widths',
                'engine::reader::cache::range::prefetch::tests::overlapping_cold_runs_prefetch_all_windows_with_one_concurrency_bound',
                'engine::reader::cache::range::prefetch::tests::cached_wide_covers_do_not_starve_sparse_readiness',
                'engine::reader::cache::range::prefetch::tests::contiguous_misses_share_one_get_and_admit_before_waking_waiters',
                'engine::reader::cache::range::prefetch::tests::raced_middle_window_is_not_downloaded_again',
                'engine::reader::cache::range::prefetch::tests::cancelled_contiguous_load_releases_all_stripes_for_retry',
                'engine::reader::cache::range::prefetch::tests::scoped_pins_survive_eviction_release_and_invalidate',
                'engine::reader::cache::range::prefetch::tests::cancelled_partial_load_releases_registered_batches',
                'engine::reader::cache::range::prefetch::tests::cached_groups_do_not_block_later_independent_misses',
                'engine::reader::cache::range::prefetch::coalesce::tests::overlapping_covers_and_bridges_keep_readiness_equal_to_missing_groups'})
            report['steps'].append(execute([binary,'engine::dataflow::statistics::tests::','--nocapture'],output,'statistics-correctness'))
            validate_tests(output/'statistics-correctness.log', {
                'engine::dataflow::statistics::tests::queued_statistics_preserve_oracle_and_cumulative_limits',
                'engine::dataflow::statistics::tests::metadata_preparation_is_bounded_and_cancelled_groups_do_no_reads',
                'engine::dataflow::statistics::tests::ordered_statistics_keep_contribution_and_intermediate_output_limits'})
            report['steps'].append(execute([binary,'engine::trace::probe_tests::','--nocapture'],output,'coefficient-correctness'))
            validate_tests(output/'coefficient-correctness.log', {
                'engine::trace::probe_tests::batched_tuple_validation_reads_each_selected_block_once',
                'engine::trace::probe_tests::coefficient_validation_overlaps_cold_runs_with_exact_cancellation'})
            report['steps'].append(execute([binary,'engine::trace::weight_bound_tests::','--nocapture'],output,'weight-bounds'))
            validate_tests(output/'weight-bounds.log', {
                'engine::trace::weight_bound_tests::signed_certificate_skips_cold_pages_but_never_proves_bag_validity',
                'engine::trace::weight_bound_tests::uncertain_bounds_preserve_duplicates_cancellation_minimum_and_overflow'})
            report['steps'].append(execute([binary,'engine::reader::object::tests::','--nocapture'],output,'object-format'))
            validate_tests(output/'object-format.log', {
                'engine::reader::object::tests::sole_format_rejects_old_layouts_and_plain_json',
                'engine::reader::object::tests::declared_weight_bound_is_rechecked_for_cold_and_cached_rows'})
            report['steps'].append(execute([binary,'engine::reader::index_page_tests::','--nocapture'],output,'index-paging'))
            validate_tests(output/'index-paging.log', {
                'engine::reader::index_page_tests::growing_indexes_keep_only_bounded_metadata_and_preserve_navigation',
                'engine::reader::index_page_tests::authenticated_index_frames_reject_corruption_and_retry_safely',
                'engine::reader::index_page_tests::failed_index_page_load_preserves_cursor_position',
                'engine::reader::index_page_tests::wide_fences_remain_supported_without_resident_full_index',
                'engine::reader::index_page_tests::foreign_selected_metadata_cannot_bypass_validation_through_head_reuse',
                'engine::reader::index_page_tests::opaque_gc_and_typed_readers_agree_on_inline_struct_field_order',
                'engine::reader::index_page_tests::coarse_fences_exclude_unrelated_runs_without_index_io',
                'engine::reader::index_page_tests::generated_index_frames_are_reused_across_namespaces_and_cold_reopen_is_independent',
                'engine::reader::index_page_tests::full_scans_leave_foreground_index_admission_and_geometry_unchanged',
                'engine::reader::index_page_tests::warmed_typed_nodes_do_not_accept_a_wrong_authenticated_frame_length',
                'engine::reader::index_page_tests::prepared_pages_recheck_synopses_learned_after_metadata_selection'})
            frame_test = 'engine::reader::index::frames::tests::only_authenticated_frames_are_retained_under_weight_and_entry_caps'
            report['steps'].append(execute([binary,frame_test,'--exact','--nocapture'],output,'index-frame-cache'))
            validate_tests(output/'index-frame-cache.log', {frame_test})
            report['steps'].append(execute([binary,'engine::reader::index::tests::','--nocapture'],output,'index-scan-navigation'))
            validate_tests(output/'index-scan-navigation.log', {
                'engine::reader::index::tests::scan_retains_parent_navigation_after_foreground_eviction'})
            report['steps'].append(execute([binary,'catalog::storage::collect::cache::','--nocapture'],output,'gc-dependencies'))
            validate_tests(output/'gc-dependencies.log', {
                'catalog::storage::collect::cache::tests::verified_dependencies_are_reused_and_only_current_roots_are_marked',
                'catalog::storage::collect::cache::tests::reference_store_failures_and_budget_changes_never_reuse_unverified_paths',
                'catalog::storage::collect::cache::tests::repeated_wide_index_marking_has_bounded_network_work',
                'catalog::storage::collect::cache::tests::admission_eviction_keeps_complete_marks_with_a_one_entry_budget'})
            encoded='engine::reader::hybrid::tests::point_prefetch_respects_encoded_cache_without_redundant_object_reads'
            report['steps'].append(execute([binary,encoded,'--exact','--nocapture'],output,'encoded-prefetch'))
            validate_tests(output/'encoded-prefetch.log',{encoded})
            if args.mode=='correctness':
                sparse='engine::reader::cache::filter::tests::sparse_statistics_growth'
                report['steps'].append(execute([binary,sparse,'--exact','--ignored','--nocapture'],output,'sparse-growth'))
                validate_tests(output/'sparse-growth.log',{sparse})
                validate_sparse_growth(output/'sparse-growth.log')

            if args.mode == 'growth':
                cases = []
                for rows in args.state_rows:
                    for repeat in range(args.repeats):
                        trace = preloaded(rows, warmup=args.warmup, measured=args.updates)
                        case = export(trace, measured_from=args.warmup + 1, final_only=True)
                        case['name'] += '-' + str(repeat)
                        cases.append(case)
            else:
                raw = [json.loads(args.replay.read_text())] if args.replay else list(traces(range(args.seeds)))
                cases = [export(trace) for trace in raw]
                for index, trace in enumerate(raw):
                    save(output / f'application-trace-{index}.json', trace)
            suite = output / 'replay.json'
            limits = growth_limits(max(args.state_rows)) if args.mode == 'growth' else {}
            suite.write_text(json.dumps({'version': 1, 'limits': limits, 'cases': cases}, ensure_ascii=False, separators=(',', ':')) + '\n')
            report['trace_sha256'] = sha256(suite)
            env = dict(os.environ, PGDERIVE_FAST_REPLAY=str(suite), PGDERIVE_FAST_REPORT=str(output / 'engine.json'))
            report['steps'].append(execute([binary, REPLAY, '--exact', '--nocapture'], output, 'engine-replay', env))
            validate_tests(output/'engine-replay.log', {REPLAY})
            validate_report(output/'engine.json', cases)
        report['passed'] = True
    except BaseException as error:
        report['error'] = repr(error)
        raise
    finally:
        save(output / 'result.json', report)
        if report['passed']:
            (output / 'pgderive-tests').unlink(missing_ok=True)
    print(json.dumps(report))


if __name__ == '__main__':
    def interrupted(_signal, _frame):
        raise KeyboardInterrupt
    signal.signal(signal.SIGTERM, interrupted)
    main()
