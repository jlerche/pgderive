"""Allocation evidence rejects incomplete workloads and drifting instrumentation."""
import json
import hashlib
import pathlib
import tempfile
import unittest
from unittest import mock

import allocation_profile as profile


class AllocationEvidenceTests(unittest.TestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory()
        self.addCleanup(self.temporary.cleanup)
        self.root = pathlib.Path(self.temporary.name)

    def write(self, name, text):
        path = self.root / name
        path.write_text(text)
        return path

    def test_printing_workload_is_accepted_but_wrong_or_multiple_tests_are_rejected(self):
        template = ('running 1 test\ntest sample::fixture ... write_us=123\n'
                    'fixture oracle passed\nok\n\n'
                    'test result: ok. 1 passed; 0 failed; 0 ignored; 0 measured; '
                    '12 filtered out; finished in 0.01s\n')
        log = self.write('run.log', template)
        profile.validate_run(log, 'sample::fixture')
        for text in [template.replace('1 passed', '0 passed'),
                     template.replace('1 passed', '2 passed'),
                     template + template, template.replace('sample::fixture', 'other::test')]:
            log.write_text(text)
            with self.assertRaisesRegex(RuntimeError, 'exactly one passing'):
                profile.validate_run(log, 'sample::fixture')

    def test_listing_rejects_zero_and_multiple_selected_tests_before_measurement(self):
        log = self.root / 'selection.log'
        for text, accepted in [('sample::fixture: test\n', True), ('', False),
                               ('sample::fixture: test\nother: test\n', False)]:
            log.write_text(text)
            with mock.patch.object(profile, 'execute'):
                if accepted:
                    profile.validate_selection(self.root/'binary', self.root, log, 'sample::fixture', {})
                else:
                    with self.assertRaisesRegex(RuntimeError, 'exactly one selected'):
                        profile.validate_selection(self.root/'binary', self.root, log, 'sample::fixture', {})

    def test_lockfile_addition_cannot_upgrade_or_rewrite_existing_dependencies(self):
        original = ('version = 4\n[[package]]\nname = "pgderive"\nversion = "0.1.0"\n'
                    'dependencies = ["bytes"]\n[[package]]\nname = "bytes"\nversion = "1.0.0"\n'
                    'source = "registry+example"\nchecksum = "original"\n')
        added = original.replace('["bytes"]', '["bytes", "dhat"]') + (
            '[[package]]\nname = "dhat"\nversion = "0.3.3"\nsource = "registry+example"\n')
        before = self.write('original.lock', original)
        after = self.write('instrumented.lock', added)
        profile.validate_instrumented_lock(before, after, 'pgderive')
        for altered in [added.replace('version = "1.0.0"', 'version = "1.1.0"'),
                        added.replace('checksum = "original"', 'checksum = "different"'),
                        added.replace('["bytes", "dhat"]', '["dhat"]')]:
            after.write_text(altered)
            with self.assertRaisesRegex(RuntimeError, 'existing locked package'):
                profile.validate_instrumented_lock(before, after, 'pgderive')

    def test_dhat_global_peak_uses_simultaneous_global_counters_and_validates_shape(self):
        points = [dict(tb=100, tbk=10, gb=20, gbk=2, eb=10, ebk=1, mb=80),
                  dict(tb=200, tbk=20, gb=40, gbk=4, eb=10, ebk=1, mb=190)]
        data = dict(dhatFileVersion=2, mode='rust-heap', pps=points)
        path = self.write('heap.json', json.dumps(data))
        totals = profile.heap_totals(path)
        self.assertEqual(totals['peak_bytes'], 60)
        self.assertEqual(totals['total_bytes'], 300)
        self.assertEqual(totals['end_bytes'], 20)
        for invalid in [dict(data, pps=[]), dict(data, mode='copy'),
                        dict(data, pps=[dict(points[0], tb=-1)]),
                        dict(data, pps=[dict(points[0], gb=True)]),
                        dict(data, pps=[dict(points[0], eb=90)])]:
            path.write_text(json.dumps(invalid))
            with self.assertRaises(RuntimeError):
                profile.heap_totals(path)

    def test_summary_keeps_plain_and_heap_separate_and_rejects_duplicate_runs(self):
        samples = [dict(workload='reader', profiled=heap, repeat=repeat,
                        **({'total_blocks': 10 + repeat} if heap else {'user_seconds': repeat + 1}))
                   for heap in (False, True) for repeat in range(3)]
        summary = profile.summarize(samples, ['reader'], 3)['reader']
        self.assertEqual(summary['heap']['total_blocks']['median'], 11)
        self.assertEqual(summary['plain']['user_seconds']['median'], 2)
        with self.assertRaisesRegex(RuntimeError, 'Missing or duplicate'):
            profile.summarize(samples[:-1], ['reader'], 3)
        with self.assertRaisesRegex(RuntimeError, 'Missing or duplicate'):
            profile.summarize(samples[:-1] + [samples[0]], ['reader'], 3)

    def test_environment_removes_inherited_profilers_and_compiler_overrides(self):
        clean = profile.clean_environment(dict(PATH='/bin', CARGO_HOME='/cache',
                                              RUSTFLAGS='-Copt-level=0', CARGO_ENCODED_RUSTFLAGS='bad',
                                              RUSTC_WRAPPER='wrapper', CARGO_BUILD_TARGET='foreign',
                                              RUSTUP_TOOLCHAIN='nightly', PGDERIVE_REPLAY='custom',
                                              LLVM_PROFILE_FILE='coverage', CARGO_PROFILE_RELEASE_OPT_LEVEL='0'))
        self.assertEqual(clean, dict(PATH='/bin', CARGO_HOME='/cache',
                                     CARGO_PROFILE_RELEASE_DEBUG='1', LC_ALL='C', TZ='UTC'))

    def test_instrumentation_is_explicit_and_rejects_missing_workload(self):
        (self.root/'src').mkdir()
        self.write('Cargo.toml', '[package]\nname="pgderive"\nversion="0.1.0"\n[dev-dependencies]\n')
        self.write('src/lib.rs', 'pub fn production() {}\n')
        self.write('src/fixture.rs', 'fn run() -> Result<()> {\n    Ok(())\n}\n')
        with mock.patch.dict(profile.WORKLOADS, {'probe': ('src/fixture.rs', 'run', 'fixture::run', False)}):
            profile.instrument(self.root, ['probe'])
        self.assertIn('dhat = "=0.3.3"', (self.root/'Cargo.toml').read_text())
        self.assertIn('crate::allocation_probe::begin()?', (self.root/'src/fixture.rs').read_text())
        with self.assertRaisesRegex(RuntimeError, 'uninstrumented'):
            profile.instrument(self.root, [])

    def test_frozen_plain_binary_uses_plain_source_and_never_receives_heap_environment(self):
        directory = self.root/'plain-run'
        def execute(command, cwd, output, env):
            self.assertEqual(cwd, self.root/'plain-source')
            self.assertEqual(command[:2], ['/usr/bin/time', '-v'])
            self.assertNotIn('PGDERIVE_ALLOCATION_PROFILE', env)
            output.write_text('unused')
            directory.with_suffix('.time.txt').write_text(
                'User time (seconds): 0.25\nSystem time (seconds): 0.01\n'
                'Maximum resident set size (kbytes): 1234\n')
            return .3
        with mock.patch.object(profile, 'execute', side_effect=execute), \
                mock.patch.object(profile, 'validate_selection'), mock.patch.object(profile, 'validate_run'), \
                mock.patch.object(profile, 'object_metrics', return_value={}):
            result = profile.run_workload(self.root/'plain-tests', self.root/'plain-source', directory,
                                          'reader', {}, False)
        self.assertEqual(result['user_seconds'], .25)
        self.assertNotIn('total_bytes', result)


class PairedComparisonTests(unittest.TestCase):
    setUp = AllocationEvidenceTests.setUp
    write = AllocationEvidenceTests.write
    def report(self, side, fixture='same fixture'):
        directory = self.root / side
        source = directory / 'source'
        relative = pathlib.Path(profile.WORKLOADS['reader'][0])
        (source / relative).parent.mkdir(parents=True)
        (source / relative).write_text(fixture)
        store = source/'src/engine/tests/store.rs'
        store.parent.mkdir(parents=True, exist_ok=True)
        store.write_text('unchanged store helper')
        profiled_source = directory / 'profiled-source'
        profiled_source.mkdir()
        (profiled_source / 'probe.rs').write_text('instrumented source')
        report = dict(version=2, complete=True, workloads=['reader'], dhat='0.3.3', rustc='pinned', measurement_probe_version=profile.MEASUREMENT_PROBE_VERSION,
                      original_lock_sha256='production-lock', instrumented_lock_sha256='profile-lock')
        for label in ('plain', 'profiled'):
            binary = directory / f'{label}-tests'
            binary.write_bytes(f'{side}-{label}'.encode())
            report[f'{label}_binary_sha256'] = profile.digest(binary)
        for tree, manifest, field in [(source, 'source-hashes.json', 'source_sha256'),
                                      (profiled_source, 'instrumented-source-hashes.json', 'instrumented_source_sha256')]:
            hashes = {str(path.relative_to(tree)): profile.digest(path)
                      for path in sorted(tree.rglob('*')) if path.is_file()}
            profile.save(directory / manifest, hashes)
            report[field] = hashlib.sha256(json.dumps(hashes, sort_keys=True).encode()).hexdigest()
        report['fixture_hashes'] = profile.fixture_hashes(source, ['reader'])
        path = directory / 'report.json'
        profile.save(path, report)
        return path

    def test_paired_runs_alternate_sides_and_keep_heap_and_plain_binaries_separate(self):
        paths = [self.report('baseline'), self.report('candidate')]
        output = self.root / 'comparison'
        output.mkdir()
        calls = []
        def run(binary, source, directory, name, env, heap):
            calls.append((binary.parent.name, heap, binary.name, source.name))
            return {**({'total_bytes': 100} if heap else {'user_seconds': .01}),
                    'object_puts': 3, 'object_put_bytes': 256}
        with mock.patch.object(profile, 'run_workload', side_effect=run):
            profile.compare(paths, output, 3, 714)
        evidence = json.loads((output/'report.json').read_text())
        self.assertTrue(evidence['complete'])
        self.assertEqual(len(calls), 12)
        for heap in (False, True):
            selected = [call for call in calls if call[1] == heap]
            self.assertEqual({call[2] for call in selected}, {'profiled-tests' if heap else 'plain-tests'})
            self.assertEqual({call[3] for call in selected}, {'profiled-source' if heap else 'source'})
            self.assertNotEqual(selected[0][0], selected[1][0])
            self.assertEqual(selected[0][0], selected[3][0])
        self.assertEqual(evidence['summary']['baseline']['reader']['heap']['total_bytes']['median'], 100)

    def test_comparison_rejects_changed_fixtures_and_tampered_frozen_binaries(self):
        baseline, candidate = self.report('baseline'), self.report('candidate', 'different fixture')
        with self.assertRaisesRegex(RuntimeError, 'fixture_hashes'):
            profile.comparison_inputs([baseline, candidate])
        (candidate.parent/'plain-tests').write_bytes(b'rebuilt outside evidence')
        with self.assertRaisesRegex(RuntimeError, 'Frozen executable changed'):
            profile.comparison_inputs([baseline, candidate])

    def test_source_mutation_is_rejected_even_when_fixture_is_unchanged(self):
        baseline, candidate = self.report('baseline'), self.report('candidate')
        (candidate.parent/'source'/'unexpected.rs').write_text('modified production')
        with self.assertRaisesRegex(RuntimeError, 'Frozen source changed'):
            profile.comparison_inputs([baseline, candidate])

    def test_failure_preserves_completed_samples_and_incomplete_report(self):
        paths = [self.report('baseline'), self.report('candidate')]
        output = self.root/'comparison'
        output.mkdir()
        with mock.patch.object(profile, 'run_workload', side_effect=[{'user_seconds': .01}, RuntimeError('fixture failure')]):
            with self.assertRaisesRegex(RuntimeError, 'fixture failure'):
                profile.compare(paths, output, 3, 714)
        evidence = json.loads((output/'report.json').read_text())
        self.assertFalse(evidence['complete'])
        self.assertEqual(len(evidence['samples']), 1)
        self.assertIn('fixture failure', evidence['failure'])


class ObjectCostEvidenceTests(unittest.TestCase):
    setUp = AllocationEvidenceTests.setUp
    write = AllocationEvidenceTests.write
    def test_reader_operation_evidence_requires_one_complete_numeric_record(self):
        values = dict(object_puts=3, object_put_bytes=100, object_reads=7,
                      object_range_bytes=300, object_delete_batches=0)
        log = self.write('reader.log', 'allocation_object_io=' + json.dumps(values) + '\n')
        self.assertEqual(profile.object_metrics('reader', log, None), values)
        for text in ['', log.read_text() * 2, 'allocation_object_io={"object_puts":3}\n',
                     'allocation_object_io=' + json.dumps(dict(values, object_put_bytes=-1))]:
            log.write_text(text)
            with self.assertRaises(RuntimeError):
                profile.object_metrics('reader', log, None)

    def test_replay_reports_total_and_measured_requests_without_replacing_lifecycle_evidence(self):
        base = dict(case='case', cold_reopen_equal=True, object_puts=9, object_put_bytes=500,
                    object_reads=8, object_range_bytes=900, object_delete_batches=2,
                    stored_objects=3, stored_bytes=200, measured_object_reads=4, measured_object_puts=1,
                    lifecycle=dict(service=[dict(requests=5, read_requested_bytes=12,
                                                  read_bytes=11, write_requested_bytes=15, written_bytes=14)]))
        path = self.write('replay.json', json.dumps([base]))
        result = profile.object_metrics('replay', self.root/'unused', path)
        self.assertEqual(result['object_puts'], 9)
        self.assertEqual(result['measured_object_puts'], 1)
        self.assertEqual(result['lifecycle_requests'], 5)
        path.write_text(json.dumps([dict(base, cold_reopen_equal=False)]))
        with self.assertRaisesRegex(RuntimeError, 'Invalid replay'):
            profile.object_metrics('replay', self.root/'unused', path)

    def test_put_gate_rejects_one_regressed_sample_even_when_median_is_unchanged(self):
        samples = [dict(side=side, workload='reader', profiled=heap, repeat=repeat,
                        object_puts=10, object_put_bytes=1024)
                   for side in ('baseline', 'candidate') for heap in (False, True) for repeat in range(3)]
        self.assertEqual(len(profile.validate_object_cost(samples, ['reader'], 3)), 12)
        samples[-1]['object_put_bytes'] += 1
        with self.assertRaisesRegex(RuntimeError, 'cost increased.*object_put_bytes'):
            profile.validate_object_cost(samples, ['reader'], 3)


class DependencyAndFixtureIntegrityTests(unittest.TestCase):
    setUp = AllocationEvidenceTests.setUp
    write = AllocationEvidenceTests.write
    report = PairedComparisonTests.report

    def test_additive_resolution_preserves_original_transitive_edges(self):
        original = ('version = 4\n[[package]]\nname="pgderive"\nversion="0.1.0"\n'
                    'dependencies=["rustix", "windows-sys 0.59.0"]\n'
                    '[[package]]\nname="rustix"\nversion="0.38.44"\n'
                    'dependencies=["windows-sys 0.52.0"]\n'
                    '[[package]]\nname="windows-sys"\nversion="0.52.0"\n'
                    '[[package]]\nname="windows-sys"\nversion="0.59.0"\n')
        resolved = original.replace('dependencies=["rustix",', 'dependencies=["dhat", "rustix",')
        resolved = resolved.replace('dependencies=["windows-sys 0.52.0"]',
                                    'dependencies=["windows-sys 0.59.0"]')
        resolved += '[[package]]\nname="dhat"\nversion="0.3.3"\n'
        before = self.write('original.lock', original)
        after = self.write('resolved.lock', resolved)
        with self.assertRaisesRegex(RuntimeError, 'changed existing'):
            profile.validate_instrumented_lock(before, after, 'pgderive')
        profile.preserve_original_lock(before, after, 'pgderive')
        profile.validate_instrumented_lock(before, after, 'pgderive')
        self.assertIn('dependencies=["windows-sys 0.52.0"]', after.read_text())
        self.assertIn('"dhat", "rustix"', after.read_text())
        after.write_text(resolved.replace('version="0.38.44"', 'version="0.38.45"'))
        with self.assertRaisesRegex(RuntimeError, 'removed or upgraded'):
            profile.preserve_original_lock(before, after, 'pgderive')

    def test_comparison_rejects_different_production_and_instrumented_lock_hashes(self):
        baseline, candidate = self.report('baseline'), self.report('candidate')
        initial = json.loads(candidate.read_text())
        for field in ('original_lock_sha256', 'instrumented_lock_sha256'):
            profile.save(candidate, dict(initial, **{field: 'different graph'}))
            with self.assertRaisesRegex(RuntimeError, field):
                profile.comparison_inputs([baseline, candidate])

    def test_store_and_spill_helpers_participate_in_fixture_identity(self):
        relative = pathlib.Path(profile.WORKLOADS['spill'][0])
        (self.root/relative).parent.mkdir(parents=True)
        test = 'fn spill_boundaries_never_finalize_intermediate_weights() -> Result<()> {\n    Ok(())\n}\n'
        helper = 'fn limits(entries: usize) -> Limits {\n    build(entries)\n}\n'
        (self.root/relative).write_text(test + helper)
        before = profile.fixture_hashes(self.root, ['spill'])
        (self.root/relative).write_text(test + helper.replace('build(entries)', 'build(entries + 1)'))
        self.assertNotEqual(before, profile.fixture_hashes(self.root, ['spill']))
        baseline = self.report('baseline')
        source = baseline.parent/'source'
        before = profile.fixture_hashes(source, ['reader'])
        (source/'src/engine/tests/store.rs').write_text('changed request accounting')
        self.assertNotEqual(before, profile.fixture_hashes(source, ['reader']))

    def test_replay_service_evidence_must_be_present_nonempty_and_typed(self):
        base = dict(case='case', cold_reopen_equal=True, object_puts=0, object_put_bytes=0,
                    object_reads=0, object_range_bytes=0, object_delete_batches=0,
                    stored_objects=0, stored_bytes=0, measured_object_reads=0, measured_object_puts=0)
        for lifecycle in ({}, {'service': []}, {'service': None}, {'service': [3]}, {'service': [{}]}):
            path = self.write('replay.json', json.dumps([dict(base, lifecycle=lifecycle)]))
            with self.assertRaisesRegex(RuntimeError, 'replay.*(evidence|counter)'):
                profile.object_metrics('replay', self.root/'unused', path)
