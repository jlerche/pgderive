#!/usr/bin/env python3
"""Fast production-path checks; optional unpaced state-size measurements or live checks."""
import argparse
import json
import os
import re
import pathlib
import signal
import subprocess
import sys
import time
from fast_replay import export, preloaded, traces
from performance.common import ROOT, save, sha256

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
        counts = {'oracle_checks': sum(step['expected'] is not None for step in steps),
                  'logical_time': sum(not step['restart'] for step in steps),
                  'measured_weighted_updates': sum(len(step['updates']) for step in measured),
                  'output_groups': len(steps[-1]['expected'])}
        if (len(times) != len(measured) or any(type(t) is not int or t < 0 for t in times)
                or any(type(row[key]) is not int or row[key] != count for key, count in counts.items())
                or any(type(row[key]) is not int or row[key] < 0 for key in [
                    'memtable_bytes', 'measured_cache_misses', 'measured_object_reads',
                    'measured_object_puts', 'object_puts', 'stored_objects', 'stored_bytes'])
                or row['measured_object_puts'] != 0):
            raise RuntimeError('Engine measurement/oracle coverage differs: ' + row['case'])


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
    return binaries[0]


def growth_limits(rows):
    # Preloading wide aggregate statistics can emit many identities per group.
    # Raise only fixture output guards; cache, spill and per-update budgets stay real.
    return {'output_entries': max(250_000, rows * 16),
            'output_bytes': max(64 * 1024 * 1024, rows * 4096)}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--output', type=pathlib.Path, required=True)
    parser.add_argument('--mode', choices=['correctness', 'growth', 'integration'], default='correctness')
    parser.add_argument('--seeds', type=int, default=3)
    parser.add_argument('--state-rows', type=int, nargs='+', default=[1000, 8000, 32000])
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
        else:
            binary = test_binary(output, args.release)
            report['steps'].append(json.loads((output / 'build.json').read_text()))
            report['test_binary_sha256'] = sha256(pathlib.Path(binary))
            report['steps'].append(execute([binary, 'virtual_', '--nocapture'], output, 'virtual-time'))
            validate_tests(output/'virtual-time.log', VIRTUAL_TESTS)
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
    print(json.dumps(report))


if __name__ == '__main__':
    def interrupted(_signal, _frame):
        raise KeyboardInterrupt
    signal.signal(signal.SIGTERM, interrupted)
    main()
