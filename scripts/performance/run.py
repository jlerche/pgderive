#!/usr/bin/env python3
"""Reproducible local autocommit comparison. See docs/performance-benchmarks.md."""
import argparse
import concurrent.futures
import datetime
import importlib.util
import json
import os
import pathlib
import platform
import signal
import subprocess
import threading
import time
import uuid

from common import ROOT, RW_IMAGE, SCHEMA_VERSION, S3_IMAGE, docker, save, sha256
from fixture import Fixture
from workload import run


def arguments():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--baseline', type=pathlib.Path, required=True)
    parser.add_argument('--candidate', type=pathlib.Path, required=True)
    parser.add_argument('--risingwave', action='store_true')
    parser.add_argument('--output', type=pathlib.Path, required=True)
    parser.add_argument('--duration', type=int, default=60)
    parser.add_argument('--rate', type=float, default=1000)
    parser.add_argument('--state-rows', type=int, default=12000)
    parser.add_argument('--groups', type=int, default=1000)
    parser.add_argument('--workload', choices=['updates', 'wide-updates', 'mixed'], default='updates')
    parser.add_argument('--proxy-scale', type=float, default=1)
    parser.add_argument('--window-seconds', type=int, default=60)
    parser.add_argument('--sample-seconds', type=float, default=10)
    parser.add_argument('--poll-ms', type=float, default=10)
    parser.add_argument('--drain-seconds', type=int, default=180)
    parser.add_argument('--rw-barrier-ms', type=int, default=100)
    parser.add_argument('--rw-memory', default='8g')
    parser.add_argument('--rw-memory-bytes', type=int, default=8589934592)
    parser.add_argument('--postgres-memory', default='4g')
    parser.add_argument('--postgres-image', default='postgres:17')
    args = parser.parse_args()
    import math
    for field in ['duration', 'rate', 'groups', 'window_seconds', 'sample_seconds', 'poll_ms',
                  'drain_seconds', 'rw_barrier_ms', 'rw_memory_bytes']:
        if not math.isfinite(getattr(args, field)) or getattr(args, field) <= 0:
            parser.error(field + ' must be finite and positive')
    if args.state_rows < 0 or args.state_rows >= 1000000000:
        parser.error('state-rows must be in [0,1000000000)')
    if not math.isfinite(args.proxy_scale) or args.proxy_scale < 0:
        parser.error('proxy-scale must be finite and nonnegative')
    args.baseline, args.candidate = args.baseline.resolve(), args.candidate.resolve()
    for binary in [args.baseline, args.candidate]:
        if not binary.is_file() or not os.access(binary, os.X_OK):
            parser.error('Executable not found: ' + str(binary))
    args.output = args.output.resolve()
    if args.output.exists():
        parser.error('output must be a new directory, preserving prior evidence')
    return args


def close_fixtures(fixtures, success):
    errors = {}
    for fixture in reversed(fixtures):
        try:
            result = fixture.close(success=success)
        except Exception as error:
            result = ['cleanup failed: ' + repr(error)]
        errors[fixture.directory.name] = result
    return errors


def main():
    args = arguments()
    if importlib.util.find_spec('psycopg') is None:
        raise SystemExit('Install psycopg: python3 -m pip install "psycopg[binary]>=3,<4"')
    if platform.system() != 'Linux':
        raise SystemExit('Linux required: /proc metrics and host-network RisingWave CDC')
    docker('info', '--format', '{{.ServerVersion}}')
    # Resolve/pull before timing. Evidence records immutable local image IDs.
    for image in [args.postgres_image, S3_IMAGE] + ([RW_IMAGE] if args.risingwave else []):
        try:
            docker('image', 'inspect', image)
        except subprocess.CalledProcessError:
            subprocess.run(['docker', 'pull', image], check=True)
    args.output.mkdir(parents=True)
    run_id = 'pgd-perf-' + uuid.uuid4().hex[:12]
    manifest = {'schema_version': SCHEMA_VERSION, 'run_id': run_id,
        'started_utc': datetime.datetime.now(datetime.timezone.utc).isoformat(),
        'git_head': subprocess.check_output(['git', 'rev-parse', 'HEAD'], cwd=ROOT, text=True).strip(),
        'git_dirty': bool(subprocess.check_output(['git', 'status', '--porcelain'], cwd=ROOT, text=True)),
        'host': {'platform': platform.platform(), 'cpu_count': os.cpu_count(),
                 'cpu_affinity': sorted(os.sched_getaffinity(0)),
                 'cpu_model': next((line.split(':', 1)[1].strip() for line in pathlib.Path('/proc/cpuinfo').read_text().splitlines() if line.startswith('model name')), 'unknown'),
                 'memory_total': pathlib.Path('/proc/meminfo').read_text().splitlines()[0]},
        'parameters': {key: str(value) if isinstance(value, pathlib.Path) else value for key, value in vars(args).items()},
        'binaries': {name: {'path': str(path), 'sha256': sha256(path)} for name, path in
                     [('baseline', args.baseline), ('candidate', args.candidate)]},
        'limitations': ['Concurrent engines compete for host resources; repeat with order/host controls.',
            'Pgderive RSS uncapped; RisingWave memory cap recorded separately.',
            'Visibility uses a monotonic aggregate marker; it does not prove intermediate sink atomicity.',
            'Mixed workload samples marker updates only; insert/delete latency is not measured.',
            'Synthetic storage latency, not cloud object storage; metrics polling adds observer overhead.',
            'Observed visibility can precede client commit acknowledgement; negative values clamp to zero.']}
    save(args.output / 'manifest.json', manifest)
    fixtures, stops, results = [], [], {}
    report = {'schema_version': SCHEMA_VERSION, 'run_id': run_id, 'passed': False, 'engines': results}
    def interrupt(signum, frame):
        for stop in stops:
            stop.set()
        raise KeyboardInterrupt
    signal.signal(signal.SIGTERM, interrupt)
    signal.signal(signal.SIGINT, interrupt)
    try:
        for label, binary in [('baseline', args.baseline), ('candidate', args.candidate)] + ([('risingwave', None)] if args.risingwave else []):
            fixture = Fixture(run_id + '-' + label, args.output / label, args, binary)
            fixtures.append(fixture)
            fixture.start()
        manifest['fixtures'] = {fixture.directory.name: fixture.fingerprint for fixture in fixtures}
        save(args.output / 'manifest.json', manifest)
        start = time.monotonic() + 1
        with concurrent.futures.ThreadPoolExecutor(max_workers=len(fixtures)) as executor:
            pending = {}
            for fixture in fixtures:
                stop = threading.Event(); stops.append(stop)
                pending[executor.submit(run, fixture, args, start, stop)] = fixture.directory.name
            for future in concurrent.futures.as_completed(pending):
                label = pending[future]
                try:
                    results[label] = future.result()
                    print(json.dumps({'engine': label, 'transactions': results[label]['transactions'],
                                      'sink_visibility_ms': results[label]['sink_visibility_ms']}), flush=True)
                except BaseException:
                    for stop in stops:
                        stop.set()
                    raise
        report['passed'] = all(result['passed'] for result in results.values())
    except BaseException as error:
        report['error'] = repr(error)
        raise
    finally:
        for stop in stops:
            stop.set()
        report['cleanup_errors'] = close_fixtures(fixtures, report['passed'])
        if any(report['cleanup_errors'].values()):
            report['passed'] = False
        save(args.output / 'report.json', report)
    if not report['passed']:
        raise SystemExit('Benchmark failed; inspect report.json')


if __name__ == '__main__':
    main()
