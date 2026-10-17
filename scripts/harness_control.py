"""Shared recorded CLI and proxy lifecycle; callers retain fault-specific assertions."""
import json
import subprocess
import sys
import time


def admin(fixture, binary, action, config=None, *, scope='query', success=True, timeout=240):
    result = subprocess.run(binary + [scope, action, str(config or fixture.config())],
                            env=fixture.environment, text=True, capture_output=True, timeout=timeout)
    with (fixture.output/'commands.jsonl').open('a') as evidence:
        evidence.write(json.dumps({'scope':scope, 'action':action, 'code':result.returncode,
                                   'stdout':result.stdout, 'stderr':result.stderr,
                                   'expected':'success' if success else 'rejection'})+'\n')
    assert (result.returncode == 0) == success, result.stderr or 'invalid command outcome'
    return json.loads(result.stdout.splitlines()[-1]) if success else result.stderr


def wait_records(path, predicate, *, process=None, timeout=20):
    deadline = time.monotonic()+timeout
    while time.monotonic() < deadline:
        # A concurrent writer may have flushed only part of its newest record.
        records = [json.loads(line) for line in path.read_text().splitlines(keepends=True)
                   if line.endswith('\n')]
        if predicate(records):
            return records
        if process is not None and process.poll() is not None:
            raise RuntimeError(f'process ended before its witness; see {path}')
        time.sleep(.05)
    raise AssertionError(f'fault witness missing: {path}')


def start_recorded(command, path, timeout=10):
    with path.open('w') as log:
        process = subprocess.Popen(command, stdout=log, stderr=log)
    try:
        records = wait_records(path, lambda rows: rows and rows[0].get('ready'),
                               process=process, timeout=timeout)
        return process, records[0]
    except BaseException:
        process.terminate()
        process.wait(timeout=10)
        raise


def commit_proxy(path, mode, *, once=False, marker=None, gate=None, upstream=None):
    command = [sys.executable, '-u', 'scripts/pg_commit_proxy.py', '--mode', mode]
    if once:
        command.append('--once')
    for flag, value in [('--arm-query', marker), ('--gate', gate), ('--upstream', upstream)]:
        if value is not None:
            command += [flag, str(value)]
    process, ready = start_recorded(command, path)
    return process, ready['port']
