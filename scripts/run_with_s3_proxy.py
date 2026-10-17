#!/usr/bin/env python3
"""Run one isolated live harness through a fresh latency proxy; retain evidence."""
import json
import math
import os
import pathlib
import subprocess
import sys
import time
import urllib.error
import urllib.request
from harness_control import commit_proxy as start_commit_proxy, start_recorded


def request(url, method="GET"):
    with urllib.request.urlopen(urllib.request.Request(url, method=method), timeout=2) as response:
        return response.read()


def commit_proxy(output, mode):
    return start_commit_proxy(output/f'sql-commit-{mode}.jsonl', mode,
                              gate=output/'sql-commit-hold.release' if mode == 'hold' else None)


def run(output, command):
    timeout = float(os.environ.get("PGDERIVE_HARNESS_TIMEOUT_SECONDS", "600"))
    if not math.isfinite(timeout) or timeout <= 0:
        raise ValueError("PGDERIVE_HARNESS_TIMEOUT_SECONDS must be finite and positive")
    upstream = "http://127.0.0.1:8333"
    try:
        request(upstream)
    except (OSError, urllib.error.URLError):
        # Reuse any running PoC service. Start only our isolated objects service
        # when unavailable; never recreate the shared PostgreSQL or PoC services.
        subprocess.run(["docker", "compose", "up", "-d", "objects"], check=True)
        for _ in range(100):
            try:
                request(upstream)
                break
            except (OSError, urllib.error.URLError):
                time.sleep(.1)
        else:
            raise RuntimeError("SeaweedFS did not become ready")
    bucket = "pgderive-mvp-tests"
    try:
        request(f"{upstream}/{bucket}")
    except urllib.error.HTTPError as error:
        if error.code != 404:
            raise
        request(f"{upstream}/{bucket}", "PUT")
    scale = os.environ.get("PGDERIVE_S3_LATENCY_SCALE", "1")
    seed = os.environ.get("PGDERIVE_S3_LATENCY_SEED", "42")
    fail = os.environ.get("PGDERIVE_S3_FAIL")
    proxy_command = [sys.executable, "-u", "scripts/s3_latency_proxy.py", "--port", "0",
                     "--scale", scale, "--seed", seed]
    if fail:
        proxy_command += ["--fail", fail]
    proxy, ready = start_recorded(proxy_command, output/"proxy.jsonl")
    commit_processes = []
    try:
        environment = os.environ.copy()
        environment["PGDERIVE__OBJECT_STORE__ENDPOINT"] = f"http://127.0.0.1:{ready['port']}"
        if fail:
            environment["PGDERIVE_EXPECT_STORAGE_FAILURE"] = "1"
        for mode in ("before", "after", "hold"):
            process, port = commit_proxy(output, mode)
            commit_processes.append(process)
            environment[f"PGDERIVE_SQL_COMMIT_{mode.upper()}_PORT"] = str(port)
            if mode == "hold":
                environment["PGDERIVE_SQL_COMMIT_HOLD_GATE"] = str(output / "sql-commit-hold.release")

        (output / "storage-profile.json").write_text(json.dumps({"upstream": upstream,
            "bucket": bucket, "seed": seed, "scale": scale, "failure": fail,
            "harness_timeout_seconds": timeout,
            "anchors_ms": ready["anchors_ms"], "model": "linear inverse CDF, p0=0, p99-clamped",
            "source": "https://topicpartition.io/misc/AWS-S3-PUT-latency-benchmark"}, indent=2))
        with (output / "transactions.jsonl").open("w") as transactions, (output / "harness.log").open("w") as stderr:
            result = subprocess.run(command, env=environment, stdout=transactions, stderr=stderr, timeout=timeout)
        return result.returncode
    finally:
        for process in commit_processes:
            process.terminate()
            process.wait(timeout=10)
        proxy.terminate()
        proxy.wait(timeout=10)


def timed_run(output, command):
    started = time.monotonic()
    code = None
    try:
        code = run(output, command)
        return code
    finally:
        elapsed = time.monotonic() - started
        (output / 'timing.json').write_text(json.dumps({
            'elapsed_seconds': elapsed, 'returncode': code,
            'completed': code is not None,
        }, indent=2)+'\n')
        print(f'{output.name}: {elapsed:.2f}s (exit {code})', file=sys.stderr, flush=True)


if __name__ == "__main__":
    sys.exit(timed_run(pathlib.Path(sys.argv[1]), sys.argv[2:]))
