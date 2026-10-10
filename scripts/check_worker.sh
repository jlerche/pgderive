#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")/.."
if [[ ${PGDERIVE_COVERAGE:-0} == 1 ]]; then
    # Run the instrumented executable directly so SIGKILL targets the worker,
    # rather than a cargo launcher whose child would survive the crash test.
    export CARGO_TARGET_DIR="$PWD/target/llvm-cov-target"
    source <(cargo llvm-cov show-env --sh)
    cargo build --locked --bin pgderive_worker
    worker=("$CARGO_TARGET_DIR/debug/pgderive_worker")
else
    cargo build --locked --bin pgderive_worker
    worker=("$PWD/target/debug/pgderive_worker")
fi
mkdir -p artifacts/worker
output=$(mktemp -d "$PWD/artifacts/worker/run-XXXXXXXX")
printf 'Worker evidence: %s\n' "$output"
mkdir "$output/transactions" "$output/crashes" "$output/compiler" "$output/projection" "$output/relational" "$output/partition" "$output/numeric" "$output/native" "$output/date-bin" "$output/hopping" "$output/ranking" "$output/peer-frames" "$output/derived" "$output/navigation" "$output/lookup" "$output/composition" "$output/case" "$output/session" "$output/distinct"
export PGDERIVE__POSTGRES__PASSWORD=${PGPASSWORD:-postgres}
if ! PGDERIVE_S3_LATENCY_SCALE=0 python3 scripts/run_with_s3_proxy.py "$output/transactions" \
    python3 scripts/worker_harness.py --faults --kill --ticks 4 --burst 8 \
    "$output/transactions" -- "${worker[@]}"; then
    cat "$output/transactions/harness.log" >&2
    exit 1
fi
if ! python3 scripts/run_with_s3_proxy.py "$output/crashes" \
    python3 scripts/worker_crash_harness.py "$output/crashes" -- "${worker[@]}"; then
    cat "$output/crashes/harness.log" >&2
    exit 1
fi
if ! PGDERIVE_S3_LATENCY_SCALE=0 python3 scripts/run_with_s3_proxy.py "$output/compiler" \
    python3 scripts/sql_compiler_harness.py "$output/compiler" -- "${worker[@]}"; then
    cat "$output/compiler/harness.log" >&2
    exit 1
fi
if ! PGDERIVE_S3_LATENCY_SCALE=0 python3 scripts/run_with_s3_proxy.py "$output/projection" \
    python3 scripts/projection_compiler_harness.py "$output/projection" -- "${worker[@]}"; then
    cat "$output/projection/harness.log" >&2
    exit 1
fi
if ! PGDERIVE_S3_LATENCY_SCALE=0 python3 scripts/run_with_s3_proxy.py "$output/relational" \
    python3 scripts/relational_compiler_harness.py "$output/relational" -- "${worker[@]}"; then
    cat "$output/relational/harness.log" >&2
    exit 1
fi
if ! PGDERIVE_S3_LATENCY_SCALE=0 python3 scripts/run_with_s3_proxy.py "$output/partition" \
    python3 scripts/partition_compiler_harness.py "$output/partition" -- "${worker[@]}"; then
    cat "$output/partition/harness.log" >&2
    exit 1
fi
if ! PGDERIVE_S3_LATENCY_SCALE=0 python3 scripts/run_with_s3_proxy.py "$output/numeric" \
    python3 scripts/numeric_compiler_harness.py "$output/numeric" -- "${worker[@]}"; then
    cat "$output/numeric/harness.log" >&2
    exit 1
fi
if ! PGDERIVE_S3_LATENCY_SCALE=0 python3 scripts/run_with_s3_proxy.py "$output/native" \
    python3 scripts/native_compiler_harness.py "$output/native" -- "${worker[@]}"; then
    cat "$output/native/harness.log" >&2
    exit 1
fi
if ! PGDERIVE_S3_LATENCY_SCALE=0 python3 scripts/run_with_s3_proxy.py "$output/date-bin" \
    python3 scripts/date_bin_compiler_harness.py "$output/date-bin" -- "${worker[@]}"; then
    cat "$output/date-bin/harness.log" >&2
    exit 1
fi
if ! PGDERIVE_S3_LATENCY_SCALE=0 python3 scripts/run_with_s3_proxy.py "$output/hopping" \
    python3 scripts/hopping_compiler_harness.py "$output/hopping" -- "${worker[@]}"; then
    cat "$output/hopping/harness.log" >&2
    exit 1
fi
if ! PGDERIVE_S3_LATENCY_SCALE=0 python3 scripts/run_with_s3_proxy.py "$output/ranking" \
    python3 scripts/ranking_compiler_harness.py "$output/ranking" -- "${worker[@]}"; then
    cat "$output/ranking/harness.log" >&2
    exit 1
fi
if ! PGDERIVE_S3_LATENCY_SCALE=0 python3 scripts/run_with_s3_proxy.py "$output/peer-frames" \
    python3 scripts/peer_frames_compiler_harness.py "$output/peer-frames" -- "${worker[@]}"; then
    cat "$output/peer-frames/harness.log" >&2
    exit 1
fi
if ! PGDERIVE_S3_LATENCY_SCALE=0 python3 scripts/run_with_s3_proxy.py "$output/derived" \
    python3 scripts/derived_compiler_harness.py "$output/derived" -- "${worker[@]}"; then
    cat "$output/derived/harness.log" >&2
    exit 1
fi
if ! PGDERIVE_S3_LATENCY_SCALE=0 python3 scripts/run_with_s3_proxy.py "$output/navigation" \
    python3 scripts/navigation_compiler_harness.py "$output/navigation" -- "${worker[@]}"; then
    cat "$output/navigation/harness.log" >&2
    exit 1
fi
if ! PGDERIVE_S3_LATENCY_SCALE=0 python3 scripts/run_with_s3_proxy.py "$output/lookup" \
    python3 scripts/lookup_compiler_harness.py "$output/lookup" -- "${worker[@]}"; then
    cat "$output/lookup/harness.log" >&2
    exit 1
fi
if ! PGDERIVE_S3_LATENCY_SCALE=0 python3 scripts/run_with_s3_proxy.py "$output/composition" \
    python3 scripts/composition_compiler_harness.py "$output/composition" -- "${worker[@]}"; then
    cat "$output/composition/harness.log" >&2
    exit 1
fi
if ! PGDERIVE_S3_LATENCY_SCALE=0 python3 scripts/run_with_s3_proxy.py "$output/case" \
    python3 scripts/case_compiler_harness.py "$output/case" -- "${worker[@]}"; then
    cat "$output/case/harness.log" >&2
    exit 1
fi
if ! PGDERIVE_S3_LATENCY_SCALE=0 python3 scripts/run_with_s3_proxy.py "$output/session" \
    python3 scripts/session_compiler_harness.py "$output/session" -- "${worker[@]}"; then
    cat "$output/session/harness.log" >&2
    exit 1
fi
if ! PGDERIVE_S3_LATENCY_SCALE=0 python3 scripts/run_with_s3_proxy.py "$output/distinct" \
    python3 scripts/distinct_compiler_harness.py "$output/distinct" -- "${worker[@]}"; then
    cat "$output/distinct/harness.log" >&2
    exit 1
fi
python3 - "$output" "${worker[0]}" <<'PY'
import hashlib,json,os,pathlib,subprocess,sys
output=pathlib.Path(sys.argv[1])
results=list(output.rglob('result.json'))
assert len(results)==92,results
for result in results:
    json.loads(result.read_text())
root=pathlib.Path.cwd()
files=[root/'Cargo.lock',root/'Cargo.toml',root/'worker.example.toml',pathlib.Path(sys.argv[2]),
       *sorted((root/'src').rglob('*.rs')), *sorted((root/'vendor/pgwire-replication/src').rglob('*.rs')),
       *sorted((root/'scripts').glob('*.py')), *sorted(output.rglob('*.json')),
       *sorted(output.rglob('*.jsonl')), *sorted(output.rglob('*.toml')), *sorted(output.rglob('*.log'))]
provenance={'git_head':subprocess.check_output(['git','rev-parse','HEAD'],text=True).strip(),
            'rust':subprocess.check_output(['rustc','--version'],text=True).strip(),
            'postgres':subprocess.check_output(['psql','-h','127.0.0.1','-p','55434','-U','postgres','-d','pgderive_dev','-Atc','SELECT version()'],text=True,env={**os.environ,"PGPASSWORD":os.environ.get("PGPASSWORD","postgres")}).strip(),
            'sha256':{str(path.relative_to(root)):hashlib.sha256(path.read_bytes()).hexdigest() for path in files}}
(output/'provenance.json').write_text(json.dumps(provenance,indent=2)+'\n')
print('Worker qualification passed: six COMMIT faults, producer burst, restart and five witnessed process kills.')
PY
