#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")/.."
if [[ ${PGDERIVE_COVERAGE:-0} == 1 ]]; then
    # Run the instrumented executable directly so SIGKILL targets the worker,
    # rather than a cargo launcher whose child would survive the crash test.
    export CARGO_TARGET_DIR="$PWD/target/llvm-cov-target"
    source <(cargo llvm-cov show-env --sh)
    cargo build --locked --bin pgderive_worker --bin pgderive --bin postgres_tls_control_probe
    worker=("$CARGO_TARGET_DIR/debug/pgderive_worker")
    admin=("$CARGO_TARGET_DIR/debug/pgderive")
else
    cargo build --locked --bin pgderive_worker --bin pgderive --bin postgres_tls_control_probe
    worker=("$PWD/target/debug/pgderive_worker")
    admin=("$PWD/target/debug/pgderive")
fi
mkdir -p artifacts/worker
output=$(mktemp -d "$PWD/artifacts/worker/run-XXXXXXXX")
printf 'Worker evidence: %s\n' "$output"
mkdir "$output/transactions" "$output/crashes" "$output/compiler" "$output/projection" "$output/relational" "$output/partition" "$output/numeric" "$output/native" "$output/date-bin" "$output/hopping" "$output/ranking" "$output/peer-frames" "$output/derived" "$output/navigation" "$output/lookup" "$output/composition" "$output/case" "$output/session" "$output/distinct" "$output/nexmark-q15" "$output/join-composition" "$output/nexmark-auction" "$output/nexmark-q22" "$output/nexmark-q1" "$output/nexmark-projection" "$output/nexmark-q20" "$output/nexmark-ordered" "$output/nexmark-temporal" "$output/nexmark-q3" "$output/nexmark-calendar" "$output/nexmark-hot" "$output/group-membership" "$output/nexmark-q8" "$output/nexmark-q13" "$output/nexmark-q14" "$output/nexmark-q21"
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
if ! PGDERIVE_S3_LATENCY_SCALE=0 python3 scripts/run_with_s3_proxy.py "$output/nexmark-q15" \
    python3 scripts/nexmark_q15_harness.py "$output/nexmark-q15" -- "${worker[@]}"; then
    cat "$output/nexmark-q15/harness.log" >&2
    exit 1
fi
if ! PGDERIVE_S3_LATENCY_SCALE=0 python3 scripts/run_with_s3_proxy.py "$output/join-composition" \
    python3 scripts/join_composition_harness.py "$output/join-composition" -- "${worker[@]}"; then
    cat "$output/join-composition/harness.log" >&2
    exit 1
fi
if ! PGDERIVE_S3_LATENCY_SCALE=0 python3 scripts/run_with_s3_proxy.py "$output/nexmark-auction" \
    python3 scripts/nexmark_auction_harness.py "$output/nexmark-auction" -- "${worker[@]}"; then
    cat "$output/nexmark-auction/harness.log" >&2
    exit 1
fi
if ! PGDERIVE_S3_LATENCY_SCALE=0 python3 scripts/run_with_s3_proxy.py "$output/nexmark-q22" \
    python3 scripts/nexmark_q22_harness.py "$output/nexmark-q22" -- "${worker[@]}"; then
    cat "$output/nexmark-q22/harness.log" >&2
    exit 1
fi
if ! PGDERIVE_S3_LATENCY_SCALE=0 python3 scripts/run_with_s3_proxy.py "$output/nexmark-q1" \
    python3 scripts/nexmark_q1_harness.py "$output/nexmark-q1" -- "${worker[@]}"; then
    cat "$output/nexmark-q1/harness.log" >&2
    exit 1
fi
if ! PGDERIVE_S3_LATENCY_SCALE=0 python3 scripts/run_with_s3_proxy.py "$output/nexmark-projection" \
    python3 scripts/nexmark_projection_harness.py "$output/nexmark-projection" -- "${worker[@]}"; then
    cat "$output/nexmark-projection/harness.log" >&2
    exit 1
fi
if ! PGDERIVE_S3_LATENCY_SCALE=0 python3 scripts/run_with_s3_proxy.py "$output/nexmark-q20" \
    python3 scripts/nexmark_q20_harness.py "$output/nexmark-q20" -- "${worker[@]}"; then
    cat "$output/nexmark-q20/harness.log" >&2
    exit 1
fi
if ! PGDERIVE_S3_LATENCY_SCALE=0 python3 scripts/run_with_s3_proxy.py "$output/nexmark-ordered" \
    python3 scripts/nexmark_ordered_harness.py "$output/nexmark-ordered" -- "${worker[@]}"; then
    cat "$output/nexmark-ordered/harness.log" >&2
    exit 1
fi
if ! PGDERIVE_S3_LATENCY_SCALE=0 python3 scripts/run_with_s3_proxy.py "$output/nexmark-temporal" \
    python3 scripts/nexmark_temporal_harness.py "$output/nexmark-temporal" -- "${worker[@]}"; then
    cat "$output/nexmark-temporal/harness.log" >&2
    exit 1
fi
if ! PGDERIVE_S3_LATENCY_SCALE=0 python3 scripts/run_with_s3_proxy.py "$output/nexmark-q3" \
    python3 scripts/nexmark_q3_harness.py "$output/nexmark-q3" -- "${worker[@]}"; then
    cat "$output/nexmark-q3/harness.log" >&2
    exit 1
fi
if ! PGDERIVE_S3_LATENCY_SCALE=0 python3 scripts/run_with_s3_proxy.py "$output/nexmark-calendar" \
    python3 scripts/nexmark_calendar_harness.py "$output/nexmark-calendar" -- "${worker[@]}"; then
    cat "$output/nexmark-calendar/harness.log" >&2
    exit 1
fi
if ! PGDERIVE_S3_LATENCY_SCALE=0 python3 scripts/run_with_s3_proxy.py "$output/nexmark-hot" \
    python3 scripts/nexmark_hot_harness.py "$output/nexmark-hot" -- "${worker[@]}"; then
    cat "$output/nexmark-hot/harness.log" >&2
    exit 1
fi
if ! PGDERIVE_S3_LATENCY_SCALE=0 python3 scripts/run_with_s3_proxy.py "$output/group-membership" \
    python3 scripts/group_membership_harness.py "$output/group-membership" -- "${worker[@]}"; then
    cat "$output/group-membership/harness.log" >&2
    exit 1
fi
if ! PGDERIVE_S3_LATENCY_SCALE=0 python3 scripts/run_with_s3_proxy.py "$output/nexmark-q8" \
    python3 scripts/nexmark_q8_harness.py "$output/nexmark-q8" -- "${worker[@]}"; then
    cat "$output/nexmark-q8/harness.log" >&2
    exit 1
fi
if ! PGDERIVE_S3_LATENCY_SCALE=0 python3 scripts/run_with_s3_proxy.py "$output/nexmark-q13" \
    python3 scripts/nexmark_q13_harness.py "$output/nexmark-q13" -- "${worker[@]}"; then
    cat "$output/nexmark-q13/harness.log" >&2
    exit 1
fi
if ! PGDERIVE_S3_LATENCY_SCALE=0 python3 scripts/run_with_s3_proxy.py "$output/nexmark-q14" \
    python3 scripts/nexmark_q14_harness.py "$output/nexmark-q14" -- "${worker[@]}"; then
    cat "$output/nexmark-q14/harness.log" >&2
    exit 1
fi
if ! PGDERIVE_S3_LATENCY_SCALE=0 python3 scripts/run_with_s3_proxy.py "$output/nexmark-q21" \
    python3 scripts/nexmark_q21_harness.py "$output/nexmark-q21" -- "${worker[@]}"; then
    cat "$output/nexmark-q21/harness.log" >&2
    exit 1
fi
for qualification in query-toast query-ordered-state query-expression query-expression-calendar query-native-join query-topk circuit-numeric; do
    mkdir "$output/$qualification"
    if ! PGDERIVE_S3_LATENCY_SCALE=0 python3 scripts/run_with_s3_proxy.py "$output/$qualification" \
        python3 "scripts/${qualification//-/_}_harness.py" "$output/$qualification" -- "${worker[@]}"; then
        cat "$output/$qualification/harness.log" >&2
        exit 1
    fi
done
mkdir "$output/query-storage-soak"
if ! PGDERIVE_S3_LATENCY_SCALE=0 python3 scripts/run_with_s3_proxy.py "$output/query-storage-soak" \
    python3 scripts/query_storage_soak_harness.py --duration 1 --transactions 6 --rows 256 "$output/query-storage-soak" -- "${worker[@]}"; then
    cat "$output/query-storage-soak/harness.log" >&2
    exit 1
fi
mkdir "$output/native-cross"
if ! PGDERIVE_S3_LATENCY_SCALE=0 python3 scripts/run_with_s3_proxy.py "$output/native-cross" \
    python3 scripts/native_cross_harness.py "$output/native-cross" -- "${worker[@]}"; then
    cat "$output/native-cross/harness.log" >&2
    exit 1
fi
mkdir "$output/nexmark-q6"
if ! PGDERIVE_S3_LATENCY_SCALE=0 python3 scripts/run_with_s3_proxy.py "$output/nexmark-q6" \
    python3 scripts/nexmark_q6_harness.py "$output/nexmark-q6" -- "${worker[@]}"; then
    cat "$output/nexmark-q6/harness.log" >&2
    exit 1
fi
mkdir "$output/query-lifecycle"
if ! PGDERIVE_S3_LATENCY_SCALE=0 python3 scripts/run_with_s3_proxy.py "$output/query-lifecycle" \
    python3 scripts/query_lifecycle_harness.py "$output/query-lifecycle" -- "${admin[@]}"; then
    cat "$output/query-lifecycle/harness.log" >&2
    exit 1
fi
mkdir "$output/query-removal"
if ! PGDERIVE_S3_LATENCY_SCALE=0 python3 scripts/run_with_s3_proxy.py "$output/query-removal" \
    python3 scripts/query_removal_harness.py "$output/query-removal" -- "${admin[@]}"; then
    cat "$output/query-removal/harness.log" >&2
    exit 1
fi
for qualification in query-rebuild query-rebuild-limits query-rebuild-fault; do
    mkdir "$output/$qualification"
    if ! PGDERIVE_S3_LATENCY_SCALE=0 python3 scripts/run_with_s3_proxy.py "$output/$qualification" \
        python3 "scripts/${qualification//-/_}_harness.py" "$output/$qualification" -- "${admin[@]}"; then
        cat "$output/$qualification/harness.log" >&2
        exit 1
    fi
done
for qualification in query-health query-health-failure query-health-slot query-storage-tls query-postgres-tls query-recovery; do
    mkdir "$output/$qualification"
    if ! PGDERIVE_S3_LATENCY_SCALE=0 python3 scripts/run_with_s3_proxy.py "$output/$qualification" \
        python3 "scripts/${qualification//-/_}_harness.py" "$output/$qualification" -- "${admin[@]}"; then
        cat "$output/$qualification/harness.log" >&2
        exit 1
    fi
done
./scripts/package_release.sh "$output/package"
container_tag="pgderive-qualification:gate-${BASHPID}"
python3 scripts/build_container.py "$output/package" "$output/container-image" --tag "$container_tag"
python3 scripts/query_container_harness.py "$output/query-container" --image "$container_tag" \
    --image-manifest "$output/container-image/manifest.json" --duration-seconds 35 --interval-seconds 1
mkdir "$output/query-capacity"
if ! PGDERIVE_S3_LATENCY_SCALE=0 python3 scripts/run_with_s3_proxy.py "$output/query-capacity" \
    python3 scripts/query_capacity_harness.py "$output/query-capacity" -- "$output/package/bin/pgderive"; then
    cat "$output/query-capacity/harness.log" >&2
    exit 1
fi
python3 - "$output" "${worker[0]}" "${admin[0]}" <<'PY'
import hashlib,json,os,pathlib,stat,subprocess,sys
output=pathlib.Path(sys.argv[1])
results=list(output.rglob('result.json'))
assert len(results)==202,results
removals=list((output/'query-removal').glob('*/qualification.json'))
assert len(removals)==11,removals
rebuild_faults=list((output/'query-rebuild-fault').glob('*/qualification.json'))
assert len(rebuild_faults)==12,rebuild_faults
health_failures=list((output/'query-health-failure').glob('*/qualification.json'))
assert len(health_failures)==6,health_failures
for result in health_failures:
    json.loads(result.read_text())
for name in ('query-rebuild','query-rebuild-limits','query-health','query-health-slot','query-storage-tls','query-postgres-tls','query-recovery','query-container','query-capacity','query-storage-soak'):
    json.loads((output/name/'qualification.json').read_text())
for fault in rebuild_faults:
    json.loads(fault.read_text())
for removal in removals:
    json.loads(removal.read_text())
for result in results:
    json.loads(result.read_text())
root=pathlib.Path.cwd()
private_credential=output/'query-container/secrets/s3.json'
credential_info=private_credential.stat()
assert stat.S_ISREG(credential_info.st_mode) and stat.S_IMODE(credential_info.st_mode)==0o600
assert credential_info.st_uid==10001
files=[root/'Cargo.lock',root/'Cargo.toml',root/'worker.example.toml',pathlib.Path(sys.argv[2]),pathlib.Path(sys.argv[3]),pathlib.Path(sys.argv[3]).with_name('postgres_tls_control_probe'),
       *sorted((root/'src').rglob('*.rs')), *sorted((root/'vendor/pgwire-replication/src').rglob('*.rs')),
       *sorted((root/'docs').glob('*.md')), root/'.dockerignore', *sorted((root/'deploy').glob('*')), output/'package/bin/pgderive',
       *sorted((root/'scripts').glob('*.sh')), *sorted((root/'scripts').glob('*.py')), *sorted((root/'queries').rglob('*.sql')), *sorted(path for path in output.rglob('*.json') if path != private_credential),
       *sorted(output.rglob('*.jsonl')), *sorted(output.rglob('*.toml')), *sorted(output.rglob('*.log'))]
provenance={'private_inputs':{str(private_credential.relative_to(root)):{'uid':credential_info.st_uid,'gid':credential_info.st_gid,'mode':'0600','bytes':credential_info.st_size,'scope':'ownership/mode attestation only; private credential contents remain unreadable to the host qualifier'}},
            'git_head':subprocess.check_output(['git','rev-parse','HEAD'],text=True).strip(),
            'rust':subprocess.check_output(['rustc','--version'],text=True).strip(),
            'postgres':subprocess.check_output(['psql','-h','127.0.0.1','-p',os.environ.get('PGDERIVE_TEST_POSTGRES_PORT','55434'),'-U','postgres','-d','pgderive_dev','-Atc','SELECT version()'],text=True,env={**os.environ,"PGPASSWORD":os.environ.get("PGPASSWORD","postgres")}).strip(),
            'sha256':{str(path.relative_to(root)):hashlib.sha256(path.read_bytes()).hexdigest() for path in files}}
(output/'provenance.json').write_text(json.dumps(provenance,indent=2)+'\n')
print('Worker qualification passed: six COMMIT faults, producer burst, restart and five witnessed process kills.')
PY
