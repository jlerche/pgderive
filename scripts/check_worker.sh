#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")/.."
profile=${PGDERIVE_CHECK_PROFILE:-full}
case "$profile" in
    core|full) ;;
    *) printf 'Invalid check profile: %s\n' "$profile" >&2; exit 2 ;;
esac
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
export PGDERIVE__POSTGRES__PASSWORD=${PGPASSWORD:-postgres}
run_qualification() {
    local name=$1
    shift
    mkdir "$output/$name"
    if ! python3 scripts/run_with_s3_proxy.py "$output/$name" "$@"; then
        cat "$output/$name/harness.log" >&2
        exit 1
    fi
}
PGDERIVE_S3_LATENCY_SCALE=0 run_qualification result-table \
    python3 scripts/result_table_harness.py "$output/result-table" -- "${worker[@]}"
PGDERIVE_S3_LATENCY_SCALE=0 run_qualification query-binding \
    python3 scripts/query_binding_harness.py "$output/query-binding" -- "${worker[@]}"
PGDERIVE_S3_LATENCY_SCALE=0 run_qualification transactions \
    python3 scripts/worker_harness.py --faults --kill --ticks 4 --burst 8 \
    "$output/transactions" -- "${worker[@]}"
crash_scale=${PGDERIVE_S3_LATENCY_SCALE:-1}
if [[ "$profile" == core ]]; then crash_scale=0; fi
PGDERIVE_S3_LATENCY_SCALE="$crash_scale" run_qualification crashes python3 scripts/worker_crash_harness.py "$output/crashes" -- "${worker[@]}"
while read -r qualification script; do
    if [[ "$profile" == core ]]; then
        case "$qualification" in
            compiler|projection|functions|relational|partition|numeric|native|navigation|case) ;;
            *) continue ;;
        esac
    fi
    PGDERIVE_S3_LATENCY_SCALE=0 run_qualification "$qualification" \
        python3 "scripts/${script}_harness.py" "$output/$qualification" -- "${worker[@]}"
done <<'QUALIFICATIONS'
compiler sql_compiler
projection projection_compiler
functions functions_compiler
relational relational_compiler
partition partition_compiler
numeric numeric_compiler
native native_compiler
date-bin date_bin_compiler
hopping hopping_compiler
ranking ranking_compiler
peer-frames peer_frames_compiler
derived derived_compiler
navigation navigation_compiler
lookup lookup_compiler
composition composition_compiler
case case_compiler
session session_compiler
distinct distinct_compiler
nexmark-q15 nexmark_q15
join-composition join_composition
nexmark-auction nexmark_auction
nexmark-q22 nexmark_q22
nexmark-q1 nexmark_q1
nexmark-projection nexmark_projection
nexmark-q20 nexmark_q20
nexmark-ordered nexmark_ordered
nexmark-temporal nexmark_temporal
nexmark-q3 nexmark_q3
nexmark-calendar nexmark_calendar
nexmark-hot nexmark_hot
group-membership group_membership
nexmark-q8 nexmark_q8
nexmark-q13 nexmark_q13
nexmark-q14 nexmark_q14
nexmark-q21 nexmark_q21
QUALIFICATIONS
if [[ "$profile" == core ]]; then
    PGDERIVE_S3_LATENCY_SCALE=0 run_qualification query-lifecycle \
        python3 scripts/query_lifecycle_harness.py "$output/query-lifecycle" -- "${admin[@]}"
    PGDERIVE_S3_LATENCY_SCALE=0 run_qualification query-rebuild \
        python3 scripts/query_rebuild_harness.py "$output/query-rebuild" -- "${admin[@]}"
    python3 scripts/verify_worker_core.py "$output" "${worker[0]}" "${admin[0]}"
    exit 0
fi
for qualification in query-toast query-ordered-state query-expression query-expression-calendar query-native-join query-topk query-membership circuit-numeric; do
    PGDERIVE_S3_LATENCY_SCALE=0 run_qualification "$qualification" \
        python3 "scripts/${qualification//-/_}_harness.py" "$output/$qualification" -- "${worker[@]}"
done
PGDERIVE_S3_LATENCY_SCALE=0 run_qualification query-storage-soak \
    python3 scripts/query_storage_soak_harness.py --duration 1 --transactions 6 --rows 256 "$output/query-storage-soak" -- "${worker[@]}"
PGDERIVE_S3_LATENCY_SCALE=0 run_qualification native-cross \
    python3 scripts/native_cross_harness.py "$output/native-cross" -- "${worker[@]}"
PGDERIVE_S3_LATENCY_SCALE=0 run_qualification nexmark-q6 \
    python3 scripts/nexmark_q6_harness.py "$output/nexmark-q6" -- "${worker[@]}"
PGDERIVE_S3_LATENCY_SCALE=0 run_qualification query-lifecycle \
    python3 scripts/query_lifecycle_harness.py "$output/query-lifecycle" -- "${admin[@]}"
PGDERIVE_S3_LATENCY_SCALE=0 run_qualification query-removal \
    python3 scripts/query_removal_harness.py "$output/query-removal" -- "${admin[@]}"
for qualification in query-rebuild query-rebuild-limits query-rebuild-fault; do
    PGDERIVE_S3_LATENCY_SCALE=0 run_qualification "$qualification" \
        python3 "scripts/${qualification//-/_}_harness.py" "$output/$qualification" -- "${admin[@]}"
done
for qualification in query-health query-health-failure query-health-slot query-storage-tls query-postgres-tls query-recovery; do
    PGDERIVE_S3_LATENCY_SCALE=0 run_qualification "$qualification" \
        python3 "scripts/${qualification//-/_}_harness.py" "$output/$qualification" -- "${admin[@]}"
done
./scripts/package_release.sh "$output/package"
container_tag="pgderive-qualification:gate-${BASHPID}"
python3 scripts/build_container.py "$output/package" "$output/container-image" --tag "$container_tag"
python3 scripts/query_container_harness.py "$output/query-container" --image "$container_tag" \
    --image-manifest "$output/container-image/manifest.json" --duration-seconds 35 --interval-seconds 1
PGDERIVE_S3_LATENCY_SCALE=0 run_qualification query-capacity \
    python3 scripts/query_capacity_harness.py "$output/query-capacity" -- "$output/package/bin/pgderive"
python3 - "$output" "${worker[0]}" "${admin[0]}" <<'PY'
import hashlib,json,os,pathlib,stat,subprocess,sys
output=pathlib.Path(sys.argv[1])
results=list(output.rglob('result.json'))
assert len(results)==227,results
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
       *sorted((root/'src').rglob('*.rs')), *sorted((root/'src').rglob('*.sql')), *sorted((root/'vendor/pgwire-replication/src').rglob('*.rs')),
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
