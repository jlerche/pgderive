#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")/.."
profile=${PGDERIVE_CHECK_PROFILE:-full}
case "$profile" in
    core|full) ;;
    *) printf 'Invalid check profile: %s\n' "$profile" >&2; exit 2 ;;
esac
if [[ -z ${PGDERIVE_GATE_BUILD:-} ]]; then
    mkdir -p artifacts
    build=$(mktemp -d "$PWD/artifacts/gate-build-XXXXXXXX")
    ./scripts/build_gate.sh "$build"
    export PGDERIVE_GATE_BUILD="$build/manifest.json"
fi
if [[ ${PGDERIVE_COVERAGE:-0} == 1 ]]; then
    source "$(dirname "$PGDERIVE_GATE_BUILD")/coverage.env"
fi
worker=("$(python3 -c 'import json,os;print(json.load(open(os.environ["PGDERIVE_GATE_BUILD"]))["worker"])')")
admin=("$(python3 -c 'import json,os;print(json.load(open(os.environ["PGDERIVE_GATE_BUILD"]))["admin"])')")
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
if [[ "$profile" == core ]]; then
    python3 scripts/live_gate.py "$output" "$PGDERIVE_GATE_BUILD"
    exit 0
fi
PGDERIVE_S3_LATENCY_SCALE=0 run_qualification result-table \
    python3 scripts/result_table_harness.py "$output/result-table" -- "${worker[@]}"
PGDERIVE_S3_LATENCY_SCALE=0 run_qualification query-binding \
    python3 scripts/query_binding_harness.py "$output/query-binding" -- "${worker[@]}"
PGDERIVE_S3_LATENCY_SCALE=0 run_qualification memtable \
    python3 scripts/memtable_harness.py "$output/memtable" -- "${worker[@]}"
PGDERIVE_S3_LATENCY_SCALE=0 run_qualification async-checkpoints \
    python3 scripts/async_checkpoint_harness.py "$output/async-checkpoints" -- "${worker[@]}"
PGDERIVE_S3_LATENCY_SCALE=0 run_qualification operation-budget \
    python3 scripts/operation_budget_harness.py "$output/operation-budget" -- "${worker[@]}"
PGDERIVE_S3_LATENCY_SCALE=0 run_qualification tenant-capacity \
    python3 scripts/multi_tenant_capacity_harness.py "$output/tenant-capacity" -- "${worker[@]}"
PGDERIVE_S3_LATENCY_SCALE=0 run_qualification tenant-storage \
    python3 scripts/tenant_storage_harness.py "$output/tenant-storage" -- "${worker[@]}"
PGDERIVE_S3_LATENCY_SCALE=0 run_qualification tenant-storage-commit \
    python3 scripts/tenant_storage_commit_harness.py "$output/tenant-storage-commit" -- "${worker[@]}"
PGDERIVE_S3_LATENCY_SCALE=0 run_qualification multi-query \
    python3 scripts/multi_query_memory_harness.py "$output/multi-query" --counts 2 --seconds 2 --mode shared -- "${worker[@]}"
PGDERIVE_S3_LATENCY_SCALE=0 run_qualification transactions \
    python3 scripts/worker_harness.py --faults --kill --ticks 4 --burst 8 \
    "$output/transactions" -- "${worker[@]}"
PGDERIVE_S3_LATENCY_SCALE="${PGDERIVE_S3_LATENCY_SCALE:-1}" run_qualification crashes python3 scripts/worker_crash_harness.py "$output/crashes" -- "${worker[@]}"
while read -r qualification script; do
    harness_timeout=${PGDERIVE_HARNESS_TIMEOUT_SECONDS:-1800}
    executable=("${worker[@]}")
    if [[ "$qualification" == query-concurrent ]]; then executable=("${admin[@]}"); fi
    PGDERIVE_HARNESS_TIMEOUT_SECONDS="$harness_timeout" PGDERIVE_S3_LATENCY_SCALE=0 run_qualification "$qualification" \
        python3 "scripts/${script}_harness.py" "$output/$qualification" -- "${executable[@]}"
done <<'QUALIFICATIONS'
compiler sql_compiler
sql-composition sql_composition
values values
set-operations set_operation
boolean-resolution boolean_resolution
temporal-resolution temporal_resolution
subqueries subquery
numeric-arithmetic numeric_arithmetic
temporal-arithmetic temporal_arithmetic
ordering ordering
ordering-domains ordering_domains
fetch-with-ties fetch_with_ties
distinct-on distinct_on
unordered unordered
query-concurrent query_concurrent
application application_portfolio
composition-matrix composition_matrix
sliding-extrema sliding_extrema
prefix-window prefix_window
expression-composition expression_composition
sliding-window sliding_window
keyed-sink keyed_sink
sink-batch sink_batch
stdout-sink stdout_sink
projection projection_compiler
star-projection star_projection
functions functions_compiler
postgres-support postgres_support
environment environment
computed-join computed_join
join-predicates join_predicate
outer-joins outer_join
natural-joins natural_join
aggregate-inputs aggregate_inputs_compiler
relational relational_compiler
partition partition_compiler
group-references group_references
boolean-aggregates boolean_aggregate
named-window named_window
multiple-windows multiple_window
numeric numeric_compiler
circuit-numeric circuit_numeric
native native_compiler
date-bin date_bin_compiler
hopping hopping_compiler
ranking ranking_compiler
ranking-distribution ranking_distribution
peer-frames peer_frames_compiler
integral-range integral_range
temporal-range temporal_range
frame-exclusion frame_exclusion
derived derived_compiler
navigation navigation_compiler
frame-values frame_value
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
for qualification in query-toast query-ordered-state query-expression query-expression-calendar query-native-join query-topk query-membership; do
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
assert len(results)==718,results
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
       *sorted((root/'src').rglob('*.rs')), *sorted((root/'src').rglob('*.sql')),
       *sorted((root/'docs').glob('*.md')), root/'docs/postgres-support-matrix.json', root/'.dockerignore', *sorted((root/'deploy').glob('*')), output/'package/bin/pgderive',
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
