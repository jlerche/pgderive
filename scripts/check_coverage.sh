#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")/.."
profile=${PGDERIVE_CHECK_PROFILE:-full}
case "$profile" in
    core) export PGDERIVE_S3_LATENCY_SCALE=0 ;;
    full) ;;
    *) printf 'Invalid check profile: %s\n' "$profile" >&2; exit 2 ;;
esac
mkdir -p artifacts/coverage
output=$(mktemp -d "$PWD/artifacts/coverage/run-XXXXXXXX")
printf 'Coverage evidence: %s\n' "$output"
exec > >(tee "$output/gate.log") 2>&1
# Discard previous counters while retaining Cargo's instrumented build cache.
cargo llvm-cov clean --profraw-only
# Always execute the checked-in oracles, regardless of benchmark shell settings.
unset PGDERIVE_SEMANTICS_REPLAY PGDERIVE_FAST_REPLAY PGDERIVE_FAST_REPORT
export PGDERIVE_SEMANTICS_REPORT="$output/semantics.json"
cargo llvm-cov --locked --all-targets --all-features --no-report | tee "$output/unit-tests.log"
python3 scripts/check_semantics_report.py "$output/semantics.json" "$output/unit-tests.log"
PGDERIVE_COVERAGE=1 ./scripts/check_integration.sh
PGDERIVE_COVERAGE=1 ./scripts/check_worker.sh
cargo llvm-cov report --ignore-filename-regex '/tests(/|\.rs$)' --json --output-path "$output/coverage.json"
cargo llvm-cov report --ignore-filename-regex '/tests(/|\.rs$)' --fail-under-lines 80 | tee "$output/summary.txt"
