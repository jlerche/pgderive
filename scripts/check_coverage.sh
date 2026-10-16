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
cargo llvm-cov clean --workspace
cargo llvm-cov --locked --all-targets --all-features --no-report
PGDERIVE_COVERAGE=1 ./scripts/check_integration.sh
PGDERIVE_COVERAGE=1 ./scripts/check_worker.sh
cargo llvm-cov report --ignore-filename-regex '/tests(/|\.rs$)' --json --output-path "$output/coverage.json"
cargo llvm-cov report --ignore-filename-regex '/tests(/|\.rs$)' --fail-under-lines 80 | tee "$output/summary.txt"
