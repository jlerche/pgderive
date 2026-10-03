#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")/.."
mkdir -p artifacts/coverage
output=$(mktemp -d "$PWD/artifacts/coverage/run-XXXXXXXX")
printf 'Coverage evidence: %s\n' "$output"
exec > >(tee "$output/gate.log") 2>&1
cargo llvm-cov clean --workspace
cargo llvm-cov --locked --all-targets --all-features --no-report
PGDERIVE_COVERAGE=1 ./scripts/check_integration.sh
cargo llvm-cov report --ignore-filename-regex '/tests(/|\.rs$)' --json --output-path "$output/coverage.json"
cargo llvm-cov report --ignore-filename-regex '/tests(/|\.rs$)' --fail-under-lines 80 | tee "$output/summary.txt"
