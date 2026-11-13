#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")/.."
output=$1
mkdir -p "$output"
if [[ ${PGDERIVE_COVERAGE:-0} == 1 ]]; then
    export CARGO_TARGET_DIR="$PWD/target/llvm-cov-target"
    cargo llvm-cov show-env --sh > "$output/coverage.env"
    source "$output/coverage.env"
fi
# --all-targets unifies dev-dependency features for tests and actual executables.
cargo build --locked --all-targets --all-features --message-format=json > "$output/cargo.jsonl"
python3 scripts/gate_build.py build "$output/cargo.jsonl" "$output/manifest.json"
