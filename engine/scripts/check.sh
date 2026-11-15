#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")/.."
case "$#:${1:-}" in
    0:) export PGDERIVE_CHECK_PROFILE=core ;;
    1:--full) export PGDERIVE_CHECK_PROFILE=full ;;
    1:--help) printf 'Usage: ./scripts/check.sh [--full]\nDefault: virtual-clock semantics, focused live durability and >=80%% coverage. --full: complete release qualification.\n'; exit 0 ;;
    *) printf 'Usage: ./scripts/check.sh [--full]\n' >&2; exit 2 ;;
esac
gate_started=$SECONDS
trap 'gate_status=$?; printf "Gate elapsed: %ss (exit %s)\n" "$((SECONDS-gate_started))" "$gate_status"' EXIT
printf 'Check profile: %s\n' "$PGDERIVE_CHECK_PROFILE"
source scripts/check_parallel.sh

check_python() {
    python3 -m unittest discover -s scripts -p 'test_*.py'
}

check_file_length() {
    python3 scripts/check_file_length.py
}

check_rust() {
    cargo fmt --all -- --check
    cargo clippy --locked --all-targets --all-features -- -D warnings
    cargo test --locked --doc
}

check_coverage() {
    ./scripts/check_coverage.sh
}

check_dependencies() {
    cargo machete
}

run_parallel_checks check_python check_file_length check_rust check_coverage check_dependencies
