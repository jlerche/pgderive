#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")/.."
source engine/scripts/check_parallel.sh

check_api() {
    cargo fmt --all -- --check
    cargo clippy --locked --all-targets --all-features -- -D warnings
    cargo machete services/api
}

check_auth() {
    ./scripts/check_control_plane.sh
}

if [[ "$#:${1:-}" == 1:--help ]]; then
    exec ./engine/scripts/check.sh --help
fi
engine_args=("$@")
check_engine() { ./engine/scripts/check.sh "${engine_args[@]}"; }
run_parallel_checks check_api check_auth check_engine
