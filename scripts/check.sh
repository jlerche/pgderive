#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")/.."
case "$#:${1:-}" in
    0:) export PGDERIVE_CHECK_PROFILE=core ;;
    1:--full) export PGDERIVE_CHECK_PROFILE=full ;;
    1:--help) printf 'Usage: ./scripts/check.sh [--full]\nDefault: core live regression and >=80%% coverage. --full: complete release qualification.\n'; exit 0 ;;
    *) printf 'Usage: ./scripts/check.sh [--full]\n' >&2; exit 2 ;;
esac
printf 'Check profile: %s\n' "$PGDERIVE_CHECK_PROFILE"
python3 -m unittest discover -s scripts -p 'test_*.py'
python3 scripts/check_file_length.py
cargo fmt --all -- --check
cargo clippy --locked --all-targets --all-features -- -D warnings
cargo test --locked --doc
./scripts/check_coverage.sh
cargo machete
