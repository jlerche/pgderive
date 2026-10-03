#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")/.."
python3 -m unittest discover -s scripts -p 'test_*.py'
python3 scripts/check_file_length.py
cargo fmt --all -- --check
cargo clippy --locked --all-targets --all-features -- -D warnings
./scripts/check_coverage.sh
cargo machete
