#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")/.."
python3 -m unittest discover -s scripts -p 'test_*.py'
python3 scripts/check_file_length.py
cargo fmt --all -- --check
cargo test --locked --manifest-path vendor/pgwire-replication/Cargo.toml --lib protocol::framing
cargo clippy --locked --all-targets --all-features -- -D warnings
cargo test --locked --doc
./scripts/check_coverage.sh
cargo machete
