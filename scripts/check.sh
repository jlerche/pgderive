#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")/.."
cargo fmt --all -- --check
cargo clippy --locked --all-targets --all-features -- -D warnings
cargo machete services/api
./scripts/check_control_plane.sh
cd engine
exec ./scripts/check.sh "$@"
