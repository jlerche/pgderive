#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")/.."
profile=${PGDERIVE_CHECK_PROFILE:-full}
case "$profile" in
    core|full) ;;
    *) printf 'Invalid check profile: %s\n' "$profile" >&2; exit 2 ;;
esac
if [[ -z ${PGDERIVE_GATE_BUILD:-} ]]; then
    mkdir -p artifacts
    build=$(mktemp -d "$PWD/artifacts/gate-build-XXXXXXXX")
    ./scripts/build_gate.sh "$build"
    export PGDERIVE_GATE_BUILD="$build/manifest.json"
fi
if [[ ${PGDERIVE_COVERAGE:-0} == 1 ]]; then
    source "$(dirname "$PGDERIVE_GATE_BUILD")/coverage.env"
fi
export PGDERIVE_GATE_REPLICATION=${PGDERIVE_GATE_REPLICATION:-1}
mkdir -p artifacts/worker
output=$(mktemp -d "$PWD/artifacts/worker/run-XXXXXXXX")
printf 'Worker evidence: %s\n' "$output"
exec python3 scripts/live_gate.py "$output" "$PGDERIVE_GATE_BUILD" "$profile"
