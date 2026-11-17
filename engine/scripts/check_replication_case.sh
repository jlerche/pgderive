#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")/.."
export PGPASSWORD=${PGPASSWORD:-postgres}
: "${PGDERIVE_TEST_POSTGRES_PORT:?replication case requires an owned PostgreSQL port}"
export PGDERIVE__POSTGRES__PORT="$PGDERIVE_TEST_POSTGRES_PORT"
# Every gate owns fresh names; interrupted fixtures remain available as evidence.
export PGDERIVE__REPLICATION__SLOT="pgderive_gate_${BASHPID}_slot"
export PGDERIVE__REPLICATION__PUBLICATION="pgderive_gate_${BASHPID}_pub"
pg=(psql -h 127.0.0.1 -p "$PGDERIVE_TEST_POSTGRES_PORT" -U postgres -d postgres -v ON_ERROR_STOP=1)
"${pg[@]}" -Atc 'SELECT 1' >/dev/null
exists=$("${pg[@]}" -Atc "SELECT 1 FROM pg_database WHERE datname='pgderive_dev'")
if [[ "$exists" != 1 ]]; then
    "${pg[@]}" -c 'CREATE DATABASE pgderive_dev'
fi

scenario=$1
output=$2
if [[ "$scenario" == protocol ]]; then
    protocol=$(python3 -c 'import json,os;print(json.load(open(os.environ["PGDERIVE_GATE_BUILD"]))["protocol_tests"])')
    # Invoke the instrumented test executable directly: no rebuild or profile reset
    # while other independent live lanes are writing their coverage counters.
    "$protocol" --exact storage::tenant::tests::tenant_storage_manifest_live --ignored --nocapture --test-threads=1 | tee "$output/protocol.log"
    python3 scripts/gate_build.py protocol "$output/protocol.log"
    exit 0
fi
harness=("$(python3 -c 'import json,os;print(json.load(open(os.environ["PGDERIVE_GATE_BUILD"]))["replication"])')" "$scenario" config.example.toml)
install -m 600 s3.example.json "$output/s3.json"
export PGDERIVE__OBJECT_STORE__CREDENTIALS_FILE="$output/s3.json"
if PGDERIVE__POSTGRES__PASSWORD="$PGPASSWORD" python3 scripts/run_with_s3_proxy.py "$output" "${harness[@]}"; then
    cat "$output/harness.log"
else
    result=$?
    cat "$output/harness.log" >&2
    exit "$result"
fi
python3 scripts/verify_replication.py "$scenario" "$output"
