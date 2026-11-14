#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")/.."
mkdir -p artifacts/control-plane
evidence=$(mktemp -d "$PWD/artifacts/control-plane/run-XXXXXXXX")
printf 'Control plane evidence: %s\n' "$evidence"

export CARGO_TARGET_DIR="$PWD/target/control-plane-coverage"
cargo llvm-cov clean --profraw-only
source <(cargo llvm-cov show-env --sh)
cargo build --locked -p pgderive-api

cat > "$evidence/compose.yaml" <<YAML
services:
  envoy:
    ports: !override
      - "127.0.0.1::8080"
  api:
    volumes:
      - "$CARGO_TARGET_DIR/debug/pgderive-api:/usr/local/bin/pgderive-api:ro"
      - "$evidence/profiles:/coverage"
    environment:
      LLVM_PROFILE_FILE: /coverage/api-%p.profraw
YAML

# The coverage build writes as the API container's unprivileged UID.
mkdir "$evidence/profiles"
chmod 1777 "$evidence/profiles"
compose=(docker compose -p "pgderive-control-plane-check-$$" -f compose.yaml -f "$evidence/compose.yaml")
cleanup() {
    "${compose[@]}" logs --no-color > "$evidence/compose.log" || true
    "${compose[@]}" down > "$evidence/cleanup.log" 2>&1
}
trap cleanup EXIT
"${compose[@]}" up -d --no-deps envoy
address=$("${compose[@]}" port envoy 8080)
export PGDERIVE_HTTP_PORT="${address##*:}"
"${compose[@]}" up --build -d
for ((attempt = 0; attempt < 30; attempt++)); do
    if curl --silent --fail --output /dev/null "http://localhost:$PGDERIVE_HTTP_PORT/api/auth/ok"; then
        break
    fi
    sleep 1
done
python3 scripts/check_control_plane.py
"${compose[@]}" stop api
cp "$evidence"/profiles/*.profraw "$CARGO_TARGET_DIR/"
cargo llvm-cov report --fail-under-lines 80 | tee "$evidence/coverage.txt"
