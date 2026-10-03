#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")/.."
export PGPASSWORD=${PGPASSWORD:-postgres}
pg=(psql -h 127.0.0.1 -p 55434 -U postgres -d postgres -v ON_ERROR_STOP=1)
if ! "${pg[@]}" -Atc 'SELECT 1' >/dev/null 2>&1; then
    docker compose up -d --wait postgres
fi
exists=$("${pg[@]}" -Atc "SELECT 1 FROM pg_database WHERE datname='pgderive_dev'")
if [[ "$exists" != 1 ]]; then
    "${pg[@]}" -c 'CREATE DATABASE pgderive_dev'
fi
cargo build --locked --bins
mkdir -p artifacts/replication
output=$(mktemp -d "$PWD/artifacts/replication/run-XXXXXXXX")
printf 'Evidence: %s\n' "$output"
if PGDERIVE__POSTGRES__PASSWORD="$PGPASSWORD" timeout 90 ./target/debug/replication_harness config.example.toml >"$output/transactions.jsonl" 2>"$output/harness.log"; then
    cat "$output/harness.log"
else
    result=$?
    cat "$output/harness.log" >&2
    exit "$result"
fi
python3 - "$output" <<'PY'
import hashlib,json,pathlib,subprocess,sys
root=pathlib.Path.cwd()
out=pathlib.Path(sys.argv[1])
transactions=[json.loads(line) for line in (out/'transactions.jsonl').read_text().splitlines()]
assert [len(tx['changes']) for tx in transactions]==[12,11,5,5]
assert [len(tx['batch']['updates']) for tx in transactions]==[12,22,5,0]
files=[root/'Cargo.lock',root/'config.example.toml',*sorted((root/'src').rglob('*.rs')),root/'target/debug/replication_harness',out/'transactions.jsonl',out/'harness.log']
provenance={
    'postgres':subprocess.check_output(['psql','-h','127.0.0.1','-p','55434','-U','postgres','-d','postgres','-Atc','SELECT version()'],text=True).strip(),
    'rust':subprocess.check_output(['rustc','--version'],text=True).strip(),
    'transactions':len(transactions), 'changes':sum(len(tx['changes']) for tx in transactions),
    'sha256':{str(p.relative_to(root)):hashlib.sha256(p.read_bytes()).hexdigest() for p in files},
}
(out/'provenance.json').write_text(json.dumps(provenance,indent=2)+'\n')
PY
