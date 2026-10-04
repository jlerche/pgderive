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
if [[ ${PGDERIVE_COVERAGE:-0} == 1 ]]; then
    harness=(cargo llvm-cov run --locked --no-report --bin replication_harness -- config.example.toml)
else
    cargo build --locked --bins
    harness=(./target/debug/replication_harness config.example.toml)
fi
mkdir -p artifacts/replication
output=$(mktemp -d "$PWD/artifacts/replication/run-XXXXXXXX")
printf 'Evidence: %s\n' "$output"
if PGDERIVE__POSTGRES__PASSWORD="$PGPASSWORD" python3 scripts/run_with_s3_proxy.py "$output" "${harness[@]}"; then
    cat "$output/harness.log"
else
    result=$?
    cat "$output/harness.log" >&2
    exit "$result"
fi
python3 - "$output" <<'PY'
import hashlib,json,os,pathlib,subprocess,sys
root=pathlib.Path.cwd()
out=pathlib.Path(sys.argv[1])
transactions=[json.loads(line) for line in (out/'transactions.jsonl').read_text().splitlines()]
requests=[json.loads(line) for line in (out/'proxy.jsonl').read_text().splitlines()]
assert any(row.get('method')=='PUT' for row in requests)
assert any(row.get('range') for row in requests)
assert any('MVP project/join/group/sum passed SQL+memory at tick 15' in line for line in (out/'harness.log').read_text().splitlines())
assert [len(tx['changes']) for tx in transactions]==[12,12,5,5,2,1,2,3,3,2,4,3,2,2,2]
assert [len(tx['batch']['updates']) for tx in transactions]==[12,24,5,0,4,2,4,6,5,2,4,4,2,2,2]
binary=root/('target/llvm-cov-target/debug/replication_harness' if os.environ.get('PGDERIVE_COVERAGE')=='1' else 'target/debug/replication_harness')
files=[root/'Cargo.lock',root/'config.example.toml',*sorted((root/'src').rglob('*.rs')),binary,out/'transactions.jsonl',out/'harness.log',out/'proxy.jsonl',out/'storage-profile.json']
provenance={
    'postgres':subprocess.check_output(['psql','-h','127.0.0.1','-p','55434','-U','postgres','-d','postgres','-Atc','SELECT version()'],text=True).strip(),
    'rust':subprocess.check_output(['rustc','--version'],text=True).strip(),
    'transactions':len(transactions), 'changes':sum(len(tx['changes']) for tx in transactions),
    'sha256':{str(p.relative_to(root)):hashlib.sha256(p.read_bytes()).hexdigest() for p in files},
}
(out/'provenance.json').write_text(json.dumps(provenance,indent=2)+'\n')
PY
