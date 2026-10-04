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
harness_log=(out/'harness.log').read_text()
assert 'MVP project/join/group/sum passed SQL+memory at tick 15' in harness_log
for tick,epoch in [(5,1),(15,2)]:
    assert f'MVP durable manifest recovered all four arrangements at tick {tick} epoch {epoch}' in harness_log
assert harness_log.count('expected cold-recovery failure for ')==2
assert 'MVP incomplete and corrupt catalog membership rejected without epoch change' in harness_log
assert harness_log.count('MVP atomic sink/membership/source publication passed at tick ')==15
assert 'MVP failed destination DML and fenced writers preserved sink/membership/progress' in harness_log
assert 'MVP suppressed destination DML rejected without progress change' in harness_log
assert 'MVP uncertain BEFORE COMMIT resolved as NotCommitted; blind retry rejected' in harness_log
assert 'MVP uncertain AFTER COMMIT resolved as Committed; blind retry rejected' in harness_log
assert 'MVP resumed from durable tick 15; received rows remained unacknowledged until atomic tick 16 publication' in harness_log
assert 'MVP fresh-process recovered atomic tick 16 with exact durable source position' in harness_log
assert 'MVP cancelled pending COMMIT blocked acknowledgement; reconciliation waited for original transaction and recovered committed state' in harness_log
held=[json.loads(line) for line in (out/'sql-commit-hold.jsonl').read_text().splitlines()][1:]
assert [row['server_committed'] for row in held]==[False,True]
for mode,committed in [('before',False),('after',True)]:
    faults=[json.loads(line) for line in (out/f'sql-commit-{mode}.jsonl').read_text().splitlines()][1:]
    assert len(faults)==2 and all(row['server_committed'] is committed for row in faults)
assert 'MVP snapshot uncertain BEFORE COMMIT resolved from authoritative registration' in harness_log
assert 'MVP snapshot uncertain AFTER COMMIT resolved from authoritative registration' in harness_log
assert 'MVP externally advanced and recreated source slots rejected before replication starts' in harness_log
assert 'MVP snapshot quoted-name/boolean/null/native codecs and bounded-copy/RLS rejection verified' in harness_log
assert 'MVP source schema drift blocked publication and preserved durable boundary' in harness_log
assert 'MVP exported snapshot plus concurrent insert/update/delete CDC reconstructed exact source state; native schema drift rejected' in harness_log
assert 'MVP full-tuple bag multiplicity, cancellation and overflow rollback passed' in harness_log
assert [len(tx['changes']) for tx in transactions]==[12,12,5,5,2,1,2,3,3,2,4,3,2,2,2]
assert [len(tx['batch']['updates']) for tx in transactions]==[12,24,5,0,4,2,4,6,5,2,4,4,2,2,2]
binary=root/('target/llvm-cov-target/debug/replication_harness' if os.environ.get('PGDERIVE_COVERAGE')=='1' else 'target/debug/replication_harness')
files=[root/'Cargo.lock',root/'config.example.toml',*sorted((root/'src').rglob('*.rs')),binary,out/'transactions.jsonl',out/'harness.log',out/'proxy.jsonl',out/'storage-profile.json',out/'sql-commit-before.jsonl',out/'sql-commit-after.jsonl',out/'sql-commit-hold.jsonl']
provenance={
    'postgres':subprocess.check_output(['psql','-h','127.0.0.1','-p','55434','-U','postgres','-d','postgres','-Atc','SELECT version()'],text=True).strip(),
    'rust':subprocess.check_output(['rustc','--version'],text=True).strip(),
    'transactions':len(transactions), 'changes':sum(len(tx['changes']) for tx in transactions),
    'sha256':{str(p.relative_to(root)):hashlib.sha256(p.read_bytes()).hexdigest() for p in files},
}
(out/'provenance.json').write_text(json.dumps(provenance,indent=2)+'\n')
PY
