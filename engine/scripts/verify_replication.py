"""Verify the evidence from one isolated replication scenario."""
import hashlib,json,os,pathlib,subprocess,sys
root=pathlib.Path.cwd()
scenario = sys.argv[1]
out=pathlib.Path(sys.argv[2])
transactions=[json.loads(line) for line in (out/'transactions.jsonl').read_text().splitlines()]
requests=[json.loads(line) for line in (out/'proxy.jsonl').read_text().splitlines()]
assert any(row.get('method')=='PUT' for row in requests)
if scenario == 'stream':
    assert any(row.get('range') for row in requests)
harness_log=(out/'harness.log').read_text()
if scenario == 'stream':
    assert 'MVP project/join/group/sum passed SQL+memory at tick 16' in harness_log
    for tick,epoch in [(6,7),(16,17)]:
        assert f'MVP durable manifest recovered all registered arrangements at tick {tick} epoch {epoch}' in harness_log
    assert harness_log.count('expected cold-recovery failure for ')==2
    assert 'MVP incomplete and corrupt catalog membership rejected without epoch change' in harness_log
    assert 'MVP unmanaged catalog roots rejected before recovery or GC' in harness_log
    assert 'MVP unmanaged publication roots rejected without progress change' in harness_log
    assert harness_log.count('MVP atomic sink/membership/source publication passed at tick ')==15
    assert 'MVP failed destination DML and fenced writers preserved sink/membership/progress' in harness_log
    assert 'MVP suppressed destination DML rejected without progress change' in harness_log
    assert 'MVP batched sink native hash collisions, arrays, large values, decode aliases, underflow rollback and missing-index rejection verified' in harness_log
    assert 'MVP uncertain BEFORE COMMIT resolved as NotCommitted; blind retry rejected' in harness_log
    assert 'MVP uncertain AFTER COMMIT resolved as Committed; blind retry rejected' in harness_log
    assert 'MVP resumed from durable tick 16; received rows remained unacknowledged until atomic tick 17 publication' in harness_log
    assert 'MVP fresh-process recovered atomic tick 17 with exact durable source position' in harness_log
    assert 'MVP cancelled pending COMMIT blocked acknowledgement; reconciliation waited for original transaction and recovered committed state' in harness_log
    assert [len(tx['changes']) for tx in transactions]==[12,12,5,5,2,1,2,3,3,2,4,3,2,2,2]
    assert [len(tx['batch']['updates']) for tx in transactions]==[12,24,5,0,4,2,4,6,5,2,4,4,2,2,2]
    held = [json.loads(line) for line in (out/'sql-commit-hold.jsonl').read_text().splitlines()][1:]
    assert [row['server_committed'] for row in held] == [False, True]
else:
    assert not transactions
if scenario == 'snapshot':
    assert 'MVP full publication and maintenance rejected shared boundary mutations without sink/progress/ACK changes' in harness_log
    assert 'MVP placement error domains: local CASE/COALESCE/casts matched PostgreSQL; filter-crossing counterexample retained' in harness_log
    assert 'MVP source schema drift blocked publication and preserved durable boundary' in harness_log
    assert 'MVP snapshot uncertain BEFORE COMMIT resolved from authoritative registration' in harness_log
    assert 'MVP snapshot uncertain AFTER COMMIT resolved from authoritative registration' in harness_log
    assert 'MVP externally advanced and recreated source slots rejected before replication starts' in harness_log
    assert 'MVP snapshot quoted-name/boolean/null/native codecs and bounded-copy/RLS rejection verified' in harness_log
    assert 'MVP exported snapshot plus concurrent insert/update/delete CDC reconstructed exact source state; native schema drift rejected' in harness_log
if scenario == 'maintenance':
    assert 'MVP maintenance uncertain BEFORE COMMIT resolved without logical/source advance' in harness_log
    assert 'MVP maintenance uncertain AFTER COMMIT resolved without logical/source advance' in harness_log
    assert 'MVP durable streaming compaction preserved source/sink/logical state and pinned reader' in harness_log
if scenario == 'collection':
    assert 'MVP GC retained committed and pinned state; fenced abandoned uploads reclaimed without source/logical advance' in harness_log
if scenario == 'paged':
    assert 'MVP incremental GC bounded pages, cursor rollback, fresh-cache resume and unknown retention verified' in harness_log
if scenario == 'cohort':
    assert 'MVP GC retirement excludes late quiescence and mid-pass upload closure' in harness_log
for mode,committed in [('before',False),('after',True)]:
    faults = [json.loads(line) for line in (out/f'sql-commit-{mode}.jsonl').read_text().splitlines()][1:]
    expected = int(scenario in ('stream', 'snapshot', 'maintenance'))
    assert len(faults) == expected and all(row['server_committed'] is committed for row in faults)
binary=pathlib.Path(json.loads(pathlib.Path(os.environ['PGDERIVE_GATE_BUILD']).read_text())['replication'])
files=[root/'Cargo.lock',root/'config.example.toml',root/'scripts/publication_mutations.json',*sorted((root/'src').rglob('*.rs')),*sorted((root/'src').rglob('*.sql')),binary,out/'transactions.jsonl',out/'harness.log',out/'proxy.jsonl',out/'storage-profile.json',out/'sql-commit-before.jsonl',out/'sql-commit-after.jsonl',out/'sql-commit-hold.jsonl']
provenance={
    'postgres':subprocess.check_output(['psql','-h','127.0.0.1','-p',os.environ['PGDERIVE_TEST_POSTGRES_PORT'],'-U','postgres','-d','postgres','-Atc','SELECT version()'],text=True).strip(),
    'rust':subprocess.check_output(['rustc','--version'],text=True).strip(),
    'scenario': scenario, 'transactions':len(transactions), 'changes':sum(len(tx['changes']) for tx in transactions),
    'sha256':{str(p.relative_to(root)):hashlib.sha256(p.read_bytes()).hexdigest() for p in files},
}
(out/'provenance.json').write_text(json.dumps(provenance,indent=2)+'\n')
