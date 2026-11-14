"""Verify the named core regression set; retain evidence without a release claim."""
import hashlib
import json
import pathlib
import subprocess
import sys

LIVE_COUNTS = {'memtable': 1, 'checkpoint': 1, 'result-table': 2, 'operation-budget': 1,
               'transactions': 7, 'crashes': 5, 'query-lifecycle': 1, 'projection-compiler': 2, 'postgres-support': 1,
               'quantified-subqueries': 34, 'residual-subqueries': 12}


def successful(report, fields):
    if not isinstance(report,dict) or any(report.get(field) is not True for field in fields):
        raise ValueError('Missing successful live qualification: '+repr(fields))


def qualifications(output):
    def read(name):
        return json.loads((output/name).read_text())
    successful(read('query-rebuild/qualification.json'),['rebuild','memory_oracle','cold_restart'])
    successful(read('tenant-storage/qualification.json'),
               ['passed','tenant_isolation','cross_database_uploader','healthy_queries_progressed','cold_reopen_equal'])
    commits=read('tenant-storage-commit/qualification.json')
    if len(commits)!=2 or {r['fault'] for r in commits}!={'before','after'}:
        raise ValueError('Missing manifest COMMIT fault coverage')
    for report in commits:
        successful(report,['passed','oracle_equal','cold_reopen_equal','ack_safe'])
    queries=read('multi-query/comparison.json')
    if len(queries)!=1 or queries[0].get('queries')!=2:
        raise ValueError('Missing multi-query live coverage')
    successful(queries[0],['passed'])


def verify(output, counts=LIVE_COUNTS):
    expected = set()
    for name, count in counts.items():
        results = list((output/name).rglob('result.json'))
        if len(results) != count:
            raise ValueError(f'{name}: expected {count} results, found {len(results)}')
        for result in results:
            if not isinstance(json.loads(result.read_text()), dict):
                raise ValueError(f'invalid result evidence: {result}')
        expected.update(results)
    if set(output.rglob('result.json')) != expected:
        raise ValueError('unexpected core result evidence')
    if not isinstance(json.loads((output/'query-rebuild/qualification.json').read_text()), dict):
        raise ValueError('invalid rebuild evidence')
    if counts is LIVE_COUNTS:
        qualifications(output)
    return len(expected)


def main(output, binaries):
    count = verify(output)
    root = pathlib.Path.cwd()
    files = [root/'Cargo.lock', root/'Cargo.toml', root/'docs/postgres-support-matrix.json', *map(pathlib.Path, binaries),
             *sorted((root/'src').rglob('*.rs')), *sorted((root/'src').rglob('*.sql')),
             *sorted((root/'scripts').glob('*.py')), *sorted((root/'scripts').glob('*.json')), *sorted((root/'scripts').glob('*.sh')),
             *sorted((root/'queries').rglob('*.sql')),
             *sorted(output.rglob('*.json')), *sorted(output.rglob('*.jsonl'))]
    summary = {'profile': 'core', 'result_count': count,
               'scope': 'focused live CDC/durability/recovery; broad semantics use virtual-clock replay; full release qualification requires --full',
               'git_head': subprocess.check_output(['git','rev-parse','HEAD'],text=True).strip(),
               'sha256': {str(path.relative_to(root)): hashlib.sha256(path.read_bytes()).hexdigest() for path in files}}
    (output/'provenance.json').write_text(json.dumps(summary,indent=2)+'\n')
    print(f'Core worker regression passed: {count} focused results plus rebuild. Full release qualification not run.')


if __name__ == '__main__':
    main(pathlib.Path(sys.argv[1]),sys.argv[2:])
