"""Verify the named core regression set; retain evidence without a release claim."""
import hashlib
import json
import pathlib
import subprocess
import sys

COUNTS = {'memtable': 3, 'keyed-sink': 3, 'result-table': 2, 'query-binding': 18, 'transactions': 7, 'crashes': 5,
          'compiler': 4, 'application': 6, 'composition-matrix': 8, 'sliding-extrema': 6, 'prefix-window': 6, 'expression-composition': 12, 'sliding-window': 5, 'sink-batch': 2, 'stdout-sink': 1, 'projection': 2, 'functions': 1, 'aggregate-inputs': 6, 'environment': 1, 'computed-join': 6, 'relational': 2, 'partition': 7,
          'numeric': 4, 'native': 4, 'navigation': 5, 'case': 6,
          'query-lifecycle': 7}


def verify(output, counts=COUNTS):
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
    return len(expected)


def main(output, binaries):
    count = verify(output)
    root = pathlib.Path.cwd()
    files = [root/'Cargo.lock', root/'Cargo.toml', *map(pathlib.Path, binaries),
             *sorted((root/'src').rglob('*.rs')), *sorted((root/'src').rglob('*.sql')),
             *sorted((root/'scripts').glob('*.py')), *sorted((root/'scripts').glob('*.sh')),
             *sorted((root/'queries').rglob('*.sql')),
             *sorted(output.rglob('*.json')), *sorted(output.rglob('*.jsonl'))]
    summary = {'profile': 'core', 'result_count': count,
               'scope': 'live functional/durability regression; full release qualification requires --full',
               'git_head': subprocess.check_output(['git','rev-parse','HEAD'],text=True).strip(),
               'sha256': {str(path.relative_to(root)): hashlib.sha256(path.read_bytes()).hexdigest() for path in files}}
    (output/'provenance.json').write_text(json.dumps(summary,indent=2)+'\n')
    print(f'Core worker regression passed: {count} results plus rebuild. Full release qualification not run.')


if __name__ == '__main__':
    main(pathlib.Path(sys.argv[1]),sys.argv[2:])
