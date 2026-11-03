#!/usr/bin/env python3
"""Check the pinned expression-error corpus against PostgreSQL, in rolled-back transactions."""
import json
import pathlib
import subprocess
import sys

SETUP = '''BEGIN;
CREATE SCHEMA source;
CREATE TABLE source.auction(id int PRIMARY KEY, category text, other int);
CREATE TABLE source.bid(id int PRIMARY KEY, auction int, price int);
'''
# A merge join reads both inputs whole, as Pgderive's incremental join does; other methods
# can skip an input or probe an index, and Pgderive documents that difference.
FINISH = '''ANALYZE source.auction;
ANALYZE source.bid;
SET LOCAL enable_nestloop=off;
SET LOCAL enable_hashjoin=off;
'''


def insert(table, rows):
    if not rows:
        return ''
    values = ','.join('(' + ','.join('NULL' if v is None else str(v) for v in row) + ')' for row in rows)
    return f'INSERT INTO source.{table} VALUES {values};\n'


def outcome(pg, case):
    auction = case.get('auction', [[1, None, 0]])
    bid = case.get('bid', [[1, 1, 1]])
    script = f"{SETUP}{insert('auction', auction)}{insert('bid', bid)}{FINISH}{case['sql']};\nROLLBACK;\n"
    result = subprocess.run(pg, input=script, capture_output=True, text=True)
    if result.returncode == 0:
        return False, sorted(result.stdout.splitlines())
    assert 'division by zero' in result.stderr, f"{case['sql']}: {result.stderr}"
    return True, []


def main():
    pg = sys.argv[1:]
    path = pathlib.Path(__file__).with_name('error_order_corpus.json')
    corpus = json.loads(path.read_text())
    different = []
    for case in corpus:
        raised, rows = outcome(pg, case)
        if raised != case['raises'] or rows != case.get('rows', []):
            different.append((case, raised, rows))
    assert not different, f'corpus differs from PostgreSQL: {different}'
    print(f'{len(corpus)} expression-error cases match PostgreSQL')


if __name__ == '__main__':
    main()
