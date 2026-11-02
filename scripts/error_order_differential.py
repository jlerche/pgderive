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
INSERT INTO source.auction VALUES (1, NULL, 0);
INSERT INTO source.bid VALUES (1, 1, 1);
ANALYZE source.auction;
ANALYZE source.bid;
'''


def raises(pg, query):
    result = subprocess.run(pg, input=f'{SETUP}{query};\nROLLBACK;\n', capture_output=True, text=True)
    if result.returncode == 0:
        return False
    assert 'division by zero' in result.stderr, f'{query}: {result.stderr}'
    return True


def main():
    pg = sys.argv[1:]
    corpus = json.loads(pathlib.Path(__file__).with_name('error_order_corpus.json').read_text())
    different = [case['sql'] for case in corpus if raises(pg, case['sql']) != case.get('raises')]
    assert not different, f'corpus differs from PostgreSQL: {different}'
    print(f'{len(corpus)} expression-error cases match PostgreSQL')


if __name__ == '__main__':
    main()
