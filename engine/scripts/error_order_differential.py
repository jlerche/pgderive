#!/usr/bin/env python3
"""Check the pinned expression-error corpus against PostgreSQL, in rolled-back transactions."""
import json
import os
import re
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


def run_case(pg, case, planning=False):
    auction = case.get('auction', [[1, None, 0]])
    bid = case.get('bid', [[1, 1, 1]])
    query = ('EXPLAIN ' if planning else '') + case['sql']
    script = f"{SETUP}{insert('auction', auction)}{insert('bid', bid)}{FINISH}{query};\nROLLBACK;\n"
    result = subprocess.run(pg + ['-v', 'VERBOSITY=verbose'], input=script, capture_output=True, text=True)
    if result.returncode == 0:
        return dict(raises=False, rows=sorted(result.stdout.splitlines()), sqlstate=None)
    state = re.search(r'ERROR:\s+([0-9A-Z]{5}):', result.stderr)
    return dict(raises=True, rows=[], sqlstate=state[1] if state else None, diagnostic=result.stderr)


def outcome(pg, case):
    planned = run_case(pg, case, planning=True)
    if planned['raises']:
        return dict(planned, phase='planning')
    executed = run_case(pg, case)
    return dict(executed, phase='execution' if executed['raises'] else 'success')


def main():
    pg = sys.argv[1:]
    path = pathlib.Path(__file__).with_name('error_order_corpus.json')
    corpus = json.loads(path.read_text())
    evidence_path = os.environ.get('PGDERIVE_ERROR_ORDER_EVIDENCE')
    environment = subprocess.run(pg, input="SELECT json_build_object('version',version(),'settings',(SELECT json_object_agg(name,setting) FROM pg_settings));", capture_output=True, text=True, check=True).stdout
    evidence = []
    different = []
    for case in corpus:
        actual = outcome(pg, case)
        evidence.append(dict(case=case, source_rows=dict(auction=case.get('auction', [[1,None,0]]), bid=case.get('bid', [[1,1,1]])), outcome=actual))
        if (actual['raises'] != case['raises'] or actual['rows'] != case.get('rows', [])
                or ('phase' in case and actual['phase'] != case['phase'])
                or (case['raises'] and actual['sqlstate'] != case.get('sqlstate', '22012'))):
            different.append((case, actual))
    if evidence_path:
        path = pathlib.Path(evidence_path)
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(json.dumps(dict(environment=environment, planner_settings=FINISH, cases=evidence), ensure_ascii=False, indent=2) + '\n')
    assert not different, f'corpus differs from PostgreSQL: {different}'
    print(f'{len(corpus)} expression-error cases match PostgreSQL')


if __name__ == '__main__':
    main()
