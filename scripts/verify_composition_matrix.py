#!/usr/bin/env python3
"""Verify the complete current matrix from retained histories and actual query evidence."""
import argparse
import hashlib
import json
import pathlib
from composition_matrix_model import FAMILIES, specification
from application_evidence import source_files
from composition_matrix_trace import validate


def case(path, repair=False):
    trace = json.loads((path/'replay.json').read_text())
    validate(trace)
    file = path/'result.json'
    result = json.loads(file.read_text())
    # Early development results inherited the base portfolio's recipe label. Preserve
    # that raw evidence and repair only this proven labeling error, never test outcomes.
    if result['query']!=trace['matrix']:
        assert repair and result['query']==trace['spec'],path
        original = file.read_bytes()
        (path/'result-before-label-correction.json').write_bytes(original)
        result['query'] = trace['matrix']
        result['label_correction'] = {'reason':'base portfolio recipe was mislabeled as matrix SQL',
                                    'original_sha256':hashlib.sha256(original).hexdigest()}
        file.write_text(json.dumps(result,indent=2)+'\n')
    assert result['query']==trace['matrix']
    assert result['transactions']==sum(not s.get('restart') and not s.get('rollback') for s in trace['history'])
    assert result['restart']
    witnesses = [json.loads(line) for line in (path/'native-mv-oracle.jsonl').read_text().splitlines()]
    assert witnesses
    schema = witnesses[0]['materialized_view'].split('.')[0].strip('"')
    actual = trace['matrix']['sql'].format(schema=schema)
    assert all(record['query_sql']==actual for record in witnesses),path
    return trace,result


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('roots',type=pathlib.Path,nargs='+')
    parser.add_argument('--repair-development-labels',action='store_true')
    parser.add_argument('--output',type=pathlib.Path,required=True)
    parser.add_argument('--worker',type=pathlib.Path)
    args = parser.parse_args()
    chosen, superseded = {}, []
    expected_binary = hashlib.sha256(args.worker.read_bytes()).hexdigest() if args.worker else None
    expected_sources = {str(path):hashlib.sha256(path.read_bytes()).hexdigest() for path in source_files()} if args.worker else None
    for root in args.roots:
        for file in sorted(root.glob('*/result.json')):
            trace,result = case(file.parent,args.repair_development_labels)
            provenance = json.loads((file.parent/'provenance.json').read_text())
            if expected_binary:
                assert expected_binary in provenance['binary_sha256'].values(),file
                assert provenance['source_sha256']==expected_sources,file
            spec = trace['matrix']
            if spec['sql']!=specification(trace['id'],trace['seed'])['sql']:
                superseded.append(str(file.parent))
                continue
            assert result['transactions']==30,file
            chosen[trace['id'],trace['seed']] = {'path':str(file.parent),'query':spec,
                'transactions':result['transactions'],'native_checks':len((file.parent/'native-mv-oracle.jsonl').read_text().splitlines()),
                'replay_sha256':hashlib.sha256((file.parent/'replay.json').read_bytes()).hexdigest(),
                'result_sha256':hashlib.sha256(file.read_bytes()).hexdigest()}
    expected = {(family,seed) for family in FAMILIES for seed in range(10000,10010)}
    assert set(chosen)==expected,sorted(expected-set(chosen))
    summary = {'version':2,'cases':list(chosen.values()),'case_count':len(chosen),
               'committed_transactions':sum(row['transactions'] for row in chosen.values()),
               'superseded_query_forms':superseded,'scope':'finite current SQL families; exact native binary bags/metadata and independent model per committed boundary'}
    args.output.write_text(json.dumps(summary,indent=2)+'\n')
    print('Current composition matrix: 80 cases, 2400 committed boundaries verified')


if __name__=='__main__': main()
