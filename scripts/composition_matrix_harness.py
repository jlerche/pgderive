#!/usr/bin/env python3
"""Eight named SQL families, native binary/metadata oracle, replay and minimization."""
import argparse
import hashlib
import json
import os
import pathlib
import sys
from application_evidence import source_files
from application_portfolio_harness import ApplicationFixture, DifferentialFailure, run_case, admin
from composition_matrix_model import FAMILIES, expected
from composition_matrix_trace import generate, validate
from native_mv_oracle import binary_difference


class MatrixFixture(ApplicationFixture):
    def create_sources(self):
        super().create_sources()
        self.sql(f'CREATE TABLE {self.name}.region(id integer PRIMARY KEY,label text,cohort integer); ALTER TABLE {self.name}.region REPLICA IDENTITY FULL; ALTER PUBLICATION {self.name} ADD TABLE {self.name}.region')

    @property
    def output_labels(self):
        return self.query_specification['labels']

    @property
    def query_specification(self):
        return self.trace['matrix']

    def memory(self):
        return expected(self.trace['matrix'],self.model)

    def verify(self):
        bag = super().verify()
        difference = binary_difference(self)
        if difference!='0':
            raise DifferentialFailure('binary_representation',self.query)
        return bag


def inventory(fixture, command):
    records = []
    manifest = json.loads(pathlib.Path('queries/composition/matrix.json').read_text())
    for item in manifest['rejected']:
        sql = item['sql'].format(schema=fixture.name)
        fixture.sql('EXPLAIN '+sql)
        result = admin(fixture,command,sql)
        records.append({**item,'native_valid':True,'returncode':result.returncode,'stderr':result.stderr})
        (fixture.output/'rejections.json').write_text(json.dumps(records,indent=2)+'\n')
        assert result.returncode!=0 and item['diagnostic'] in result.stderr,records[-1]

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('output',type=pathlib.Path)
    parser.add_argument('--replay',type=pathlib.Path)
    parser.add_argument('--family',choices=FAMILIES)
    parser.add_argument('--seed',type=int)
    parser.add_argument('--full',action='store_true')
    parser.add_argument('--completed-root',type=pathlib.Path,action='append',default=[])
    split = sys.argv.index('--')
    args = parser.parse_args(sys.argv[1:split])
    command = sys.argv[split+1:]
    assert command
    traces = []
    if args.replay:
        traces = [json.loads(args.replay.read_text())]
    else:
        full = args.full or os.environ.get('PGDERIVE_CHECK_PROFILE')=='full'
        for ordinal,family in enumerate(FAMILIES):
            if args.family and args.family!=family:
                continue
            seeds = [args.seed] if args.seed is not None else range(10000,10010) if full else [10000+ordinal]
            for seed in seeds:
                trace = generate(family,seed)
                if not full and args.seed is None:
                    steps = trace['history']
                    committed = [step for step in steps if not step.get('restart') and not step['rollback']]
                    rollback = next(step for step in steps if step.get('rollback'))
                    trace['history'] = committed[:3]+[rollback,{'restart':True}]+committed[3:7]
                traces.append(trace)
    if args.completed_root:
        from verify_composition_matrix import case
        complete = []
        binaries = {str(path):hashlib.sha256(path.read_bytes()).hexdigest()
                    for item in command if (path:=pathlib.Path(item)).is_file()}
        assert binaries,'resume requires a directly identified executable'
        sources = {str(path):hashlib.sha256(path.read_bytes()).hexdigest() for path in source_files()}
        for root in args.completed_root:
            for file in root.glob('*/result.json'):
                prior = json.loads((file.parent/'provenance.json').read_text())
                if prior['binary_sha256']!=binaries or prior['source_sha256']!=sources:
                    continue
                saved,result = case(file.parent)
                if result['transactions']==30:
                    complete.append(saved)
        traces = [trace for trace in traces if not any(saved['id']==trace['id'] and saved['seed']==trace['seed']
                  and saved['matrix']['sql']==trace['matrix']['sql'] and saved['initial']==trace['initial']
                  and saved['history']==trace['history'] for saved in complete)]
    if not args.replay and traces:
        inspection = args.output/'inventory'
        inspection.mkdir()
        fixture = MatrixFixture(inspection,traces[0])
        with fixture.managed():
            inventory(fixture,command)
    records = []
    for trace in traces:
        label = trace['id']+'-'+str(trace['seed'])
        result = run_case(args.output,command,trace,label,fixture_factory=MatrixFixture,validator=validate)
        records.append({'family':trace['id'],'seed':trace['seed'],
                        'transactions':result['transactions'],'restart':result['restart'],
                        'oracles':'native binary bag + output metadata + independent source model'})
        (args.output/'matrix.json').write_text(json.dumps(records,indent=2)+'\n')


if __name__=='__main__':
    main()
