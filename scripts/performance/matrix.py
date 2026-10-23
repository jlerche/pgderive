#!/usr/bin/env python3
"""Run catalog scenarios repeatedly, retaining a separate evidence directory each time."""
import argparse
import json
import pathlib
import subprocess
import sys
from common import SCHEMA_VERSION


def main():
    root = pathlib.Path(__file__).resolve().parent
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--baseline', required=True)
    parser.add_argument('--candidate', required=True)
    parser.add_argument('--output', type=pathlib.Path, required=True)
    parser.add_argument('--scenarios', nargs='+', default=['smoke'])
    parser.add_argument('--repeats', type=int, default=3)
    parser.add_argument('--risingwave', action='store_true')
    args = parser.parse_args()
    specification = json.loads((root / 'scenarios.json').read_text())
    if specification['schema_version'] != SCHEMA_VERSION:
        parser.error('Scenario catalog must use current evidence schema')
    catalog = specification['scenarios']
    if args.repeats < 1 or any(name not in catalog for name in args.scenarios):
        parser.error('Positive repeats and valid catalog scenario names required')
    if args.output.exists():
        parser.error('Output directory must be new')
    args.output.mkdir(parents=True)
    for name in args.scenarios:
        for repeat in range(args.repeats):
            command = [sys.executable, str(root / 'run.py'), '--baseline', args.baseline,
                       '--candidate', args.candidate, '--output', str(args.output / f'{name}-{repeat + 1}')]
            if args.risingwave:
                command.append('--risingwave')
            for key, value in catalog[name].items():
                command.extend(['--' + key.replace('_', '-'), str(value)])
            subprocess.run(command, check=True)


if __name__ == '__main__':
    main()
