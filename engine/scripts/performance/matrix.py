#!/usr/bin/env python3
"""Run catalog scenarios repeatedly, retaining a separate evidence directory each time."""
import argparse
import json
import pathlib
import subprocess
import sys
from common import DEFAULT_WARMUP_WINDOWS, DEFAULT_WINDOW_SECONDS, SCHEMA_VERSION, growth_intervals



def validate_catalog(specification):
    profiles = specification['qualification_profiles']
    catalog = specification['scenarios']
    if not profiles or len(profiles) != len(set(profiles)) or any(name not in catalog for name in profiles):
        raise ValueError('Qualification profiles must be unique known catalog scenarios')
    for name in profiles:
        parameters = catalog[name]
        duration = parameters['duration']
        width = parameters.get('window_seconds', DEFAULT_WINDOW_SECONDS)
        warmup = parameters.get('warmup_windows', DEFAULT_WARMUP_WINDOWS)
        try:
            growth_intervals(duration, width, warmup)
        except ValueError as error:
            raise ValueError(name + ': ' + str(error)) from error


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
    try:
        validate_catalog(specification)
    except ValueError as error:
        parser.error(str(error))
    catalog = specification['scenarios']
    if args.repeats < 1 or any(name not in catalog for name in args.scenarios):
        parser.error('Positive repeats and valid catalog scenario names required')
    if args.output.exists():
        parser.error('Output directory must be new')
    args.output.mkdir(parents=True)
    for name in args.scenarios:
        for repeat in range(args.repeats):
            command = [sys.executable, str(root / 'run.py'), '--baseline', args.baseline,
                       '--candidate', args.candidate, '--output', str(args.output / f'{name}-{repeat + 1}'),
                       '--execution-mode', 'sequential', '--pair-order',
                       'baseline-first' if repeat % 2 == 0 else 'candidate-first']
            if args.risingwave:
                command.append('--risingwave')
            for key, value in catalog[name].items():
                command.extend(['--' + key.replace('_', '-'), str(value)])
            subprocess.run(command, check=True)


if __name__ == '__main__':
    main()
