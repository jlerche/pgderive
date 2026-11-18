"""Select disjoint sets of whole histories for live gate lanes."""
import os


def select_cases(cases):
    selection = os.environ.get('PGDERIVE_TEST_CASE_SHARD')
    if selection is None:
        return cases
    index, count = map(int, selection.split('/'))
    cases = list(cases)
    if not 0 <= index < count <= len(cases):
        raise ValueError(f'Invalid case shard {selection!r} for {len(cases)} cases')
    return cases[index::count]


def main():
    import importlib
    import sys

    from worker_harness import run_cases, run_modes

    module = importlib.import_module(sys.argv.pop(1))
    name = sys.argv.pop(1)
    cases = select_cases(getattr(module, name))
    run = run_cases if name == 'CASES' else run_modes
    run(cases, module.qualify)


if __name__ == '__main__':
    main()
