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
