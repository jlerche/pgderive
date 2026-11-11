#!/usr/bin/env python3
"""NATURAL joins through existing native and independent outer-join oracles."""
from dataclasses import replace
import json

from outer_join_harness import OuterFixture, changes
from worker_harness import History, run_cases


class NaturalFixture(OuterFixture):
    def __init__(self, output, kind, shared):
        self.kind, self.shared = kind, shared
        super().__init__(output, 'full-using-composite' if shared else 'full-true')

    def statement(self):
        self.case = replace(self.case, kind=self.kind)
        labels, query = super().statement()
        a, b = self.name + '.auction', self.name + '.bid'
        left = f'(SELECT k,tk,category FROM {a}) a'
        right = f'(SELECT tk,k,price FROM {b}) b' if self.shared else f'(SELECT k AS rk,price FROM {b}) b'
        targets, _ = query.rsplit(' FROM ', 1)
        if not self.shared:
            targets = targets.replace('b.k AS bk', 'b.rk AS bk')
        self.explicit = f'{targets} FROM {left} {self.kind} JOIN {right}'
        self.explicit += ' USING(k,tk)' if self.shared else ' ON true'
        return labels, f'{targets} FROM {left} NATURAL {self.kind} JOIN {right}'


def qualify(output, command, kind, shared):
    fixture = NaturalFixture(output, kind, shared)
    with fixture.managed():
        fixture.run_history(command, History(
            changes=changes(fixture),
            final=f'DELETE FROM {fixture.name}.bid',
            normalized=fixture.explicit if shared else fixture.query.replace('SELECT', 'select'),
            rejected=[],
        ))
        (output/'result.json').write_text(json.dumps({
            'kind': kind, 'shared_columns': shared, 'native_send_and_metadata': True,
            'independent_nested_loop': True, 'empty_sides': True, 'cold_restart': True,
        })+'\n')


if __name__ == '__main__':
    run_cases(((f'{kind.lower()}-{"shared" if shared else "disjoint"}', kind, shared)
               for kind in ('INNER', 'LEFT', 'RIGHT', 'FULL') for shared in (True, False)), qualify)
