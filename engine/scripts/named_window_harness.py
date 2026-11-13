#!/usr/bin/env python3
"""Exercise named window inheritance and cold restart with an inline specification."""
from partition_compiler_harness import PartitionFixture, qualify
from worker_harness import run_cases


class NamedWindowFixture(PartitionFixture):
    def __init__(self, output, window, frame_spec):
        super().__init__(output, window, frame_spec)
        self.inline_query = self.query
        clause, _, _, descending = self.frame_spec
        direction = 'DESC NULLS FIRST' if descending else 'ASC NULLS LAST'
        order = f'ORDER BY b.price {direction},b.id'
        frame = f'ROWS BETWEEN {clause}'
        inline = f'OVER (PARTITION BY b.auction {order} {frame})'
        assert inline in self.inline_query
        self.query = (self.inline_query.replace(inline, 'OVER w')
                      + f' WINDOW base AS (PARTITION BY b.auction), w AS (base {order} {frame})')

    def run_history(self, command, history):
        history.normalized = self.inline_query.replace('b.', '"B".').replace('bid b', 'bid AS "B"')
        return super().run_history(command, history)


if __name__ == '__main__':
    run_cases((('rows', True),), qualify, fixture_class=NamedWindowFixture)
