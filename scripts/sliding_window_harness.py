#!/usr/bin/env python3
"""Eligible sliding windows retain native-MV, source-model and restart semantics."""
import collections
import json
from expression_composition_harness import CompositionFixture, qualify
from worker_harness import run_cases


class SlidingFixture(CompositionFixture):
    def __init__(self, output, mode):
        base = mode.split('-i')[0]
        super().__init__(output,base)
        if mode.endswith('-i16') or mode.endswith('-i32'):
            kind = 'smallint' if mode.endswith('-i16') else 'integer'
            self.sql(f'ALTER TABLE {self.name}.entry ALTER COLUMN amount TYPE {kind}')
        if base=='window-rows':
            self.query = self.query.split(',MIN(')[0]+f' FROM {self.name}.entry'
            self.labels = ['id','v','n','mean']
        (output/'sliding-query.sql').write_text(self.query+'\n')
        saved = json.loads((output/'case.json').read_text())
        saved.update(mode=mode,sql=self.query,source_amount_type=('smallint' if mode.endswith('-i16') else 'integer' if mode.endswith('-i32') else 'bigint'))
        (output/'case.json').write_text(json.dumps(saved,indent=2)+'\n')

    def memory(self):
        values = super().memory()
        if self.mode!='window-rows': return values
        result = collections.Counter()
        for row,weight in values.items(): result[row[:4]] += weight
        return result


if __name__ == '__main__':
    run_cases(((mode, mode, SlidingFixture) for mode in
               ('window-rows','window-rows-i16','window-rows-i32','window-peers','window-empty')), qualify)
