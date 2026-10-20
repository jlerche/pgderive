#!/usr/bin/env python3
"""Prior-release column-only membership checkpoints reopen without a rebuild."""
import collections
import json
import pathlib
import sys
from sql_compiler_harness import SqlFixture
from native_mv_oracle import expected, binary_difference


class CompatibilityFixture(SqlFixture):
    def __init__(self, output, existence):
        super().__init__(output,False)
        self.existence = existence
        self.labels = ['g']
        self.query = (f'SELECT a.id AS g FROM {self.name}.auction a WHERE EXISTS(SELECT 1 FROM {self.name}.bid b WHERE a.id=b.auction)'
                      if existence else f'SELECT DISTINCT auction AS g FROM {self.name}.bid')

    def verify(self):
        native = expected(self,self.labels)
        assert self.bag_difference(native).strip()=='0'
        assert binary_difference(self)=='0'
        bids = json.loads(self.sql(f"SELECT coalesce(json_agg(auction),'[]') FROM {self.name}.bid"))
        keys = set(bids)
        if self.existence:
            keys = {value for value in json.loads(self.sql(f"SELECT coalesce(json_agg(id),'[]') FROM {self.name}.auction")) if value in keys}
        rows = json.loads(self.sql(f"SELECT coalesce(json_agg(g),'[]') FROM {self.name}.groups"))
        assert collections.Counter(rows)==collections.Counter({value:1 for value in keys})


def main():
    output,before,after = pathlib.Path(sys.argv[1]),sys.argv[2],sys.argv[3]
    for existence in (False,True):
        path = output/('exists' if existence else 'distinct');path.mkdir()
        fixture = CompatibilityFixture(path,existence)
        with fixture.managed():
            old = fixture.start([before],'before')
            boundary = old.event('ready')
            fixture.verify()
            fixture.sql(f'INSERT INTO {fixture.name}.bid VALUES(999,1,NULL)')
            old.event('published')
            fixture.verify()
            old.finish(graceful=True)
            binding = fixture.sql(f'SELECT binding FROM {fixture.name}.pgderive_progress')
            current = fixture.start([after],'after')
            reopened = current.event('ready')
            assert reopened['time']==boundary['time']+1 and reopened['slot']==boundary['slot']
            assert fixture.sql(f'SELECT binding FROM {fixture.name}.pgderive_progress')==binding
            fixture.verify()
            fixture.sql(f'DELETE FROM {fixture.name}.bid WHERE auction=1')
            current.event('published')
            fixture.verify()
            current.finish(graceful=True)
            (path/'result.json').write_text(json.dumps({'prior_release':'f58e076','existence':existence,
                'reopen_without_rebuild':True,'same_binding_and_slot':True,'native_binary_and_memory':True})+'\n')


if __name__=='__main__': main()
