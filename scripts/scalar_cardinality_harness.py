#!/usr/bin/env python3
"""Check nonaggregate scalar cardinality, CDC publication, and repair."""
from collections import Counter
import json
import subprocess

from native_mv_oracle import binary_difference, expected
from sql_compiler_harness import SqlFixture
from worker_harness import History, blocked_recovery, run_modes

CARDINALITY_ERROR = 'more than one row returned by a subquery used as an expression'


class ScalarFixture(SqlFixture):
    labels = ['id', 'value']

    def __init__(self, output, mode):
        super().__init__(output, False)
        self.mode = mode
        self.outer = self.name + '.auction'
        self.inner = self.name + '.bid'
        self.sql(f'''DELETE FROM {self.outer}; DELETE FROM {self.inner};
            INSERT INTO {self.outer}(id,group_id) VALUES(1,1),(2,2),(3,3),(4,NULL);
            INSERT INTO {self.inner} VALUES(1,1,7),(2,2,NULL),(3,NULL,11),
                (4,99,5),(5,99,5);''')
        predicate = 'b.auction=a.group_id' if mode == 'correlated' else 'b.auction=1'
        self.query = f'''SELECT a.id,(SELECT b.price FROM {self.inner} b WHERE {predicate}) AS value
            FROM {self.outer} a WHERE a.id<90'''

    def verify(self):
        self.verify_sql(expected(self, self.labels))
        assert binary_difference(self) == '0'
        outer = json.loads(self.sql(f"SELECT json_agg(a) FROM {self.outer} a WHERE id<90"))
        inner = json.loads(self.sql(f"SELECT COALESCE(json_agg(b),'[]') FROM {self.inner} b"))
        memory = Counter()
        for row in outer:
            key = row['group_id'] if self.mode == 'correlated' else 1
            values = [item['price'] for item in inner if key is not None and item['auction'] == key]
            assert len(values) <= 1
            memory[(row['id'], values[0] if values else None)] += 1
        self.verify_memory(memory)

    def native_error(self):
        result = subprocess.run(self.pg + [self.query], env=self.environment, capture_output=True, text=True)
        (self.output/'native-error.txt').write_text(result.stderr)
        assert result.returncode and CARDINALITY_ERROR in result.stderr


def qualify(output, command, mode):
    fixture = ScalarFixture(output, mode)
    a, b = fixture.outer, fixture.inner
    with fixture.managed():
        fixture.run_history(command, History(
            changes=[
                f'DELETE FROM {b} WHERE id=1',
                f'INSERT INTO {b} VALUES(10,1,NULL); UPDATE {a} SET group_id=1 WHERE id=3',
                f'UPDATE {b} SET price=9 WHERE id=10; UPDATE {a} SET group_id=2 WHERE id=2',
                f'DELETE FROM {b} WHERE id=10; INSERT INTO {b} VALUES(11,1,9)',
            ],
            final=f'UPDATE {a} SET group_id=NULL WHERE id=3',
            normalized=fixture.query.replace('SELECT', 'select'),
            rejected=[],
        ))
        worker = fixture.start(command, 'cardinality-error')
        worker.event('ready')
        fixture.sql(f'INSERT INTO {b} VALUES(99,1,9)')
        fixture.native_error()
        assert CARDINALITY_ERROR in blocked_recovery(fixture, worker, f'DELETE FROM {b} WHERE id=99')
        fixture.verify()
        assert fixture.acknowledgement_ahead().strip() == '0'
        (output/'result.json').write_text(json.dumps({
            'mode': mode, 'cold_restart': True, 'native_bag_and_binary': True,
            'independent_oracle': True, 'duplicate_error_and_recovery': True,
        })+'\n')


if __name__ == '__main__':
    run_modes(('uncorrelated', 'correlated'), qualify)
