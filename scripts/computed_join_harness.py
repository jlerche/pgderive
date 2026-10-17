#!/usr/bin/env python3
"""Side-local computed equijoins, with independent source bags and native MV."""
import collections
import json
from sql_compiler_harness import SqlFixture
from worker_harness import harness_arguments, History
MODES = ('inner', 'swapped', 'composite', 'function', 'derived', 'scoped')


class JoinKeysFixture(SqlFixture):
    def __init__(self, output, mode):
        super().__init__(output, False)
        self.mode = mode
        self.sql(f"UPDATE {self.name}.auction SET group_id=CASE WHEN id=1 THEN 1 WHEN id=2 THEN -1 WHEN id=3 THEN NULL ELSE id%3 END,category=CASE WHEN id IN(1,2) THEN 'collision' ELSE category END; UPDATE {self.name}.bid SET auction=CASE WHEN id%5=0 THEN NULL WHEN id%2=0 THEN -1 ELSE 1 END,price=CASE WHEN id%3=0 THEN NULL ELSE 2 END")
        left, right = f'{self.name}.auction', f'{self.name}.bid'
        if mode in ('derived', 'scoped'):
            left = f'(SELECT group_id,SUM(id) AS total FROM {left} GROUP BY group_id)'
        if mode == 'scoped':
            right = f'(SELECT auction,COUNT(abs(price)) AS n FROM {right} GROUP BY auction)'
        a = 'length(a.category)' if mode == 'function' else 'abs(a.group_id)'
        condition = f'{a}=abs(b.auction)'
        if mode == 'swapped':
            condition = f'abs(b.auction)={a}'
        if mode == 'composite':
            condition += ' AND (a.id+1)=(b.id+1)'
        values = ('a.total' if mode in ('derived', 'scoped') else 'a.category', 'b.n' if mode == 'scoped' else 'b.price')
        self.query = f'SELECT {values[0]} AS c,{values[1]} AS p FROM {left} a JOIN {right} b ON {condition}'
        self.sql(f'CREATE MATERIALIZED VIEW {self.name}.oracle AS {self.query}')

    def memory(self):
        left = json.loads(self.sql(f'SELECT json_agg(a) FROM {self.name}.auction a')) or []
        right = json.loads(self.sql(f'SELECT json_agg(b) FROM {self.name}.bid b')) or []
        if self.mode in ('derived', 'scoped'):
            groups = collections.defaultdict(int)
            for row in left:
                groups[row['group_id']] += row['id']
            left = [{'group_id': key, 'total': value} for key, value in groups.items()]
        if self.mode == 'scoped':
            groups = collections.defaultdict(int)
            for row in right:
                groups[row['auction']] += row['price'] is not None
            right = [{'auction': key, 'n': value} for key, value in groups.items()]
        expected = collections.Counter()
        for a in left:
            value = a.get('category') if self.mode == 'function' else a['group_id']
            key = len(value) if self.mode == 'function' and value is not None else abs(value) if value is not None else None
            for b in right:
                if key is None or b['auction'] is None or key != abs(b['auction']):
                    continue
                if self.mode == 'composite' and a['id'] != b['id']:
                    continue
                expected[(a['total'] if self.mode in ('derived', 'scoped') else a['category'], b['n'] if self.mode == 'scoped' else b['price'])] += 1
        return expected

    def verify(self):
        self.sql(f'REFRESH MATERIALIZED VIEW {self.name}.oracle')
        expected = f'SELECT jsonb_build_array(NULL,jsonb_build_array(c,p)) tuple,count(*)::bigint weight FROM {self.name}.oracle GROUP BY 1'
        assert self.bag_difference(expected).strip() == '0', 'computed join differs from native MV'
        self.verify_memory(dict(self.memory()))


def qualify(output, command, mode):
    fixture = JoinKeysFixture(output, mode)
    with fixture.managed():
        if mode == 'inner':
            for label, condition in [('mixed', 'a.id+b.id=b.auction'), ('same-side','abs(a.id)=abs(a.group_id)'), ('constant','1=abs(b.auction)'), ('numeric','abs(a.id)::numeric=b.price::numeric')]:
                fixture.rejected(command, label, fixture.query.split(' ON ')[0]+' ON '+condition)
            assert fixture.sql(f"SELECT count(*) FROM pg_replication_slots WHERE slot_name LIKE '{fixture.name}%'").strip() == '0'
        changes = [
            f"UPDATE {fixture.name}.auction SET group_id=2,category='moved' WHERE id=1; UPDATE {fixture.name}.bid SET auction=2 WHERE id=2",
            f"INSERT INTO {fixture.name}.auction(id,category,group_id) VALUES(100,'collision',-1); INSERT INTO {fixture.name}.bid VALUES(100,-1,2),(101,NULL,NULL)",
            f"UPDATE {fixture.name}.auction SET group_id=1,category='x' WHERE id=3; UPDATE {fixture.name}.bid SET auction=1 WHERE id=101",
            f"DELETE FROM {fixture.name}.auction WHERE id=2; DELETE FROM {fixture.name}.bid WHERE id=100",
            f"UPDATE {fixture.name}.auction SET group_id=NULL WHERE id=100; UPDATE {fixture.name}.bid SET auction=NULL WHERE id=2",
        ]
        fixture.run_history(command, History(
            changes=changes,
            final=f"UPDATE {fixture.name}.auction SET category='xx',group_id=-1 WHERE id=100; DELETE FROM {fixture.name}.bid WHERE id=101",
            normalized=fixture.query,
            rejected=[],
        ))
        (output/'result.json').write_text(json.dumps({'mode':mode,'sql_oracle':'native MV exact bag',
            'memory_oracle':'independent raw-source nested loop/groups','simultaneous_changes':True,
            'identity':'duplicates, NULL keys, retractions, key moves','restart':'cold, same slot'})+'\n')


if __name__ == '__main__':
    output, command = harness_arguments()
    for mode in MODES:
        directory = output/mode
        directory.mkdir()
        qualify(directory,command,mode)
