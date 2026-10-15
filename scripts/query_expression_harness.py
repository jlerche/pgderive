#!/usr/bin/env python3
"""Owned exact native-MV/raw-row expression histories and cold restart."""
import collections
import json
from native_mv_oracle import expected
from sql_compiler_harness import SqlFixture
from worker_harness import Worker, harness_arguments


class ExpressionFixture(SqlFixture):
    def __init__(self, output, grouped):
        super().__init__(output, False)
        self.grouped = grouped
        if grouped:
            self.query = f'''SELECT x.key,SUM(x.value) AS total FROM
                (SELECT COALESCE(price,1)::bigint AS key,
                CASE WHEN price IS NULL THEN 0 ELSE price*2 END AS value
                FROM {self.name}.bid WHERE COALESCE(price,0)+1>0) x GROUP BY x.key'''
            self.labels = ['key', 'total']
        else:
            self.query = f'''SELECT COALESCE(price,1)::bigint*2+auction AS value
                FROM {self.name}.bid WHERE COALESCE(price,0)+1>0'''
            self.labels = ['value']

    def verify(self):
        native = expected(self, self.labels)
        difference = self.bag_difference(native)
        assert difference.strip() == '0', 'expression results differ from native MV'
        rows = json.loads(self.sql(f"SELECT COALESCE(json_agg(b),'[]') FROM {self.name}.bid b"))
        oracle = collections.Counter()
        groups = collections.defaultdict(int)
        for row in rows:
            price = row['price']
            if (price if price is not None else 0)+1 <= 0:
                continue
            key = price if price is not None else 1
            if self.grouped:
                groups[key] += 0 if price is None else price*2
            else:
                oracle[(None if row['auction'] is None else key*2+row['auction'],)] += 1
        if self.grouped:
            oracle = collections.Counter({(key,total):1 for key,total in groups.items()})
        actual = json.loads(self.sql(f"SELECT COALESCE(json_agg(g),'[]') FROM {self.name}.groups g"))
        assert {tuple(row['tuple'][1]):row['weight'] for row in actual} == dict(oracle), (actual,oracle)
        assert self.acknowledgement_ahead().strip() == '0'


def qualify(output, command, grouped):
    fixture = ExpressionFixture(output, grouped)
    with fixture.managed():
        fixture.rejected(command, 'unsupported-function', fixture.query.replace('COALESCE(price,1)', 'random()'))
        fixture.rejected(command, 'constant-coalesce-overflow', f'SELECT COALESCE(price,32768::smallint) AS value FROM {fixture.name}.bid')
        fixture.rejected(command, 'constant-case-overflow', f'SELECT CASE WHEN price>0 THEN price ELSE 32768::smallint END AS value FROM {fixture.name}.bid')
        assert fixture.sql(f"SELECT to_regclass('{fixture.name}.pgderive_worker_registration') IS NULL").strip() == 't'
        assert fixture.sql(f"SELECT count(*) FROM pg_replication_slots WHERE slot_name LIKE '{fixture.name}%'").strip() == '0'
        worker = fixture.start(command, 'first')
        ready = worker.event('ready')
        fixture.verify()
        changes = [
            f'INSERT INTO {fixture.name}.bid VALUES(100,1,NULL),(101,NULL,7),(102,1,7)',
            f'UPDATE {fixture.name}.bid SET price=9 WHERE id=100; UPDATE {fixture.name}.bid SET price=NULL WHERE id=101',
            f'UPDATE {fixture.name}.bid SET price=-1 WHERE id=102; DELETE FROM {fixture.name}.bid WHERE id=100',
            f'UPDATE {fixture.name}.bid SET price=0 WHERE id=101; INSERT INTO {fixture.name}.bid VALUES(103,2,0)',
            f'DELETE FROM {fixture.name}.bid WHERE id IN(101,102,103)',
        ]
        for index, change in enumerate(changes,1):
            fixture.sql('BEGIN;'+change+';COMMIT')
            worker.event('published',minimum_time=ready['time']+index)
            fixture.verify()
        worker.abort()
        prior = fixture.sql(f'SELECT row_to_json(p) FROM {fixture.name}.pgderive_progress p')
        fixture.rejected(command,'changed-expression',fixture.query.replace('price*2','price*3').replace('bigint*2','bigint*3'))
        assert prior == fixture.sql(f'SELECT row_to_json(p) FROM {fixture.name}.pgderive_progress p')
        resumed = fixture.start(command,'resumed',maximum=1)
        reopened = resumed.event('ready')
        assert reopened['slot']==ready['slot'] and reopened['time']==ready['time']+len(changes)
        fixture.verify()
        fixture.sql(f'INSERT INTO {fixture.name}.bid VALUES(104,NULL,NULL)')
        resumed.event('published')
        resumed.finish()
        fixture.verify()
        (output/'result.json').write_text(json.dumps({'native_mv':'identical SQL exact bag','memory':'independent integer/source-row oracle','cold_restart':True,'grouped':grouped,'transactions':len(changes)+1})+'\n')


def overflow(output, command, grouped):
    fixture = ExpressionFixture(output, grouped)
    fixture.query = (f'SELECT price::smallint AS key,COUNT(*) AS n FROM {fixture.name}.bid GROUP BY price::smallint'
                     if grouped else f'SELECT price::smallint AS value FROM {fixture.name}.bid')
    with fixture.managed():
        config = fixture.config()
        config.write_text(config.read_text().replace('retry_attempts=8','retry_attempts=1'))
        logs = output/'first'
        logs.mkdir()
        worker = Worker(command,config,logs,fixture.environment)
        fixture.workers.append(worker)
        ready = worker.event('ready')
        before = fixture.sql(f'SELECT row_to_json(p) FROM {fixture.name}.pgderive_progress p')
        slot = ready['slot']
        acknowledged = fixture.sql(f"SELECT confirmed_flush_lsn FROM pg_replication_slots WHERE slot_name='{slot}'")
        sink = fixture.sql(f'SELECT json_agg(g ORDER BY tuple) FROM {fixture.name}.groups g')
        fixture.sql(f'UPDATE {fixture.name}.bid SET price=32768 WHERE id=1')
        worker.process.wait(timeout=60)
        assert worker.process.returncode != 0
        worker.abort()
        assert 'overflow' in (logs/'stderr.log').read_text() or 'out of range' in (logs/'stderr.log').read_text()
        assert fixture.sql(f'SELECT row_to_json(p) FROM {fixture.name}.pgderive_progress p') == before
        assert fixture.sql(f"SELECT confirmed_flush_lsn FROM pg_replication_slots WHERE slot_name='{slot}'") == acknowledged
        assert fixture.sql(f'SELECT json_agg(g ORDER BY tuple) FROM {fixture.name}.groups g') == sink
        (output/'result.json').write_text(json.dumps({'narrowing_overflow':True,'grouped':grouped,'unchanged_progress_sink_and_ack':True})+'\n')


if __name__ == '__main__':
    output, command = harness_arguments()
    for grouped in [False,True]:
        directory=output/('grouped' if grouped else 'terminal')
        directory.mkdir()
        qualify(directory,command,grouped)

    for grouped in [False,True]:
        directory=output/('overflow-grouped' if grouped else 'overflow-terminal')
        directory.mkdir()
        overflow(directory,command,grouped)
