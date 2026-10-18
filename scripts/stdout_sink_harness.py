#!/usr/bin/env python3
"""Diagnostic SQL batches replay exactly, without an engine-owned destination."""
import json
import pathlib
from result_table_harness import NumericFixture
from worker_harness import harness_arguments
from harness_control import admin


class StdoutFixture(NumericFixture):
    def config(self, maximum=0):
        path = super().config(maximum)
        path.write_text(path.read_text().replace(f'SELECT price FROM {self.name}.bid', f'SELECT auction AS key, price AS number FROM {self.name}.bid').replace('sink_table="groups"', 'sink_table="groups"\nsink_output="stdout"'))
        return path


    def cleanup(self):
        slots = self.sql(f"SELECT s.slot_name FROM pg_replication_slots s JOIN {self.name}.pgderive_worker_registration r ON r.slot_name=s.slot_name").splitlines()
        for slot in slots:
            self.sql(f"SELECT pg_drop_replication_slot('{slot}')")
        self.sql(f'DROP PUBLICATION {self.name}; DROP SCHEMA {self.name} CASCADE')

    def verify(self):
        oracle = f'{self.name}.stdout_oracle'
        self.sql(f'CREATE MATERIALIZED VIEW IF NOT EXISTS {oracle} AS SELECT auction AS key,price AS number FROM {self.name}.bid WITH NO DATA; REFRESH MATERIALIZED VIEW {oracle}')
        source = f'SELECT int4send(key),numeric_send(number) FROM {oracle}'
        result = f'SELECT int4send(key),numeric_send(number) FROM {self.name}.groups'
        assert self.sql(f'SELECT count(*) FROM (({source} EXCEPT ALL {result}) UNION ALL ({result} EXCEPT ALL {source})) d').strip() == '0'
        assert self.acknowledgement_ahead().strip() == '0'


def batches(output, label):
    return [event for event in map(json.loads, (output/label/'metrics.jsonl').read_text().splitlines())
            if event['event'] == 'sink_batch']


def replay(fixture, events):
    for event in events:
        assert event['version'] == 1 and event['status'] == 'attempted'
        context = event['context']
        assert context['schema'] == fixture.name and context['table'] == 'groups'
        assert [column['name'] for column in context['columns']] == ['key', 'number']
        assert sum(row[1] for row in event['parameters'][0]) == event['occurrences']
        value = json.dumps(event['parameters'][0], ensure_ascii=False).replace("'", "''")
        fixture.sql(event['sql'].replace('$1', "'"+value+"'::jsonb"))


def qualify(output, command):
    fixture = StdoutFixture(output)
    binary = [str(pathlib.Path(command[0]).with_name('pgderive'))]
    with fixture.managed():
        worker = fixture.start(command, 'initial')
        ready = worker.event('ready')
        assert fixture.sql(f"SELECT to_regclass('{fixture.name}.groups') IS NULL").strip() == 't'
        assert fixture.sql(f'SELECT count(*) FROM {fixture.name}.pgderive_sink_ownership').strip() == '0'
        initial = batches(output, 'initial')
        assert initial and all(event['context']['time'] == 1 for event in initial)
        # A manually owned verifier consumes diagnostics. The worker never owns this table.
        fixture.sql(f'CREATE TABLE {fixture.name}.groups(key integer, number numeric)')
        replay(fixture, initial)
        fixture.verify()
        fixture.sql(f"BEGIN; UPDATE {fixture.name}.bid SET price=1.000 WHERE id=1; INSERT INTO {fixture.name}.bid VALUES(9001,1,NULL),(9002,1,NULL); COMMIT")
        worker.event('published', minimum_time=ready['time']+1)
        worker.finish(graceful=True)
        changed = batches(output, 'initial')[len(initial):]
        assert any('sink-delete-v1' in event['sql'] for event in changed)
        assert any('sink-insert-v1' in event['sql'] for event in changed)
        replay(fixture, changed)
        fixture.verify()
        prior = fixture.sql(f'SELECT to_jsonb(p) FROM {fixture.name}.pgderive_progress p')
        config = fixture.config()
        config.write_text(config.read_text().replace('sink_output="stdout"', 'sink_output="postgres"'))
        admin(fixture, binary, 'restart', config, success=False, timeout=40)
        assert fixture.sql(f'SELECT to_jsonb(p) FROM {fixture.name}.pgderive_progress p') == prior
        error = admin(fixture, binary, 'rebuild', config, success=False)
        assert 'stdout sink does not support rebuild' in error
        assert fixture.sql(f'SELECT to_jsonb(p) FROM {fixture.name}.pgderive_progress p') == prior
        assert fixture.sql(f"SELECT to_regclass('{fixture.name}.pgderive_lifecycle') IS NULL").strip() == 't'
        config = fixture.config()
        error = admin(fixture, binary, 'rebuild', config, success=False)
        assert 'stdout sink does not support rebuild' in error
        worker = fixture.start(command, 'restart', maximum=1)
        worker.event('ready')
        assert not batches(output, 'restart'), 'restart must not reprint the snapshot'
        fixture.sql(f'DELETE FROM {fixture.name}.bid')
        worker.event('published')
        worker.finish()
        replay(fixture, batches(output, 'restart'))
        fixture.verify()
        admin(fixture, binary, 'drop', fixture.config())
        assert fixture.sql(f"SELECT to_regclass('{fixture.name}.groups') IS NOT NULL").strip() == 't'
        assert fixture.sql(f'SELECT count(*) FROM {fixture.name}.groups').strip() == '0'
        # Recreate after a tombstone exercises virtual generation renaming.
        worker = fixture.start(binary+['query','create'], 'recreate', maximum=1)
        worker.event('ready')
        fixture.sql(f'INSERT INTO {fixture.name}.bid VALUES(9999,1,42)')
        worker.event('published')
        worker.finish()
        assert batches(output, 'recreate')
        admin(fixture, binary, 'drop', fixture.config())
        (output/'result.json').write_text(json.dumps({'snapshot_and_cdc_sql_replayed':True,
            'no_owned_destination':True,'cold_restart':True,'mode_switch_rejected':True,
            'rebuild_rejected':True,'drop_preserves_external_table':True,'recreate':True})+'\n')


if __name__ == '__main__':
    output, command = harness_arguments()
    qualify(output, command)
