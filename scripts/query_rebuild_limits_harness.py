#!/usr/bin/env python3
"""An oversized replacement signature must fail rebuild before any durable generation intent."""
import json
import pathlib
import sys

from query_lifecycle_harness import grouped, configured, rejected
from query_rebuild_harness import command, verify
from worker_harness import Worker


def qualify(output,binary):
    fixture = grouped(output)
    try:
        config = configured(fixture)
        config.write_text(config.read_text().replace("record_bytes=16384","record_bytes=65536").replace("resident_bytes=65536","resident_bytes=262144"))
        description = command(fixture,binary,'validate',config)
        labels = [column['name'] for column in description['columns']]
        logs = output/'initial'
        logs.mkdir()
        worker = Worker(binary+['query','create'],config,logs,fixture.environment)
        fixture.workers.append(worker)
        ready = worker.event('ready')
        verify(fixture,labels)
        worker.finish(graceful=True)
        registration = fixture.sql(f'SELECT to_jsonb(r) FROM {fixture.name}.pgderive_worker_registration r')
        progress = fixture.sql(f'SELECT to_jsonb(p) FROM {fixture.name}.pgderive_progress p')
        # Schema evolution is a rebuild input. Keep initial activation below its
        # independent source-binding limit; only the replacement intent is huge.
        additions = ','.join(f'ADD COLUMN extra_{index:04d} text' for index in range(1590))
        fixture.sql(f'ALTER TABLE {fixture.name}.bid {additions}')
        description = command(fixture,binary,'validate',config)
        description.pop('query_id')
        signature = json.dumps(description, separators=(',',':'), ensure_ascii=False)
        assert len(json.dumps(signature, ensure_ascii=False).encode()) > 196_608, 'signature does not exercise journal limit'
        assert 'lifecycle metadata limit exceeded' in rejected(binary,fixture,'rebuild',config)
        assert fixture.sql(f"SELECT to_regclass('{fixture.name}.pgderive_lifecycle') IS NULL").strip() == 't'
        assert fixture.sql(f"SELECT to_regclass('{fixture.name}.pgderive_generations') IS NULL").strip() == 't'
        assert fixture.sql(f'SELECT count(*) FROM {fixture.name}.pgderive_worker_registration').strip() == '1'
        assert fixture.sql(f'SELECT to_jsonb(r) FROM {fixture.name}.pgderive_worker_registration r') == registration
        assert fixture.sql(f'SELECT to_jsonb(p) FROM {fixture.name}.pgderive_progress p') == progress
        assert fixture.sql(f'SELECT count(*) FROM {fixture.name}.pgderive_queries').strip() == '1'
        assert fixture.sql(f"SELECT count(*) FROM pg_replication_slots WHERE slot_name='{ready['slot']}'").strip() == '1'
        verify(fixture,labels)
        assert command(fixture,binary,'status',config)['registration']['slot_name'] == ready['slot']
        command(fixture,binary,'drop',config)
        (output/'qualification.json').write_text(json.dumps({'native_columns':1593,
            'oversized_intent':'rejected before durable writes','original_query':'preserved'})+'\n')
        fixture.sql(f'DROP PUBLICATION {fixture.name}; DROP SCHEMA {fixture.name} CASCADE')
    except BaseException:
        fixture.preserve()
        raise


if __name__ == '__main__':
    output = pathlib.Path(sys.argv[1])
    output.mkdir(parents=True,exist_ok=True)
    binary = sys.argv[2:]
    if binary and binary[0] == '--':
        binary = binary[1:]
    qualify(output,binary)
