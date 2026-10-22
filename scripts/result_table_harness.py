#!/usr/bin/env python3
"""Physical result occurrences and real pgoutput INSERT/DELETE qualification."""
import collections
import json
import struct
from nexmark_projection_harness import ProjectionFixture
from worker_harness import Fixture, harness_arguments


def tuple_values(data, offset):
    count = struct.unpack_from('!H', data, offset)[0]
    offset += 2
    values = []
    for _ in range(count):
        kind = chr(data[offset])
        offset += 1
        if kind == 'n':
            values.append(None)
        else:
            assert kind == 't', 'unexpected binary or unchanged TOAST result value'
            size = struct.unpack_from('!I', data, offset)[0]
            offset += 4
            values.append(data[offset:offset+size].decode())
            offset += size
    assert offset == len(data)
    return tuple(values)


def decoded(fixture, slot, publication):
    lines = fixture.sql(f"SELECT encode(data,'hex') FROM pg_logical_slot_get_binary_changes('{slot}',NULL,NULL,'proto_version','1','publication_names','{publication}')").splitlines()
    (fixture.output/'pgoutput.hex').write_text('\n'.join(lines)+'\n')
    events = []
    relation = None
    for line in lines:
        data = bytes.fromhex(line)
        kind = chr(data[0])
        if kind == 'R':
            relation = struct.unpack_from('!I',data,1)[0]
            schema, table, tail = data[5:].split(b'\0',2)
            assert (schema.decode(),table.decode()) == (fixture.name,'groups')
            assert tail[0] == ord('f'), 'result must publish FULL old tuples'
        elif kind in ('I','D'):
            assert struct.unpack_from('!I',data,1)[0] == relation
            assert chr(data[5]) == ('N' if kind == 'I' else 'O')
            events.append((kind,tuple_values(data,6)))
        else:
            assert kind in ('B','C'), 'unexpected result stream operation '+kind
    return events


def qualify(output, command):
    fixture = ProjectionFixture(output,'q2','int8')
    slot = fixture.name+'_out'
    publication = fixture.name+'_out'
    with fixture.managed():
        worker = fixture.start(command,'initial')
        ready = worker.event('ready')
        fixture.verify()
        assert fixture.sql(f"SELECT relkind FROM pg_class WHERE oid='{fixture.name}.groups'::regclass").strip() == 'r'
        assert fixture.sql(f"SELECT to_regclass('{fixture.name}.pgderive_result_views') IS NULL").strip() == 't'
        fixture.sql(f"CREATE PUBLICATION {publication} FOR TABLE {fixture.name}.groups")
        fixture.sql(f"SELECT pg_create_logical_replication_slot('{slot}','pgoutput')")
        try:
            fixture.sql(f"BEGIN; INSERT INTO {fixture.name}.bid(id,auction,price) VALUES(9001,246,NULL),(9002,246,NULL); COMMIT")
            worker.event('published',minimum_time=ready['time']+1)
            fixture.verify()
            fixture.sql(f"BEGIN; DELETE FROM {fixture.name}.bid WHERE id=9001; ROLLBACK")
            fixture.sql(f"DELETE FROM {fixture.name}.bid WHERE id=9001")
            worker.event('published',minimum_time=ready['time']+2)
            fixture.verify()
            worker.finish(graceful=True)
            worker = fixture.start(command,'restarted',maximum=1)
            worker.event('ready')
            fixture.sql(f"UPDATE {fixture.name}.bid SET price=42 WHERE id=9002")
            worker.event('published')
            worker.finish()
            fixture.verify()
            events = decoded(fixture,slot,publication)
            assert collections.Counter(events) == collections.Counter({('I',('246',None)):2,('D',('246',None)):2,('I',('246','42')):1}), events
            (output/'result.json').write_text(json.dumps({'ordinary_table':True,'pgoutput':events,'duplicates_and_nulls':'query-produced occurrences','rollback':'no result change','cold_restart':True})+'\n')
        except BaseException:
            fixture.preserve()
            raise
        else:
            fixture.sql(f"SELECT pg_drop_replication_slot('{slot}'); DROP PUBLICATION {publication}")


class NumericFixture(Fixture):
    def __init__(self, output):
        super().__init__(output,'numeric_occurrences',rows=4,auctions=2)
        self.sql(f"ALTER TABLE {self.name}.bid ALTER COLUMN price TYPE numeric; DELETE FROM {self.name}.bid; INSERT INTO {self.name}.bid VALUES(1,1,1.0),(2,1,1.00),(3,1,NULL)")

    def config(self, maximum=0):
        path = super().config(maximum, query=f'SELECT price FROM {self.name}.bid', execution=False)
        path.write_text(path.read_text().replace('retry_attempts=8','retry_attempts=1'))
        return path

    def verify(self):
        source = f'SELECT numeric_send(price) FROM {self.name}.bid'
        result = f'SELECT numeric_send(price) FROM {self.name}.groups'
        assert self.sql(f'SELECT count(*) FROM (({source} EXCEPT ALL {result}) UNION ALL ({result} EXCEPT ALL {source})) d').strip() == '0', 'numeric display representation differs'
        assert self.acknowledgement_ahead().strip() == '0'


def numeric(output, command):
    fixture = NumericFixture(output)
    with fixture.managed():
        worker = fixture.start(command,'initial')
        worker.event('ready')
        fixture.verify()  # Bootstrap must retain both 1.0 and 1.00 through the map.
        worker.finish(graceful=True)
        before = fixture.sql(f'SELECT to_jsonb(p) FROM {fixture.name}.pgderive_progress p')
        fixture.sql(f'ALTER TABLE {fixture.name}.groups SET UNLOGGED')
        invalid = fixture.start(command,'unlogged')
        invalid.process.wait(timeout=40)
        assert invalid.process.returncode != 0
        invalid.abort()
        assert 'persistence' in (output/'unlogged/stderr.log').read_text()
        assert fixture.sql(f'SELECT to_jsonb(p) FROM {fixture.name}.pgderive_progress p') == before
        fixture.sql(f'ALTER TABLE {fixture.name}.groups SET LOGGED')
        worker = fixture.start(command,'restored')
        ready = worker.event('ready')
        fixture.verify()
        for index, change in enumerate([f'DELETE FROM {fixture.name}.bid WHERE id=2',f'UPDATE {fixture.name}.bid SET price=1.000 WHERE id=1'],1):
            fixture.sql(change)
            worker.event('published',minimum_time=ready['time']+index)
            fixture.verify()
        worker.finish(graceful=True)
        worker = fixture.start(command,'restarted',maximum=1)
        worker.event('ready')
        fixture.sql(f'DELETE FROM {fixture.name}.bid')
        worker.event('published')
        worker.finish()
        fixture.verify()
        (output/'result.json').write_text(json.dumps({'numeric_binary_identity':True,'same_value_scale_update':True,'cold_restart':True,'unlogged_rejected_without_progress_change':True})+'\n')


if __name__ == '__main__':
    output, command = harness_arguments()
    qualify(output,command)
    directory = output/"numeric"
    directory.mkdir()
    numeric(directory,command)
