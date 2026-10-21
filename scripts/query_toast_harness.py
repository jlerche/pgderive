#!/usr/bin/env python3
"""FULL old-image TOAST reconstruction, bounded failure, cold restart and two oracles.

Run sequentially against an owned PostgreSQL fixture. Failure preserves all evidence.
"""
import collections
import json
import pathlib
import struct
import sys

from worker_harness import Fixture, Worker
from native_mv_oracle import expected as native_expected


class ToastFixture(Fixture):
    def __init__(self, output, mode):
        super().__init__(output, 'toast_'+mode, rows=4, auctions=2)
        self.mode = mode
        # STORAGE EXTERNAL prevents compression; each value is > TOAST threshold.
        self.sql(f'''ALTER TABLE {self.name}.auction ALTER COLUMN category SET STORAGE EXTERNAL;
            UPDATE {self.name}.auction SET category=repeat(md5(id::text),256)''')
        self.query = (f'SELECT a.id AS auction_id,a.category AS label,b.price AS price '
                      f'FROM {self.name}.auction a JOIN {self.name}.bid b ON a.id=b.auction')
        self.witness = self.name+'_witness'
        self.sql(f"SELECT pg_create_logical_replication_slot('{self.witness}','pgoutput')")

    def config(self, maximum=0):
        path = super().config(maximum)
        before, rest = path.read_text().split('[worker.query]')
        _, after = rest.split('[execution]')
        limit = 12000 if self.mode == 'budget' else 262144
        path.write_text(before.replace('retry_attempts=8','retry_attempts=1') +
                        '[worker.query]\nsql='+json.dumps(self.query)+'\n[execution]'+
                        after.replace('resident_bytes=65536','resident_bytes=1048576').replace('record_bytes=16384',f'record_bytes={limit}'))
        return path

    def verify(self):
        query = native_expected(self, ['auction_id','label','price'])
        self.sql(f'''DO $$ BEGIN IF EXISTS((SELECT tuple,weight FROM ({query}) oracle
          EXCEPT ALL SELECT tuple,weight FROM {self.bag_relation()}) UNION ALL
          (SELECT tuple,weight FROM {self.bag_relation()} EXCEPT ALL SELECT tuple,weight
          FROM ({query}) oracle)) THEN RAISE EXCEPTION 'native MV mismatch'; END IF; END $$''')
        left = json.loads(self.sql(f'SELECT json_agg(a) FROM {self.name}.auction a'))
        right = json.loads(self.sql(f'SELECT json_agg(b) FROM {self.name}.bid b')) or []
        memory = collections.Counter((a['id'],a['category'],b['price']) for a in left
                                     for b in right if a['id'] == b['auction'])
        rows = json.loads(self.sql(f"SELECT COALESCE(json_agg(g),'[]') FROM {self.bag_relation()} g"))
        assert {tuple(row['tuple'][1]):row['weight'] for row in rows} == dict(memory)
        assert self.sql(f'''SELECT count(*) FROM pg_replication_slots s
            JOIN {self.name}.pgderive_worker_registration r ON s.slot_name=r.slot_name
            CROSS JOIN {self.name}.pgderive_progress p WHERE s.confirmed_flush_lsn>p.checkpoint_lsn''').strip() == '0'

    def progress(self):
        return self.sql(f'SELECT json_build_array(q.logical_time,p.xid,p.commit_lsn,p.end_lsn) '
                        f'FROM {self.name}.pgderive_queries q CROSS JOIN {self.name}.pgderive_progress p')

    def raw_witness(self):
        raw = self.sql(f"SELECT encode(data,'hex') FROM pg_logical_slot_peek_binary_changes('{self.witness}',NULL,NULL,'proto_version','1','publication_names','{self.name}')")
        (self.output/'wire-messages.hex').write_text(raw)
        witnessed = 0
        for line in raw.splitlines():
            message = bytes.fromhex(line)
            if message[:1] != b'U':
                continue
            offset = 5
            assert message[offset:offset+1] == b'O', 'FULL update lacks full old tuple'
            old, offset = tuple_tags(message,offset+1)
            assert b'u' not in old, 'old tuple has unresolved TOAST'
            assert message[offset:offset+1] == b'N'
            new, offset = tuple_tags(message,offset+1)
            assert offset == len(message)
            if b'u' in new:
                witnessed += 1
        assert witnessed > 0, 'fixture did not exercise actual unchanged TOAST marker'
        return witnessed

    def cleanup(self):
        self.sql(f"SELECT pg_drop_replication_slot('{self.witness}')")
        super().cleanup()


def tuple_tags(message,offset):
    count = struct.unpack_from('!H',message,offset)[0]
    offset += 2
    tags = []
    for _ in range(count):
        tag = message[offset:offset+1]
        tags.append(tag)
        offset += 1
        if tag in (b't',b'b'):
            length = struct.unpack_from('!I',message,offset)[0]
            offset += 4+length
        else:
            assert tag in (b'n',b'u')
    return tags,offset


def start(fixture,command,label,config=None):
    directory = fixture.output/label
    directory.mkdir()
    worker = Worker(command,config or fixture.config(),directory,fixture.environment)
    fixture.workers.append(worker)
    return worker


def qualify(output,command,mode):
    fixture = ToastFixture(output,mode)
    with fixture.managed():
        worker = start(fixture,command,'first')
        ready = worker.event('ready')
        fixture.verify()
        if mode == 'budget':
            before = fixture.progress()
            slot = ready['slot']
            acknowledged = fixture.sql(f"SELECT confirmed_flush_lsn FROM pg_replication_slots WHERE slot_name='{slot}'")
            # Old and inherited new are each 8KiB; the complete change exceeds 12KiB.
            fixture.sql(f'UPDATE {fixture.name}.auction SET id=10 WHERE id=1')
            worker.process.wait(timeout=60)
            assert worker.process.returncode != 0
            worker.abort()
            assert 'scratch record limit exceeded' in (output/'first'/'stderr.log').read_text()
            assert fixture.progress() == before
            assert fixture.sql(f"SELECT confirmed_flush_lsn FROM pg_replication_slots WHERE slot_name='{slot}'") == acknowledged
            config = fixture.config()
            config.write_text(config.read_text().replace('record_bytes=12000','record_bytes=262144'))
            worker = start(fixture,command,'budget-recovery',config)
            settled = worker.event('published',minimum_time=ready['time']+1)
            fixture.verify()
            worker.finish(graceful=True)
        else:
            n = fixture.name
            changes = [
                f'UPDATE {n}.auction SET id=10 WHERE id=1; UPDATE {n}.bid SET auction=10,price=99 WHERE auction=1',
                f"UPDATE {n}.auction SET category=repeat(md5('changed'),256) WHERE id=10; UPDATE {n}.auction SET id=11 WHERE id=10; UPDATE {n}.bid SET auction=11 WHERE auction=10",
                f'UPDATE {n}.auction SET category=NULL WHERE id=11; UPDATE {n}.bid SET price=NULL WHERE id=1',
                f"UPDATE {n}.auction SET category=repeat(md5('restored'),256) WHERE id=11; UPDATE {n}.auction SET id=12 WHERE id=11; UPDATE {n}.bid SET auction=12 WHERE auction=11",
                f'DELETE FROM {n}.auction WHERE id=12; DELETE FROM {n}.bid WHERE auction=12',
                f"INSERT INTO {n}.auction VALUES(12,repeat(md5('insert'),256)); INSERT INTO {n}.bid VALUES(100,12,7); UPDATE {n}.auction SET id=13 WHERE id=12; UPDATE {n}.bid SET auction=13 WHERE id=100",
            ]
            for index,change in enumerate(changes,1):
                fixture.sql('BEGIN;'+change+';COMMIT')
                settled = worker.event('published',minimum_time=ready['time']+index)
                fixture.verify()
            worker.finish(graceful=True)
            # Queue a later committed update before cold reopen; replay must use its
            # own historical old image rather than the already-changed SQL row.
            fixture.sql(f'BEGIN; UPDATE {n}.auction SET id=14 WHERE id=13; UPDATE {n}.bid SET auction=14 WHERE id=100; COMMIT')
            worker = start(fixture,command,'cold')
            settled = worker.event('published',minimum_time=settled['time']+1)
            fixture.verify()
            worker.finish(graceful=True)
        witnessed = fixture.raw_witness()
        (output/'result.json').write_text(json.dumps({'oracles':'native PostgreSQL MV and independent raw-row memory bag',
            'unchanged_new_wire_updates':witnessed,'old_wire_images':'complete, no unchanged markers',
            'mode':mode,'reconstructed_record_budget':'exact complete old/new images',
            'restart':'cold exact source boundary','no_ack_beyond_publication':True})+'\n')


if __name__ == '__main__':
    output = pathlib.Path(sys.argv[1])
    command = sys.argv[2:]
    if command[:1] == ['--']:
        command = command[1:]
    for mode in ('history','budget'):
        directory = output/mode
        directory.mkdir()
        qualify(directory,command,mode)
