#!/usr/bin/env python3
"""Proven grouped primary keys, atomic replacement, restart and layout tampering."""
import json
from worker_harness import Fixture, harness_arguments
from native_mv_oracle import expected, binary_difference


class KeyedFixture(Fixture):
    def __init__(self, output, rows=64, auctions=4):
        super().__init__(output, 'keyed', rows=rows, auctions=auctions)
        self.sql(f'DELETE FROM {self.name}.bid WHERE auction IS NULL; ALTER TABLE {self.name}.bid ALTER COLUMN auction SET NOT NULL')
        self.query=f'SELECT auction AS book,COUNT(*) AS positions,SUM(price) AS units FROM {self.name}.bid GROUP BY auction'
        self.key=['book']

    def config(self, maximum=0):
        path=super().config(maximum, query=self.query)
        path.write_text(path.read_text().replace('retry_attempts=8','retry_attempts=1').replace('[worker.query]','sink_key='+json.dumps(self.key)+'\n[worker.query]'))
        return path

    def verify(self):
        expected(self,['book','positions','units'])
        assert binary_difference(self)=='0'
        assert self.acknowledgement_ahead().strip()=='0'


def qualify(output,command):
    fixture=KeyedFixture(output)
    with fixture.managed():
        for label,key in [('missing',['missing']),('duplicate',['book','book']),('aggregate',['units'])]:
            fixture.key=key
            worker=fixture.start(command,label)
            worker.process.wait(timeout=40);worker.abort()
            assert worker.process.returncode != 0
            assert fixture.sql(f"SELECT to_regclass('{fixture.name}.pgderive_worker_registration') IS NULL").strip()=='t'
        fixture.key=['book']
        worker=fixture.start(command,'initial');ready=worker.event('ready');fixture.verify()
        assert fixture.sql(f"SELECT count(*) FROM pg_constraint WHERE conrelid='{fixture.name}.groups'::regclass AND contype='p'").strip()=='1'
        # Replacement keys overlap old keys: stable keys must become native UPDATEs.
        for tick,change in enumerate([
            f'UPDATE {fixture.name}.bid SET auction=5-auction',
            f'UPDATE {fixture.name}.bid SET price=coalesce(price,0)+7',
            f'UPDATE {fixture.name}.bid SET auction=5 WHERE auction=1',
            f'UPDATE {fixture.name}.bid SET price=NULL WHERE auction=2',
            f'UPDATE {fixture.name}.bid SET price=9 WHERE auction=2',
            f'DELETE FROM {fixture.name}.bid WHERE auction=5',
        ],1):
            fixture.sql('BEGIN;'+change+';COMMIT')
            worker.event('published',minimum_time=ready['time']+tick);fixture.verify()
        fixture.sql(f'BEGIN; DELETE FROM {fixture.name}.bid; ROLLBACK');fixture.verify()
        worker.finish(graceful=True)
        mutations=json.loads(fixture.sql(f"SELECT json_build_object('inserts',n_tup_ins,'updates',n_tup_upd,'deletes',n_tup_del) FROM pg_stat_all_tables WHERE relid='{fixture.name}.groups'::regclass"))
        assert mutations['updates']>0, mutations
        assert mutations['deletes']>0, mutations
        boundary=fixture.sql(f'SELECT to_jsonb(p) FROM {fixture.name}.pgderive_progress p')
        fixture.sql(f'ALTER TABLE {fixture.name}.groups DROP CONSTRAINT groups_pkey')
        broken=fixture.start(command,'missing-pk');broken.process.wait(timeout=40);broken.abort()
        assert broken.process.returncode != 0
        assert 'primary key' in (output/'missing-pk/stderr.log').read_text()
        assert fixture.sql(f'SELECT to_jsonb(p) FROM {fixture.name}.pgderive_progress p')==boundary
        fixture.sql(f'ALTER TABLE {fixture.name}.groups ADD PRIMARY KEY(book)')
        worker=fixture.start(command,'restart',maximum=1);worker.event('ready');fixture.verify()
        fixture.sql(f'DELETE FROM {fixture.name}.bid');worker.event('published');worker.finish();fixture.verify()
        (output/'result.json').write_text(json.dumps({'static_rejections':True,'native_binary_equality':True,'overlapping_key_replacements':True,'physical_sink_mutations':mutations,'rollback':True,'missing_pk_blocks_restart_without_progress':True,'restart':True,'empty_result':True})+'\n')



def bounded_updates(output,command):
    output.mkdir()
    fixture=KeyedFixture(output,rows=1800,auctions=600)
    with fixture.managed():
        worker=fixture.start(command,'bounded');worker.event('ready');fixture.verify()
        groups=int(fixture.sql(f'SELECT count(*) FROM {fixture.name}.groups'))
        assert groups>512
        fixture.sql(f'UPDATE {fixture.name}.bid SET price=coalesce(price,0)+1')
        worker.event('published');fixture.verify();worker.finish(graceful=True)
        updates=int(fixture.sql(f"SELECT n_tup_upd FROM pg_stat_all_tables WHERE relid='{fixture.name}.groups'::regclass"))
        assert updates==groups,(updates,groups)
        (output/'result.json').write_text(json.dumps({'native_updates':updates,'crosses_identity_chunk_boundary':True,'native_binary_equality':True})+'\n')


def stale_destination(output,command):
    output.mkdir()
    fixture=KeyedFixture(output)
    with fixture.managed():
        worker=fixture.start(command,'stale-old');worker.event('ready');fixture.verify()
        fixture.sql(f'UPDATE {fixture.name}.groups SET units=units+1 WHERE book=1')
        before=fixture.sql(f'SELECT row_to_json(g) FROM {fixture.name}.groups g ORDER BY book')
        boundary=fixture.sql(f'SELECT to_jsonb(p) FROM {fixture.name}.pgderive_progress p')
        fixture.sql(f'UPDATE {fixture.name}.bid SET price=coalesce(price,0)+11')
        worker.process.wait(timeout=40);worker.abort()
        assert worker.process.returncode != 0
        assert 'result occurrence missing' in (output/'stale-old/stderr.log').read_text()
        assert fixture.sql(f'SELECT row_to_json(g) FROM {fixture.name}.groups g ORDER BY book')==before
        assert fixture.sql(f'SELECT to_jsonb(p) FROM {fixture.name}.pgderive_progress p')==boundary
        (output/'result.json').write_text(json.dumps({'stale_old_tuple_rejected':True,'all_sink_updates_rolled_back':True,'progress_unchanged':True})+'\n')


if __name__=='__main__':
    output,command=harness_arguments()
    qualify(output,command)
    stale_destination(output/'stale-destination',command)
    bounded_updates(output/'bounded-updates',command)
