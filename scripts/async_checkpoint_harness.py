#!/usr/bin/env python3
"""Witness publication during checkpoint PUT and replay after two crash boundaries."""
import json
import pathlib
import signal
from worker_harness import Fixture, harness_arguments
from worker_crash_harness import arm, proxy, wait_for


def boundary(fixture):
    return json.loads(fixture.sql(f'''SELECT json_build_object(
      'checkpoint_time',q.logical_time,'published_time',p.published_time,
      'checkpoint_lsn',p.checkpoint_lsn::text,'end_lsn',p.end_lsn::text,
      'ack_safe',s.confirmed_flush_lsn<=p.checkpoint_lsn AND p.checkpoint_lsn<=p.end_lsn,
      'journal',(SELECT count(*) FROM {fixture.name}.pgderive_epoch_boundaries))
      FROM {fixture.name}.pgderive_queries q JOIN {fixture.name}.pgderive_progress p USING(query_id)
      JOIN {fixture.name}.pgderive_worker_registration r USING(query_id)
      JOIN pg_replication_slots s ON s.slot_name=r.slot_name'''))


def case(output, command, after_checkpoint, shutdown=False):
    output.mkdir(parents=True)
    fixture = Fixture(output, 'async', rows=16, auctions=4)
    gate = output / 'checkpoint.release'
    process, log, error, port = proxy(output, 's3', gate)
    fixture.environment['PGDERIVE__OBJECT_STORE__ENDPOINT'] = f'http://127.0.0.1:{port}'
    original = fixture.config
    suffix_epochs = 160
    final_time = suffix_epochs + 2
    def config(maximum=0):
        path = original(maximum)
        path.write_text(path.read_text().replace('maintenance_ticks=4',
            'maintenance_ticks=64\ncheckpoint_interval_ms=500\nphase_metrics=true'))
        return path
    fixture.config = config
    try:
        with fixture.managed():
            worker = fixture.start(command, 'uploading')
            worker.event('ready')
            arm(gate, 'PUT')
            fixture.sql(f'UPDATE {fixture.name}.bid SET price=111 WHERE id=1')
            worker.event('published', minimum_time=2)
            wait_for(lambda: pathlib.Path(str(gate)+'.pending').exists(), 'checkpoint PUT held')
            for tick in range(suffix_epochs):
                fixture.sql(f'UPDATE {fixture.name}.bid SET price={112+tick} WHERE id=1')
                worker.event('published', minimum_time=tick+3)
            before = boundary(fixture)
            assert before['published_time'] == final_time and before['checkpoint_time'] == 1, before
            assert before['ack_safe'] and before['journal'] == suffix_epochs + 1, before
            fixture.verify()
            if shutdown:
                audit_sql = f'SELECT json_agg(json_build_array(ctid::text,xmin::text,to_jsonb(g)) ORDER BY ctid) FROM {fixture.name}.groups g'
                audit = fixture.sql(audit_sql)
                worker.process.send_signal(signal.SIGINT)
                gate.touch()
                worker.finish()
                final = boundary(fixture)
                assert final['checkpoint_time'] == final['published_time'] == final_time, final
                assert final['journal'] == 0 and final['ack_safe'], final
                assert fixture.sql(audit_sql) == audit, 'shutdown rewrote published sink'
                (output/'result.json').write_text(json.dumps({'passed':True,
                    'shutdown_with_inflight_upload_and_suffix':True,'before':before,'final':final},indent=2)+'\n')
                return
            captured = None
            if after_checkpoint:
                gate.touch()
                captured = worker.event('maintenance')
                assert captured['time'] == 2 and captured['published_time'] == final_time, captured
                committed = boundary(fixture)
                assert committed['checkpoint_time'] == 2 and committed['journal'] == suffix_epochs, committed
                assert committed['ack_safe'], committed
            audit_sql = f'SELECT json_agg(json_build_array(ctid::text,xmin::text,to_jsonb(g)) ORDER BY ctid) FROM {fixture.name}.groups g'
            audit = fixture.sql(audit_sql)
            worker.abort()
            gate.touch()
            resumed = fixture.start(command, 'replay')
            ready = resumed.event('ready')
            assert ready['time'] == final_time, ready
            assert fixture.sql(audit_sql) == audit, 'replay rewrote already published sink rows'
            fixture.verify()
            resumed.finish(graceful=True)
            final = boundary(fixture)
            assert final['checkpoint_time'] == final['published_time'] == final_time, final
            assert final['checkpoint_lsn'] == final['end_lsn'] and final['journal'] == 0, final
            assert final['ack_safe'], final
            (output/'result.json').write_text(json.dumps({'passed':True,
                'kill':'after older checkpoint commit' if after_checkpoint else 'during held checkpoint PUT',
                'published_while_upload_held':suffix_epochs,'sink_replay_writes':0,
                'before':before,'captured_checkpoint':captured,'final':final},indent=2)+'\n')
    finally:
        gate.touch()
        process.terminate()
        process.wait(timeout=10)
        log.close()
        error.close()


def memory_pressure(output, command):
    output.mkdir(parents=True)
    fixture = Fixture(output, 'async_pressure', rows=16, auctions=4)
    gate = output / 'checkpoint.release'
    process, log, error, port = proxy(output, 's3', gate)
    fixture.environment['PGDERIVE__OBJECT_STORE__ENDPOINT'] = f'http://127.0.0.1:{port}'
    original = fixture.config
    cap = 64*1024*1024
    def config(maximum=0):
        path = original(maximum)
        path.write_text(path.read_text().replace('maintenance_ticks=4',
            f'maintenance_ticks=64\ncheckpoint_interval_ms=500\nmemtable_bytes={cap}'))
        return path
    fixture.config = config
    try:
        with fixture.managed():
            calibrate = fixture.start(command, 'calibrate')
            calibrate.event('ready')
            fixture.sql(f'UPDATE {fixture.name}.bid SET price=id+100')
            cap = calibrate.event('published')['memtable_bytes']*3//2
            calibrate.finish(graceful=True)
            worker = fixture.start(command, 'pressure')
            ready = worker.event('ready')
            arm(gate, 'PUT')
            fixture.sql(f'UPDATE {fixture.name}.bid SET price=id+200')
            worker.event('published', minimum_time=ready['time']+1)
            wait_for(lambda: pathlib.Path(str(gate)+'.pending').exists(), 'pressure checkpoint PUT held')
            fixture.sql(f'UPDATE {fixture.name}.bid SET price=id+300')
            pressure = worker.event('checkpoint_backpressure')
            assert pressure['checkpoint_in_flight'], pressure
            held = boundary(fixture)
            assert held['published_time'] == ready['time']+1 and held['ack_safe'], held
            gate.touch()
            published = worker.event('published', minimum_time=ready['time']+2)
            assert published['memtable_bytes'] <= cap, published
            fixture.verify()
            worker.finish(graceful=True)
            final = boundary(fixture)
            assert final['checkpoint_lsn'] == final['end_lsn'] and final['ack_safe'], final
            (output/'result.json').write_text(json.dumps({'passed':True,'cap':cap,
                'backpressure':pressure,'held':held,'final':final},indent=2)+'\n')
    finally:
        gate.touch()
        process.terminate()
        process.wait(timeout=10)
        log.close()
        error.close()


def debt_pressure(output, command):
    """Held uploads consume headroom; pause before a failed preparation."""
    output.mkdir(parents=True)
    fixture = Fixture(output, 'async_debt', rows=16, auctions=4)
    gate = output / 'checkpoint.release'
    process, log, error, port = proxy(output, 's3', gate)
    fixture.environment['PGDERIVE__OBJECT_STORE__ENDPOINT'] = f'http://127.0.0.1:{port}'
    original = fixture.config
    cap = 64*1024*1024
    def config(maximum=0):
        path = original(maximum)
        text = path.read_text().replace('maintenance_ticks=4',
            f'maintenance_ticks=64\ncheckpoint_interval_ms=500\nmemtable_bytes={cap}')
        text = text.replace('[execution]\n', '[execution]\nworkspace_slots=2\nobject_requests=2\n')
        path.write_text(text + '\n[worker.epoch]\nenabled=false\n')
        return path
    fixture.config = config
    try:
        with fixture.managed():
            calibrate = fixture.start(command, 'calibrate')
            calibrate.event('ready')
            fixture.sql(f'UPDATE {fixture.name}.bid SET price=id+100')
            cap = calibrate.event('published')['memtable_bytes']*8
            calibrate.finish(graceful=True)
            worker = fixture.start(command, 'debt')
            ready = worker.event('ready')
            arm(gate, 'PUT')
            fixture.sql(f'UPDATE {fixture.name}.bid SET price=id+200')
            worker.event('published', minimum_time=ready['time']+1)
            wait_for(lambda: pathlib.Path(str(gate)+'.pending').exists(), 'debt checkpoint held')
            for tick in range(32):
                first = 1000 + tick*16
                fixture.sql(f'INSERT INTO {fixture.name}.bid SELECT i,1,i FROM generate_series({first},{first+15}) i')
            pressure = worker.event('checkpoint_backpressure')
            assert pressure['reason'] == 'maintenance_debt', pressure
            assert pressure['memtable_bytes'] < cap, pressure
            held = boundary(fixture)
            assert held['published_time'] == pressure['time'] and held['ack_safe'], held
            assert held['checkpoint_time'] == ready['time'], held
            gate.touch()
            completed = worker.event('maintenance')
            assert completed['time'] == ready['time']+1, completed
            assert completed['published_time'] == pressure['time'], completed
            published = worker.event('published', minimum_time=ready['time']+33)
            usage = published['workspaces']
            assert usage['foreground_demand'] == 1, usage
            assert usage['capacity'] == 2 and usage['peak'] == 2, usage
            assert usage['active'] <= 2, usage
            fixture.verify()
            worker.finish(graceful=True)
            final = boundary(fixture)
            assert final['checkpoint_lsn'] == final['end_lsn'] and final['ack_safe'], final
            (output/'result.json').write_text(json.dumps({'passed':True,'cap':cap,
                'backpressure':pressure,'held':held,'completed_existing_prefix':completed,
                'workspaces':usage,'final':final},indent=2)+'\n')
    finally:
        gate.touch()
        process.terminate()
        process.wait(timeout=10)
        log.close()
        error.close()


def upload_timeout(output, command):
    output.mkdir(parents=True)
    fixture = Fixture(output, 'async_timeout', rows=16, auctions=4)
    gate = output / 'checkpoint.release'
    process, log, error, port = proxy(output, 's3', gate)
    fixture.environment['PGDERIVE__OBJECT_STORE__ENDPOINT'] = f'http://127.0.0.1:{port}'
    original = fixture.config
    failing = True
    def config(maximum=0):
        path = original(maximum)
        text = path.read_text().replace('maintenance_ticks=4',
            'maintenance_ticks=64\ncheckpoint_interval_ms=500')
        if failing:
            text = text.replace('retry_attempts=8', 'retry_attempts=1')
            text += '\n[worker.health]\noperation_timeout_secs=1\n'
        path.write_text(text)
        return path
    fixture.config = config
    try:
        with fixture.managed():
            worker = fixture.start(command, 'timeout')
            worker.event('ready')
            arm(gate, 'PUT')
            fixture.sql(f'UPDATE {fixture.name}.bid SET price=111 WHERE id=1')
            worker.event('published', minimum_time=2)
            wait_for(lambda: pathlib.Path(str(gate)+'.pending').exists(), 'timeout checkpoint PUT held')
            fixture.sql(f'UPDATE {fixture.name}.bid SET price=112 WHERE id=1')
            worker.event('published', minimum_time=3)
            worker.process.wait(timeout=30)
            worker.abort()
            assert worker.process.returncode != 0
            assert 'checkpoint upload timed out' in pathlib.Path(worker.stderr.name).read_text()
            held = boundary(fixture)
            assert held['checkpoint_time'] == 1 and held['published_time'] == 3 and held['ack_safe'], held
            protected = int(fixture.sql(f'SELECT count(*) FROM {fixture.name}.pgderive_protections WHERE uploading AND active AND NOT quiescent').strip())
            assert protected == 1, protected
            fixture.verify()
            gate.touch()
            failing = False
            resumed = fixture.start(command, 'recovered')
            assert resumed.event('ready')['time'] == 3
            fixture.verify()
            resumed.finish(graceful=True)
            final = boundary(fixture)
            assert final['checkpoint_time'] == final['published_time'] == 3 and final['ack_safe'], final
            (output/'result.json').write_text(json.dumps({'passed':True,
                'timeout_kept_nonquiescent_upload_protection':protected,'held':held,'final':final},indent=2)+'\n')
    finally:
        gate.touch()
        process.terminate()
        process.wait(timeout=10)
        log.close()
        error.close()


def qualify(output, command):
    for after in [False, True]:
        case(output/('after-commit' if after else 'during-upload'), command, after)
    memory_pressure(output/'pressure', command)
    debt_pressure(output/'debt', command)
    case(output/'shutdown', command, False, shutdown=True)
    upload_timeout(output/'timeout', command)


if __name__ == '__main__':
    qualify(*harness_arguments())
