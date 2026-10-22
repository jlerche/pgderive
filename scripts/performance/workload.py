"""Single-row autocommit load, independent of downstream durable progress."""
import gzip
import json
import threading
import time
import urllib.request

from common import distribution, process_stats, wait


def operation(tick, args):
    """Return one DML statement and whether it advances the publication marker.

    The marker owns book 1. Other rows never enter it. Updates advance its
    monotonically increasing sum, allowing coalesced sink epochs to be observed.
    Mixed traffic samples every third transaction; unsampled latency is unknown.
    """
    if args.workload == 'wide-updates':
        return ('UPDATE positions SET quantity=COALESCE(quantity,0)+1 WHERE id=%s',
                (1 + (tick * 37) % (args.state_rows + 1),), True)
    if args.workload == 'updates' or tick % 3 == 1:
        return ('UPDATE positions SET quantity=%s WHERE id=1', (tick,), True)
    ordinal = tick // 3
    if tick % 3 == 2:
        return ('INSERT INTO positions VALUES(%s,%s,NULL)',
                (args.state_rows + 2 + ordinal, 2 + ordinal), False)
    # Delete half the inserted rows: source state grows without a fixed cap.
    if ordinal % 2 == 1:
        return ('DELETE FROM positions WHERE id=%s', (args.state_rows + 2 + ordinal - 1,), False)
    return ('INSERT INTO positions VALUES(%s,%s,NULL)',
            (args.state_rows + 2 + ordinal + 1000000000, 2 + ordinal), False)


def observe_prefix(records, index, visible, seen, observed_at):
    """Advance only over recorded acknowledgements, retaining raced commits."""
    while index < len(records) and records[index]['tick'] <= visible:
        record = records[index]
        if record['marker']:
            seen[record['tick']] = observed_at
        index += 1
    return index


def run(fixture, args, start, stop):
    records, arrivals, native_arrivals, samples, errors = [], {}, {}, [], []
    producers_done = threading.Event()
    connections = []
    def connect(native=False):
        connection = fixture.connect(native=native)
        connections.append(connection)
        return connection
    initial_process = process_stats(fixture.pid)
    duration_end = start + args.duration
    total = int(args.duration * args.rate)

    def produce():
        try:
            with connect() as conn:
                conn.execute("SET statement_timeout='15s'")
                for tick in range(1, total + 1):
                    if stop.wait(max(0, start + (tick - 1) / args.rate - time.monotonic())):
                        break
                    if time.monotonic() >= duration_end:
                        break
                    statement, values, marker = operation(tick, args)
                    begin = time.monotonic()
                    changed = conn.execute(statement, values).rowcount
                    committed = time.monotonic()
                    if changed != 1:
                        raise RuntimeError(f'Expected one affected row, got {changed} at {tick}')
                    records.append({'tick': tick, 'begin': begin, 'commit': committed, 'marker': marker})
        except BaseException as error:
            errors.append('producer: ' + repr(error)); stop.set()
        finally:
            producers_done.set()

    def observe():
        try:
            with connect() as conn:
                native = connect(native=True) if not fixture.binary else None
                try:
                    indices = {'sink': 0, 'native': 0}
                    while not stop.is_set():
                        targets = [('sink', conn, 'pgd.groups' if fixture.binary else 'rw_sink', arrivals)]
                        if native:
                            targets.append(('native', native, 'totals', native_arrivals))
                        for name, connection, table, seen in targets:
                            query = ('SELECT SUM(units) FROM ' + table if args.workload == 'wide-updates' else
                                     'SELECT units FROM ' + table + ' WHERE book_id=1')
                            row = connection.execute(query).fetchone()
                            after = time.monotonic()
                            if row and row[0] is not None:
                                value = int(row[0])
                                indices[name] = observe_prefix(records, indices[name], value, seen, after)
                        if producers_done.is_set():
                            markers = sum(record['marker'] for record in records)
                            if len(arrivals) == markers and (not native or len(native_arrivals) == markers):
                                return
                        stop.wait(args.poll_ms / 1000)
                finally:
                    if native:
                        native.close()
        except BaseException as error:
            errors.append('observer: ' + repr(error)); stop.set()

    producer = threading.Thread(target=produce, daemon=True)
    observer = threading.Thread(target=observe, daemon=True)
    producer.start(); observer.start()
    next_sample = start
    try:
        while not producers_done.is_set() and not stop.is_set():
            if time.monotonic() >= next_sample:
                sample = {'seconds': time.monotonic() - start, **process_stats(fixture.pid),
                          'submitted_transactions': len(records), 'visible_marker': max(arrivals, default=0),
                          'pending_transactions_after_marker': max(0, len(records) - max(arrivals, default=0)),
                          'state': fixture.objects()}
                with connect() as conn:
                    row = conn.execute("SELECT pg_wal_lsn_diff(pg_current_wal_lsn(),restart_lsn)::bigint "
                                       'FROM pg_replication_slots WHERE restart_lsn IS NOT NULL').fetchall()
                    sample['retained_wal_bytes'] = sum(r[0] for r in row)
                    counters = conn.execute("SELECT n_live_tup,n_tup_ins,n_tup_upd,n_tup_del FROM pg_stat_user_tables WHERE schemaname='public' AND relname='positions'").fetchone()
                    sample['source_statistics'] = dict(zip(['approx_live_rows', 'inserts', 'updates', 'deletes'], counters))
                if not fixture.binary:
                    with urllib.request.urlopen(f'http://127.0.0.1:{fixture.metrics_port}/metrics', timeout=10) as response:
                        (fixture.directory / f'rw-metrics-{len(samples):05}.txt').write_bytes(response.read())
                samples.append(sample)
                with (fixture.directory / 'samples.jsonl').open('a') as stream:
                    stream.write(json.dumps(sample) + '\n')
                next_sample += args.sample_seconds
            stop.wait(.05)
        producer.join(timeout=20)
        observer.join(timeout=args.drain_seconds)
        if producer.is_alive() or observer.is_alive():
            raise TimeoutError('Producer or visibility drain timed out')
        if errors:
            raise RuntimeError(errors)
        if fixture.error or (fixture.worker and fixture.worker.poll() is not None):
            raise RuntimeError(fixture.error or 'Pgderive exited during measurement')
        wait(fixture.equal, args.drain_seconds)
        ack_safe = None
        if fixture.binary:
            with connect() as conn:
                ack_safe = conn.execute('SELECT count(*)=1 AND bool_and(s.confirmed_flush_lsn<=p.checkpoint_lsn '
                    'AND p.checkpoint_lsn<=p.end_lsn) FROM pgd.pgderive_progress p '
                    'JOIN pgd.pgderive_worker_registration r USING(query_id) '
                    'JOIN pg_replication_slots s ON s.slot_name=r.slot_name').fetchone()[0]
                if not ack_safe:
                    raise RuntimeError('Durable progress ordering failed')
        bins = []
        for first in range(0, args.duration, args.window_seconds):
            batch = [r for r in records if first <= r['commit'] - start < first + args.window_seconds]
            events = [e for e in fixture.events if e.get('event') == 'published' and
                      first <= e.get('_observed_at', -1) - start < first + args.window_seconds]
            bins.append({'start_seconds': first, 'transactions': len(batch),
                'sink_visibility_ms': distribution([max(0, (arrivals[r['tick']] - r['commit']) * 1000)
                                                    for r in batch if r['marker']]),
                'processing_ms': distribution([e['elapsed_ms'] for e in events])})
        with connect() as conn:
            source = conn.execute("SELECT count(*),count(DISTINCT book_id),md5(string_agg(id::text||':'||book_id::text||':'||coalesce(quantity::text,'NULL'),',' ORDER BY id)) FROM positions").fetchone()
        return {'passed': True, 'transactions': len(records), 'actual_rows_per_second': len(records) / args.duration,
                'source_digest': source, 'process_cpu_seconds': process_stats(fixture.pid)['cpu_seconds'] - initial_process['cpu_seconds'], 'oracle_equal': True, 'ack_safe': ack_safe, 'windows': bins,
                'visibility_sample_fraction': sum(r['marker'] for r in records) / max(1, len(records)),
                'sink_visibility_ms': distribution([max(0, (arrivals[r['tick']] - r['commit']) * 1000)
                                                   for r in records if r['marker']]),
                'native_visibility_ms': distribution([max(0, (native_arrivals[r['tick']] - r['commit']) * 1000)
                                                      for r in records if r['marker']]) if not fixture.binary else None,
                'samples': samples}
    finally:
        stop.set()
        for connection in connections:
            try:
                if not connection.closed:
                    connection.cancel()
                    connection.close()
            except Exception as error:
                errors.append('connection cleanup: ' + repr(error))
        producer.join(timeout=20); observer.join(timeout=20)
        with gzip.open(fixture.directory / 'timeline.json.gz', 'wt') as stream:
            json.dump({'start_monotonic': start, 'transactions': records,
                       'sink_arrivals': arrivals, 'native_arrivals': native_arrivals,
                       'errors': errors}, stream, separators=(',', ':'))
