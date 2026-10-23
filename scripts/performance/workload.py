"""Paced row changes with complete autocommit/varied transactions and optional bursts."""
from contextlib import nullcontext
from collections import Counter
import gzip
import json
import math
import threading
import time
import urllib.request

from common import distribution, process_stats, wait


def operation(tick, args):
    """Return one DML statement and whether it advances the publication marker.

    The marker owns book 1. Other rows never enter it. Updates advance its
    monotonically increasing sum, allowing coalesced sink epochs to be observed.
    Mixed traffic marks every third statement; unmarked transaction latency is unknown.
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


VARIED_SIZES = (1, 2, 1, 8, 1, 32, 1, 128, 1, 512, 1, 4, 1, 16, 1, 64, 1, 256, 1, 1000)


def transaction_size(index, remaining, mix):
    return min(remaining, VARIED_SIZES[index % len(VARIED_SIZES)] if mix == 'varied' else 1)


def scheduled_at(start, completed_changes, rate, burst_seconds):
    seconds = completed_changes / rate
    return start + (int(seconds / burst_seconds) * burst_seconds if burst_seconds else seconds)


def offered_changes(first, end, rate, duration, burst_seconds):
    """Rows scheduled in a half-open window, before transaction-boundary shifts."""
    total = int(rate * duration)
    def prefix(seconds):
        horizon = math.ceil(seconds / burst_seconds) * burst_seconds if burst_seconds else seconds
        return min(total, math.ceil(horizon * rate))
    return prefix(end) - prefix(first)


def execute_transaction(connection, args, first, size):
    """One source commit, preserving the last monotonic marker inside that commit."""
    marker = None
    begin = time.monotonic()
    with connection.transaction() if size > 1 else nullcontext():
        for change in range(first, first + size):
            statement, values, marked = operation(change, args)
            changed = connection.execute(statement, values).rowcount
            if changed != 1:
                raise RuntimeError(f'Expected one affected row, got {changed} at change {change}')
            if marked:
                marker = change
    return {'begin': begin, 'commit': time.monotonic(), 'marker': marker is not None,
            'marker_value': marker, 'source_changes': size,
            'first_change': first, 'last_change': first + size - 1}


def observe_prefix(records, index, visible, seen, observed_at):
    """Advance only over recorded acknowledgements, retaining raced commits."""
    while index < len(records):
        record = records[index]
        if record['marker']:
            if record['marker_value'] > visible:
                break
            seen[record['tick']] = observed_at
        index += 1
    return index


def consumer_progress(records, arrivals, start, duration):
    end = start + duration
    visible = max((r['tick'] for r in records if r['marker'] and
                   arrivals.get(r['tick'], float('inf')) <= end), default=0)
    prefix_rows = records[visible-1]['last_change'] if visible else 0
    last = next((r for r in reversed(records) if r['marker']), None)
    return {'visible_prefix_source_changes_at_input_end': prefix_rows,
            'visible_prefix_rows_per_second': prefix_rows/duration,
            'last_marker_drain_ms': max(0,(arrivals[last['tick']]-end)*1000)
                if last is not None and last['tick'] in arrivals else None,
            'scope': 'Observed marker prefix; mixed trailing non-marker changes require final native equality.'}


def in_window(timestamp, start, first, seconds, duration):
    return first <= timestamp - start < min(first + seconds, duration)


def sampling_active(producer, observer, now, input_end, drain_seconds):
    return now < input_end + drain_seconds and (
        now < input_end or producer.is_alive() or observer.is_alive())


def run(fixture, args, start, stop):
    records, arrivals, native_arrivals, samples, errors = [], {}, {}, [], []
    emitted_changes = 0
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
        nonlocal emitted_changes
        try:
            with connect() as conn:
                conn.execute("SET statement_timeout='15s'; SET transaction_timeout='15s'")
                while emitted_changes < total:
                    scheduled = scheduled_at(start, emitted_changes, args.rate, args.burst_seconds)
                    if stop.wait(max(0, scheduled - time.monotonic())):
                        break
                    if time.monotonic() >= duration_end:
                        break
                    size = transaction_size(len(records), total - emitted_changes, args.transaction_mix)
                    record = execute_transaction(conn, args, emitted_changes + 1, size)
                    record['tick'] = len(records) + 1
                    records.append(record)
                    emitted_changes += size
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
        while sampling_active(producer, observer, time.monotonic(), duration_end,
                              args.drain_seconds) and not stop.is_set():
            if time.monotonic() >= next_sample:
                visible_tick = max(arrivals, default=0)
                visible_rows = records[visible_tick-1]['last_change'] if visible_tick else 0
                sample = {'seconds': time.monotonic() - start, **process_stats(fixture.pid),
                          'submitted_transactions': len(records), 'submitted_source_changes': emitted_changes, 'visible_marker': max(arrivals, default=0),
                          'pending_transactions_after_marker': max(0, len(records) - visible_tick),
                          'pending_source_changes_after_marker': max(0, emitted_changes-visible_rows),
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
        observer.join(timeout=max(0, duration_end + args.drain_seconds - time.monotonic()))
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
            batch = [r for r in records if in_window(r['commit'], start, first, args.window_seconds, args.duration)]
            events = [e for e in fixture.events if e.get('event') == 'published' and
                      in_window(e.get('_observed_at', -1), start, first, args.window_seconds, args.duration)]
            bins.append({'start_seconds': first, 'transactions': len(batch),
                'source_changes': sum(r['source_changes'] for r in batch),
                'sink_visibility_ms': distribution([max(0, (arrivals[r['tick']] - r['commit']) * 1000)
                                                    for r in batch if r['marker']]),
                'processing_ms': distribution([e['elapsed_ms'] for e in events]),
                'received_to_sink_ms': distribution([e['received_to_sink_ms'] for e in fixture.events
                    if e['event'] == 'epoch' and in_window(e['_observed_at'], start, first, args.window_seconds, args.duration)]),
                'checkpoint_wall_ms': distribution([e['elapsed_us']/1000 for e in fixture.events
                    if e['event'] == 'maintenance' and in_window(e['_observed_at'], start, first, args.window_seconds, args.duration)])})
        with connect() as conn:
            source = conn.execute("SELECT count(*),count(DISTINCT book_id),md5(string_agg(id::text||':'||book_id::text||':'||coalesce(quantity::text,'NULL'),',' ORDER BY id)) FROM positions").fetchone()
        return {'passed': True, 'transactions': len(records), 'source_changes': emitted_changes,
                'actual_rows_per_second': emitted_changes / args.duration,
                'transaction_sizes': dict(Counter(r['source_changes'] for r in records)),
                'consumer_progress': consumer_progress(records, arrivals, start, args.duration),
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
