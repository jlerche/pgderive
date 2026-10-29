#!/usr/bin/env python3
"""Read-only publication observer. No replication feedback and no source writes.

The pre-COMMIT WAL target is an inclusion proxy only while this generator is
the sole writer of the published source tables. Never use this measurement
as an authoritative transaction durability or acknowledgement mechanism.
"""
import argparse
import collections
import json
import math
import os
from pathlib import Path
import selectors
import signal
import subprocess
import time
from queries import QUERIES

ROOT = Path(__file__).resolve().parents[1]
RUNNING = True

def lsn(value):
    high, low = value.split('/')
    return (int(high, 16) << 32) + int(low, 16)

def summary(samples, now, window=60):
    values = sorted(value for observed, value in samples if now-observed <= window)
    return {'samples': len(values), 'p50_ms': values[max(0, math.ceil(len(values)*.5)-1)] if values else None,
            'p95_ms': values[max(0, math.ceil(len(values)*.95)-1)] if values else None}

def atomic(path, value):
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_suffix('.tmp')
    temporary.write_text(json.dumps(value)+'\n')
    temporary.chmod(0o600)
    os.replace(temporary, path)

class EventTail:
    """Read bounded atomic generator snapshots and de-duplicate persistent ticks."""
    def __init__(self, path):
        self.path, self.tick, self.skipped = path, None, 0
    def read(self):
        try:
            state=json.loads(self.path.read_text())
        except FileNotFoundError:
            return []
        if self.tick is None:
            self.tick=state.get('transactions', 0)
            return []
        events=[event for event in state.get('recent', []) if event['tick'] > self.tick]
        if events:
            self.skipped += max(0, min(event['tick'] for event in events)-self.tick-1)
            self.tick=max(event['tick'] for event in events)
        return events

def size_bin(size):
    if size == 1: return '1'
    if size < 32: return '2-31'
    if size < 128: return '32-127'
    if size < 512: return '128-511'
    return '512-1000'

def stop(_signal, _frame):
    global RUNNING
    RUNNING=False

class QueryObserver:
    """Observe this query's publication progress, including transactions with no result delta."""
    def __init__(self):
        self.pending = collections.deque()
        self.samples = collections.deque(maxlen=10000)
        self.bins = {name: collections.deque(maxlen=10000) for name in ['1','2-31','32-127','128-511','512-1000']}
        self.dropped = self.expired = self.observed = 0
        self.last_poll = None

    def add(self, events):
        for event in events:
            if len(self.pending) >= 4096:
                self.pending.popleft(); self.dropped += 1
            self.pending.append(event)

    def advance(self, progress, now):
        self.last_poll = now if progress is not None else None
        completed = []
        while self.pending:
            event = self.pending[0]
            committed = float(event['committed_at_unix'])
            if now - committed > 120:
                self.pending.popleft(); self.expired += 1; continue
            if progress is None or lsn(event['target_lsn']) > lsn(progress):
                break
            self.pending.popleft()
            elapsed = max(0, (now - committed) * 1000)
            self.samples.append((now, elapsed))
            self.bins[size_bin(event['source_changes'])].append((now, elapsed))
            self.observed += 1
            completed.append({'observed_at': now, 'elapsed_ms': elapsed, 'sequence': event.get('tick'),
                              'source_changes': event['source_changes'], 'target_lsn': event['target_lsn']})
        return completed

    def snapshot(self, now, skipped):
        return {'observed_at': now, 'last_progress_poll_at': self.last_poll,
                'window_seconds': 60, 'write_to_sink': summary(self.samples, now),
                'by_source_changes': {name: summary(values, now) for name, values in self.bins.items()},
                'poll_interval_ms': 50, 'pending': len(self.pending), 'dropped': self.dropped,
                'expired': self.expired, 'generator_samples_skipped': skipped, 'observed_total': self.observed,
                'endpoint': 'PostgreSQL source COMMIT acknowledgement to observed durable query progress; up to 50 ms polling quantization',
                'inclusion_assumption': 'Generator is sole source writer; pre-COMMIT WAL target is an inclusion proxy under that condition'}

def main():
    parser=argparse.ArgumentParser()
    parser.add_argument('--duration', type=float, default=0)
    parser.add_argument('--root', type=Path, default=ROOT)
    args=parser.parse_args(); root=args.root
    for sig in (signal.SIGTERM, signal.SIGINT): signal.signal(sig, stop)
    command=['docker','compose','--env-file',str(root/'stack-r2/.env'),'-f',str(root/'stack-r2/compose.yaml'),
             'exec','-T','db','psql','-X','-qAt','-v','ON_ERROR_STOP=1','-U','supabase_admin','-d','postgres']
    process=subprocess.Popen(command, stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.DEVNULL, text=True, bufsize=1)
    selector=selectors.DefaultSelector(); selector.register(process.stdout, selectors.EVENT_READ)
    tail=EventTail(root/'generator/activity-state.json')
    observers = {name: QueryObserver() for name in QUERIES}
    progress_sql = 'SELECT json_build_object(' + ','.join(
        f"'{name}',(SELECT end_lsn FROM {spec['schema']}.pgderive_progress WHERE sink_table='{spec['table']}')"
        for name, spec in QUERIES.items()) + ');\n'
    started=time.monotonic(); last_write=0
    output=root/'metrics/latency-observer.json'; history=root/'metrics/publication-observations.jsonl'
    try:
        process.stdin.write("SET statement_timeout='5s'; SET lock_timeout='1s';\n"); process.stdin.flush()
        while RUNNING and (not args.duration or time.monotonic()-started < args.duration):
            before=time.monotonic()
            events = tail.read()
            for observer in observers.values(): observer.add(events)
            process.stdin.write(progress_sql); process.stdin.flush()
            if not selector.select(6): raise TimeoutError('Publication progress query timed out')
            line=process.stdout.readline().strip()
            if not line: raise RuntimeError('Publication progress query returned no row')
            progress=json.loads(line); now=time.time()
            observations = [row | {'query': name} for name, observer in observers.items()
                            for row in observer.advance(progress.get(name), now)]
            if observations:
                if history.exists() and history.stat().st_size > 2*1024*1024:
                    os.replace(history, history.with_name('publication-observations.previous.jsonl'))
                history.parent.mkdir(parents=True, exist_ok=True)
                with history.open('a') as stream:
                    for row in observations: stream.write(json.dumps(row)+'\n')
            if now-last_write >= 1:
                atomic(output, {'observed_at': now, 'queries': {name: observer.snapshot(now, tail.skipped)
                                                             for name, observer in observers.items()}})
                last_write=now
            time.sleep(max(0, .05-(time.monotonic()-before)))
    finally:
        process.stdin.close()
        try: process.wait(timeout=3)
        except subprocess.TimeoutExpired:
            process.terminate()
            try: process.wait(timeout=3)
            except subprocess.TimeoutExpired: process.kill(); process.wait()
        selector.close()

if __name__ == '__main__': main()
