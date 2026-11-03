#!/usr/bin/env python3
"""Compare two independent tiny-state workers; isolated fixtures, never live demo."""
import argparse
import collections
import json
import pathlib
import time
import tomllib
from contextlib import ExitStack
from worker_harness import Fixture


def requests(path, started, ended):
    rows = [json.loads(line) for line in path.read_text().splitlines()]
    selected = [row for row in rows if 'status' in row
                and started <= row.get('completed_monotonic', -1) <= ended]
    counts = dict(collections.Counter(row['method'] for row in selected if 'method' in row))
    counts['listing_pages'] = sum(bool(row.get('listing')) for row in selected)
    counts['conditional_put_412'] = sum(row.get('method') == 'PUT' and row.get('status') == 412 for row in selected)
    counts['http_5xx'] = sum(row.get('status', 0) >= 500 for row in selected)
    counts['class_a_attempts'] = counts.get('PUT', 0) + counts['listing_pages']
    return counts


def state(fixture):
    return json.loads(fixture.sql(f'''SELECT json_agg(json_build_object(
      'query',p.query_id,'published_time',p.published_time,'checkpoint_time',q.logical_time,
      'ack_safe',s.confirmed_flush_lsn<=p.checkpoint_lsn AND p.checkpoint_lsn<=p.end_lsn))
      FROM {fixture.name}.pgderive_progress p
      JOIN {fixture.name}.pgderive_queries q USING(query_id)
      JOIN {fixture.name}.pgderive_worker_registration r USING(query_id)
      JOIN pg_replication_slots s ON s.slot_name=r.slot_name'''))


def qualify(output, command, *, interval_ms, ticks, gc_ms, samples=13, demo=False):
    output.mkdir()
    case_started = time.monotonic()
    label = output.name.replace('-', '_')
    if demo:
        from portfolio_cadence_fixture import DemoFixture
        fixture = DemoFixture(output, label)
    else:
        fixture = Fixture(output, label, rows=16, auctions=4)
    budget_limit = 10_000
    ledger_path = (output/"operation-budget.json").resolve()
    def configure(target, source):
        original = target.config
        def config(maximum=0):
            path = original(maximum)
            text = path.read_text().replace('maintenance_ticks=4',
                f'maintenance_ticks={ticks}\ncheckpoint_interval_ms={interval_ms}\ngc_interval_ms={gc_ms}\nphase_metrics=true')
            text = text.replace(f'publication="{target.name}"', f'publication="{source.name}"')
            # Bind the peer query to the same source publication while retaining
            # its own catalog, result relation, slot and physical prefix.
            text = text.replace(f'FROM {target.name}.auction', f'FROM {source.name}.auction')
            text = text.replace(f'JOIN {target.name}.bid', f'JOIN {source.name}.bid')
            if target is not source and not demo:
                query = tomllib.loads(text)['worker']['query']['sql']
                text = text.replace('sql='+json.dumps(query),
                    'sql='+json.dumps(query+' ORDER BY SUM(b.price) DESC NULLS LAST LIMIT 10'))
            text = text.replace('[worker]',
                '[object_store.operation_budget]\nledger='+json.dumps(str(ledger_path))+
                f'\nclass_a_limit={budget_limit}\nwindow_secs=86400\n[worker]')
            path.write_text(text)
            return path
        target.config = config
    configure(fixture, fixture)
    latencies = []
    with ExitStack() as cleanup:
        cleanup.enter_context(fixture.managed())
        peer_output = output/'peer-fixture'
        peer_output.mkdir()
        peer_fixture = (DemoFixture(peer_output, label+'_peer', source=fixture, top10=True) if demo
                        else Fixture(peer_output, label+'_peer', rows=1, auctions=1))
        cleanup.enter_context(peer_fixture.managed())
        configure(peer_fixture, fixture)
        primary = fixture.start(command, 'primary')
        primary.event('ready')
        peer = peer_fixture.start(command, 'peer')
        peer.event('ready')
        fixture.verify()
        if demo:
            peer_fixture.verify()
        started = time.monotonic()
        for tick in range(samples):
            time.sleep(max(0, started + (tick+1)*5-time.monotonic()))
            sent = time.monotonic()
            if demo:
                fixture.change(tick)
            else:
                fixture.sql(f'UPDATE {fixture.name}.bid SET price={100+tick} WHERE id=1')
            for worker in [primary, peer]:
                event = worker.event('published', seconds=5, minimum_time=tick+2)
                assert event['time'] == tick+2, event
            latencies.append(time.monotonic()-sent)
            assert latencies[-1] < 5, latencies
            fixture.verify()
            if demo:
                peer_fixture.verify()
            else:
                difference = fixture.sql(f'''SELECT count(*) FROM (
                    (SELECT * FROM {fixture.name}.groups EXCEPT ALL SELECT * FROM {peer_fixture.name}.groups)
                    UNION ALL (SELECT * FROM {peer_fixture.name}.groups EXCEPT ALL SELECT * FROM {fixture.name}.groups)) d''')
                assert difference.strip() == '0', 'independent peer differs'
        before = state(fixture) + state(peer_fixture)
        assert len(before) == 2 and all(row['ack_safe'] for row in before), before
        # Exclude graceful final flush from cadence counts; report it separately.
        measured_until = time.monotonic()
        for worker in [primary, peer]:
            worker.finish(graceful=True)
        final = state(fixture) + state(peer_fixture)
        assert all(row['checkpoint_time'] == row['published_time'] == samples+1 for row in final), final
        assert all(row['ack_safe'] for row in final), final
        # Reopen primary through production replay/recovery without changing sources.
        reopened = fixture.start(command, 'cold-reopen')
        ready = reopened.event('ready')
        assert ready['time'] == samples+1, ready
        fixture.verify()
        reopened.finish(graceful=True)
        events = [[json.loads(line) for line in path.read_text().splitlines()]
                  for path in [output/'primary/metrics.jsonl', peer_output/'peer/metrics.jsonl']]
        report = {'passed': True, 'query_workers': 2, 'same_sources': True,
                  'source_updates_per_worker': samples, 'input_interval_seconds': 5,
                  'publication_latency_seconds': {'max': max(latencies), 'samples': latencies},
                  'checkpoint_interval_ms': interval_ms, 'maintenance_ticks': ticks,
                  'gc_interval_ms': gc_ms, 'before_shutdown': before, 'after_shutdown': final,
                  'checkpoint_events_including_shutdown': sum(sum(row.get('event') == 'maintenance' for row in rows) for rows in events),
                  'worker_retries': sum(sum(row.get('event') == 'retry' for row in rows) for rows in events),
                  'request_attempts_during_cadence': requests(output.parent/'proxy.jsonl', started, measured_until),
                  'request_attempts_including_shutdown_reopen': requests(output.parent/'proxy.jsonl', started, time.monotonic()),
                  'request_attempts_including_bootstrap_shutdown_reopen': requests(output.parent/'proxy.jsonl', case_started, time.monotonic()),
                  'scope': 'current packing on both controls; aggregate and top10 workers share tiny isolated sources; cadence-only comparison, no live workload or compaction-rate claim'}
        if demo:
            report['scope'] = 'production storefront aggregate-before-catalog TOTALS_SQL and TOP10_SQL; 20-product production seed and five-second generator phases; isolated sources, SQL and shared independent memory oracles; current packing in both controls'
            report['demo_workload'] = {'products': fixture.product_count, 'transactions': samples,
                'catalog_and_fact_changes_same_transaction': True, 'native_and_memory_oracles': True}
        ledger = json.loads(ledger_path.read_text())
        counted = report['request_attempts_including_bootstrap_shutdown_reopen']
        assert ledger['used'] == counted['class_a_attempts'], (ledger, counted)
        assert ledger['puts'] == counted.get('PUT', 0), (ledger, counted)
        assert ledger['listing_pages'] == counted['listing_pages'], (ledger, counted)
        report['shared_budget_after_cold_reopen'] = ledger
        report['budget_restart_retains_usage'] = True
        (output/'result.json').write_text(json.dumps(report, indent=2)+'\n')
        return report


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--demo', action='store_true', help='use exact storefront queries and production workload')
    parser.add_argument('output', type=pathlib.Path)
    parser.add_argument('command', nargs=argparse.REMAINDER)
    args = parser.parse_args()
    output = args.output
    command = args.command[1:] if args.command and args.command[0] == '--' else args.command
    reports = {}
    for name, interval, ticks, gc in [('five-second-control', 5000, 16, 5000), ('minute-checkpoint', 60000, 0, 300000)]:
        reports[name] = qualify(output/name, command, interval_ms=interval, ticks=ticks, gc_ms=gc, demo=args.demo)
    old = reports['five-second-control']['request_attempts_during_cadence']
    new = reports['minute-checkpoint']['request_attempts_during_cadence']
    assert new.get('PUT', 0) < old.get('PUT', 0), (old, new)
    assert new['listing_pages'] < old['listing_pages'], (old, new)
    assert new['class_a_attempts'] < old['class_a_attempts'], (old, new)
    assert reports['minute-checkpoint']['before_shutdown'][0]['checkpoint_time'] > 1, reports
    (output/'comparison.json').write_text(json.dumps(reports, indent=2)+'\n')


if __name__ == '__main__':
    main()
