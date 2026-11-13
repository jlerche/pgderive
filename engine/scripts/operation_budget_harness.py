#!/usr/bin/env python3
"""Exhaust production worker capacity and restart without more Class A requests."""
import json
import time
from worker_harness import Fixture, harness_arguments
from async_checkpoint_harness import boundary
from request_cadence_harness import requests


def attempts(path, start):
    rows = [json.loads(line) for line in path.read_text().splitlines()]
    return [row for row in rows if row.get('completed_monotonic', -1) >= start
            and (row.get('method') == 'PUT' or row.get('listing'))]


def exhausted(worker, output, label):
    event = worker.event('capacity_exceeded', seconds=30)
    assert 'operation budget exceeded' in event['error'], event
    worker.process.wait(timeout=30)
    worker.close_logs()
    assert worker.process.returncode != 0
    events = [json.loads(line) for line in (output/label/'metrics.jsonl').read_text().splitlines()]
    assert not any(row['event'] == 'retry' for row in events), events
    return {'attempt': label, 'exit_code': worker.process.returncode, 'retry_events': 0}


def sink_identity(fixture):
    return fixture.sql(f"SELECT json_agg(json_build_array(ctid::text,xmin::text,to_jsonb(g)) ORDER BY ctid) FROM {fixture.name}.groups g")


def assert_counted(ledger, output, started):
    saved = json.loads(ledger.read_text())
    counted = requests(output/'proxy.jsonl', started, time.monotonic())
    assert saved['used'] == counted['class_a_attempts'], (saved, counted)
    assert saved['puts'] == counted.get('PUT', 0), (saved, counted)
    assert saved['listing_pages'] == counted['listing_pages'], (saved, counted)
    return saved


def main():
    output, command = harness_arguments()
    fixture = Fixture(output, 'operation_budget', rows=16, auctions=4)
    original = fixture.config
    ledger = (output/'class-a.json').resolve()
    allowance = 1
    ticks = 4
    def config(maximum=0):
        path = original(maximum)
        path.write_text(path.read_text().replace('maintenance_ticks=4', f'maintenance_ticks={ticks}') + f'''\n[object_store.operation_budget]
ledger={json.dumps(str(ledger))}
class_a_limit={allowance}
window_secs=86400
''')
        return path
    fixture.config = config
    started = time.monotonic()
    observations = []
    with fixture.managed():
        for label in ['exhaust', 'restart']:
            worker = fixture.start(command, label)
            observation = exhausted(worker, output, label)
            saved = json.loads(ledger.read_text())
            # The tenant root's branch marker uses the allowance. Its child
            # ownership probe must then fail before issuing another Class A call.
            assert saved['used'] == saved['puts'] == 1 and saved['listing_pages'] == 0 and saved['remaining'] == 0, saved
            calls = attempts(output/'proxy.jsonl', started)
            assert len(calls) == 1 and calls[0]['method'] == 'PUT', calls
            # An incomplete bootstrap must publish neither durable progress nor a result epoch.
            present = fixture.sql(f"SELECT to_regclass('{fixture.name}.pgderive_progress') IS NOT NULL").strip() == 't'
            count = fixture.sql(f'SELECT count(*) FROM {fixture.name}.pgderive_progress').strip() if present else '0'
            assert count == '0', count
            observations.append({**observation, 'class_a_attempts_total': len(calls)})
        allowance = 1000
        recovered = fixture.start(command, 'renewed-allowance')
        recovered.event('ready', seconds=30)
        fixture.verify()
        recovered.finish(graceful=True)
        renewed = json.loads(ledger.read_text())
        assert renewed['used'] > saved['used'] and renewed['class_a_limit'] == allowance, renewed
        assert renewed['used'] == len(attempts(output/'proxy.jsonl', started)), renewed
        # Exhaust a checkpoint after its source epoch and sink changes are durable.
        # Logical collection now lists the manifest without a Class A request.
        # The data pack spends one PUT; metadata must fail before checkpointing.
        ticks = 1
        allowance = renewed['used'] + 1
        running = fixture.start(command, 'checkpoint-exhaust')
        ready = running.event('ready', seconds=30)
        running.event('collection', seconds=30)
        fixture.sql(f'UPDATE {fixture.name}.bid SET price=price+100 WHERE id=1')
        published = running.event('published', seconds=30, minimum_time=ready['time']+1)
        checkpoint_failure = exhausted(running, output, 'checkpoint-exhaust')
        fixture.verify()
        held = boundary(fixture)
        assert held['published_time'] == published['time'] > held['checkpoint_time'], held
        assert held['journal'] == 1 and held['ack_safe'], held
        audit = sink_identity(fixture)
        checkpoint_ledger = assert_counted(ledger, output, started)
        assert checkpoint_ledger['used'] == allowance and checkpoint_ledger['remaining'] == 0, checkpoint_ledger
        protected = int(fixture.sql(f'SELECT count(*) FROM {fixture.name}.pgderive_protections WHERE uploading AND active AND NOT quiescent').strip())
        assert protected > 0, protected
        retry = fixture.start(command, 'checkpoint-restart')
        checkpoint_restart = exhausted(retry, output, 'checkpoint-restart')
        assert assert_counted(ledger, output, started) == checkpoint_ledger
        assert sink_identity(fixture) == audit, 'exhausted restart rewrote the published sink'
        after_restart = boundary(fixture)
        assert after_restart == held, (held, after_restart)
        fixture.verify()
        allowance = 1000
        final_worker = fixture.start(command, 'checkpoint-renewed')
        final_ready = final_worker.event('ready', seconds=30)
        assert final_ready['time'] == held['published_time'], (final_ready, held)
        assert sink_identity(fixture) == audit, 'recovery replay rewrote the published sink'
        fixture.verify()
        final_worker.finish(graceful=True)
        final = boundary(fixture)
        assert final['checkpoint_time'] == final['published_time'] == held['published_time'], final
        assert final['journal'] == 0 and final['ack_safe'], final
        final_ledger = assert_counted(ledger, output, started)
        assert final_ledger['used'] > checkpoint_ledger['used'], (checkpoint_ledger, final_ledger)
        (output/'result.json').write_text(json.dumps({
            'passed': True, 'observations': observations, 'ledger_at_exhaustion': saved,
            'ledger_after_authorized_increase': renewed, 'renewed_allowance_recovered': True,
            'checkpoint_failure': checkpoint_failure, 'checkpoint_restart': checkpoint_restart,
            'checkpoint_ledger': checkpoint_ledger, 'checkpoint_boundary_held': held,
            'checkpoint_boundary_after_restart': after_restart, 'nonquiescent_protections': protected,
            'checkpoint_recovery': final, 'final_ledger': final_ledger,
            'sink_replay_writes': 0,
            'scope': 'actual worker bootstrap and post-publication checkpoint exhaustion; persistent shared ledger; same-cap restart spends no Class A requests; allowance increase recovers journal without sink rewrites; no injected remote failure'
        }, indent=2)+'\n')


if __name__ == '__main__':
    main()
