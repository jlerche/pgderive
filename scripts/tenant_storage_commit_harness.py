#!/usr/bin/env python3
"""Lost central manifest COMMIT replies must not expose missing durable state."""
import json

from harness_control import commit_proxy, wait_records
from multi_query_memory_harness import progress
from worker_harness import Fixture, harness_arguments


def qualify(output, command, mode):
    output.mkdir()
    fixture = Fixture(output, 'tenant_commit_'+mode, rows=32, auctions=4)
    proxy = None
    with fixture.managed():
        try:
            marker = f'INSERT INTO {fixture.name}_storage.chunks('
            proxy, port = commit_proxy(output/'manifest-commit.jsonl', mode, once=True,
                                       marker=marker, upstream=fixture.port)
            fixture.environment['PGDERIVE__TENANT_STORAGE__POSTGRES__PORT'] = str(port)
            worker = fixture.start(command, 'lost-reply')
            worker.event('ready', seconds=120)
            rows = wait_records(output/'manifest-commit.jsonl', lambda rows: any(r.get('fault') for r in rows))
            fixture.verify()
            worker.finish(graceful=True)
            fixture.verify()
            # Reopen through the real endpoint; retained orphan intents may be
            # collected, but every published checkpoint must remain readable.
            fixture.environment.pop('PGDERIVE__TENANT_STORAGE__POSTGRES__PORT')
            reopened = fixture.start(command, 'reopened')
            ready = reopened.event('ready', seconds=60)
            fixture.verify()
            assert progress([fixture], ready['end_lsn'])[0]['ack_safe']
            reopened.finish(graceful=True)
            fixture.verify()
            report = {'passed': True, 'fault': mode, 'oracle_equal': True,
                      'cold_reopen_equal': True, 'ack_safe': True, 'fault_witness': rows}
            (output/'qualification.json').write_text(json.dumps(report, indent=2))
            return report
        finally:
            if proxy is not None:
                proxy.terminate()
                proxy.wait(timeout=10)


def main():
    output, command = harness_arguments()
    reports = [qualify(output/mode, command, mode) for mode in ['before', 'after']]
    (output/'qualification.json').write_text(json.dumps(reports, indent=2))


if __name__ == '__main__':
    main()
