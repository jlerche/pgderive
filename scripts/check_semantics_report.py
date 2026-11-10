#!/usr/bin/env python3
"""Fail closed if the gate skips a frozen history or a production virtual timer."""
import json
import pathlib
import sys
from fast_validation import VIRTUAL_TESTS, validate_tests

TIMER_TESTS = VIRTUAL_TESTS | {
    'storage::tenant::batch::tests::virtual_batch_deadline_accepts_arrivals_without_waiting_for_wall_time',
    'storage::tenant::batch::tests::virtual_batch_overflow_preserves_the_next_request_without_delay',
    'storage::tenant::batch::tests::virtual_batch_closed_queue_drains_immediately',
    'worker::tests::fast_replay::semantics::sql_semantics_virtual_clock',
    'worker::tests::error_order::expression_errors_match_postgresql',
}


def verify(report, manifest):
    if (not isinstance(report,list) or len(report)!=len(manifest['cases'])
            or {r['case'] for r in report}!=set(manifest['cases'])):
        raise ValueError('Semantic history coverage differs from frozen manifest')
    for row in report:
        expected=manifest['histories'][row['case']]
        if (row.get('cold_reopen_equal') is not True
                or row.get('oracle_checks')!=expected['oracle_checks']
                or row.get('logical_time')!=expected['commits']
                or row['lifecycle'].get('restarts')!=expected['restarts']
                or row['lifecycle'].get('maintenance_quiescent') is not True):
            raise ValueError('Missing semantic oracle/recovery checks: '+row['case'])
    return len(report)


def main(report, log):
    root=pathlib.Path(__file__).resolve().parents[1]
    manifest=json.loads((root/'tests/fixtures/semantics-manifest.json').read_text())
    count=verify(json.loads(report.read_text()),manifest)
    validate_tests(log,TIMER_TESTS)
    print(f'Virtual-clock semantics passed: {count} native-oracle histories; production timer tests verified.')


if __name__=='__main__':
    main(*map(pathlib.Path,sys.argv[1:]))
