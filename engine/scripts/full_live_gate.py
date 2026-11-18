"""Full release qualification inventory and evidence verification."""
import hashlib
import json
import os
import pathlib
import stat
import subprocess
import sys

MATRIX = dict(line.split() for line in """compiler sql_compiler
sql-composition sql_composition
values values
set-operations set_operation
boolean-resolution boolean_resolution
temporal-resolution temporal_resolution
subqueries subquery
scalar-cardinality scalar_cardinality
quantified-subqueries quantified_subqueries
residual-subqueries residual_subqueries
numeric-arithmetic numeric_arithmetic
temporal-arithmetic temporal_arithmetic
ordering ordering
ordering-domains ordering_domains
fetch-with-ties fetch_with_ties
distinct-on distinct_on
unordered unordered
query-concurrent query_concurrent
application application_portfolio
composition-matrix composition_matrix
composition-inventory composition_matrix
sliding-extrema sliding_extrema
prefix-window prefix_window
expression-composition expression_composition
sliding-window sliding_window
keyed-sink keyed_sink
sink-batch sink_batch
stdout-sink stdout_sink
projection projection_compiler
star-projection star_projection
functions functions_compiler
postgres-support postgres_support
environment environment
computed-join computed_join
join-predicates join_predicate
outer-joins outer_join
natural-joins natural_join
aggregate-inputs aggregate_inputs_compiler
relational relational_compiler
partition partition_compiler
group-references group_references
grouping-sets grouping_sets
string-agg string_agg
text-ordering text_ordering
boolean-aggregates boolean_aggregate
named-window named_window
multiple-windows multiple_window
numeric numeric_compiler
circuit-numeric circuit_numeric
native native_compiler
date-bin date_bin_compiler
hopping hopping_compiler
ranking ranking_compiler
ranking-distribution ranking_distribution
peer-frames peer_frames_compiler
integral-range integral_range
temporal-range temporal_range
frame-exclusion frame_exclusion
derived derived_compiler
navigation navigation_compiler
frame-values frame_value
lookup lookup_compiler
composition composition_compiler
case case_compiler
session session_compiler
distinct distinct_compiler
nexmark-q15 nexmark_q15
join-composition join_composition
nexmark-auction nexmark_auction
nexmark-q22 nexmark_q22
nexmark-q1 nexmark_q1
nexmark-projection nexmark_projection
nexmark-q20 nexmark_q20
nexmark-ordered nexmark_ordered
nexmark-temporal nexmark_temporal
nexmark-q3 nexmark_q3
nexmark-calendar nexmark_calendar
nexmark-hot nexmark_hot
group-membership group_membership
nexmark-q8 nexmark_q8
nexmark-q13 nexmark_q13
nexmark-q14 nexmark_q14
nexmark-q21 nexmark_q21""".splitlines())

WORKER = {
    'result-table': 'result_table',
    'query-binding': 'query_binding',
    'memtable': 'memtable',
    'async-checkpoints': 'async_checkpoint',
    'operation-budget': 'operation_budget',
    'tenant-capacity': 'multi_tenant_capacity',
    'tenant-storage': 'tenant_storage',
    'tenant-storage-commit': 'tenant_storage_commit',
    'multi-query': 'multi_query_memory',
    'crashes': 'worker_crash',
}
ADMIN = (
    'query-lifecycle', 'query-removal', 'query-rebuild', 'query-rebuild-limits',
    'query-rebuild-fault', 'query-health', 'query-health-failure', 'query-health-slot',
    'query-storage-tls', 'query-postgres-tls', 'query-recovery',
)
EXTRA = (
    'query-toast', 'query-ordered-state', 'query-expression', 'query-expression-calendar',
    'query-native-join', 'query-topk', 'query-membership', 'query-storage-soak',
    'native-cross', 'nexmark-q6',
)
OPTIONS = {
    'composition-matrix': ['--skip-inventory'],
    'composition-inventory': ['--inventory-only'],
    'multi-query': ['--counts', '2', '--seconds', '2', '--mode', 'shared'],
    'query-storage-soak': ['--duration', '1', '--transactions', '6', '--rows', '256'],
}
RELEASE = ('query-container', 'query-capacity')
REPLICATION = tuple(f'replication-{scenario}' for scenario in (
    'stream', 'snapshot', 'maintenance', 'collection', 'paged', 'cohort', 'protocol',
))


# Each harness exposes its original named history inventory and qualifier.
MODE_SHARDS = {
    'subqueries': 75, 'outer-joins': 45, 'set-operations': 44,
    'temporal-arithmetic': 40, 'join-predicates': 29,
    'sql-composition': 28, 'numeric-arithmetic': 19, 'ordering': 18, 'unordered': 17,
    'ordering-domains': 14, 'boolean-resolution': 15, 'nexmark-q8': 8,
    'query-native-join': 8, 'query-topk': 10, 'expression-composition': 12,
    'circuit-numeric': 9, 'computed-join': 6, 'aggregate-inputs': 6,
}
CASE_SHARDS = {'natural-joins': 8, 'prefix-window': 6, 'sliding-extrema': 6}
OPERATION_SHARDS = {'query-rebuild-fault': 12, 'query-removal': 11,
                    'query-health-failure': 6, 'memtable': 8, 'application': 6,
                    'tenant-capacity': 2, 'query-lifecycle': 8, 'async-checkpoints': 6,
                    'query-binding': 18, 'keyed-sink': 6}
COMPOSITION_SHARDS = 80
ERROR_ORDER_SHARDS = 8
FULL_RESULT_COUNT = 918


def suites():
    return (*REPLICATION, 'error-order', *MATRIX, *WORKER, 'transactions', *EXTRA, *ADMIN)


def replication_command(name, directory):
    return ['bash', 'scripts/check_replication_case.sh', name.removeprefix('replication-'), str(directory)]


def command(name, directory, binaries, postgres_port):
    if name in REPLICATION:
        return replication_command(name, directory)
    if name == 'error-order':
        return [sys.executable, 'scripts/error_order_differential.py', 'psql', '-h', '127.0.0.1',
                '-p', postgres_port, '-U', 'postgres', '-d', 'postgres', '-v', 'ON_ERROR_STOP=1', '-Atq']
    if name == 'transactions':
        return [sys.executable, 'scripts/worker_harness.py', '--faults', '--kill',
                '--ticks', '4', '--burst', '8', str(directory), '--', binaries['worker']]
    executable = binaries['admin'] if name in (*ADMIN, 'query-concurrent') else binaries['worker']
    if name == 'query-capacity':
        executable = str(directory.parent / 'package/bin/pgderive')
    script = MATRIX.get(name, WORKER.get(name, name.replace('-', '_')))
    arguments = [str(directory), *OPTIONS.get(name, [])]
    if name == 'query-storage-soak':
        arguments = [*OPTIONS[name], str(directory)]
    entrypoint = [f'scripts/{script}_harness.py']
    if name in MODE_SHARDS:
        inventory = 'COUNTS' if name == 'unordered' else 'MODES'
        entrypoint = ['scripts/worker_case_shard.py', f'{script}_harness', inventory]
    if name in CASE_SHARDS:
        entrypoint = ['scripts/worker_case_shard.py', f'{script}_harness', 'CASES']
    return [sys.executable, *entrypoint, *arguments, '--', executable]


def qualify_container(output, commands, environment):
    tag = f'pgderive-qualification:gate-{os.getpid()}'
    image = output / 'container-image'
    commands.run([sys.executable, 'scripts/build_container.py', str(output / 'package'),
                  str(image), '--tag', tag], environment, output / 'container-image.log')
    commands.run([sys.executable, 'scripts/query_container_harness.py', str(output / 'query-container'),
                  '--image', tag, '--image-manifest', str(image / 'manifest.json'),
                  '--transactions', '13'],
                 environment, output / 'query-container.log')


def verify_error_order(output):
    corpus = json.loads(pathlib.Path('scripts/error_order_corpus.json').read_text())
    for index in range(ERROR_ORDER_SHARDS):
        directory = output / 'error-order' / f'shard-{index + 1}'
        report = json.loads((directory / 'cases.json').read_text())
        cases = [row['case'] for row in report['cases']]
        if cases != corpus[index::ERROR_ORDER_SHARDS]:
            raise ValueError(f'Expression-error case inventory differs in {directory}')


def verify(output, binaries, postgres_port, with_replication):
    if with_replication:
        verify_error_order(output)
    results = list(output.rglob('result.json'))
    assert len(results) == FULL_RESULT_COUNT, (len(results), FULL_RESULT_COUNT)
    identities = set()
    for result in results:
        parts = result.relative_to(output).parts
        if len(parts) > 2 and parts[1].startswith('shard-'):
            parts = (parts[0], *parts[2:])
        if parts in identities:
            raise ValueError(f'Duplicate result evidence: {result}')
        identities.add(parts)

    reports = [
        output / name / 'qualification.json' for name in
        ('query-rebuild', 'query-rebuild-limits', 'query-health', 'query-health-slot',
         'query-storage-tls', 'query-postgres-tls', 'query-recovery', 'query-container',
         'query-capacity', 'query-storage-soak')
    ]
    for name, count in (('query-removal', 11), ('query-rebuild-fault', 12), ('query-health-failure', 6)):
        cases = list((output / name).rglob('qualification.json'))
        assert len(cases) == count and len({case.parent.name for case in cases}) == count, (
            name, len(cases), count)
        reports.extend(cases)
    for report in (*results, *reports):
        json.loads(report.read_text())

    attest(output, binaries, postgres_port)
    print(f'Worker qualification passed: {len(results)} results, six COMMIT faults, producer burst, '
          'restart and five witnessed process kills.')


def attest(output, binaries, postgres_port):
    root = pathlib.Path.cwd()
    private_credential = output / 'query-container/secrets/s3.json'
    credential_info = private_credential.stat()
    assert stat.S_ISREG(credential_info.st_mode) and stat.S_IMODE(credential_info.st_mode) == 0o600
    assert credential_info.st_uid == 10001
    admin = pathlib.Path(binaries['admin'])
    files = [
        root / name for name in ('Cargo.lock', 'worker/Cargo.toml', 'worker/src/main.rs', 'Cargo.toml',
                                 'worker.example.toml', 'docs/postgres-support-matrix.json', '.dockerignore')
    ]
    files.extend((pathlib.Path(binaries['worker']), admin, admin.with_name('postgres_tls_control_probe'),
                  output / 'package/bin/pgderive'))
    for pattern in ('src/**/*.rs', 'src/**/*.sql', 'docs/*.md', 'deploy/*',
                    'scripts/*.sh', 'scripts/*.py', 'queries/**/*.sql'):
        files.extend(sorted(root.glob(pattern)))
    for pattern in ('*.json', '*.jsonl', '*.toml', '*.log'):
        files.extend(sorted(path for path in output.rglob(pattern) if path != private_credential))
    postgres = subprocess.check_output(
        ['psql', '-h', '127.0.0.1', '-p', postgres_port, '-U', 'postgres',
         '-d', 'pgderive_dev', '-Atc', 'SELECT version()'],
        text=True, env={**os.environ, 'PGPASSWORD': 'postgres'},
    ).strip()
    provenance = {
        'private_inputs': {str(private_credential.relative_to(root)): {
            'uid': credential_info.st_uid, 'gid': credential_info.st_gid, 'mode': '0600',
            'bytes': credential_info.st_size,
            'scope': 'ownership/mode attestation only; private credential contents remain unreadable to the host qualifier',
        }},
        'git_head': subprocess.check_output(['git', 'rev-parse', 'HEAD'], text=True).strip(),
        'rust': subprocess.check_output(['rustc', '--version'], text=True).strip(),
        'postgres': postgres,
        'sha256': {str(path.relative_to(root)): hashlib.sha256(path.read_bytes()).hexdigest() for path in files},
    }
    (output / 'provenance.json').write_text(json.dumps(provenance, indent=2) + '\n')
