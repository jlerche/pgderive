# Streaming snapshot bootstrap

Bootstrap imports the logical replication slot's exported PostgreSQL snapshot,
reads it under the existing frozen codec settings, and publishes one complete
candidate at DBSP time 1. The consistent source LSN remains distinct from this
logical time. The sink and durable source progress become visible in the same
catalog transaction, after protected object uploads. CDC starts at that boundary;
no snapshot chunk advances durable progress or acknowledges WAL.

The compiler freezes a source-column projection from resolved expression column
references, including every source primary key. Snapshot and CDC use the same
projection and source mapping. Unused physical relations are skipped. This is a
hard cutover in plan identity; existing persisted plans must be rebuilt.

Snapshot reads return individual text columns rather than PostgreSQL-generated
JSON. A server-side size guard prevents oversized values from being transferred;
client-side encoded-record limits still apply. Pipelined subqueries prevent the
size guard from repeating casts for each returned field. Native codec settings,
quoted identifiers, NULL, boolean encoding and RLS rejection remain enforced.

Positive snapshot chunks flow immediately through projection, expansion and
union nodes. Additive statistics accumulate raw algebra values in bounded maps,
spilling partial values through the existing consolidator when necessary. The
shared algebra combines partials and encodes each completed group once. SQL
aggregate finalization and downstream expressions run only after the complete
input, avoiding errors or results from intermediate group values.

Nonlinear operators reuse the existing complete-edge evaluators, backed by
bounded spillable inputs. Maintained outputs alone are staged as arrangements;
the old whole-source snapshot read/consolidate/seal path has been removed from
production bootstrap. Joins can still require complete bounded, spillable edges;
this change does not implement a pipelined symmetric hash join.

CDC continues to consolidate complete committed source transactions before
executing fallible expressions. Applying that positive-snapshot shortcut to
signed CDC would incorrectly evaluate canceled rows. Both paths share source
mapping and operator implementations while preserving their respective input
proofs.

## Resource limits

The bootstrap workspace partitions resident and scratch allowances across
buffers, folds and temporary operator evaluation. Nonlinear finalization uses
these reduced limits too. Capacity failures abort the unpublished candidate.
The workspace is poisoned after an ingestion failure and cannot be reused.
Raw expansion emissions are counted across chunks before consolidation, including
duplicate fanout; splitting transport batches cannot reset the work limit.

These are charged workspace limits, not an RSS bound. Existing page caches,
callback state allowances, merge heads, indexes, allocator overhead, upload
buffers and final materialized sink delta have separate limits. Output entry and
byte limits remain enforced; arbitrary-sized sink publication is not introduced.

## Validation

Fast replay now runs initialization through the production source-key-aware
streaming workspace and reports bootstrap rows and preparation time separately.
CDC replay retains committed transaction boundaries. Cold restart creates a fresh
query with the same production bindings. Its report schema changes in place;
there is no legacy bootstrap fallback.

Tests cover raw partial spills, finalization timing, source projection identity,
quoted column names, repeated union edges, joins, poisoned workspaces and
snapshot/CDC agreement. Independent review checked source identity, nonlinear
finalization, resource accounting and the unchanged publication boundary.

Benchmark evidence, datasets and failure logs are retained under ignored
`artifacts/benchmarks/streaming-bootstrap` and `artifacts/fast-validation`.
The million-row component's scratch allowance is explicitly raised to 64 GiB;
the original partitioned scratch-capacity failure is retained. The growth fixture
also scales its explicit total scratch allowance with preload size while keeping
resident and cache budgets unchanged. A frozen previous
binary also failed its fixed memtable allowance, so that component is not a valid
before/after throughput comparison. SF10 uses the previous completed run as its
end-to-end baseline.

Earlier ingestion validation is recorded in
[streaming-bootstrap-validation.md](streaming-bootstrap-validation.md). Its
whole-source sealing design and timings predate this cutover.

## SF10 bootstrap measurement

The fixed TPC-H-derived lineitem source has 59,986,052 rows, a 10,371,596,288-byte
PostgreSQL heap and 12,265,627,648 bytes including indexes. This workload maintains
100,000 supplier groups:

```sql
SELECT l_suppkey AS book_id, COUNT(*) AS positions, SUM(l_quantity) AS units
FROM public.lineitem
GROUP BY l_suppkey;
```

Complete worker bootstrap fell from 2958.527 seconds to
403.079 seconds (7.34 times faster), or
6.72 seconds per million source rows. This includes source
reads, operator preparation, object uploads, sink activation and recovery to
readiness; dataset loading and independent native-result scans are outside it.
A separate gate-blocked run measured 404.786 seconds and verified the same native
result before its test-only lint failure prevented CDC qualification.

Both runs produced exactly four initial objects totaling 4,142,528 bytes, matching
the previous implementation. Bootstrap sampled peak RSS was
223,776,768 bytes versus 223,883,264 bytes previously.
The PostgreSQL projected read plan contains two Subquery Scan nodes over a Seq
Scan, with no Materialize node. Ten-million-row ingestion windows remained near
6–7 seconds per million after the group map filled; there was no upward slope
with total source rows processed.

These are local tmpfs PostgreSQL/object-storage fixtures. Bootstrap uses the
proxy without injected latency, matching the baseline; only timed CDC receives
the configured remote-storage latency profile. Bootstrap can overlap gate work,
while timed CDC waits for the final common gate. This is a measured workload,
not a general throughput guarantee for arbitrary joins or row widths.

## Completed CDC and recovery qualification

The final common gate passed: 491 unit tests, three intentionally ignored tests,
114 Python tests, the sequential live PostgreSQL harness, the worker core matrix
and lifecycle/rebuild checks, strict Clippy, file-size checks and dependency
checks. Production line coverage was 97.37%. Independent implementation and
report reviews found no remaining actionable correctness or durability issues.

SF10 matched the native PostgreSQL result after bootstrap, after all 30,000
changes in 300 varied-size transactions offered at 1,000 rows/s for 30 seconds,
and after a fresh-process cold restart. Cold restart reached readiness in
0.646 seconds. Six asynchronous checkpoints completed without retry events.
All offered rows and transactions published before the result comparison.

| Measurement | Previous run | Streaming bootstrap |
| --- | ---: | ---: |
| Complete bootstrap, seconds | 2,958.527 | 403.079 |
| Bootstrap seconds / million rows | 49.32 | 6.72 |
| Initial objects / bytes | 4 / 4,142,528 | 4 / 4,142,528 |
| Sampled peak scratch, bytes | 92,087,577,458 | 34,335,176 |
| Bootstrap peak RSS, bytes | 223,883,264 | 223,776,768 |
| Whole-run peak RSS, bytes | 326,856,704 | 371,720,192 |
| Whole-run PUT / GET / HEAD attempts | 28 / 134 / 70 | 28 / 134 / 70 |
| Timed CDC processing p50 / p95 / p99, ms | 82 / 269 / 318 | 13 / 191 / 247 |
| Timed CDC received-to-sink p50 / p95 / p99, ms | 121 / 321 / 417 | 20 / 203 / 255 |
| Published CDC epochs | 139 | 210 |

The scratch peaks are sampled every five seconds; short-lived files can be
missed. The earlier gate-blocked run sampled a 42,932,468-byte peak, consistent
with a small finalization spill rather than the former whole-source sort.

The adaptive scheduler chose different epoch boundaries, so the per-epoch CDC
latency distributions are not an isolated measure of a bootstrap optimization.
Whole-run peak RSS increased by about 14%, mainly during CDC/drain, despite the
unchanged bootstrap peak. These 30-second runs do not establish long-term memory
behavior or an RSS improvement. After CDC, both runs had 28 objects and about
5.80 MB of physical state; different epoch boundaries changed compressed bytes
slightly. Object request counts include drain and cold restart and are proxy
attempt counts, not a cloud billing statement.

All run-owned PostgreSQL/S3 Docker fixtures, workers and benchmark scratch were
removed. No Docker volumes were created. SF1/SF10 datasets, result/proxy/resource
logs, source/binary provenance and identified failure evidence remain ignored.
See `artifacts/benchmarks/streaming-bootstrap/comparison.json`,
`cleanup-verification.json` and the final common-gate evidence under
`artifacts/fast-validation/streaming-bootstrap-final-gate`.
