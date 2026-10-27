# Keyed PostgreSQL sinks

The default sink preserves arbitrary SQL bags and NULLs. An opt-in key gives a
proven unique, non-NULL output a physical PostgreSQL primary key, for consumers
such as Supabase Realtime. It does not change weighted engine identity.

```toml
[worker]
sink_key = ["book_id"]
# Other worker settings omitted.
[worker.query]
sql = "SELECT book_id, COUNT(*) AS positions FROM public.positions GROUP BY book_id"
```

The current proof accepts a final grouped aggregate (including ordered grouped
aggregates), optionally followed by filters or maps that leave the key unchanged. Every grouping column must appear
unchanged in the declared output key and must be statically non-NULL. Aliased
output labels and composite keys are supported. Keys are currently limited to
boolean, integer, UUID, timestamp and timestamptz types. Nullable keys, incomplete
grouping keys, projections without a grouping proof, transformed output keys,
text/numeric keys and other unproven shapes fail initialization. These are proof
limitations, not claims that the SQL could never have a key. PostgreSQL enforces
the proven constraint as a final safety check; there is no upsert or row collapse.

The key is persisted in the sink binding. Restart checks column nullability and
the exact immediate, validated primary key. A changed or missing key is rejected.
Within each complete result delta, a retraction and insertion with the same key
become one native PostgreSQL UPDATE. The UPDATE verifies the entire old tuple
using its binary representation; a missing or changed destination row aborts
publication. Each key must have at most one unit retraction and one unit insertion.
Unmatched retractions and insertions remain DELETE and INSERT operations,
respectively. All operations share the existing atomic publication transaction. Object publication,
source progress and slot acknowledgement retain the existing durability ordering.
Both UPDATE and DELETE look up candidates using the complete declared key and
then verify the exact old tuple's binary representation. The keyed path creates
only its primary-key index; it does not create or depend on the generic full-row
hash index. PostgreSQL remains free to choose a sequential scan for large
batches. Non-key value updates can therefore use HOT when page space and any
other user-created indexes permit it; HOT is not guaranteed.

Existing development sinks may still have the obsolete full-row hash index.
This change does not silently drop an existing index or provide a migration
adapter: explicitly remove the owned obsolete index while stopped, or recreate
the sink as part of the deployment. Restart alone retains its index-maintenance
and HOT penalty. This slice does not change immutable arrangement formats.

Consumers receive UPDATE events for replacements, INSERT events for new keys,
and DELETE events for removed keys. Unkeyed bag sinks retain occurrence-based
DELETE/INSERT publication.

Existing unkeyed registrations retain their serialized binding. A key cannot be
added through restart or a layout-preserving rebuild: use a new registration and
sink, then switch consumers and resnapshot. Rebuilds preserve the public table OID
only when both the result columns and key contract match. Stdout sinks do not
support keys. This feature creates no RLS policy or public grants.

`scripts/keyed_sink_harness.py` runs in the common live gate and compares binary
results against a refreshed native materialized view. It exercises initialization
rejections, overlapping key replacement, rollback, missing-key detection,
unchanged durable progress on rejection, restart and an empty result. PostgreSQL
table statistics also verify that replacements execute actual UPDATE statements.

The keyed harness also tests reordered composite keys across 512-identity chunk
boundaries, key-directed deletions, absence of a second lookup index, and stale
UPDATE/DELETE rejection. A numerically equal destination value with different
binary numeric representation still rejects the publication without changing
progress or other sink rows.

`scripts/sink_publication_measure.py` reuses the keyed fixture and native binary
oracle for paired production-worker measurements: UPDATE/DELETE batches of
1, 32 and 600 keys, three repetitions, worker CPU, publication clocks,
`pg_stat_statements` execution/WAL, HOT counts and rolled-back EXPLAIN ANALYZE of
the actual emitted DML. Use a disposable PostgreSQL with pg_stat_statements
preloaded and JSON statement logging enabled; the harness resets its statement
statistics. Run through the existing zero-latency S3 proxy and repeat with
`--candidate-first` before the two worker executable paths. Matched fillfactor60
provides controlled page-space headroom; it does not predict a deployed table's
HOT rate or production throughput. See the [physical strategy roadmap](physical-strategy-roadmap.md)
for publication reuse and subsequent property/maintenance work.

Local paired measurements (2026-10-27, baseline `a4cca7d`) used 1,899
UPDATEs and 1,899 DELETEs per worker. At 2,000 groups, repeating in both
execution orders reduced total PostgreSQL sink execution from 22.6–22.9 ms to
16.1–16.7 ms and sink DML WAL from 758,162 to 394,798 bytes. Eligible HOT
updates were 1,729/1,899 versus zero with the full-row index. Publication medians
were 2.87–2.93 ms versus 3.17–3.20 ms; worker CPU changed only modestly.
At 20,000 groups, execution was 9.29 versus 18.77 ms and WAL 376,910 versus
774,959 bytes, but worker CPU was 510 versus 490 ms. These short local trials
establish a sink-side improvement, not a general CPU or freshness guarantee.
Captured single-row plans used the primary-key index at 20,000 groups;
PostgreSQL selected sequential scans at 2,000 groups. Evidence and actual
rolled-back plans are retained under the ignored
`artifacts/benchmarks/keyed-access-20261027/` directory.

Publication reuse compiles sink SQL, native codecs and key positions once for a
physical SQL connection and exact sink binding. The same connection owns the
prepared DML, boundary guard and epoch-journal statements. Claim prepares them
before readiness; reconnecting starts with an empty cache. The cache retains at
most one binding, replacing it on a table/column/key/output change. Explicit raw
mutable client access clears it before the underlying connection can change.
Snapshot activation uses the same sink compiler after installing the table.
Stdout still emits the compiled SQL and never prepares sink DML on PostgreSQL.

Every publication continues to lock and compare the complete durable boundary,
verify source layout, check exact affected rows, and synchronously commit sink
changes with progress. Cached SQL does not stand in for those checks. PostgreSQL
may replan prepared statements following DDL and may select a generic plan after
repeated executions; a cached statement does not grant ownership or authorize
acknowledgement. SQL errors remain fail-closed and uncertain COMMIT recovery is
unchanged. Encoded binding/plan metadata is now retained per claimed writer.
The complete query/checkpoint comparison and checksum are encoded once per
confirmed physical publication epoch. Progress-only epochs reuse those bytes;
checkpoint installation, same-time maintenance and committed recovery advance
the authoritative epoch and refresh the comparison before the next publication.
The expected fence, source addresses, transaction identity and published time
remain fresh for every transaction. A different caller plan is rejected.

Cached parameters use the driver's version-1 JSONB wire representation, checked
against the standard driver encoder. PostgreSQL still compares the complete
plan definition, checkpoint body, digest, epoch and binding under the original
query-to-progress lock order. The epoch selects a cache entry; it does not replace
any database comparison with an epoch-only or hash-only check. The cache retains
one current encoded query/checkpoint and one immutable encoded binding per
writer, adding resident bytes proportional to current coarse metadata; it does
not establish a process-memory plateau. No catalog or arrangement format changes
are involved.

Prepared-reuse measurements (2026-10-27, baseline `0f87256`) used 20,000 groups
and 60 source epochs per worker: ten repetitions each of 1/32/600-key UPDATE and
DELETE batches, totaling 6,330 UPDATEs and 6,330 DELETEs. Repeating in reversed
execution order reduced publication p50 from 2.54/2.58 ms to 2.09/2.15 ms
(about 17%). All six batch categories improved; publication p95 was
6.40/6.48 ms versus 5.76/5.85 ms. Boundary-guard, epoch-progress and sink-DML
phase medians improved; source-layout time remained similar. Worker CPU and
PostgreSQL sink execution time were effectively unchanged. WAL was identical in
one pair and increased 576 bytes (0.046%) in the reverse pair.

Each candidate UPDATE/DELETE used one named handle for 40 executions, including
600-key batches crossing the 512-identity boundary and executions beyond the
custom/generic-plan decision threshold. Bootstrap INSERT reused one handle for
40 chunks; this workload does not test INSERT reuse across CDC epochs. Statement
logs are isolated by byte offset per worker; literal EXPLAIN probes describe the
SQL template, not the worker's cached generic plan. Native binary equality and
ACK checks passed each epoch. Local fixtures exclude network latency and an
independently arriving source stream, so these numbers do not establish a live
freshness or throughput SLO. Results, binary hashes and the retained failed
log-rotation attempt are under ignored
`artifacts/benchmarks/publication-reuse-20261027/`.

The COMMIT fault proxy tracks Parse/Bind/Execute through the existing protocol
observer and arms only when the marked statement executes. Preparing epoch SQL
at startup must not arm an unrelated COMMIT fault. Transaction boundaries reset
the arm. The original atomicity assertion and recovery scenarios are unchanged;
the five-scenario targeted crash run passed after correcting this harness
assumption. The initial failed common-gate evidence remains retained.

The reuse slice passed independent review and the common `./scripts/check.sh`
gate, including sequential live PostgreSQL recovery/lifecycle tests and 97.14%
production line coverage. Disposable fixtures were removed; measured binaries,
results and failure evidence were retained.


Encoded-metadata measurements (2026-10-27, baseline `56c0260`) reused the same
production-worker workload and binary native oracle. With 20,000 groups,
60 epochs and 16-tick maintenance, boundary-guard p50 fell from 0.682/0.707 ms
to 0.628/0.656 ms in the two execution orders (7–8%). Total publication p50
was 2.170/2.177 ms versus 2.083/2.157 ms, a modest 1–4% improvement.
A fixture variation with 64 extra nullable, unused source attributes kept the
same result query: 2,000 groups and 18 epochs showed guard p50 0.981/0.995 ms
versus 0.837/0.838 ms (15–16%), with total publication gains of 1.5–6%.
These are small local timings; CPU remains inconclusive and sink WAL was equal
or differed by less than 0.05%. They do not establish a live freshness SLO.

The measurement tool now accepts `--maintenance-ticks` (default 16) and records
individual publication-phase medians. Both sides use the same setting; the
preceding prepared-reuse trials used four ticks and are not an identical
workload. Fourteen existing production-path tampering cases rejected altered
plan, definition, checkpoint, checksum, epoch, clocks, fence, binding,
destination, transaction identity, source positions/ownership and missing progress, with no
sink writes, progress advance or ACK. Evidence, the reused wider-contract fixture
and frozen binary hashes are retained under ignored
`artifacts/benchmarks/publication-metadata-20261027/`.

The encoded-metadata slice passed independent review and `./scripts/check.sh`,
including 648 Rust tests, sequential PostgreSQL durability/lifecycle checks and
97.14% production line coverage. The owned tmpfs fixture was removed without
creating persistent Docker volumes; results and failure evidence were retained.


The publication pipeline slice (2026-10-27, baseline `b47e73e`) queues lifecycle
protection before the boundary guard using biased first polling on the same
transaction connection. PostgreSQL still executes both original statements
serially with separate snapshots. Both must complete before source locking,
layout validation, progress, sink DML and synchronous commit. Checkpoint publication
is unchanged. This removes one serial response wait, not a SQL statement.
Combining the SQL was rejected because its statement snapshot would predate a
wait for lifecycle protection.

Fast paired production-worker measurements used 20,000 groups, 60 epochs,
16-tick maintenance, the native binary oracle and a constant 5 ms downstream
PostgreSQL response delay. Publication p50 fell from 45.141 to 39.462 ms;
reverse-order testing measured 44.958 versus 39.707 ms (12–13% lower).
Boundary-guard p50 fell from 16.758/16.707 to 11.288/11.369 ms.
Physical sink updates/deletes and index size were equal; WAL was identical in
the first pair and differed by 112 bytes (less than 0.01%) in the reverse pair.
CPU is inconclusive. The localhost control was essentially flat: publication
p50 2.138 versus 2.174 ms, with guard p50 0.597 versus 0.599 ms. The benefit
therefore depends on database response latency. These timings isolate network
waiting in the publication path; they do not establish a live freshness or steady-state SLO.

The default memtable harness now holds an exclusive lifecycle barrier and proves
publication has not acquired query/progress row locks while waiting. It revokes
source ownership before releasing the barrier and requires rejection with exact
sink tuple identity, progress and ACK unchanged. The fourteen existing boundary
tampering cases also pass. An initial test observer incorrectly searched for a
schema literal in parameterized SQL; the failed evidence remains saved, and the
corrected observer checks the advisory wait. Reused proxy/measurement scripts,
frozen binaries and results remain ignored under
`artifacts/benchmarks/publication-roundtrips-20261027/`.

The pipeline slice passed independent review and the common `./scripts/check.sh`
core gate: 648 Rust tests, 147 Python tests, 154 live worker results plus rebuild,
and 97.15% production line coverage. The additional barrier result is required
by both core and full result-count verification. Disposable PostgreSQL used
tmpfs with no persistent volumes; results and failure evidence were retained.
