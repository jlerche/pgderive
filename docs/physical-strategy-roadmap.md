# Physical strategies from proven properties and measured cost

Choose strategies from validated relational properties, never SQL text, table
names, sampled uniqueness or workload recognition. Preserve complete committed
transactions, full-tuple weighted identity, exact sink occurrence checks, fencing,
uncertain-COMMIT recovery and PUT → atomic PG publication → slot ACK ordering.

## Publication first

1. **Key-directed sinks.** Proven non-NULL unique outputs use primary-key
   predicates for both UPDATE and DELETE, followed by exact old-tuple binary
   verification. Stop creating the full-row hash index for these outputs.
   Arbitrary bags keep full-tuple candidate lookup and occurrence accounting.
   Validate composite keys, NULL payloads, representation mismatches, stale
   deletes/updates, affected-row suppression, rollback, restart and native bag
   equality. Measure small and large publications, PG execution/WAL, HOT updates,
   worker CPU and freshness; key predicates enable an index path but do not force
   the planner to choose it. See [keyed sinks](keyed-sinks.md).
2. **Bind publication work once.** Reuse immutable DML templates, codecs and
   encoded binding/plan data. Prepare statements per connection, replacing them
   on reconnect. Refresh membership-dependent data when membership changes.
   Combine compatible checks under the existing lock order and carry validation
   capabilities only while their protecting locks remain held. Reuse the current
   binding/source-DDL/fencing/uncertain-COMMIT fixtures. Acceptance requires fewer
   round trips and preparation allocations without weakening those checks; test
   both one-row and chunk-crossing publications before changing epoch policy.

## Shared properties and execution cost

3. **Relational property analysis.** Replace the isolated sink-key recognizer
   with candidate keys, nullability/equality compatibility, provable multiplicity
   bounds and unchanged-column lineage on plan edges. Filters preserve keys;
   projections preserve retained unchanged keys; grouping establishes the entire
   grouping tuple. Joins preserve a side's key only with a proof of at most one
   matching row on the other side. Many-to-many joins and UNION ALL need further
   proof. Reuse the same properties for sink, join and storage decisions. Include
   keyed projections/key-preserving joins in qualification only once the proof
   exists; never bypass the compiler just to label a benchmark keyed.
4. **Extend the existing epoch controller.** Feed input bytes, affected keys,
   output DML mix, fixed publication cost, admission wait and maintenance
   interference into the existing scheduler. Combine available transactions only
   while this amortizes fixed cost within freshness/resource bounds. Compare
   offered/completed rate, transaction mix, oldest pending transaction and
   source-to-sink tails, not just processing time or epoch size.

## Bound total maintenance work

5. **Explicit maintenance service budgets.** Extend current debt/admission
   controls with estimated/actual bytes read, rewritten and retained, overlap and
   low-utilization-pack debt. Account jointly for foreground and maintenance I/O;
   reserve minimum maintenance service so low priority cannot accumulate debt
   until a forced stall. Qualify both foreground service and debt clearance.
6. **Local maintenance and hierarchical membership.** Select dirty intervals and
   overlapping history with encoded-byte/fan-in budgets. Coalesce fragments
   locally before root pressure; pack multiple small intervals together to limit
   PUTs and share unchanged authenticated index subtrees. Replace the flat root
   capacity boundary with scalable membership rather than just raising its cap.
   Reuse the fixed/moving-key cold-root and complete-cycle fixtures, then validate
   independently arriving traffic, checkpoint lag and retained WAL.
7. **Budgeted partial page promotion.** Replace all-or-nothing generated-page
   warming only after measuring its cliff. Promote useful authenticated pages
   within the shared budget without displacing the foreground hot set blindly.
   Logical-key answers remain complete-or-absent. Compare threshold-adjacent
   outputs, memory, GET/PUT counts and bytes; keep rejected warming experiments.

Each slice needs an independent review, fast production-path/oracle validation
and the common gate before commit. Keep current failed cost/qualification checks
visible. Component improvements are not a claim of production steady state.

## Current slice boundary

Key-directed sink access is implemented. Publication reuse now covers compiled
sink templates/codecs/key positions and connection-local prepared sink DML,
memtable boundary comparison and epoch-progress SQL. The connection API owns its
cache and invalidates it before mutable raw access; there is no uncached DML
fallback. Encoded immutable plan/binding data and the complete expected
checkpoint comparison are also cached: physical publication epochs refresh
checkpoint-dependent bytes before use, while per-transaction progress stays
fresh. This reduces repeated construction/serialization/parse/planning work,
not the number of foreground database round trips.

Lifecycle protection and the boundary check now pipeline on the same
transaction connection. Biased first polling queues protection first; PostgreSQL
executes separate statements in order, so the boundary snapshot remains fresh
after a lifecycle wait. Both must succeed before source locks, progress, sink DML
and synchronous commit. This removes one serial response wait, not a SQL statement
or a durability barrier. The source contract now also pipelines source-table
locking and layout inspection using separate statements: inspection executes
after the source lock is acquired, including after a concurrent DDL commit.
Both must succeed before progress or sink changes. This shared source-contract
path serves every caller rather than recognizing a query or workload.
Concurrent-DDL commit and rollback tests cover rejection without sink/progress/
ACK changes and successful native-equal publication, respectively. Round-trip
reduction and general validation reuse are not complete.


## Status against the original seven-point list

The implementation order above places publication reuse before property analysis.
In the original pasted list, property analysis was **#2** and publication reuse
was **#3**. The status below preserves that original numbering.

| Original item | Status | Completed work and remaining scope |
|---|---|---|
| 1. Keyed sink access | Implemented | Both UPDATE and DELETE use proven key predicates plus exact old-tuple/affected-row checks; keyed sinks no longer create the changing full-row hash index. Generic bag behavior remains intact. |
| 2. Relational property analysis | Outstanding | The existing sink-key proof remains specialized; shared edge properties for candidate keys, nullability/equality, multiplicity and lineage have not replaced it. |
| 3. Publication reuse | In progress | Bound sink templates/codecs, connection-local prepared DML/guard/epoch statements, encoded immutable metadata and checkpoint refresh, and ordered lifecycle/boundary pipelining are implemented. Source-lock/layout pipelining is implemented and qualified. General lock-scoped validation reuse is not complete. |
| 4. Epoch controller cost model | Outstanding extension | Adaptive scheduling exists. The requested combination of bytes, affected keys, output DML mix, fixed publication cost and maintenance/admission interference has not been implemented as a complete model. |
| 5. Explicit maintenance service budgets | Outstanding extension | Debt/admission controls and shared worker resource budgets exist. Byte-based job accounting, pack-utilization debt and a guaranteed minimum maintenance service allocation remain. |
| 6. Bounded local maintenance and hierarchical membership | Partial foundations | Range-local selection, packed outputs and authenticated block sharing exist. Broader byte/fan-in-budgeted local histories, index-subtree sharing and scalable hierarchical membership remain. |
| 7. Budgeted partial page promotion | Outstanding | Existing bounded caches and prior warming experiments do not implement measured partial generated-page promotion across its overflow threshold. |

These statuses describe implementation scope, not a proof of sustained production
service or completion of the remaining steady-state qualification gates.


## Source-lock/layout pipeline validation (2026-10-27)

Baseline `abef0a2` and the candidate use the same production worker measurement:
20,000 groups, 60 epochs, 16-tick maintenance, native binary equality and safe
ACK checks. With 5 ms downstream PostgreSQL response delay, publication p50
fell from 39.739 to 34.297 ms; the reverse order measured 39.532 versus 34.518 ms
(about 13% lower). The source-layout phase fell from 11.825/11.753 ms to
6.440/6.469 ms. Localhost was flat: 2.084 versus 2.095 ms total publication.
This reduces serial network waiting rather than SQL statement count or CPU.

The new production-worker regression first queues publication behind lifecycle
protection, then behind a second session's exclusive source-table lock. Committed
DDL must produce the exact layout-drift rejection with sink tuple identity,
progress and ACK unchanged; rolled-back DDL must publish the native-equal result.
Both cases and the refactored existing lifecycle barrier passed fast validation
and the coverage-instrumented common gate. The fourteen existing boundary
mutations also passed. Shared fixture helpers replace the previous inline holder
implementation; there is one production layout path and no compatibility fallback.

Independent review found no actionable issues. The common `./scripts/check.sh`
core gate passed 648 Rust tests, 147 Python tests, 156 live worker results plus
rebuild, 97.14% production line coverage and dependency checks. Full release
qualification and sustained production steady state are not claimed. Frozen
binaries, measurement scripts and failure evidence remain ignored under
`artifacts/benchmarks/source-layout-pipeline-20261027/`.

One reverse-order candidate trial recorded extra WAL while a timed PostgreSQL
checkpoint began; that raw result is retained. A follow-up inserted the same
explicit CHECKPOINT before each trial's measurement reset and recorded full-page
images: both builds produced exactly 2,222,443 sink WAL bytes, 195 full-page images
and 12,660 WAL records, with 6,330 HOT updates and 6,330 deletes. Publication p50
remained 39.704 versus 34.418 ms. This controls a checkpoint-dependent measurement
confounder; it is not a claim that pipelining reduces WAL.
