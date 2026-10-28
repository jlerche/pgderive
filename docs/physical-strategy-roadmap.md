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
4. **Extend the existing epoch controller.** See [cost feedback](epoch-cost-model.md) for the implementation and measurement boundaries. Feed input bytes, affected keys,
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
complete boundary comparison for all writer paths and epoch-progress SQL. The connection API owns its
cache and invalidates it before mutable raw access; there is no uncached DML
fallback. Encoded immutable plan/binding data and the complete expected
checkpoint comparison are also cached: physical publication epochs refresh
checkpoint-dependent bytes before use, while per-transaction progress stays
fresh. Encoding and template reuse reduce repeated construction, serialization,
parse and planning work.

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
ACK changes and successful native-equal publication, respectively.

Publication, checkpoint and physical maintenance now share a private,
transaction-borrowing `LockedBoundary`. It is created only after lifecycle
protection, synchronous durability configuration and the complete prepared
boundary guard succeed. The guard locks query, progress and source-ownership
rows; staging reuses those facts while the transaction retains its locks.
Membership writes keep their epoch CAS, clock condition, upload provenance and
affected-row checks. Epoch journal writes reuse the established catalog-format
check. Bootstrap uses a separate fresh-insert path under lifecycle protection.
Claims, recovery and ACK confirmation continue full authoritative validation in
their own transactions. This completes the planned publication validation reuse;
remaining performance work is evaluated against measurements.


## Status against the original seven-point list

The implementation order above places publication reuse before property analysis.
In the original pasted list, property analysis was **#2** and publication reuse
was **#3**. The status below preserves that original numbering.

| Original item | Status | Completed work and remaining scope |
|---|---|---|
| 1. Keyed sink access | Implemented | Both UPDATE and DELETE use proven key predicates plus exact old-tuple/affected-row checks; keyed sinks no longer create the changing full-row hash index. Bounded key probes plus statement-local tuple-location targeting prevent the demo-sized full-sink hash scan in tested generic/custom plans. Generic bag behavior remains intact. |
| 2. Relational property analysis | Implemented | Shared candidate keys, nullability/equality, cardinality and unchanged-value identities replace the three old walkers and serve sinks, positional windows and committed statistics. Unsupported shapes remain conservative proof barriers. |
| 3. Publication reuse | Implemented | Bound templates/codecs and prepared statements, encoded immutable metadata/checkpoint refresh, both ordered pipelines and transaction-scoped validation reuse across publication, checkpoint and maintenance are implemented. Claims, recovery and ACK confirmation retain independent authoritative validation. |
| 4. Epoch controller cost model | Implemented cost-feedback slice | Bounded typed rows/bytes/source-key images, forecast output DML mix, measured non-DML publication, admission and maintenance overhead feed the existing controller. See [cost model](epoch-cost-model.md); empirical estimates and short regression checks are not production SLO guarantees. |
| 5. Explicit maintenance service budgets | Service controls/accounting implemented; qualification open | Existing admission now gives physical object requests foreground headroom and fair maintenance service. Foreground-aware byte/operation quanta and per-job requested/delivered/written/retained-byte and observed sparse-pack accounting are implemented. This is not a CPU-share or global physical-debt guarantee; sustained qualification remains open. |
| 6. Bounded local maintenance and hierarchical membership | Partial foundations | Range-local selection, packed outputs, authenticated block sharing and [byte/fan-in selection](maintenance-selection.md) with explicit progress exceptions exist. Smaller independently scheduled local histories, index-subtree sharing and scalable hierarchical membership remain. |
| 7. Budgeted partial page promotion | Bounded admission slice | [Partial promotion](generated-page-promotion.md) retains complete fitting pages across workspace overflow and recycles unused warming without evicting foreground-consumed decoded pages. Hot-key prediction and sustained production qualification remain open. |

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


## Locked publication validation reuse (2026-10-27)

`LockedBoundary` is private, cannot be cloned or serialized, and borrows the
exact transaction and expected writer boundary. Complete plan/checkpoint JSON,
checksum, format, clocks, binding, fence, source positions and ownership are
compared before the capability is created. Plan/checkpoint and binding JSONB are
compared through canonical `jsonb_send` bytes, preserving numeric scale as well
as values: semantic JSONB equality accepts `1.0` as `1`, while integer metadata
decoders reject the floating representation. Both aliases are tested across all
four publication paths. There is no semantic-equality fallback. Physical publication refreshes cached
checkpoint metadata before the next guard. Source ownership is held `FOR SHARE`
through COMMIT; a held-COMMIT regression proves deletion blocks until release.

Checkpoint/maintenance no longer reload and decode the same complete durable
boundary or reacquire lifecycle protection during staging. The separate initial
INSERT keeps conflict/affected-row rejection; existing-membership replacement
requires the locked capability. Candidate validation, captured checkpoint LSN
and published-suffix checks, namespace/extent provenance, epoch CAS, exact sink
checks and uncertain-COMMIT recovery remain. The superseded guard and unlocked
staging path have been removed.

The shared sixteen-mutation fixture now exercises foreground publication,
checkpoint publication after a held upload, full publication and maintenance.
Each rejection preserves exact sink tuple identity, metadata and source ACK.
The existing concurrent-DDL, fencing, cold recovery, extent/GC and before/after
COMMIT checks also pass targeted validation. The integration fixture waits for
its previous approved asynchronous feedback before measuring ACK stability and
keeps setup writes synchronous while leaving the session deliberately off for
actual publication durability assertions. Original failed fixture evidence and
schema dumps remain retained.

Paired production-worker measurements against `3f66250` use 20,000 groups,
60 epochs and four-tick maintenance, with 5 ms downstream PostgreSQL response
delay and matched explicit CHECKPOINTs. With canonical wire comparison, whole
checkpoint elapsed p50 fell from 225.396/242.582 ms to 171.228/172.763 ms in the
two execution orders (24–29%). These events include upload and publication,
rather than pure validation time. Logged statement executions between BEGIN and
COMMIT fell from 11 to 7 for every measured checkpoint. Captured checkpoint
logical times matched within both pairs; one baseline event published an extra
suffix epoch while checkpoint installation finished. Foreground publication
stayed near 34.1–34.4 ms.

Localhost checkpoint p50 was 15.570 versus 15.945 ms and foreground 2.009 versus
2.081 ms: a small local cost. The reused 64-extra-nullable-column source fixture
(2,000 groups, 18 epochs) recorded guard p50 0.774 versus 0.836 ms, total
publication 3.377 versus 3.362 ms, and checkpoint 21.631 versus 21.359 ms.
This identifies the small canonical-comparison cost rather than hiding it behind
network savings. CPU remains inconclusive. Both delayed pairs recorded five
checkpoints per build, localhost four, and the wider trial two. These runs use
maintenance_ticks=4, unlike the preceding source-layout slice's 16; comparisons
are within each pair. Sink row changes and index size matched. Both delayed
pairs and the wider trial recorded identical sink WAL; the uncontrolled local
trial differed by 1,560 bytes (0.13%).

No live freshness or production steady-state claim follows from these component
measurements. Final frozen binaries, scripts, SQL traces and alias checks are
ignored under `artifacts/benchmarks/publication-validation-20261027/canonical/`.
Earlier results using semantic JSONB comparison remain in the parent directory,
explicitly superseded and ineligible for deployment. Failed fixture evidence and
schema dumps are retained there as well.

The corrected canonical-comparison implementation passed independent review and
the final common `./scripts/check.sh` core gate: 648 Rust tests, 147 Python tests,
158 live worker results plus rebuild, 97.14% production line coverage and no
unused dependencies. This includes all 64 shared mutation checks and the
ownership-lock, DDL, replay and uncertain-COMMIT regressions. The disposable
PostgreSQL fixture used tmpfs with no persistent Docker volumes and was removed;
final results and failure evidence were retained.


## Shared relational analysis

The shared analysis now serves sink admission, positional-window ordering and
committed aggregate state selection. It replaces all three earlier walkers.
Candidate keys, unchanged-value identities, NULL facts, equality compatibility
and provable cardinality bounds propagate over resolved edges. Keyed source
projections and key-preserving joins are admitted from these proofs. Pair keys
support independent many-to-many join occurrences; a single-side key does not.
No cost model, join algorithm replacement or new epoch policy is included.
Unsupported shapes remain proof barriers. Details and validation are recorded in
[relational properties](relational-properties.md).


## Maintenance service budgets

[Maintenance service](maintenance-service.md) documents the shared policy, metric
boundaries, cancellation guarantees and fast validation. The implementation adds
no PUTs or alternate execution path. Global sparse-pack reclamation and local
byte/fan-in selection remain #6 work; the existing growth/cost failures stay visible.
