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

The next slice pipelines lifecycle protection and the boundary check on the same
transaction connection. Biased first polling queues protection first; PostgreSQL
executes separate statements in order, so the boundary snapshot remains fresh
after a lifecycle wait. Both must succeed before source locks, progress, sink DML
and synchronous commit. This removes one serial response wait, not a SQL statement
or a durability barrier. Checkpoint publication is unchanged. Further candidates
include source-table locking and layout inspection, subject to the same ordering
and failure tests; round-trip reduction is not complete.
