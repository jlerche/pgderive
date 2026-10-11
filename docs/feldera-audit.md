# Feldera incremental execution audit

The existing local Feldera checkout was pulled with `git pull --ff-only` without
changing user files. Inspection is pinned to
`b6bf5f997653ae4099a75e7eb77eb1298c89f638`. The implementation is a reference for
algorithms; PostgreSQL semantics and Pgderive's durable publication remain binding.

## Retained partition output

Feldera's
[DiffGroupTransformer](https://github.com/feldera/feldera/blob/b6bf5f997653ae4099a75e7eb77eb1298c89f638/crates/dbsp/src/operator/dynamic/group.rs)
computes the new complete affected group and subtracts its retained output. Its
comments explicitly describe the alternative of retaining only input and
recomputing both old and new outputs. Pgderive previously used that alternative.

Pgderive now supplies partition operators with both prior input and exact prior
output, under the original partition key before downstream projection. The
operator computes the new output once and subtracts the retained output. Both
arrangements belong to the same prior logical boundary and stage with every
other circuit node. Publication remains object PUT, then atomic PostgreSQL
membership/result/progress COMMIT, then slot ACK. Compaction changes physical
membership without changing these semantics or advancing source progress.

The new `Partition::new_zero_preserving` constructor enforces empty input to empty
output by bypassing the callback. The generic constructor and evaluation path
remain available for callbacks where F(empty) is nonempty. Such callbacks cannot
use retained evaluation: the original integrated result is F(state) minus
F(empty), which differs from simply retaining F(state). Unseeded global SQL
aggregates cannot acquire the zero-preserving contract automatically.

Production grouped/window partitions and predecessor lookup use the enforced
contract and retain their intermediate outputs. Their compiler identity adds
`retained-partition-output-v1`; the worker layout uses
`worker-relational-retained-partition-v2`. Old checkpoints for these plans must
use explicit rebuild rather than ordinary restart. Relational plans without
partition/lookup nodes retain their previous runtime revision.

A controlled 1,000-row partition insertion test compares the legacy and retained
paths against identical exact deltas: callback row visits fall from 2,001 to
1,001. This is a callback-work measurement, not an end-to-end throughput or
object-read reduction. Retaining output adds storage and an output-trace read;
new input still requires an affected-partition scan. Tests cover weighted output
collisions, filtered empty output, disappearance/reintroduction, both trace
reopens, clocks, malformed bags, work limits and atomic circuit rejection.

## Remaining algorithms and constraints

COUNT, nullable integral SUM and AVG already use exact additive sufficient
statistics in Pgderive. Feldera's linear aggregation and average implementations
use the same general sum/count abstraction. Integer widths, overflow checks,
NULLs, FILTER and PostgreSQL finalization must follow the actual bound query.

Feldera's distinct operator maintains tuple counts and emits zero/positive
membership transitions. Pgderive's current COUNT(DISTINCT integral) evaluator
still builds a set from the affected partition. Retained per-group/value counts
can remove that scan. Group-presence counts must include rows excluded by NULL
or FILTER so a surviving group can output zero; negative final value counts must
fail rather than be thresholded to absence. This improvement is not yet implemented.

Feldera MIN/MAX uses ordered retained value counts and stops at the first nonzero
value. Its SQL variants exclude NULL. A scalar minimum is not retractable when
the current minimum is deleted. Pgderive still scans affected partitions;
reducing those reads requires an ordered SQL-value index with explicit codecs,
NULL handling and positive integrated-bag validation.

Top-k combines retained output with a cursor that can stop after K ordered
occurrences. Retained output alone does not make Pgderive's rank-filter queries
bounded K scans. Ranking updates can seek the predecessor of the earliest changed
value and update a suffix; inserting an early row can inherently change every
later ROW_NUMBER. LAG/LEAD can use local ordered seeks for fixed offsets. These
read improvements require an ordered occurrence index that preserves PostgreSQL
NULL ordering, peers, tie identity and multiplicity.

Rolling aggregation uses range cursors and radix aggregate state. Watermark-based
truncation cannot be adopted for a PostgreSQL materialized view that permits
arbitrary late inserts, updates and deletes. Revisable range aggregates need
retained history and an index that proves which ranges were affected.

Feldera's SessionRewriteRule lowers SESSION through LAG, a break marker, running
SUM with RANGE peers, grouped bounds and a null-safe join. It is not evidence of
a dedicated bounded-delta session algorithm. Pgderive must preserve the actual
PostgreSQL query's gap comparison, frame, NULL predicate and tie order. Bridging
rows can merge/split components and renumber a suffix; emitted retractions can
be large even with efficient predecessor lookup.

Inspected source locations at the pinned revision include dynamic/aggregate/
average.rs, min.rs and max.rs; dynamic/distinct.rs; dynamic/group.rs and
group/topk.rs, row_number.rs and lag.rs; dynamic/time_series/rolling_aggregate.rs;
and the SQL compiler's SessionRewriteRule.java. No insertion-only optimization,
watermark eviction, recursive circuit or Feldera numeric behavior is implied by
this audit.
