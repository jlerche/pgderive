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
membership transitions. Pgderive now lowers grouped COUNT(DISTINCT integral),
combined with COUNT/SUM/AVG, through executable expansion, exact sparse statistics,
membership projection, UNION ALL and grouped statistics. Positive-to-positive
count changes produce no membership delta; first and last occurrences do. The
ordinary-row branch retains groups with only NULL or FILTER-excluded distinct
inputs. Negative final occurrence counts fail rather than become absence.

Sparse occurrence counts and total group presence use BigInt in unit-weight
state rows. Full original row identities remain separate until exact aggregation;
two different rows with i64::MAX weight sharing one value therefore count as one
distinct value without premature occurrence-weight overflow. This is a weighted
engine boundary test, not a PostgreSQL capacity measurement. Only public COUNT
results narrow to PostgreSQL bigint. Canonical aggregate-index/value tags separate
multiple distinct arguments and their filters; internal markers cannot collide
with ordinary rows. The compiler binds pg-exact-distinct-membership-v1 and the
worker binds worker-relational-exact-distinct-v3; earlier layouts require rebuild.

A controlled query with 1,000 source rows and ten distinct values updates one row
between two already-present values. With eight-row blocks and cache disabled,
the retained affected-group baseline reads 128 blocks and the new path reads 14,
with identical empty result deltas. These are data-block cache misses during the
operator tick, excluding cold restore, not total object GETs or end-to-end latency.
Tests cover exact large occurrence counts, mixed ordinary aggregates, NULL/FILTER,
value/group changes, malformed state, full-tuple rejection and cold reopen. Live
smallint/integer/bigint and nested scopes plus native q15 match PostgreSQL and
independent memory oracles through retractions and restart.

The path retains original rows in the mixed stream and full rows per distinct
aggregate in its sparse input. It adds persisted state and write work to reduce
recomputation and reads; this measurement does not establish lower storage bytes
or universal throughput. Removing repeated input state needs a separate proof
that complete source-tuple validation still runs before atomic publication.
Distinct expressions mixed with MIN/MAX remain on the retained partition path;
splitting and recombining nonlinear aggregates is additional graph/index work.

Feldera MIN/MAX uses ordered retained value counts and stops at the first nonzero
value. Its SQL variants exclude NULL. A scalar minimum is not retractable when
the current minimum is deleted. Pgderive now uses an ordered full-identity index for grouped MIN/MAX-only queries,
with explicit integral/timestamp codecs, NULL/FILTER sentinels and changed-bag
validation. Mixed aggregate and window paths still scan affected partitions.
Unlike a scalar minimum, retained full identities permit retracting the current
extreme and selecting its successor. The complete-prepare cold-read comparison
is 127 versus 12 blocks, with tick writes increasing from four/918 bytes to
six/1,698 bytes. This reuses immutable object-local fences and keeps PostgreSQL
metadata coarse. It is a read/write tradeoff, not a general throughput result.

At the pinned revision, dynamic/group/topk.rs distinguishes plain
`TopK::transform` (counts full identities while preserving their weights) from
`TopKRowNumber::transform` (expands positive multiplicity into unit occurrences
until K). The latter is the direct precedent for PostgreSQL ROW_NUMBER cutoffs.
Pgderive's OrderedTopK follows occurrence counting and clips multiplicity at K;
it validates every changed identity before merging an ordered prefix. Its SQL
specialization keeps the outer predicate and explicit occurrence-identity proof.
Unfiltered ranking still can change an entire suffix after an early insertion.
RANK/DENSE_RANK, mixed windows and navigation retain affected-partition execution;
this slice does not claim bounded reads for those queries or discard their peers.

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


M7 now [reuses transient sliding COUNT/integral SUM/AVG state](m7-window-work.md)
while recomputing an affected ROWS or peer partition. This removes repeated frame
scans for eligible aggregates, but retains full partition reads and output
retractions. It does not add a radix/range index or adopt watermark eviction.
