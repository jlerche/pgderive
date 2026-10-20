# Durable prefix RANGE windows

Pgderive maintains a persistent ordered-data arrangement and additive binary
prefix summaries for this selected family:

- One ascending, nonnullable integral ordering column (`smallint`, `integer` or
  `bigint`).
- `RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW`, including its default-frame
  spelling; equal ordering values form one peer group.
- COUNT, integral SUM and integral AVG, including admitted FILTER/argument
  expressions and NULL arguments. Up to 64 aggregate expressions fit the shared
  statistics codec. Partition keys retain the existing SQL equality semantics.

Other window forms keep their existing exact in-engine evaluator: descending or
nullable ordering, ROWS/GROUPS frames, nonintegral order, multiple ordering columns,
MIN/MAX, numeric SUM/AVG and other aggregates are not indexed by this optimization.
There are no PostgreSQL computation calls or SQL views implementing the state.

## State and execution

The compiler emits `PrefixInput`, `PrefixStatistics` and `PrefixWindow` nodes.
Ordered data keys append a canonical biased signed-integer ordering token to the
partition key, retaining the entire raw tuple and its multiplicity. The fused
`IndexedStatistics` operator validates changes against that full-tuple data
arrangement before expanding each changed row into its 65 binary-prefix nodes.
Only the sufficient statistics are persisted per node: row count and each
aggregate's non-NULL count/sum, all exact intermediate integers. There is no
persisted 65-fold copy of raw input merely to validate aggregate contributions.

The window operator receives both complete data and summary deltas at the same
logical tick, and both old arrangements. New summary lookups overlay the summary
replacement delta on the old snapshot; old lookups use only the old snapshot.
An inclusive prefix query decomposes the ordering range into at most 65 disjoint
subtrees. Existing SQL aggregate finalization supplies checked COUNT, SUM types
and AVG rounding/scale behavior.

For each affected partition, only raw rows at or after the minimum changed order
can change their prefix results. Inclusive range cursors skip disjoint immutable
roots and earlier/later blocks. The new-row cursor overlays that partition's
signed delta on its old ordered range without materializing the partition.
Transaction deltas are routed once into per-partition slices; there is no scan of
the complete tick delta per affected partition. Output is exactly the weighted
new affected result minus the weighted old affected result.

Every changed raw identity is checked even if its range has no surviving output.
Prepared objects and result deltas use the existing PUT, PostgreSQL publication,
then slot-acknowledgement protocol. Overflow, invalid retractions, storage or work
errors leave the old visible roots and logical time intact. The new graph/codec
identity rejects old eligible-window checkpoints; use explicit `query rebuild
CONFIG.toml`. Unchanged window layouts keep their existing identities.

## Evidence

The actual Rust trie path/cover functions are compared against 21,000 independent
full signed-order scans over 3,000 mutations, including signed i64 extremes,
duplicate orders, weights, NULLs and retractions. Worker histories compare a
separate raw-source model and the generic frame evaluator through peer changes,
NULL partitions, empty ticks/groups, cold reopen and compaction. SUM(int4) and
COUNT overflow tests require unchanged checkpoints and a successful same-tick
retry. A composed downstream positional window checks preserved occurrence
lineage and aggregate namespaces.

`scripts/prefix_window_harness.py` runs six live cases: three integral types with
ordered and peer histories. Each compares PostgreSQL materialized-view binary
bags and output metadata, independent source/frame models, rollback and cold
restart. Published checkpoints must contain both ordered-data and prefix-summary
arrangements, preventing qualification from silently exercising another path.
Initial fixture failures remain under `artifacts/worker/prefix-targeted-1`; the
corrected standalone qualification is `artifacts/worker/prefix-targeted-2`.
The six cases are also part of the common core gate.

## Costs and limits

A complete cold tick updating the last row of a 4,096-row prefix window, with
one run, eight-row blocks, disabled caches and cycling integral values, measured:

| Implementation | Block misses | New objects | New encoded bytes |
| --- | ---: | ---: | ---: |
| Existing affected-partition/sliding evaluator | 1,030 | 6 | 1,772 |
| Durable prefix index | 345 | 22 | 13,182 |

Both emitted identical exact deltas. The index also completed that tick with a
1,000-contribution per-operator budget that rejected the partition evaluator.
This is a read/work improvement with increased writes; it is not a throughput,
latency, RSS or universal capacity claim. Independent core tests still measure
multi-run amplification and compaction for joins/group probes.

An early change can legitimately affect almost every following row. Those output
rows and their prefix queries still require work. Overlapping runs increase each
lookup's reads until compaction. The tree has fixed 64-bit height, not a claim of
constant work for an arbitrary SQL frame. Up to 65 summary nodes per distinct
ordering value may be retained; common prefixes share nodes. Bounded contribution
expansion, exact consolidation and configured entry/byte/work caps still apply,
including bootstrap. Small partitions can cost more than the prior evaluator.
Compaction scans and uploads physical state separately from query update work.

See the [complete adaptation scope](feldera-adaptation-roadmap.md) and its current
[qualification evidence](feldera-adaptation-evidence.json). The common gate passed 337 Rust tests, 45 Python tests and 133 worker cases plus
rebuild, with 97.45% production line coverage. The full release profile was not run.
