# Source-key statistics

Production PostgreSQL snapshots and complete, ordered CDC transactions can use
scalar aggregate state without retaining the full aggregate input. This follows
the same state-selection principle as RisingWave's `AggStateStorage::Value`
(`src/stream/src/executor/aggregate/agg_state.rs`, inspected at 8184836f24):
linear aggregates update scalar state; retractable extrema need richer state.

The shared relational property analysis proves committed full-row uniqueness on
resolved plan edges. It starts from complete source primary keys, preserves
unchanged keys through filters, maps and rebinding, and establishes the complete
grouping tuple. It also proves pair keys for independent join occurrences and
preserves a side's key only when the other side has at most one matching row.
Expansion requires the source key plus the generated ordinal. UNION ALL drops
key proofs; unsupported internal shapes remain conservative barriers. See
[relational properties](relational-properties.md).

For authoritative snapshots and committed transactions, proven full tuples have
multiplicity zero or one; a consolidated change has weight -1 or +1. This proves
nonnegativity and coefficient representability without reading historical input
tuples. Runtime checks still reject nonunit certified deltas. This policy is
private to production worker construction, never generic weighted execution.

Only grouped COUNT, SUM and AVG paths with this proof omit aggregate input state.
Generic weighted execution retains strict historical validation. Other consumers
can still require an input arrangement; requirements combine across all consumers.
When retained, its append proof stays tied to the exact immutable trace and batch.

The physical revisions `source-key-statistics-v3` and `relational-additive-v3`
include the source contract and certified node set in plan identity. Existing
registrations require an explicit `query rebuild` before using the new binary;
there is no state migration or progress reset fallback. Object publication,
atomic PostgreSQL result/membership/progress publication, transaction ordering
and post-publication acknowledgement are unchanged.

Preparation metrics now record `input_bag_validation` separately from aggregate
operator computation. Overall processing time still includes preparation and
publication phases; it is not pure aggregate arithmetic time. Cache misses and
per-stage measurements are needed to distinguish state-access growth from more
changed rows, more affected groups, maintenance work, or transport latency.
