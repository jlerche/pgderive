# Source-key statistics

Production PostgreSQL snapshots and complete, ordered CDC transactions can use
scalar aggregate state without retaining the full aggregate input. This follows
the same state-selection principle as RisingWave's `AggStateStorage::Value`
(`src/stream/src/executor/aggregate/agg_state.rs`, inspected at 8184836f24):
linear aggregates update scalar state; retractable extrema need richer state.

Pgderive's compiler proves that the canonical row retains every column of one
source primary key through filters, appended computed columns, navigation-key
changes and injective rebinding. Canonical rows are distinct from navigation
prefixes. For authoritative snapshots and committed transactions each full tuple
then has multiplicity zero or one; a consolidated change has weight -1 or +1.
This proves both nonnegativity and coefficient representability without reading
historical input tuples. Runtime checks still reject nonunit certified deltas.
The source policy is private to the production worker construction path; it is
not a replacement for validating arbitrary weighted inputs.

Only grouped COUNT, SUM and AVG paths covered by this proof omit aggregate input
state. Joins, unions, expansion, distinct operators, windows and loss of any
primary-key field stop the proof. Generic weighted execution retains strict
historical validation. Other consumers can still require an input arrangement;
state requirements are combined across all consumers. When such an arrangement
is retained, its append proof remains tied to the exact immutable trace and batch.

The physical revision `source-key-statistics-v1` prevents reopening older layouts
as though they had this invariant. Existing affected registrations require an
explicit `query rebuild`; there is no silent state migration or progress reset.
Object publication, atomic PostgreSQL result/membership/progress publication,
source transaction ordering and post-publication acknowledgement are unchanged.

Preparation metrics now record `input_bag_validation` separately from aggregate
operator computation. Overall processing time still includes preparation and
publication phases; it is not pure aggregate arithmetic time. Cache misses and
per-stage measurements are needed to distinguish state-access growth from more
changed rows, more affected groups, maintenance work, or transport latency.
