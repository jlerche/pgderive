# ORDER BY and limited ordering

The SQL frontend accepts ORDER BY at a native or derived SELECT boundary,
including supported joins, groups, windows and DISTINCT. Sort keys must produce
int2/int4/int8, timestamp or timestamptz values. Each of at most 16 keys has its
own ASC/DESC and NULLS FIRST/LAST contract. PostgreSQL defaults apply: ASC puts
NULL last and DESC puts NULL first. Timestamp infinities and timestamptz instant
ordering use the existing typed codec.

ORDER BY accepts output aliases, one-based output ordinals, input columns and
supported expressions. A bare alias takes precedence over an input name; aliases
are unavailable inside expressions. Hidden sort targets use the ordinary target
binder and never become destination columns. DISTINCT requires sort expressions
to resolve to selected values. The existing 64-target budget includes hidden
targets; aggregate expressions can also be referenced by their selected alias.

```sql
SELECT b.auction, b.price
FROM public.bid b
ORDER BY b.price DESC NULLS LAST, b.id
LIMIT 10
```

LIMIT (or FETCH FIRST n ROWS ONLY) requires ORDER BY and a literal integer in
0..=1,000,000. Positive limits require the explicit sort keys to cover a proven
logical occurrence key. For example, include the source primary key after a
nonunique price, or all grouping keys after an aggregate. SQL primary-key proofs
only govern admission; the runtime still preserves complete weighted tuples and
clips duplicate occurrence weights at the cutoff. LIMIT 0 is empty and needs no
uniqueness proof. Text/collation, numeric, Boolean and UUID ordering, OFFSET,
WITH TIES, LIMIT ALL/NULL, dynamic limits and custom sort operators reject before
activation. Other execution and publication budgets still apply.

The destination remains an ordinary PostgreSQL table. ORDER BY without LIMIT
validates the sort expressions and preserves the entire result bag. It does not
create a sequence-valued result or guarantee table scan order. Readers must
specify ORDER BY when selecting from the destination, including for limited
results. A limited derived scope selects its rows before an outer filter, sort,
group or join; downstream predicates cannot pass through that selection.

## Feldera reference and implementation

The local reference checkouts were updated with `git pull --ff-only` on
2026-10-28; RisingWave's pre-existing untracked file was preserved. Inspected
revisions are Feldera `a2e791e12dd2fe02906a6fa2065520d5fcc73760` and RisingWave
`3cb1a41aba83ef66d6209b0712fa20b4e0bc6a3a`.

Feldera's [visitSort](https://github.com/feldera/feldera/blob/a2e791e12dd2fe02906a6fa2065520d5fcc73760/sql-to-dbsp-compiler/SQL-compiler/src/main/java/org/dbsp/sqlCompiler/compiler/frontend/CalciteToDBSPCompiler.java)
indexes all rows under the empty key and lowers LIMIT to ROW_NUMBER top-k with
an output producer that returns only the row. Its
[TopKRowNumber](https://github.com/feldera/feldera/blob/a2e791e12dd2fe02906a6fa2065520d5fcc73760/crates/dbsp/src/operator/dynamic/group/topk.rs)
counts positive weighted occurrences, stopping at K. OFFSET uses prefix
subtraction. For ordered presentation, visitSort can collect the result into a
single sorted vector; its ignoreOrderBy path retains the relational bag.

Pgderive follows Feldera's global-key, occurrence-top-k, deindex lowering. It
reuses the production ordered selector used by ROW_NUMBER cutoffs. The optional
row-number field is absent for plain LIMIT, so a position change within the
selected prefix does not manufacture result changes. Full ordered input and
selected output remain immutable engine arrangements. Changed identities are
validated even outside the cutoff and at K=0; selection merges changed rows with
the retained ordered prefix and subtracts the prior selection. Deleted winners
can therefore expose previously unselected rows after cold restore. This reads
the selected prefix and changed identities, not a full partition sort, but it is
not a constant-work algorithm: work scales with K, changes and storage layout.

The deliberate presentation difference is that PostgreSQL result tables retain
native rows rather than Feldera's optional sorted-vector output. Pgderive also
keeps its existing reproducible-selection admission rule and bounded ordering
types. No watermark truncates the retained tail. Durable publication is unchanged:
object PUT, atomic PostgreSQL membership/result/source-progress commit, then ACK.

RisingWave corroborates the selection/presentation distinction in
[LogicalTopN](https://github.com/risingwavelabs/risingwave/blob/3cb1a41aba83ef66d6209b0712fa20b4e0bc6a3a/src/frontend/src/optimizer/plan_node/logical_topn.rs)
and its [materialized-view handler](https://github.com/risingwavelabs/risingwave/blob/3cb1a41aba83ef66d6209b0712fa20b4e0bc6a3a/src/frontend/src/handler/create_mv.rs).
Its retractable [top-n executor](https://github.com/risingwavelabs/risingwave/blob/3cb1a41aba83ef66d6209b0712fa20b4e0bc6a3a/src/stream/src/executor/top_n/top_n_plain.rs)
retains all input in state and uses low/middle/high caches with refill on deletion.
That cache architecture is not introduced here; Feldera remains the algorithmic
reference.

## Qualification

`src/worker/tests/ordering.rs` checks binding, fail-closed forms, normalized
aliases/ordinals, hidden expressions, weighted cutoff clipping, global selection,
nested limit/filter placement and restart/compaction. Existing generic selector
tests independently cover negative tail changes, failed prepare and bounded
prefix reads. `scripts/ordering_harness.py` reuses the top-k transaction history
and compares identical native PostgreSQL materialized-view bags with an
independent raw-source Python sort. Its twelve modes cover both directions,
NULL placement, empty/unbounded results, computed outputs, groups, DISTINCT,
nested limits, timestamps, timestamptz and integral extremes, including
CDC, durable identity rejection and cold restart. It runs sequentially in the
common core gate.
