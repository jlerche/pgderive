# ORDER BY and limited ordering

The SQL frontend accepts ORDER BY at a native or derived SELECT boundary,
including supported joins, groups, windows and DISTINCT. Sort keys must produce
Boolean, int2/int4/int8, UUID, exact numeric, timestamp/timestamptz or qualified
text/varchar values. Each of at most 16 keys has its
own ASC/DESC and NULLS FIRST/LAST contract. PostgreSQL defaults apply: ASC puts
NULL last and DESC puts NULL first. Timestamp infinities and timestamptz instant
ordering use the existing typed codec. Numeric ordering shares the production
Numeric comparator's canonical order key, including scale-equivalent values,
negative infinity, positive infinity and PostgreSQL NaN (greater than infinity).

Text/varchar keys must have the built-in C/POSIX source collation (OID 950/951),
or use explicit `COLLATE pg_catalog."C"` / `COLLATE pg_catalog."POSIX"` in ORDER BY.
Computed text requires explicit COLLATE because its source collation is not
proven. Unqualified built-in C/POSIX names also resolve in the fixed pg_catalog
binding context. Default and other locale/provider collations reject; the engine
does not substitute byte ordering for locale ordering. UTF8 byte comparison
preserves prefixes, case, normalization distinctions and varchar trailing spaces.
Explicit COLLATE resolves its argument in the input scope, as PostgreSQL does;
an output alias cannot be used inside that expression.

ORDER BY accepts output aliases, one-based output ordinals, input columns and
supported expressions. A bare alias takes precedence over an input name; aliases
are unavailable inside expressions. Hidden sort targets use the ordinary target
binder and never become destination columns. DISTINCT requires sort expressions
to resolve to selected values. The existing 64-target budget includes hidden
targets; aggregate expressions can also be referenced by their selected alias.
Explicit ORDER BY COLLATE with DISTINCT rejects because collated SELECT targets
are not yet admitted; use an already C/POSIX-collated source column with ordinary
ORDER BY for this bounded DISTINCT form.

```sql
SELECT b.auction, b.price
FROM public.bid b
ORDER BY b.price DESC NULLS LAST, b.id
LIMIT 10
```

LIMIT (or FETCH FIRST n ROWS ONLY) requires ORDER BY and a literal integer in
0..=1,000,000. OFFSET accepts the same literal domain, with or without LIMIT;
when both occur their sum must fit the 1,000,000 occurrence prefix budget.
OFFSET 0 is a no-op. OFFSET without LIMIT returns the remaining bag and requires
scanning/retaining that tail within execution budgets. NULL/ALL, negative,
computed and out-of-budget counts reject.

Plain SQL ordering permits nonunique keys. Like Feldera's generated comparator,
it orders ties by remaining fields of the complete logical tuple in tuple order,
ascending with NULL first. The logical tuple contains selected values followed
by hidden sort values; source primary keys are not silently added. Comparison
uses native values, with C byte order for text secondary fields. Native-equal
representations retain their raw full identity as a final physical comparison;
numeric display scales are not consolidated away. Weighted multiplicity is
clipped at both the offset and limit boundaries. ROW_NUMBER/positional window
ordering keeps its existing uniqueness proof; this change does not relax it.

After binding and type validation, LIMIT 0 removes its upstream
maps, joins, windows and maintained state. Row-dependent expressions are not
evaluated, including overflowing ABS arguments. Ordered scopes over a proven
empty derived input are pruned too. One source remains as a transaction-tick
anchor; the complete frozen publication contract still governs catalog checks,
capture, plan identity and durable source progress. Constant-expression errors
and unsupported syntax/types can still reject during binding.
The changed sort codec/state and added POSIX semantic attestation require rebuild
of existing registrations with the old plan/environment identity. There is no
compatibility decoder or migration fallback.

Locale-dependent text ordering, WITH TIES, LIMIT ALL/NULL, dynamic limits and custom sort operators reject before
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

Feldera's [generated comparator](https://github.com/feldera/feldera/blob/a2e791e12dd2fe02906a6fa2065520d5fcc73760/sql-to-dbsp-compiler/SQL-compiler/src/main/java/org/dbsp/sqlCompiler/compiler/backend/rust/ToRustInnerVisitor.java)
compares the requested fields first and then remaining tuple fields ascending,
NULL first; [WithCustomOrd](https://github.com/feldera/feldera/blob/a2e791e12dd2fe02906a6fa2065520d5fcc73760/crates/dbsp/src/operator/group/custom_ord.rs)
delegates to that comparator. Pgderive uses the same logical-tuple rule and
retains raw representations to satisfy its PostgreSQL binary-result contract.

Pgderive follows Feldera's global-key, occurrence-top-k, deindex lowering. It
reuses the production ordered selector used by ROW_NUMBER cutoffs. The optional
row-number field is absent for plain LIMIT, so a position change within the
selected prefix does not manufacture result changes. Full ordered input and
selected output remain immutable engine arrangements. The generic selector validates
changed identities even outside the cutoff and at K=0; SQL LIMIT 0 instead uses
the constant-empty lowering above. Nonzero selection merges changed rows with
the retained ordered prefix and subtracts the prior selection. Deleted winners
can therefore expose previously unselected rows after cold restore. This reads
the selected prefix and changed identities, not a full partition sort, but it is
not a constant-work algorithm: work scales with the offset plus K, changes and
storage layout. The shared selector subtracts skipped occurrences directly
from the selected prefix, equivalent to Feldera's two-prefix subtraction.

The deliberate presentation difference is that PostgreSQL result tables retain
native rows rather than Feldera's optional sorted-vector output. Pgderive also
keeps positional-window occurrence proofs and bounded ordering types. SQL LIMIT
uses Feldera's deterministic complete-tuple secondary ordering instead of a
uniqueness restriction. No watermark truncates the retained tail. Durable publication is unchanged:
object PUT, atomic PostgreSQL membership/result/source-progress commit, then ACK.

RisingWave corroborates the selection/presentation distinction in
[LogicalTopN](https://github.com/risingwavelabs/risingwave/blob/3cb1a41aba83ef66d6209b0712fa20b4e0bc6a3a/src/frontend/src/optimizer/plan_node/logical_topn.rs)
and its [materialized-view handler](https://github.com/risingwavelabs/risingwave/blob/3cb1a41aba83ef66d6209b0712fa20b4e0bc6a3a/src/frontend/src/handler/create_mv.rs).
Its retractable [top-n executor](https://github.com/risingwavelabs/risingwave/blob/3cb1a41aba83ef66d6209b0712fa20b4e0bc6a3a/src/stream/src/executor/top_n/top_n_plain.rs)
retains all input in state and uses low/middle/high caches with refill on deletion.
That cache architecture is not introduced here; Feldera remains the algorithmic
reference.

## Qualification

Feldera also eliminates zero-fetch sorts with `SORT_FETCH_ZERO_INSTANCE` in its
[Calcite optimizer](https://github.com/feldera/feldera/blob/a2e791e12dd2fe02906a6fa2065520d5fcc73760/sql-to-dbsp-compiler/SQL-compiler/src/main/java/org/dbsp/sqlCompiler/compiler/frontend/calciteCompiler/optimizer/CalciteOptimizer.java).
The SQL empty lowering follows that rule without weakening the generic
selector's negative-membership validation.

`src/worker/tests/ordering.rs` checks binding, fail-closed forms, normalized
aliases/ordinals, hidden expressions, weighted cutoff clipping, global selection,
nested limit/filter placement and restart/compaction. Existing generic selector
tests independently cover negative tail changes, failed prepare and bounded
prefix reads. `scripts/ordering_harness.py` reuses the top-k transaction history
and compares identical native PostgreSQL materialized-view bags with an
independent raw-source Python sort. Its eighteen modes cover both directions,
NULL placement, empty/unbounded results, computed outputs, groups, DISTINCT,
nested limits, timestamps, timestamptz and integral extremes. Ordered limits after
an inner join include simultaneous changes to both sources and the join cross
term. Ordered limits after a ROWS window independently recompute counts before
sorting. Four zero-limit modes place overflowing ABS inputs behind root,
derived, join and window scopes. All include CDC, durable identity rejection and
cold restart. The harness runs sequentially in the
common core gate.

`scripts/ordering_domains_harness.py` adds fourteen native-type/offset histories:
Boolean, UUID, numeric extrema/specials/display scales, C/POSIX text in both
directions, explicit C on a default-collated input, tied cutoffs, offset-only and
zero-limit offsets. Independent Python ordering uses Decimal/UUID/UTF8 domains.
Its native MV oracle explicitly refines ties with the matching logical-tuple
comparison, instead of assuming PostgreSQL chooses the same arbitrary tied rows;
numeric text refines remaining representation ties. Native send bytes and bags
are both compared through CDC and restart. Rust histories separately exercise
weighted duplicates across offset/limit boundaries, tail refill and compaction.
The numeric kernel is compared against independent scaled-integer arithmetic.
