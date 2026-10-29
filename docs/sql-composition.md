# HAVING, nonrecursive CTEs and UNION ALL

These forms compose the existing typed scalar, grouped, join, window and ordered
operators. They execute inside Pgderive; PostgreSQL remains the source, metadata
and publication destination, rather than a computation fallback. Existing type,
expression and execution budgets still apply.

## HAVING

HAVING tests each completed group, after WHERE and aggregation and before output
windows, DISTINCT and outer ORDER BY/LIMIT. Only SQL TRUE retains a group; FALSE
and UNKNOWN remove it. Aggregates referenced only by HAVING are internal fields,
not extra destination columns. Grouping expressions remain available to HAVING
even when they are absent from SELECT.

```sql
SELECT b.auction, COUNT(*) AS bids
FROM public.bid b
GROUP BY b.auction
HAVING COUNT(*) >= 2 AND SUM(b.price) > 100
ORDER BY bids DESC, b.auction
LIMIT 10;
```

Output aliases do not resolve inside HAVING, matching PostgreSQL. Use the actual
aggregate or grouping expression. Window calls inside HAVING and nested ordinary
aggregates reject. Input columns must resolve to grouping keys; this lowering does
not add functional-dependency inference for other grouped columns. Supporting
HAVING does not admit overloads outside the existing typed domain.
The restricted EXISTS inner query and specialized predecessor-lookup SELECT
forms still reject HAVING; the compiler does not discard those clauses.

Without GROUP BY, an aggregate or HAVING creates one implicit group. On empty
input, COUNT is zero and SUM/AVG/MIN/MAX are NULL. HAVING then decides whether that
single row survives. A constant-TRUE HAVING also creates the implicit group even
without an aggregate call. Grouped queries with actual grouping keys still produce
no groups on empty input.

The implementation preserves the generic partition rule that empty input maps to
empty output. An internal seed keeps the global partition present; aggregate
contributions ignore that seed. A constant operator emits its positive bag once
at logical time one. Snapshot transport chunks do not emit extra seeds, and
subsequent transactions and checkpoint recovery do not reinsert them. This also
provides constant SELECT inputs without FROM.
Constant queries still require a nonempty native publication contract; a source
relation supplies transaction ticks, while its row values do not create or remove
the constant result.

## Nonrecursive CTEs

WITH names ordinary derived queries. Earlier CTEs are visible to later siblings;
inner WITH scopes can shadow outer names. Explicit aliases and column names are
resolved before ordinary compilation. Repeated references preserve separate
relational occurrences over the same synchronized source history.

```sql
WITH counts AS (
    SELECT b.auction, COUNT(*) AS bids
    FROM public.bid b
    GROUP BY b.auction
    HAVING COUNT(*) >= 2
)
SELECT c.auction, c.bids
FROM counts c
ORDER BY c.bids DESC, c.auction
LIMIT 10;
```

The lowering expands references into the existing derived-scope path, with
bounded expansion. It does not introduce separately scheduled queries, extra
logical ticks or a PostgreSQL temporary-table evaluator. Schema-qualified source
names continue to identify physical publication relations rather than CTE names.
Recursive definitions and data-modifying CTEs are outside this feature.
Expansion is bounded to 64 levels, 128 SELECT scopes and 1 MiB of expanded query
data, in addition to the ordinary SQL/scanner and source-occurrence budgets.

## UNION ALL

UNION ALL combines rows positionally and preserves every occurrence, including
NULL-valued duplicates. Branch column names may differ; result labels come from
the left branch. Types are reconciled before branches enter the existing weighted
Union node. No primary-key uniqueness is inferred merely because each individual
branch has a unique key.

Common-type resolution includes integral/numeric widening, compatible text/varchar
inputs and timestamp/timestamptz under the attested UTC context. Direct unknown
NULL/string literals receive the union's type context; values already resolved by
an inner query keep that inner type. Equal compatible modifiers are retained and
differing modifiers are cleared. Incompatible native type domains reject before
activation. Collation-dependent consumers retain their existing admission rules;
the compiler does not replace an indeterminate or unsupported SQL collation with
byte ordering. Contextual integral string literals support signed decimal input,
rather than every PostgreSQL input spelling.

```sql
WITH activity AS (
    SELECT b.auction AS auction FROM public.bid b
    UNION ALL
    SELECT a.id FROM public.auction a
)
SELECT x.auction, COUNT(*) AS occurrences
FROM activity x
GROUP BY x.auction
HAVING COUNT(*) >= 2;
```

Both branches can change in the same committed source transaction. Their deltas
are combined at the same logical boundary; equal tuples add weights rather than
overwrite each other. Retractions remove only the corresponding occurrences.

A parenthesized branch's ORDER BY/LIMIT applies to that branch. Outer
ORDER BY/LIMIT/OFFSET applies to the combined result. PostgreSQL's set-operation
ordering restrictions still apply. Destination tables remain relational bags;
consumers must specify their own ORDER BY when reading them.

UNION without ALL, INTERSECT and EXCEPT are separate capabilities.

## Feldera reference and qualification

The local reference is Feldera
`a2e791e12dd2fe02906a6fa2065520d5fcc73760`, also used for the ordering audit.
`CalciteToDBSPCompiler` lowers UNION ALL to `DBSPSumOperator` after tuple casts,
and filters retain only TRUE. CTE references become ordinary relational plans
under name resolution.

Feldera's `DBSPAggregateZeroOperator` and `ExpandAggregateZero` supply the default
row for an empty global aggregate. `IncrementalizeVisitor` differentiates the
constant input; Pgderive's once-at-time-one constant is the corresponding delta
representation. Pgderive uses a retained seed with its existing aggregate
operators. PostgreSQL alias rules take precedence over Calcite dialect extensions.

`scripts/sql_composition_harness.py` compares original SQL materialized views,
output names/types/modifiers/collations and an independent raw-source Counter
oracle through snapshot, updates, deletions, empty input and cold restart. Its
27 modes cover hidden HAVING aggregates, NULL filters, implicit groups, CTE chains,
shadowing and repeated references, union multiplicity and common types, branch
limits, windows, grouped unions and all three features composed with top-k.
Transactions change both union sources together. The common core and full gates
include this qualifier. Failures retain the owned fixture and logs.

Durable publication remains object PUT, one PostgreSQL membership/result/progress
commit, then logical-slot acknowledgement. No feature adds recursion, frontiers,
wall-clock expiry or a new publication path.
