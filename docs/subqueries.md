# Scalar aggregate subqueries and IN

Scalar global-aggregate subqueries and scalar IN/NOT IN now compose the existing
relational engine. Their values can appear in projections and supported predicates,
including expressions and ordinary NOT/AND/OR contexts. Existing function, native
type, source-occurrence and work budgets still apply.

## Scalar aggregation

A scalar subquery must expose exactly one global aggregate result, without an
explicit GROUP BY or set operation. Existing COUNT, SUM, AVG, MIN/MAX, DISTINCT
aggregate arguments and FILTER domains are reused. Empty input returns COUNT 0
and NULL for SUM/AVG/MIN/MAX. HAVING or LIMIT/OFFSET can suppress the single result
row; a suppressed scalar result is NULL. Arbitrary nonaggregate/multirow scalar
queries reject before activation rather than guessing which row to select.

Uncorrelated aggregates lower through existing global-aggregate scopes and a
cardinality-checked LEFT join. Equality-correlated single-native-table aggregates
lower to grouped scopes plus the existing empty-global aggregate plan. A separate
presence/count marker distinguishes a missing group from a real group suppressed
by HAVING. This preserves COUNT 0 for missing groups while returning scalar NULL
when HAVING rejects an existing group. Correlation NULL keys never match RHS NULL
keys: their correlated input is empty.

Outer references must occur in top-level AND equality terms connecting side-local
operands. Computed/composite keys use the shared join binder; at most eight
correlation pairs are admitted. Other RHS filters remain local. Outer references
in aggregate arguments/HAVING, inequality or OR correlation, deeper outer scopes,
explicit grouped scalar queries and nested lifted subqueries remain unsupported.
Correlated ordering/LIMIT is bounded to supported suppressed-global-result cases;
LIMIT 0 and positive OFFSET produce scalar NULL. Inner aliases retain normal
lexical shadowing, and unqualified inner names are local; qualify outer references.

## Membership and NULLs

IN subqueries require one output column and otherwise use supported relational
queries, including CTEs, grouping and set operations. IN lists resolve one common
comparison type before coercing unknown literals; numeric comparisons stay exact.
An independently resolved subquery output retains its type: bigint IN (SELECT
NULL) or bigint IN (SELECT '10') rejects as bigint/text, as PostgreSQL does. Cast
such RHS values explicitly. Unknown probe literals receive the RHS type context through the existing bounded
literal input kernels.

| RHS and comparison | IN | NOT IN |
| --- | --- | --- |
| Empty RHS, including a NULL probe | FALSE | TRUE |
| Any equality is TRUE | TRUE | FALSE |
| Nonempty RHS, NULL probe | NULL | NULL |
| No TRUE equality, but an RHS NULL exists | NULL | NULL |
| Otherwise | FALSE | TRUE |

A retained three-valued mark preserves every outer occurrence; RHS duplicates do
not multiply output rows. Correlated marks restrict matches and NULL poisoning to
the current correlation group. Removing the last match, last NULL or last RHS row
can each change the result. Projection preserves NULL; WHERE retains only TRUE.
An aggregate RHS can contain one NULL row even when its base input is empty;
HAVING/LIMIT suppression instead makes the RHS empty. These cases are distinct.

Marks scan candidate rows in each affected correlation bucket under the existing
work/storage budgets. Uncorrelated membership uses a global bucket; this slice
does not add an indexed or constant-work membership algorithm.

## Evaluation and durability boundaries

Lifted RHS relations and probe maps are dataflow dependencies evaluated eagerly,
including when the opposite input is empty or an outer filter removes its rows.
As with existing computed join maps, PostgreSQL planner-specific error timing is
not promised. Guard unsafe arithmetic inside the RHS with admitted filters/CASE.

Subqueries inside lazy CASE results/defaults/later conditions, later COALESCE
arguments, FILTER-guarded aggregate arguments and explicitly strict constant-NULL
or dominating constant-Boolean guards reject. The compiler does not silently
weaken those guards by evaluating a lifted subquery early. First COALESCE arguments
and mandatory expression positions can lift; CASE over an already derived mark is
also supported. Runtime failure preserves the last complete sink/root/progress
and cannot acknowledge beyond that durable checkpoint.

## Feldera reference and qualification

Feldera a2e791e12dd2fe02906a6fa2065520d5fcc73760 rewrites RexSubQuery to relational
correlation and decorrelates into weighted operators. Its Foodmart scalar tests
and issue7212 correlated NOT IN regression supply the reference empty/NULL rules.
Pgderive reuses its global/group aggregate, LEFT join, scalar comparison and
retained cursor machinery. Mark groups differentiate complete old/new output and
preserve full-tuple outer identity and signed weighted deltas. Publication remains
object PUT, one PostgreSQL result/membership/progress transaction, then slot ACK.

The 75 modes in scripts/subquery_harness.py compare original-SQL native materialized
views, exact send bags, names/OIDs/modifiers/collations and independent raw-source
recomputation. Histories cover empty/global/correlated aggregates, HAVING/FILTER,
scalar suppression, NULL/match/duplicate transitions, numeric/text/boolean/UUID/
time membership, mixed list coercions, labels, RHS ordering aliases and grouping and shadowing, self dependencies,
computed/composite correlations, Boolean projections/filters/derived CASE, CTEs,
set-operation RHS queries, simultaneous source changes and cold restart. A native
lazy-guard witness accompanies fail-closed admission tests; a division-by-zero
history verifies the durable failure boundary. Rust weighted histories add
repeated restart/compaction and correlated COUNT/HAVING/default-row transitions.
