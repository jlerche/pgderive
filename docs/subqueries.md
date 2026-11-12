# Scalar and quantified subqueries

Scalar subqueries and scalar IN/NOT IN compose the existing
relational engine. Their values can appear in projections and supported predicates,
including expressions and ordinary NOT/AND/OR contexts. Existing function, native
type, source-occurrence and work budgets still apply.

## Scalar aggregation

A scalar aggregate subquery exposes one global aggregate result, without an
explicit GROUP BY or set operation. Existing COUNT, SUM, AVG, MIN/MAX, DISTINCT
aggregate arguments and FILTER domains are reused. Empty input returns COUNT 0
and NULL for SUM/AVG/MIN/MAX. HAVING or LIMIT/OFFSET can suppress the single result
row; a suppressed scalar result is NULL.

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

## Nonaggregate scalar cardinality

A nonaggregate scalar subquery exposes one column. An empty result yields NULL,
one occurrence yields its value (which can be NULL), and multiple occurrences
raise a cardinality error, including identical values with weight greater than
one. The initial slice supports column and literal projections, simple local
predicates, constant VALUES, and supported set operations over these inputs.
Equality-correlated RHS queries use a native table and indexable column
equalities. Correlated DISTINCT, ordering, and LIMIT/OFFSET are outside this
slice. The RHS is maintained independently, so admission excludes fallible
row-dependent expressions that would otherwise run without outer demand.

The keyed join maintains the RHS cardinality independently for each matching
key and preserves each outer row's weight. A scalar read checks the cardinality
marker at the expression's evaluation site. An unused key can have multiple
rows without failing the query, and NULL correlation keys have no match.
Failed evaluations leave the result and progress at the last successful epoch.
The worker can publish again after a source change repairs the cardinality.

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

Marks read each affected correlation bucket under the existing work/storage
budgets. Without a residual correlation, one scan collects the bucket's RHS values
into a resident set bounded by the output-byte limit, so a tick costs time linear in the bucket
for equality comparisons whose sides share a value domain. Uncorrelated membership uses
a global bucket, so its tick still reads every outer and RHS row. A residual
correlation or a cross-domain date/timestamp comparison scans the RHS per outer row.

## Inequality correlation

A correlated `IN` or `NOT IN` subquery can add up to eight ordinary inequalities
(`<`, `<=`, `>`, `>=`, or `<>`) as AND terms alongside an indexable
column equality. Each inequality compares a column from each scope using the
existing native comparison rules. The same correlation is supported for
`WHERE EXISTS (SELECT 1 ...)` and `WHERE NOT EXISTS (SELECT 1 ...)`.
The RHS uses a native table, a simple projection, and supported local predicates.
The membership probe must be safe to evaluate independently of the RHS.

The mark scans the matching equality bucket under the existing work limits.
Only pairs for which every inequality is TRUE contribute. A NULL inequality
operand therefore cannot establish existence or make an `IN` result NULL.
A qualifying row whose projected value is NULL can still make `IN` and
`NOT IN` NULL. When no pair qualifies, `EXISTS` and `IN` are FALSE and
their negations are TRUE, even for a NULL membership probe. The mark preserves
outer multiplicity and remains valid across duplicate RHS changes and cold reopen.

## Integral quantified comparisons

The operators `=`, `<>` (also spelled `!=`), `<`, `<=`, `>`, and `>=`
support `ANY` (also spelled `SOME`) and `ALL` over a one-column subquery. Both operands
must be smallint, integer, or bigint. Unknown probe literals take the RHS type, and typed integral
operands retain their widths. A simple column or literal probe can appear
in SELECT or WHERE. The RHS uses the nonaggregate scalar input shape above,
with indexable column equalities for correlation. A correlated query may add
the inequalities described above. Rows rejected by any inequality are absent
from the quantified input, including for NULL propagation and empty groups.

- `ANY` returns TRUE if any comparison is TRUE. Otherwise it returns NULL
  if any comparison is NULL, or FALSE if every comparison is FALSE or the group is empty.
- `ALL` returns FALSE if any comparison is FALSE. Otherwise it returns NULL
  if any comparison is NULL, or TRUE if every comparison is TRUE or the group is empty.

An empty group has these results even when the probe is NULL.

The mark keeps each outer row's multiplicity. An equality mark without a residual
uses the existing bounded value set. Other marks scan the matching RHS bucket
under the existing work limits. NULL correlation keys have an empty group. PostgreSQL materialized-view and independent raw-source oracles
check correlated and uncorrelated CDC histories, including cold reopen.

## Evaluation and durability boundaries

Lifted RHS relations and probe maps are dataflow dependencies evaluated even when
the opposite input is empty. A constant equality on the probe filters the RHS, and
the [planner rewrites](sql-compiler.md#planner-rewrites) place outer filters.
Guard unsafe arithmetic inside the RHS with admitted filters/CASE.

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
