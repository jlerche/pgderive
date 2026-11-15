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

## Residual correlation

A correlated `IN` or `NOT IN` subquery can add up to eight residual comparisons
with or without an indexable column equality. The six ordinary comparison operators may be combined with AND, OR,
and NOT. A top-level equality can supply an index key. Equalities inside OR or
NOT stay in the residual predicate. Each residual compares operands from opposite scopes using the existing
native comparison rules. Operands may use total conversions, such as
widening an integer or converting it to numeric. The same correlation is supported for
`EXISTS` and `NOT EXISTS` in projections and
Boolean predicates. A standalone equality-only WHERE can use the dedicated
presence operator. EXISTS discards row-preserving projections, including
column lists, stars, literals, casts, and built-in arithmetic on integer literals, before maintaining presence.
PostgreSQL still validates the original select list during query registration.
Aggregate and set-returning targets are outside this projection subset.
For EXISTS, sorting and DISTINCT ON do not change whether rows exist.
A positive literal LIMIT, LIMIT ALL, and LIMIT NULL also leave presence
unchanged. A literal LIMIT 0 yields FALSE for EXISTS and TRUE for NOT EXISTS.
Plain-column GROUP BY without HAVING, and literal OFFSET 0 or OFFSET NULL,
also preserve presence. A positive literal OFFSET counts qualifying weighted
occurrences for an ungrouped RHS without DISTINCT. For plain-column GROUP BY,
SELECT DISTINCT with explicit column or literal targets, and plain-column
DISTINCT ON, it counts unique key tuples after applying correlation. NULLs
form one key per otherwise equal tuple. Scans and resident keys are bounded
by the existing work and output-byte limits. Combining grouping and DISTINCT
with a positive offset and HAVING remain unsupported. FETCH with a literal
count and WITH TIES has the same presence semantics as ordinary LIMIT.
A positive fetch tests for at least one occurrence after OFFSET, including
when further rows tie with the cutoff. The original ORDER BY is required.
A correlated membership RHS can read native tables through supported
joins with simple local ON predicates. Correlation is extracted from WHERE,
and the RHS keeps a simple projection and supported local filters. Without
a cutoff, repeated comparison values leave membership and quantified truth
unchanged. With equality-only correlation,
ORDER BY, literal LIMIT and OFFSET, and FETCH WITH TIES can select RHS
occurrences separately for each key. A cutoff that can cut through a peer
group requires the sort and correlation keys to determine the compared
values, or to identify each original occurrence. Selection precedes
three-valued comparison and keeps the existing ordering budgets. With residual
correlation, an ordered RHS uses the same ordering codec
and scans the applicable bucket for each outer row. Only qualifying
occurrences consume OFFSET or LIMIT, and only selected values contribute to
the three-valued comparison. Scans charge candidate visits against the
existing work limits. Plain DISTINCT deduplicates compared value tuples
before the cutoff. DISTINCT ON chooses the first qualifying row per key
and then applies the cutoff. Its ORDER BY must follow PostgreSQL's prefix
rule and determine the compared values of each representative. Retained
distinct keys also count against the resident output-byte limit.
Qualify outer references in residual comparisons. A positive EXISTS
conjunct inside another EXISTS can reference qualified native columns
from multiple enclosing levels. The compiler checks each reference in
its original scope and joins the witnesses before evaluating presence.
Direct witness-to-witness column equalities supply native join keys.
It admits up to eight witness sources with row-preserving bounds.
Repeated qualifiers resolve to the closest enclosing source, and the
compiler gives each witness occurrence its own internal alias. Nested EXISTS inside OR or NOT, and other
multilevel correlation shapes, remain unsupported.
The membership probe must be safe to evaluate independently of the RHS.

The mark scans the matching equality bucket, or the shared bucket for a
correlation without a key. Both scans charge candidate visits against the
existing work limits.
Only pairs for which the full residual expression is TRUE contribute. An
OR group is TRUE if any arm is TRUE, even when another arm evaluates to NULL.
NOT preserves NULL.
A qualifying row whose projected value is NULL can still make `IN` and
`NOT IN` NULL. When no pair qualifies, `EXISTS` and `IN` are FALSE and
their negations are TRUE, even for a NULL membership probe. The mark preserves
outer multiplicity and remains valid across duplicate RHS changes and cold reopen.

## Quantified comparisons

A supported single-result global aggregate can feed `IN`, `ANY`, or `ALL`
for ordinary scalar comparisons supported by the binder. Explicit quantified
aggregate inputs require simple column or literal arguments and total
filter and HAVING expressions. This uses the same lowering as a scalar
subquery. A row suppressed
by HAVING or a supported bound gives the empty-input result. A present row
with a NULL aggregate result participates in three-valued comparison.

Uncorrelated explicit `ANY` and `ALL` can also compare against grouped
outputs using the same expression restrictions. The comparison sees one
row per surviving group, including any NULL result. When no group survives,
`ANY` is FALSE and `ALL` is TRUE, even with a NULL probe.

`IN`, `ANY`, and `ALL` also support ordinary GROUP BY and HAVING with
column equality correlation keys. Each outer row compares only against
groups formed from the rows matching its keys. NULL correlation keys
have no matches. Supported ordering and literal row bounds select
rows per key after grouping and HAVING. Plain DISTINCT deduplicates
within each key before a cutoff. Grouping sets and residual correlation
in grouped inputs remain unsupported.

The operators `=`, `<>` (also spelled `!=`), `<`, `<=`, `>`, and `>=`
support `ANY` (also spelled `SOME`) and `ALL` over a one-column subquery. Both operands
may be smallint, integer, bigint, or numeric, including mixed pairs. Comparisons
use exact values and ignore differences in numeric display scale. Unknown probe
literals take the RHS type, and typed operands retain their types. Text and
varchar operands may be mixed when the resolved comparison collation is
a supported built-in bytewise collation (C, POSIX, `ucs_basic`, or `pg_c_utf8`). These comparisons use byte order and keep trailing
spaces significant. Boolean comparisons order FALSE before TRUE. UUID values
compare by their 16 bytes. Dates, timestamps, and timestamps with time zone
can be compared with each other in the engine's UTC session. A date compares
as midnight, including when its finite value exceeds the timestamp range.
Intervals compare by duration using PostgreSQL's 30-day month convention.
A probe can appear in SELECT or WHERE and may use total scalar conversions,
such as widening an integer to numeric. Built-in comparison operators can be
qualified with pg_catalog. The RHS uses the nonaggregate scalar input shape
above and can project total scalar conversions,
with indexable column equalities, the residual comparisons described above, or both. Rows for which the residual expression
is FALSE or NULL are absent from the quantified input.

- `ANY` returns TRUE if any comparison is TRUE. Otherwise it returns NULL
  if any comparison is NULL, or FALSE if every comparison is FALSE or the group is empty.
- `ALL` returns FALSE if any comparison is FALSE. Otherwise it returns NULL
  if any comparison is NULL, or TRUE if every comparison is TRUE or the group is empty.

An empty group has these results even when the probe is NULL.

The mark keeps each outer row's multiplicity. An equality mark without a residual
uses the existing bounded value set. Other marks scan the matching RHS bucket
under the existing work limits. NULL correlation keys have an empty group. PostgreSQL materialized-view and independent raw-source oracles
check correlated and uncorrelated CDC histories, including cold reopen.

### Row-valued membership

A row constructor can compare up to eight fields with a matching RHS
projection. Equality is FALSE if any known pair differs, and otherwise
NULL if any pair is unknown. Ordering compares fields from left to right.
A NULL pair before the first difference makes the comparison unknown.
Each field uses its native scalar comparison domain. Row comparisons scan
the affected correlation bucket under the existing contribution limit.

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

The common gate also runs 14 residual-correlation and 36 quantified-comparison
histories against native PostgreSQL and independent recomputation. The histories
cover both keyed and keyless predicates, projected existence, all six quantifier
comparisons, empty groups, NULL transitions, and cold restart. A focused work
limit test verifies that an over-limit scan leaves the prior epoch available
for retry.
