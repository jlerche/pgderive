# Join keys and ON predicates

INNER, LEFT, RIGHT and FULL joins over native tables and supported non-LATERAL derived scopes
accept the existing local scalar predicate domain in ON. This includes AND, OR,
NOT, NULL tests, constants, inequalities and expressions referencing both inputs.
Only TRUE matches; FALSE and UNKNOWN do not. [Outer joins and USING](outer-joins.md)
extend the shared match machinery and name/type binding. NATURAL joins, lateral
references and arbitrary subqueries remain outside this slice.

## Keys and types

Top-level conjunctive equalities between side-local expressions become navigation
keys when their native domains agree. Numeric keys compare canonical values, so
1.20 and 1.2000 match without erasing their original tuple representation. Numeric
modifiers need not match. Computed numeric and text expressions use the existing
bound scalar kernels. Text/varchar keys share byte equality under compatible
deterministic collations; different varchar lengths do not change equality.
Mixed integral/numeric comparisons use exact numeric comparison. Conditions that
cannot become keys remain predicates over candidate pairs; a keyless condition
uses the existing Cartesian join key and work limits.

Text predicate binding follows source and derived expression collation lineage.
Conflicting collations reject. Text ordering requires C/POSIX byte order; no locale
comparison approximation is introduced. Ordinary join comparisons reject explicit
`COLLATE`. `LIKE` predicates accept the built-in C and POSIX collations.
Existing strictness, resource budgets and unsupported-overload checks still apply.

Key maps retain complete source rows and evaluate on each input row that passes
that input's filters. A key-expression error can therefore occur even with an
empty opposite input, as under a merge join; guard unsafe expressions with a
filter or CASE. Residuals over both inputs evaluate on candidate pairs. The
[planner rewrites](sql-compiler.md#planner-rewrites) place every other condition.
Failed evaluation preserves the last published sink, root and
source progress, without acknowledging beyond that durable checkpoint.

## Incremental match semantics

INNER joins retain the existing three-term weighted delta join, followed by the
residual predicate. LEFT joins without residuals retain their existing optimized
cursor. LEFT joins with residuals differentiate the old and new affected-key
outputs. Each full left tuple tracks whether any right tuple passes ON; NULL
extension happens only when none pass. Two left tuples with the same navigation
key can therefore have different match status. Duplicate passing right rows
multiply output occurrences; they do not multiply unmatched rows.

This follows Feldera a2e791e12's separation of equality keys and residual match
conditions in CalciteToDBSPCompiler.visitJoin. Its matched-join/antijoin construction
and this engine's affected-key differentiation implement the same bag semantics.
Residual LEFT evaluation can scan all candidate pairs in an affected key bucket;
keyless predicates can scan the whole Cartesian product. Existing work/storage
budgets bound this cost. This slice does not add a range join index.

The LEFT join plan records its ON predicate explicitly, including its absence.
Existing LEFT join registrations therefore require an explicit rebuild before
restarting with this worker; their captured physical plan identity changes.
The RIGHT/FULL extension also changes the shared outer specification; see its
[rebuild and qualification contract](outer-joins.md).

## Qualification

The 29 modes in scripts/join_predicate_harness.py compare the original SQL's
native materialized view, binary send values, output names/types/modifiers/
collations and an independent raw-source nested-loop oracle. They cover numeric
scales, differing typmods, NaN/infinity, computed text collisions, text/varchar,
derived text lineage, same-side equality, residual and keyless joins, constant ON conditions, per-left
match transitions, nested LEFT joins, aggregation and top-K. Histories change both
inputs in one transaction, remove the last passing match, preserve duplicate
occurrences, empty the right side and restart from object-backed state. A residual
division-by-zero case checks native failure and unchanged sink/progress/ACK.

The expanded Nexmark Q8 harness adds native PostgreSQL and independent grouped-pair
histories for OR and non-equality joins between temporal derived scopes. Its eight
modes cover local/instant timestamps, C/ICU output collations, NULLs, infinities,
duplicate seller memberships, simultaneous changes and cold restart.
