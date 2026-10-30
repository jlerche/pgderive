# Outer joins and USING

The shared native join tree accepts INNER, LEFT, RIGHT and FULL joins between
publication tables and supported non-LATERAL derived scopes. Projection lists
remain explicit. Existing local ON expressions, WHERE, grouping, windows and
ORDER BY/LIMIT compose under their existing restrictions.

RIGHT lowers to the existing LEFT machinery with physical inputs exchanged;
logical column names and output order remain those of the original statement.
FULL uses the shared retained outer-join cursor and differentiates old/new
affected-key outputs. It emits passing pairs, unmatched left occurrences and
unmatched right occurrences. NULL extension includes every column of the missing
scope, including hidden qualified originals and earlier merged USING keys.

Only TRUE satisfies ON. FALSE and UNKNOWN do not match, and NULL equality keys
on opposite inputs remain separate occurrences. Duplicate passing tuples multiply
pair weights; each unmatched occurrence retains its original multiplicity.
Match existence is per full tuple, so residual predicates can distinguish two
tuples with the same navigation key. WHERE runs after the join: filtering a
matched pair never creates a new unmatched row.

Complete committed transactions include changes on both inputs and preserve the
join cross term. Source primary-key checks remain separate from generic weighted
identity. A FULL join supplies no uniqueness proof merely because its inputs
have keys; null extension and fanout must retain their actual occurrence semantics.

## USING names, values and metadata

USING accepts up to 16 unique names, each unambiguously present on both inputs.
Each name exposes one merged unqualified output, while qualified originals such
as `a.k` and `b.k` remain independently addressable. A subsequent join can use
the earlier merged key. Missing, repeated or ambiguous USING names reject.

Columns reconcile through the supported native common types and local casts:
integral widths, integral/numeric combinations, text/varchar, and supported
same-type Boolean, UUID and timestamp domains. Qualified originals retain their
own types, modifiers, collations and values. Equal source typmods can survive the
merge; differing modifiers or promoted types produce the appropriate unconstrained
common typmod.

LEFT selects the coerced left value, RIGHT the coerced right value, and FULL
coalesces left then right. INNER prefers an operand already represented as the
common-typed native column; this can preserve the right numeric value's display
scale when the left needs coercion. For example, bigint `1` joined to unconstrained
numeric `1.0000` exposes `1.0000` for INNER/RIGHT and numeric `1` for LEFT/FULL.
Numeric equality still compares canonical values and preserves each original row.

Comparison collation and selected output collation are distinct. For text with
default collation on the left and C on the right, the comparison uses C, while
the selected INNER/LEFT value can retain default collation; RIGHT/FULL use C.
Existing deterministic/compatible collation checks apply. Explicit COLLATE syntax
and unsupported locale comparisons retain their existing rejection boundaries.

NATURAL joins, aliases on a whole join or USING clause, lateral derived references
and wildcard projection are not added. FULL ON clauses require at least one
supported cross-input equality key or a fully constant condition. The compiler
rejects keyless nonconstant conditions, including inequality-only and OR-only
FULL joins, before activation; preparing a result description alone does not
prove PostgreSQL can plan them. Constant TRUE/FALSE/NULL and supported equality
plus residual predicates are qualified. RIGHT retains the existing supported
inequality-only and Boolean predicate domains.

## State, costs and reference

The implementation follows Feldera a2e791e12dd2fe02906a6fa2065520d5fcc73760's
matched-join plus unmatched-side construction in
`CalciteToDBSPCompiler.visitJoin`. Physical RIGHT normalization and shared
affected-key differentiation implement the same bag result without a second
maintenance engine. Existing work, output, record and resident-state budgets
remain in force. Keyless matches can inspect the Cartesian product, and FULL
right-unmatched checks can revisit candidate pairs; no new range index or
constant-cost outer-join claim is introduced.

The shared serialized outer specification now records both nullable scopes and
FULL behavior. Existing plans using that specification require the ordinary
explicit rebuild, including scalar-subquery plans that lower through an outer
join. This is a hard cutover, without a compatibility decoder. Durable publication
remains immutable object PUT, one PostgreSQL result/membership/progress transaction,
then logical-slot acknowledgement.

## Qualification

The 43 cases in `scripts/outer_join_harness.py` compare the original SQL's native
materialized view, native send bytes, output labels/types/modifiers/collations and
an independent raw-source nested-loop Counter oracle. They cover RIGHT/FULL ON
conditions and derived scopes, composite/chained USING and USING between joined
subtrees, mixed native types,
numeric display scales, same/different typmods, C/default collation selection,
qualified originals, post-join WHERE, aggregation and ordered LIMIT.

Histories change both inputs in one transaction, insert new matching pairs,
remove last matches, retain duplicate projected tuples, change NULL keys, empty
either side and both sides, and restart from retained object state. The chained
fixture captures a third table with third-only rows and verifies NULL extension
of all earlier columns. Pre-activation checks reject unsupported/ambiguous scope
forms before registration or slot creation. Seven local oracle unit tests check
bag multiplicity, NULLs, match status, output scale selection and nested extension.
