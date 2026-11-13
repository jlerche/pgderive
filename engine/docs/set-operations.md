# SQL set operations

UNION (including UNION DISTINCT), INTERSECT, INTERSECT ALL, EXCEPT and EXCEPT ALL
compose the existing UNION ALL branch binding and weighted engine. Omitted ALL
means DISTINCT. The output names come from the left operand; columns reconcile by
position using the existing native common-type rules, not by label.

For each complete SQL tuple, let L and R be the nonnegative integrated occurrence
counts on the two sides. NULLs compare equal for set membership.

| Operation | Output occurrences |
| --- | --- |
| UNION ALL | L + R |
| UNION / UNION DISTINCT | 1 when L + R > 0, otherwise 0 |
| INTERSECT / INTERSECT DISTINCT | 1 when L > 0 and R > 0, otherwise 0 |
| INTERSECT ALL | min(L, R) |
| EXCEPT / EXCEPT DISTINCT | 1 when L > 0 and R = 0, otherwise 0 |
| EXCEPT ALL | max(L - R, 0) |

UNION DISTINCT reuses the existing projected DISTINCT operator. Intersection and
difference retain side-tagged full tuples under canonical equality navigation,
compute exact BigInt side counts, and differentiate the old/new output of each
affected equality group. Negative integrated memberships reject; output weights
narrow only at the existing checked i64 edge. Both sources can change in one
transaction. Retractions never remove an entire equality class merely because
one duplicate disappears.

## Types, representations and composition

Admitted domains include integral, Boolean, UUID, text/varchar, numeric, date,
timestamp and timestamptz under the existing type and coercion budgets.
Interval is admitted for UNION ALL, where its raw representations are preserved.
Text set equality
requires compatible, resolved deterministic collations. C/POSIX, default and
attested deterministic ICU equality preserve byte distinctions. Conflicting or
indeterminate collations reject for duplicate-eliminating operations; UNION ALL
retains its existing broader collation behavior.

Numeric equality ignores display scale, whereas observable numeric send values
do not. New set operations therefore require each numeric operand to have a
provably uniform effective display scale. Numeric(p,s) operands can differ in
precision when their effective scales max(s,0) agree. Integral operands contribute
scale zero, and a contextual NULL contributes no numeric representation. Cast
unconstrained numeric operands explicitly to numeric(p,s) before using these
operations. Unknown string numeric literals need an explicit constrained cast.
NaN and NULL membership follow PostgreSQL under this bound. UNION ALL keeps its
unconstrained numeric support and raw occurrence representations.

Branch numeric modifiers are enforced before alignment, so a grouped numeric key
retains its declared scale when a surrounding set result clears differing typmods.
Nested set results with cleared modifiers may require another explicit constrained
cast before a further duplicate-eliminating set operation. Equal compatible result
modifiers are retained; differing modifiers are cleared as PostgreSQL requires.

PostgreSQL parsing supplies INTERSECT precedence and left association for UNION/
EXCEPT. Parenthesized branch ORDER BY/LIMIT/OFFSET stays inside that operand;
outer ordering and bounds run after the set operation. Set-level ORDER BY names
only exposed labels or ordinals. CTEs, derived scopes, joins, grouping/HAVING and
supported windows reuse the shared relational graph and their existing admission
rules. Destination tables remain bags with no physical order guarantee.

Existing UNION ALL registrations require an explicit rebuild because the normalized
set-operation plan identity changes. There is no old-plan compatibility path.

## Reference and qualification

Feldera a2e791e12dd2fe02906a6fa2065520d5fcc73760 lowers UNION to sum/distinct,
EXCEPT to distinct/positive subtraction and INTERSECT through nested differences
in CalciteToDBSPCompiler. The count formulas above implement the same weighted bag
semantics using Pgderive's affected-group differentiation and strict membership
checks. PostgreSQL controls native types, collations, scales and SQL scope.

The 43 modes in scripts/set_operation_harness.py compare original-SQL native
materialized views, binary send bags, output names/OIDs/modifiers/collations and an
independent raw-source Counter oracle. Histories exercise unequal duplicates,
NULL tuples, NaN, fixed numeric scales, grouped numeric keys, negative scales,
C/default/ICU text, UUID/boolean/time values, self operations, precedence, branch/
outer limits, CTEs, aggregation, joins and windows. Both sources change together;
partial/last deletions, empty inputs, reinsertion and cold restart are checked.
Rust histories additionally test signed weighted deltas, compaction/recovery,
counts above i64 internally and checked final narrowing. Durable publication
remains object PUT, one atomic PostgreSQL result/membership/progress transaction,
then source acknowledgement.
