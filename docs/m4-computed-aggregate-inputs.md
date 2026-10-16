# M4 slice: computed grouped aggregate inputs

This is the first relational-coverage slice of M4. Dependency invalidation,
additional joins/subqueries and window input expressions remain separate work.
All expressions execute locally through M3's typed scalar IR.

## Admission and qualification matrix

| Query shape | This slice | Required evidence |
| --- | --- | --- |
| GROUP BY native/computed keys, native source | COUNT(expr), integral SUM/AVG/MIN/MAX, numeric SUM/MIN/MAX, integral COUNT(DISTINCT expr) | Native MV and independent source recomputation; NULLs, collisions, group moves, retractions and restart |
| Grouped aggregation over an already supported derived source | Same expression/type limits | Derived output resolution and fresh aggregate state; native MV and restart |
| Aggregate FILTER with a computed argument | Guard argument evaluation with FILTER; FALSE/UNKNOWN produce a NULL argument | Excluded overflowing operands remain unevaluated; distinct filters remain independent |
| WHERE with a computed argument | Apply WHERE before the aggregate-input map | Excluded source rows cannot trigger operand errors |
| Computed scalar grouping expression also selected | Reuse the same bound map within the stage | Shared field identity, repeated expressions and full-tuple weighted identity |
| COUNT constants / typed NULL operands | Local supported scalar types; untyped COUNT(NULL) resolves to text | Row counting versus non-NULL counting and all-NULL groups |
| Computed window / navigation arguments | Explicit rejection | Frames can omit rows whose arguments must never be evaluated; requires frame-aware lowering |
| Ungrouped aggregates, nested aggregates, hidden relation reads | Existing restrictions remain | Explicit rejection before activation |

Bind each argument and FILTER once. For computed arguments, lower FILTER into a
lazy searched CASE returning typed NULL when it does not qualify, then remove the
redundant aggregate FILTER. Every enabled aggregate ignores NULL arguments. Keep
group membership independent of argument presence, including all-filtered groups.
Constant-error rejection remains conservative, as qualified in M2/M3.

No engine-owned argument columns become public output columns. Source rows retain
complete weighted identity; computed-value collisions do not erase input identity.
The existing publication transaction and source ACK ordering remain unchanged.
New executable identities require explicit rebuild.
