# Grouping sets

`GROUPING SETS`, `ROLLUP` and `CUBE` use the ordinary grouping expressions
and aggregates supported by the compiler. Multiple items in GROUP BY form a
Cartesian product of their grouping sets. Parenthesized key lists inside a
rollup or cube are treated as one item.

Each grouping set produces a separate weighted aggregate result. An empty
set produces the global aggregate row even when WHERE selects no rows.
Columns omitted by a set produce typed NULLs. `GROUPING(...)` returns an
integer mask with the rightmost argument in the least significant bit.
A bit is set when its argument is omitted. A real NULL in an included
grouping column leaves that bit clear. Arguments must match grouping
expressions and are used to identify keys without evaluating them again.

Repeated sets contribute repeated occurrences, including when different
sets produce the same visible tuple. `GROUP BY DISTINCT` deduplicates the
sets before aggregation. A final `SELECT DISTINCT` deduplicates output
tuples according to the ordinary DISTINCT rules.

The compiler expands at most 64 sets and admits at most 64 grouping
expression occurrences and 31 arguments per GROUPING call. Expansions
past those limits reject during compilation. Each branch reuses the
engine's ordinary weighted grouping and empty-input seed. The branches
share the input stream and their outputs are consolidated as a bag at
the same transaction boundary. The existing work and memory limits
apply to the expanded plan.

A query can also project columns of supported grouping types from a base
table when every grouping set includes that occurrence’s full nondeferrable primary key.
GROUPING() arguments still have to be explicit grouping expressions.
The same rule applies to ordinary GROUP BY and joins. A merged USING key
qualifies when it resolves to the original primary-key column. Derived
outputs and join equalities do not transfer primary-key dependencies.

Source contracts now record primary-key deferrability. Retire existing
registrations with the previous binary before upgrading, then use
`query create` with the updated binary to capture the new contract.
