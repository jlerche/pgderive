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

The compiler still requires projected input columns to be grouping keys or
aggregate inputs. It does not infer PostgreSQL's exception for columns that
functionally depend on a grouped base-table primary key. This restriction
also applies to ordinary GROUP BY.
