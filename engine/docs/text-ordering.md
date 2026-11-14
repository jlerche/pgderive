# Text extrema and ordering

`MIN` and `MAX` accept text and varchar with the built-in C or POSIX
collation. They return text with the argument's collation and no type
modifier. NULL inputs are ignored, including for DISTINCT. Empty input,
all-NULL input, and empty window frames return NULL. FILTER admits only
rows for which its condition is true. Constant text extrema also accept
the default collation since there are no differing values to compare.

Query ordering and non-offset window ordering support integral, numeric,
Boolean, UUID, date, timestamp, timestamptz, interval, and C/POSIX text
and varchar keys. PostgreSQL's direction and NULL placement apply to
each key, and peer groups use equality in the ordering domain. ROWS and
row-position functions continue to require complete occurrence identity
when row order affects the result. RANGE offsets retain their existing
integral and timestamp input domains. Interval values can order DISTINCT ON
queries, but interval DISTINCT ON keys require equality grouping support
that is outside the current implementation.

Pgderive compares C and POSIX text by UTF-8 bytes. Other locales need
their PostgreSQL collation provider and version semantics, so variable
text extrema and ordering require a supported collation. Specify
`COLLATE "C"` or `COLLATE "POSIX"` in the expression when that order is
appropriate. Existing per-tick work, record, and output limits apply.
