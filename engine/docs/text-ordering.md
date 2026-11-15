# Text extrema and ordering

`MIN` and `MAX` accept text and varchar with the built-in C, POSIX, `ucs_basic`, or `pg_c_utf8`
collation. They return text with the argument's collation and no type
modifier. NULL inputs are ignored, including for DISTINCT. Empty input,
all-NULL input, and empty window frames return NULL. FILTER admits only
rows for which its condition is true. Constant text extrema also accept
the default collation since there are no differing values to compare.

Query ordering and non-offset window ordering support integral, numeric,
Boolean, UUID, date, timestamp, timestamptz, interval, and text
and varchar keys with those collations. PostgreSQL's direction and NULL placement apply to
each key, and peer groups use equality in the ordering domain. ROWS and
row-position functions continue to require complete occurrence identity
when row order affects the result. RANGE offsets support integral, exact numeric,
date, timestamp, and interval keys. Date offsets use PostgreSQL’s conversion
to timestamp, including an error when a finite date is out of timestamp range. Numeric
frames admit fractional offsets, preserve numeric peers, and handle NaN and infinity. Interval DISTINCT ON keys use PostgreSQL
interval equality, including the equivalence between one month and 30 days.
The selected row retains its original interval representation. Date MIN and
MAX also support DISTINCT and preserve the full PostgreSQL date range.

Pgderive compares these four collations by UTF-8 bytes. The source catalog
fingerprint includes all four collation definitions, so registrations made
with the previous fingerprint require a rebuild. The PostgreSQL 18 built-in `pg_unicode_fast` collation is outside this set.
Locale-sensitive collations need
their PostgreSQL collation provider and version semantics, so variable
text extrema and ordering require a supported collation. Specify
`COLLATE "C"` or `COLLATE "POSIX"` in the expression when that order is
appropriate. Existing per-tick work, record, and output limits apply.
