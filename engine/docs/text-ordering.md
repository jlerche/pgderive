# Text extrema and ordering

`MIN` and `MAX` accept text and varchar with the built-in C, POSIX, `ucs_basic`, `pg_c_utf8`, or `pg_unicode_fast`
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

`MIN` and `MAX`, top-level `ORDER BY`, and window ordering also accept
the database default when PostgreSQL selects libc `C` or `POSIX`, or
the built-in `C`, `C.UTF-8`, or `PG_UNICODE_FAST` locale. The compiler
checks the provider and locale on the source connection. Explicit
`COLLATE pg_catalog."default"` can select that same ordering. Results retain
PostgreSQL's default collation identity. Other libc and ICU database
defaults still require locale-aware ordering support.

Pgderive compares these collations by UTF-8 bytes. The source catalog
fingerprint includes their definitions and provider versions. The
`pg_unicode_fast` collation is available on PostgreSQL 18 and later.
Existing registrations on those versions need a rebuild to capture
the added collation in the fingerprint.
Locale-sensitive collations need
their PostgreSQL collation provider and version semantics, so variable
text extrema and ordering require a supported collation. Specify
`COLLATE "C"` or `COLLATE "POSIX"` in the expression when that order is
appropriate. Existing per-tick work, record, and output limits apply.
