# Ordered string_agg

Pgderive supports the grouped text overload of
`string_agg(value, delimiter ORDER BY ...)`, including `FILTER` and
`DISTINCT`. Each non-NULL value after the first is preceded by the
delimiter from its own row, if that delimiter is non-NULL. If no value
qualifies, the result is SQL NULL. An empty string is a value. DISTINCT
deduplicates the pair of value and delimiter.

Aggregate ORDER BY uses the supported native ordering types and sort
directions, with explicit or default NULL placement. Text ordering
requires the built-in C or POSIX collation for nonconstant keys. Sort expressions refer to
input expressions. Select-list aliases and ordinals do not apply.

When differing input pairs tie on every ordering key, PostgreSQL does
not specify their relative order. Pgderive breaks those ties by the
text value and then the delimiter in byte order, with NULL delimiters
first. Include enough ordering keys to distinguish different pairs
when the materialized value must match across engines. Unordered
string_agg also depends on PostgreSQL's unspecified input order and
requires an aggregate ORDER BY in Pgderive.

The operator reevaluates only affected groups over their weighted row
bags at transaction boundaries. Duplicate occurrences are preserved
unless DISTINCT is specified. Output is limited to 16 MiB of UTF-8
bytes. Sorting scratch is bounded by the configured scratch byte limit,
and the existing partition work and state limits also apply.

The native string_agg overloads and their implementation functions now
participate in the source catalog fingerprint. Registrations captured
before this change require a rebuild.
