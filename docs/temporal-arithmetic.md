# Calendar arithmetic, EXTRACT and date_trunc

Date, timestamp, UTC timestamptz and full interval values execute inside the
existing typed scalar maps, predicates and relational operators. PostgreSQL
supplies source values, metadata and publication transactions; it does not
evaluate temporal operands for the engine.

## Values and arithmetic

Date uses PostgreSQL's civil-day range, including BC dates and signed infinities.
Timestamp types retain microsecond precision, their existing range and UTC
normalization. Interval retains three independent fields: calendar months,
calendar days and signed microseconds, plus PostgreSQL 17 interval infinities.
Source/output ISO representations preserve those fields. For example, one month,
30 days and 720 hours compare equal as intervals but remain observably different
values for arithmetic and native send representation.

Supported operators include date plus/minus integer, date minus date,
date/timestamp/timestamptz plus/minus interval, interval plus date/timestamp,
timestamp differences, interval plus/minus interval and unary interval negation.
Calendar addition applies months first with end-of-month clamping, then days,
then microseconds. Each phase checks native bounds; a later component cannot
rescue an earlier overflow. In UTC, calendar days have no daylight-saving offset.
Timestamp differences justify whole time days: a 25-hour difference returns
one day and one hour, while a stored 25-hour interval keeps its hour field.

Date casts to timestamp/timestamptz and timestamp types cast to date under the
fixed UTC context. Wide dates can be valid dates yet outside timestamp range;
an explicit/output coercion then errors. Mixed date/timestamp comparisons,
equality ON conditions and IN/NOT IN compare complete civil values without an
eager conversion of every date to timestamp. Noon differs from date midnight.

Date supports qualified grouping, DISTINCT, equality joins, same-type USING,
set operations and ORDER BY. Mixed date/timestamp set outputs use their native
common-type coercion. Mixed-type USING remains rejected for all join kinds;
use an explicit ON condition or an intentional supported cast. Date MIN/MAX and
additional date window-order domains are not added. Interval supports scalar
projection, arithmetic, comparison and CASE, while interval grouping, DISTINCT,
set operations and ordering remain rejected, including interval fields in an
ORDER BY/LIMIT fallback tuple.

## Exact fields and truncation

EXTRACT returns numeric, using integer and exact numeric arithmetic rather than float8.
Date fields include year, month, day, quarter, week, ISO year/weekdays, weekday,
day-of-year, decade, century, millennium, epoch and Julian day. Timestamp types
add hour, minute, second, milliseconds and microseconds; UTC timestamptz also
exposes its zero timezone fields. Interval supports its calendar/time fields,
quarter, decade, century, millennium and epoch, with PostgreSQL's signed-field
rules and 365.25-day year convention for epoch.

Second and timestamp/interval epoch retain six decimal places, milliseconds
three and microseconds zero. Date epoch is integral. Julian timestamp extraction
uses the existing exact numeric division scale. The extreme positive timestamp
epoch path deliberately reproduces PostgreSQL's intermediate numeric-division
rounding before the final six-place result; it is not replaced with a different
mathematically exact decimal result.

Date_trunc accepts constant supported units from millennium through microseconds,
including ISO weeks for timestamp types. Interval week truncation is unsupported.
Date inputs follow PostgreSQL's implicit timestamptz overload under UTC. The
three-argument timestamptz overload accepts constant UTC only. BC century/decade
boundaries, infinity results, cyclic-field NULL extraction and strict NULL
arguments follow the native routines.

Named zones, new AT TIME ZONE forms, date_part's float8 result, formatting,
interval scaling, temporal precision casts and arbitrary literal grammars are
not added. The registered environment still freezes UTC, ISO dates and ISO
interval output; there is no ambient-session or timezone-database fallback.

## State and qualification

Full interval values replace the previous duration-only gap representation.
Changed scalar plans, codecs and semantic bindings require the normal explicit
rebuild; no compatibility decoder is provided. Full-tuple identity remains
separate from interval comparison equality. Native publication matches observable
interval representations rather than deleting an arbitrary equality-equivalent
row. Generic work, state and record budgets still apply; mixed comparisons can
use bounded candidate scans rather than an eager unsafe key coercion.

The 40 modes in `scripts/temporal_arithmetic_harness.py` compare original-SQL
native materialized views, send bytes, labels/types/modifiers/collations and an
independent integer-calendar/Decimal bag oracle through CDC and cold restart.
Cases include Jan 31/leap-day clamping, ISO week 53, BC years 1/2/11/100,
pre-epoch microseconds, extreme dates/timestamps, mixed-sign intervals, infinities,
NULLs, grouping/joins/order/set composition and three-valued mixed IN predicates.
An interval-only sink retracts individual month/day/hour representations while
equivalent peers remain, including after restart. Six runtime-error modes check
native rejection and unchanged sink/progress with no ACK beyond the checkpoint.
Local oracle tests cover arithmetic phase bounds and the extreme epoch branch.

The closed support matrix adds all nine EXTRACT/date_trunc signatures, including
explicit UTC truncation. The local q14 fixture now qualifies its EXTRACT/CASE
composition; the AT TIME ZONE variant and formatting-dependent q10/q16 retain
their explicit rejection boundaries.

The scalar reference is PostgreSQL 17 [date/time operations](https://www.postgresql.org/docs/17/functions-datetime.html),
[date.c](https://github.com/postgres/postgres/blob/REL_17_STABLE/src/backend/utils/adt/date.c)
and [timestamp.c](https://github.com/postgres/postgres/blob/REL_17_STABLE/src/backend/utils/adt/timestamp.c).
The maintained dataflow continues to reuse the shared DBSP operators.
Feldera [a2e791e12's timestamp kernels](https://github.com/feldera/feldera/blob/a2e791e12dd2fe02906a6fa2065520d5fcc73760/crates/sqllib/src/timestamp.rs)
provide the calendar-field reference. PostgreSQL's numeric EXTRACT results take
precedence over Feldera's integer extraction results, preserving fractional fields
and native display scales.
