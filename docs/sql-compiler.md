# SQL compiler boundary and slices

For SQL ORDER BY and ORDER BY/LIMIT, see the [ordering contract](ordering.md).

For the fidelity target, implementation inventory and known qualification gaps,
see the [compatibility contract](postgres-compatibility.md) and
[capability inventory](postgres-capabilities.md).

The compiler executes a deliberately bounded PostgreSQL SELECT through durable
grouped-join, single-source projection, ungrouped inner-join and affected-partition workers. It is not a general SQL planner
or an interpreter for plan declarations. Configure it with SQL:

```toml
[worker.query]
sql = """
SELECT a.id AS auction_id, COUNT(*) AS matches, SUM(b.price) AS total
FROM public.auction AS a INNER JOIN public.bid AS b ON a.id = b.auction
WHERE a.category IS NOT NULL
GROUP BY a.id
"""
```

## Grouped two-source join subset

One SELECT, with exactly three outputs in order: a left column, COUNT(*), and
SUM(right integral column). GROUP BY contains exactly the same left column.
FROM names two distinct publication relations using explicit schema.table names,
connected by JOIN or INNER JOIN and one ON column = column equality. Reversing
the equality operands is accepted. Native join types, modifiers and collations
must match exactly; there are no implicit casts. Join/group keys are boolean,
int2/int4/int8 or UUID. The current shared join builder additionally supports [numeric/text keys and
ON predicates](join-predicates.md); this section describes the initial baseline. All frozen native types can be tested for NULL.

WHERE supports column IS NULL / IS NOT NULL, Boolean columns and TRUE/FALSE/NULL,
and scalar comparisons =, <>, !=, <, <=, >, >= composed with AND/OR/NOT.
Boolean expressions in SELECT and WHERE can use `IS TRUE`, `IS FALSE`, and
`IS UNKNOWN` and their `IS NOT` forms. Each test returns a non-NULL Boolean,
including when its input is NULL. Scalar `IS DISTINCT FROM` and
`IS NOT DISTINCT FROM` in SELECT and WHERE use the supported equality types
and coercions. Two NULLs are equal for these tests, and a NULL differs from
any non-NULL value. Scalar `NULLIF(x, y)` uses the same equality rules and
returns NULL when the arguments compare equal. Otherwise it returns the first
argument after the equality operator's coercions. For example,
`NULLIF(1, 2.0)` returns a numeric value. Compute scalar subqueries used as the
second argument in a derived scope. Ordinary scalar `BETWEEN` and
`NOT BETWEEN` use the supported ordering comparisons and resolve each bound
independently. At runtime in projections, evaluation reaches the upper comparison only
when the lower comparison has yet to determine the result. WHERE uses the
normal predicate planning and evaluation rules. Compute scalar subqueries
used as range operands in a derived scope.
Comparisons require at least one native bool, int2/int4/int8 or UUID column.
Integral columns may compare across integral widths and to signed i64 integer
literals; evaluation is exact, without narrowing the literal to the column width.
Boolean columns compare to Boolean literals. UUID columns compare to unknown
string literals using PostgreSQL UUID input forms and byte ordering. Quoted
numeric/Boolean literals, explicit casts, floating/numeric literals and other
coercions are rejected. Native text/varchar columns support equality and inequality
with unknown string literals or NULL. Source collations must be deterministic:
PostgreSQL text equality then compares bytes, preserving case, trailing spaces and
Unicode normalization distinctions. Rust evaluates the same UTF-8 strings. Text
ordering, text column-to-column comparisons (which require collation resolution),
and nondeterministic source collations are rejected. NULL comparison produces unknown;
WHERE retains only true, including on retractions. No transform_null_equals
session setting is applied. Logical expressions follow PostgreSQL three-valued
truth tables; pure AND/OR trees are flattened and deduplicated, keeping written order.

Integral `mod(x, y)` and `x % y` share a typed scalar expression and accept
computed operands. Mixed integer widths promote to the wider type. The
remainder has the dividend's sign. A zero divisor raises division by zero,
and the minimum signed integer modulo −1 returns zero. NULL arguments
produce NULL after evaluating the operands. With an exact numeric operand,
`mod` and `%` return numeric. For two finite operands the remainder uses the
greater input scale. A finite dividend modulo infinity retains its original
scale. NaN propagates, including with a zero divisor. Infinity modulo a
nonzero divisor returns NaN. The existing numeric digit budget applies.

ON equality with either key NULL is unknown and yields no match. Grouping preserves
NULL, COUNT(*) includes NULL measures, SUM ignores NULL measures and returns NULL
for an all-NULL group. Empty groups disappear. Zero SUM is distinct from absence.

Table aliases and output aliases support AS or an implicit alias. Output aliases
in grouped queries are presentation-only: the owned destination still uses group_key, row_count,
and total, in that fixed order. group_key is JSONB containing the canonical source
text representation (or JSON null), even for integral/boolean/UUID groups; it is
not a native SQL output-column codec. Output aliases are not visible in WHERE.
Columns may be unqualified when unique, table-qualified, or schema.table-qualified
when the table has no alias. An alias hides the original table name. Ambiguous
columns and duplicate exposed table names are errors. PostgreSQL 17.7 determines
identifier keyword categories, case folding, quoted names and Unicode escapes
(including UESCAPE). Redundant parentheses and PostgreSQL's ISNULL/NOTNULL
spellings are accepted when their AST has the supported semantics. Comments and
terminal semicolons follow PostgreSQL syntax; exactly one statement is required.
PostgreSQL truncates identifiers to 63 UTF-8 bytes, including quoted identifiers;
resolution and plan identity use that native normalized name. This replaces the
an overlong-name rejection. Unquoted non-ASCII identifiers are accepted
with PostgreSQL's folding behavior, rather than requiring quotes.

Input is bounded to 16 KiB, 2,048 scanner tokens, 64 parenthesis/expression levels and 512
WHERE expression nodes; NUL is rejected. Scanner tokens exclude parentheses in strings,
comments and quoted identifiers. These are input budgets, not a general native
parser resource guarantee. Only built-in unqualified COUNT and SUM are supported.

This initial grouped-join shape is one lowering path; the sections below describe
the broader typed projections, aggregates, joins, derived queries and windows.
[HAVING, nonrecursive CTEs and UNION ALL](sql-composition.md) compose those paths.
[UNION DISTINCT, INTERSECT and EXCEPT with ALL variants](set-operations.md) extend
that composition. Parameters, recursive WITH and multiple statements remain
outside the accepted grammar. Unsupported SQL is rejected before a registration
row or replication slot is created.
Administrative metadata table installation may precede compilation; it does not
register a query. Resolution uses the explicit frozen publication catalog, not
search_path or live SQL name interpolation. ORDER BY and LIMIT follow the
[ordering contract](ordering.md).

## Boolean and UUID common types

Searched CASE, simple CASE, COALESCE, and the set operations coerce unknown
string literals and untyped NULLs to Boolean or UUID when that is the known
result type. Literals use PostgreSQL's native input spellings, and UUID output
uses canonical lowercase hyphenation. A group of entirely unknown inputs
resolves to text, including all-NULL CASE and COALESCE. Explicitly typed text
is incompatible with Boolean and UUID. An invalid literal fails during
binding, even in an arm that execution would skip.

Set operations resolve each pair of operands separately. An inner pair that
already resolved to text stays text in the outer operation. UNION, INTERSECT,
and EXCEPT retain their existing DISTINCT and ALL multiplicities after coercion.

## GROUP BY names and positions

Supported GROUP BY scopes accept a one-based select-list ordinal or a bare
output label. A bare name resolves against the current input first. An
ambiguous input name is an error. When no input column matches, duplicate
output labels are accepted only if their bound expressions agree. Qualified
names and names inside GROUP BY expressions resolve in the input scope.
Ordinals count expanded wildcard columns and exclude hidden ORDER BY targets.
Aggregate and window targets cannot be grouping keys.

## Single-source projection/filter subset

With one schema-qualified publication table in FROM, `SELECT *` and
`SELECT alias.*` expand at their positions in the select list. Use the table
name or schema-qualified table name as the qualifier when FROM has no alias.
The expansion follows the frozen visible attribute order, skipping dropped
attributes and preserving each column's name, type, type modifier, collation
and nullability. Expanded columns count toward the 64-target limit and can be
referenced by ORDER BY label or ordinal. The same rules apply inside supported
subqueries. Join and general derived-table wildcard expansion are outside this
slice.

The same output and WHERE subset also supports two distinct source tables joined
by one inner equijoin, with columns selected from either input. Join keys must
have identical native bool, integral, UUID or timestamp contracts; NULL keys never match.
The shared join-tree builder now broadens this initial baseline; see
[join keys and predicates](join-predicates.md). Aliases and reversed equality operands normalize to the same semantics;
output labels and order remain part of durable identity.

Ungrouped joins lower to explicit Source, KeyBy, Join and Project IR nodes. The
worker binds those nodes to a reusable durable acyclic circuit executor. Both keyed inputs retain complete native
source tuples. The Join evaluates ΔL⋈R + L⋈ΔR + ΔL⋈ΔR against immutable prior
state; WHERE and final projection operate on signed joined contributions. The
final output arrangement preserves projected bag collisions. Every source receives
one delta per logical tick, including empty deltas. The executor can compose
source/project/join DAGs with maintained join parents and one maintained output;
the SQL frontend deliberately accepts only the two-table shape in this slice.
Aggregate kernels still use the existing grouped bridge.

Every projection uses the shared relational circuit: source rows feed explicit
Filter, Map and Output nodes as needed. Complete source tuples remain the input
identity, including rows with NULL projected columns. Signed contributions use
the same operators. Final output collisions consolidate full projected tuples,
preserving SQL bag multiplicity without source primary keys as output identity.
The output arrangement supports cold restore and fenced compaction/GC. Complete
transactions on unrelated source relations advance the query with empty deltas.

The owned destination is an ordinary native-column PostgreSQL table. The engine
encodes signed output deltas as `[null, [column_1, ..., column_n]]` for publication
transport only; these tuples and their weights are not persisted in PostgreSQL.
The first null is the unit navigation key. Resolved output labels, OIDs, modifiers
and collations bind the table layout. Publication casts qualified native values,
inserts each positive occurrence and deletes exactly the required occurrences for
negative deltas. A missing occurrence rejects the entire transaction. Binary native
comparison preserves representations which SQL equality can ignore. There are no
visible bookkeeping columns, persisted weights, presentation views or SQL-side
occurrence maps. Engine arrangements and retained operands remain object-backed.

The first implementation limits each publication to 1,000,000 physical row changes
and scans the destination for retractions. Supported output types remain bounded:
bool, int2/int4/int8, text/varchar, UUID, timestamp/timestamptz, numeric and text[].
Unique output labels are required. Destinations use FULL replica identity and must
be excluded from source capture. Application writes and destination triggers,
rules or RLS are unsupported; initialization verifies the owned table layout.

## In-engine expression execution

All accepted scalar functions, casts, predicates, arithmetic and aggregate
finalization execute inside the engine. The PostgreSQL sink accepts fully computed
rows; it performs native type decoding and ordinary result-table DML, not query
expression evaluation. There is no connected PostgreSQL computation fallback.
Unimplemented operations reject validation with the function/feature reason.
Formatting, collation-sensitive functions, regex and text-array extraction are
currently unavailable. The typed runtime supports integral `abs`/`to_hex` and
UTF8 text/varchar lengths, code points, reverse, trim, slicing, replacement,
translation, position/prefix checks, repetition and split fields. The
`text || text` operator accepts text operands and resolves string and NULL
literals as text. It returns NULL if either operand is NULL. The result
shares the 16 MiB text expansion limit and is also subject to the normal
record and sink limits. Scalar `GREATEST` and `LEAST` accept up to 64 integral
or exact numeric arguments, resolve a common type, and ignore NULLs after
evaluating the arguments. They return NULL when all arguments are NULL and
keep the first tied value's representation. A list of bare NULL literals
resolves to text. `sign(numeric)` returns numeric −1, 0, 1, or NaN with
zero scale for finite results. `div` accepts integral and exact numeric
arguments through the numeric signature and returns a numeric quotient
truncated toward zero with scale zero. Numeric division errors, special values,
and existing digit budgets apply.

The text-only `concat` and `concat_ws` functions accept up to 64 arguments
and share the 16 MiB output limit, including separator bytes. Both evaluate
all arguments and skip NULL values. Empty strings still count as values.
`concat_ws` returns NULL when its separator is NULL. With a non-NULL
separator, an all-NULL value list returns the empty string, as does
`concat` with all-NULL arguments. Cast varchar and other values to text
before passing them.

Explicit casts to `text` support `smallint`, `integer`, `bigint`,
`boolean`, and `uuid`. Integers use decimal digits with a leading minus
for negative values. Boolean casts produce `true` or `false`, and UUID
casts produce lowercase, hyphenated text. NULL inputs yield NULL.

Explicit casts from text and varchar to `varchar(n)` truncate to `n`
Unicode characters, preserve collation, and retain the declared length in
the result type. Shorter strings and NULLs are preserved. These casts do
not change assignment coercion.

`LIKE` and `NOT LIKE` match text and varchar under the built-in deterministic
`C` and `POSIX` collations. A column declaration or explicit `COLLATE` clause
can supply that collation. Matching is case-sensitive over the whole string.
`%` matches zero or more characters and `_` matches one Unicode character.
Backslash is the default escape. `ESCAPE` accepts one character or the empty
string to disable escaping. NULL operands, including a NULL escape, yield NULL.
Patterns are limited to 16 MiB before and after escape normalization.
Matching allows up to 16,777,216 steps and 128 nested wildcard searches.

Integral bitwise operators support `&`, `|`, `#`, `~`, `<<`, and `>>`.
Binary bitwise operators promote mixed integer widths. Shifts keep the left
operand's width and take an integer count. On the qualified PostgreSQL 17
x86-64 build, negative and oversized counts wrap modulo 32 for
smallint and integer shifts and modulo 64 for bigint shifts. Use the qualified PostgreSQL x86-64 source
and an x86-64 worker for those counts.
The exact function signatures are in
`src/compiler/capabilities.rs`. Overload errors list argument type names and
OIDs alongside the supported signatures. Nested calls and text CASE/COALESCE share the local evaluator. The evaluator returns native values for predicates, keys and computed columns; text encoding happens at row boundaries. CASE and COALESCE select lazily, while strict calls evaluate all arguments before NULL propagation. Integral widths are checked at each expression node.

Native and derived scalar projections use one AST-to-bound-expression path. There is no retry through a second projection binder. Aggregate AVG results are finalized by typed Map nodes before every SQL output boundary, including the root; output layouts carry columns rather than deferred computation.
Expansion budgets and the rejected `right(text, -2147483648)` boundary are listed
in the [fidelity roadmap](postgres-fidelity-roadmap.md#qualified-local-function-batch).
The current plan contains no sink-expression programs or placement reports.
Development changes cut over directly to the current compiler and state format;
incompatible state is rejected and requires a fresh owned registration. Historical
binaries and migration adapters are outside the development workflow.

## Compiler and runtime boundaries

The existing runtime retains complete full-tuple bag identity, assembles both
inputs from a complete committed transaction, and includes the simultaneous-input
cross term. No source PK is used as generic weighted identity. No durable protocol
is replaced: protected object PUT precedes atomic PG membership/result/progress
COMMIT; only authoritative publication permits slot ACK. Source LSN never becomes
DBSP time. The compiler adds no acknowledgement or publication capability.

COUNT and final weights must fit signed i64. Sufficient statistics use exact
intermediate arithmetic. SUM(int8) emits PostgreSQL numeric; SUM(int2/int4) must fit i64. Output conversion
and publication failures cannot advance progress or ACK. These are explicit
operator/result limits, not a claim of unrestricted PostgreSQL aggregate coverage.

## Identity and restart

Function/operator spelling and redundant logical syntax normalize where supported.
The resolved expression tree, including constants such as remainder divisors,
participates directly in graph identity.

Formatting, keyword case, comments, redundant identical conjuncts,
source alias spelling and reversed equality operands normalize where supported.
Visible output names and order are part of the layout for every query, including
grouped joins. Semantic equivalence is not a general theorem prover.

Pgderive is unreleased and provides no compatibility contract for serialized
compiler IR, callbacks or object layouts. Restart still recompiles and verifies
plan identity before adopting state. Incompatible registrations must be rebuilt
with a fresh owned destination, slot and object namespace; old evidence is kept
for diagnosis and data is never reset automatically. Refactors need not retain
old compiler identities or implement migration adapters.

The complete q0–q22 target and temporal/runtime prerequisites are specified in
[the Nexmark portfolio contract](nexmark.md). That target exceeds current compiled
execution and is not established by the existing qualification.

## Native build dependency

pg_query builds its bundled PostgreSQL parser with a C toolchain and bindgen.
Install libclang (for example `libclang-dev` on Debian/Ubuntu) alongside the Rust
toolchain. The application uses only the safe Rust API; its own unsafe-code ban
remains in force. No PostgreSQL server headers or runtime extension are required.
The pinned parser grammar is PostgreSQL 17.7, not whichever server version happens
to be running; newer syntax remains unsupported until deliberately qualified.

All joins use the normalized relational graph and full frozen native contract.
Changed predicates, selected columns or labels change graph identity. The live
relational harness compares complete insert/update/delete transactions against
PostgreSQL and an independent nested-loop memory bag, including NULLs,
simultaneous changes, projection collisions, empty output ticks and cold restart.

## Affected partitions, aggregates and non-temporal ROWS frames

Single-source grouped SELECT supports multiple native bool/integral/UUID/timestamp column
keys and arbitrary output ordering of those keys and COUNT(*), COUNT(column),
SUM(int2/int4/int8), AVG(int2/int4/int8), integral/temporal MIN/MAX, and
`bool_and(boolean)`, `bool_or(boolean)`, and `every(boolean)`. Each aggregate may have a FILTER using the
supported three-valued predicate subset. NULL grouping keys remain present;
COUNT(column) ignores NULL; filters retain only true. Existing groups with no
qualifying non-NULL measures produce COUNT zero and SUM/MIN/MAX NULL. Removing
all input rows removes a grouped result. Integral COUNT DISTINCT is described
below. Later sections add global groups, DISTINCT, derived composition and exact
numeric input aggregates; see also [numeric arithmetic](numeric-arithmetic.md).

`bool_and` and `every` return true when all qualifying non-NULL values are
true. `bool_or` returns true when any qualifying non-NULL value is true.
All three return NULL when no qualifying non-NULL values remain, including
empty global groups and empty window frames.

Single-source window SELECT supports the same aggregate functions with explicit
ROWS frames and integral/timestamp column ORDER BY, ASC/DESC and NULLS FIRST/LAST. Partition
keys use the grouped key subset, including NULL. All window calls in one SELECT
must share one specification. Frame boundaries support CURRENT ROW, nonnegative
integer PRECEDING/FOLLOWING offsets and valid UNBOUNDED endpoints. The ordering
must include every source primary-key column, explicitly in SQL, so positional
frames have deterministic occurrence order. The compiler adds no hidden tiebreaker.
Peer-aware RANGE/GROUPS frames are described below. Frame exclusions remain
unsupported. Named `WINDOW` definitions are scoped to their SELECT and
resolve through the same single-window lowering as inline specifications.
`OVER name` uses the entire definition. `OVER (name ...)` and inherited
definitions may add ordering if the base has none and may supply their own
frame. They cannot copy an explicit frame or add a partition clause.
Every declaration must be referenced directly or through inheritance.
Peer ranking, native lag/lead and qualified temporal expressions are described below. Windows preserve
source rows and attach frame aggregates, whereas grouped aggregates replace each
group with one row. Empty frames yield COUNT zero and other supported aggregates
NULL, matching PostgreSQL. Source WHERE runs before partition/window evaluation.

The reusable Partition kernel follows Feldera's group-transform/difference pattern:
read only changed partitions from the pinned prior input trace, apply the complete
signed delta, evaluate prior/new partition bags and emit their difference. The
maintained full-tuple input and final output are checkpointed in the same durable
circuit; no whole-database SQL recomputation occurs in production. This initial
kernel still handles MIN/MAX and ROWS frames. Grouped COUNT/SUM/AVG-only plans
use the exact linear statistics path described below.
Large or skewed partitions and expansive ROWS frames can hit explicit row, byte
and frame-work limits; failure preserves prior visibility and progress. Output
contributions use exact spillable consolidation before finalized i64 narrowing.
SUM(int2/int4) results and COUNT must fit PostgreSQL bigint. AVG(int2/int4)
also requires its finalized sum to fit bigint; exceeding this limit fails closed.
AVG non-NULL counts must fit bigint. SUM(int8) and AVG(int8) keep arbitrary-precision
integer sums; PostgreSQL numeric output and division are finalized at publication.

The work budget counts materialized occurrences, frame-row visits and aggregate
record visits across affected partitions in one operator tick. It bounds those
operations; it does not measure total CPU instructions or sort comparisons.

## PostgreSQL semantics progression

The next implementation sequence builds on the same relational executor and durable
publication boundary:

Each step requires PostgreSQL and independent memory bag comparisons through
complete insert/update/delete ticks and restart. Changed semantics cannot adopt
an incompatible trace.
The affected-partition implementation above is the correctness foundation, with
bounded recomputation costs explicitly exposed rather than a claim that every
window or partition scales linearly. Object-local indexing, batched reads and
compaction optimizations must preserve this result/delta contract.


## Exact integer numeric output in the engine

Integral AVG uses arbitrary-precision sum and non-NULL count state. The engine
selects PostgreSQL's numeric division scale using base-10000 leading digits,
rounds ties away from zero with integer arithmetic, and emits the exact decimal
text. No numeric_div query or float conversion occurs. SUM(int8) also emits exact
decimal text. The ordinary typed sink decodes numeric values; internal ratios
remain engine-owned. All-NULL groups produce NULL without division.

The live harness compares AVG display scale and rounding with PostgreSQL and the
independent Decimal oracle across extreme integers, negative values, NULLs,
FILTER, source updates and restart. Supported derived scopes now render AVG internally
before consuming it; see the M6 composition boundary below. Arbitrary derived SQL
remains bounded; [numeric AVG](numeric-arithmetic.md) now uses exact division and
reversible numeric summaries.

## Native timestamp and numeric source values

Frozen source contracts now accept timestamp (OID 1114), timestamptz (OID 1184)
and numeric (OID 1700). Snapshot and replication connections explicitly use
ISO/MDY DateStyle, UTC TimeZone and ISO interval output. Source rows retain the
complete canonical native text layout; this prevents text-layout drift between
snapshot, old/new CDC tuples and recovery. PostgreSQL source primary keys remain
source validation, separate from generic full-tuple weighted identity.

Timestamp values use PostgreSQL's microsecond epoch/range, proleptic Gregorian
calendar and infinities. Native projection emits PostgreSQL-compatible JSON strings,
including BC dates and UTC timestamptz offsets. Equality/grouping and ROWS ordering
support timestamps with existing NULL and signed-delta rules. WHERE supports native
timestamp comparisons and unknown string constants in the qualified canonical ISO
form; timestamptz constants need explicit numeric offsets or Z. The later
[calendar arithmetic](temporal-arithmetic.md) slice adds native date and interval
values, selected UTC casts, exact EXTRACT and date_trunc. Ambiguous date formats,
named zones and unsupported casts/functions still reject rather than using string
ordering or a PostgreSQL computation fallback.

Numeric projection safely defers native input conversion to PostgreSQL inside
atomic publication, preserving exact decimals and PostgreSQL JSON representations
of NaN/Infinity. Later circuit numeric work adds keys, comparisons, arithmetic and
numeric SUM/AVG with explicit scale/result budgets; see the numeric sections below.

The owned native harness checks snapshot/CDC/restart projection, timestamp grouping,
ordered windows and same-type joins against PostgreSQL and independent memory
recomputation. It includes BC/min/max dates, infinities, NULLs, decimal precision,
numeric special values, writer-session DateStyle/TimeZone changes and simultaneous
join-input changes. Source types and their resolved expressions participate in
the same normalized graph identity. Bounded hopping expansion and peer ranking
are described separately below.


## Fixed-duration date_bin execution

Single-source projection and GROUP BY can evaluate `date_bin(stride, column,
origin)`. The compiler resolves and type-checks a pure scalar map node before
partition state; the runtime evaluates that node on signed full-row contributions.
WHERE runs first, preserving errors only for qualifying input rows. Identical
expressions share a computed field, and grouped outputs must match a grouping
expression. The resolved scalar map is explicit in normalized IR.

The input is a native timestamp or timestamptz column. Origin must be an explicit
constant of the same timestamp type in the supported canonical ISO form;
timestamptz origins require a numeric offset or Z. Stride is an interval constant
(or unknown string) containing one positive decimal quantity and a unit from
microseconds through days, with at most six fractional digits and an exact integral
microsecond result. Months, years, mixed-unit intervals, type modifiers and implicit
cross-type conversions are rejected before registration.

Execution uses PostgreSQL's checked microsecond arithmetic, including flooring
before the origin, finite range errors and infinite inputs. Strict NULL inputs
produce NULL, and GROUP BY retains that NULL group. A timestamptz day stride is
exactly 86,400 seconds; it is not a local calendar day across daylight-saving
transitions. Nothing expires because time passes. The materialized weighted bag
matches this explicit SQL over the current source snapshot.

The common gate compares both timestamp overloads and projection collisions with
PostgreSQL and an independent datetime oracle through updates, deletes, NULLs,
infinities and cold restart. Unit checks cover pre-origin arithmetic, range errors,
normalized intervals, unsupported expressions and filtering before scalar errors.


## Exact linear grouped statistics

Grouped plans containing only COUNT, integral SUM and integral AVG now lower to an
explicit Statistics node and a separate finalization map. The reusable operator
maps each signed full-tuple contribution to exact integer statistics, consolidates
them with spilling, then reads one prior statistic for each changed group. It
emits unit-weight retract/replace state deltas. It does not reread every source row
in that group. FILTER and non-NULL counts are separate for every aggregate; total
group multiplicity retains all-NULL/all-filtered groups and removes empty groups.
Integer overflow checks apply after combining the complete tick, preserving
cancellation and simultaneous input changes.

The circuit checks changed full-tuple coefficients against the pinned input trace
before evaluating statistics, rejecting invalid retractions even when group totals
cancel. Object-local full-identity fences select one candidate block per run and a
binary search resolves the tuple; no primary-key or last-write-wins interpretation
is introduced. Generic signed trace validation continues to permit negative weights. The circuit retains full-tuple input identity and the exact statistics arrangement,
then projects SQL results from the changed statistics. Both are included in object
membership/checkpoint/recovery/compaction alongside output state. Numeric SUM/AVG
finalizers execute locally before publication. Source LSN, logical time and PUT/atomic-publication/ACK remain separate boundaries.

Qualification includes PostgreSQL and independent full-bag oracles for NULLs,
FILTER, weighted changes, deletion of the last row and cold restart. A memory-store
case bootstraps a 256-row group, restores it under a 16-contribution tick budget,
and updates one row with forced consolidation spilling. This verifies the delta
path under that budget; it does not establish arbitrary group size or throughput.
Mixed MIN/MAX aggregates and ROWS retain their affected-partition costs and limits;
grouped MIN/MAX-only queries use the ordered state described below.


## Explicit bounded hopping expansion

A single-source CROSS JOIN with `generate_series(start, stop) AS w(n)` now lowers
to a pure Expand node. Both bounds must be int4 constants from 0 through 1023;
step defaults to one and start greater than stop produces an empty relation.
The relation and column aliases are required and obey identifier/ambiguity rules.
The function may be unqualified or qualified by pg_catalog. Other series overloads,
LATERAL, ordinality, column definitions and arbitrary set-returning functions are
rejected before registration. Generated values can be projected, grouped or used
as supported integral aggregate inputs. WHERE currently addresses source columns;
generated-column filters require a later post-expansion filter node.

Hopping starts are expressed in SQL as `date_bin(...) - w.n * INTERVAL '2 seconds'`.
The date-bin base keeps its existing typed origin/stride rules. Offset intervals
use one positive time quantity from microseconds through hours, with exact
microsecond precision. Calendar days/months/years are rejected. The largest
ordinal times the duration must fit the exact double-precision integer range used
by PostgreSQL's interval multiplication; finite timestamp arithmetic retains
PostgreSQL range errors. NULL and infinite timestamp inputs retain their SQL
values for every generated membership, so NULL/infinite groups receive all the
rows that the explicit cross join produces. The compiler does not add a NULL filter.

Expansion emits lazily into the exact spillable consolidator. Every emitted full
row includes the generated ordinal and carries the source contribution's signed
weight; final projection collisions consolidate normally. The series bounds,
resolved scalar expressions are explicit in normalized graph identity. There is no clock-driven expiry or late-data rule.
Non-temporal ROWS over expanded inputs additionally requires the generated ordinal
in the SQL ORDER BY alongside every source primary-key column, preserving explicit
occurrence order. Hopping windows are SQL relational membership, distinct from
ROWS/RANGE/GROUPS aggregate frames.

The common gate compares both timestamp overloads, pure series projection,
hopping projection collisions and empty expansion with PostgreSQL and an
independent datetime/full-bag oracle through inserts, updates, deletes, NULLs,
infinities, pre-origin/DST instants, alias normalization and cold restart. Unit
checks cover weighted membership, complete-tick cancellation and fanout failure
without changed visibility. Bounds and these checks establish this subset only.

## PostgreSQL peer and occurrence ranking

Single-source ordered partitions now lower `rank()`, `dense_rank()` and
`row_number()` into the existing durable affected-partition evaluator. They take
no arguments, require OVER, and reject FILTER. This slice accepts the default
window frame only and one shared partition/order specification; mixing ranking
and framed aggregates requires subsequent window composition. Integral and native
timestamp ORDER BY columns support ASC/DESC and PostgreSQL default or explicit
NULL placement. PARTITION BY uses the existing native equality types.

Peers compare only SQL ORDER BY values. RANK counts preceding row occurrences,
including weighted multiplicities, and DENSE_RANK counts preceding peer groups.
An empty ORDER BY makes the entire partition one peer group. ROW_NUMBER starts at
one and requires SQL ORDER BY to include the complete native primary key; after
series expansion it also requires the generated ordinal. No hidden tie-breaker is
added. Repeated copies of the same full tuple receive successive row numbers and
remain a bag of occurrences. Outputs are bigint with checked conversion.

Complete source ticks revise old and new affected partitions and emit the
snapshot difference. Independent weighted unit histories and three sequential owned worker
fixtures compare inserts, updates, deletes, NULL peers, empty query ticks and cold
restart against memory and PostgreSQL bags. A top-k predicate over these results
uses the derived-query and post-window filter lowering described below.

## Native PostgreSQL ranking distributions

PERCENT_RANK returns `(rank - 1) / (partition size - 1)`, with zero for a
single-row partition. CUME_DIST returns the fraction of the partition through
the end of the current peer group. Both return double precision and count
weighted row occurrences. With no window ORDER BY, every row is a peer and
the results are zero and one, respectively. Double precision here is supported
as a projected result type.

NTILE returns integer bucket numbers starting at one. The larger buckets come
first, and bucket counts above the partition size give each row its own bucket.
It accepts an integer expression and uses the positional window ordering
requirements. PostgreSQL evaluates that expression until the first non-NULL
count and uses that count for the rest of the partition. The preceding rows
return NULL, and a nonpositive count at initialization raises an evaluation
error. Bucket sizes are based on the entire partition, including preceding
rows with NULL counts.

These functions accept named windows and use the whole partition regardless of
the supported frame specified in OVER. The PostgreSQL harness compares result
bags, binary values, and type OIDs with the native query across updates, deletes,
and cold restart.

## Peer-aware aggregate frames

Window aggregates also support PostgreSQL's default RANGE frame, RANGE with
CURRENT ROW or UNBOUNDED endpoints, and GROUPS with nonnegative integral constant
PRECEDING/FOLLOWING offsets or standard endpoints. GROUPS requires ORDER BY.
RANGE distance offsets and exclusions remain rejected before
registration. All window expressions must share the same typed specification.

Peer equality uses exactly the SQL ORDER BY values, including NULL placement and
direction; these frames do not require a primary-key tie-breaker. CURRENT ROW
includes the entire current peer group. GROUPS counts peer groups rather than
weighted row occurrences; frame aggregates still count/sum every occurrence.
Without ORDER BY, the default RANGE frame includes the whole partition. Empty
frames yield COUNT zero and nullable aggregates NULL. Supported FILTER predicates
apply to aggregate contributions rather than removing peer membership.

The runtime orders only changed partitions, identifies peer ranges, computes the
aggregate fields once per peer group's shared frame, and copies them into every
output occurrence before emitting the old/new bag difference. It charges frame
construction, aggregate scans and copied fields against the work budget. An
unordered 64-row COUNT/SUM partition fits a 1,024-unit contribution budget; the
previous per-row rescans exhausted that same budget. Ordered frames with many
distinct peer groups can still require quadratic scans. Explicit default
RANGE and omitted default syntax normalize identically. Weighted independent unit
histories and six sequential PostgreSQL/memory fixtures cover default, GROUPS
neighbors, current-peer, empty, full and unordered frames through source changes
and cold restart. This qualifies peer-boundary semantics, not temporal-distance
RANGE execution or unbounded partition scale.

## Derived partition scopes and top-k filtering

The frontend supports aliased derived partition scopes with outer column
projection and optional WHERE predicates using the existing typed three-valued
subset. Scopes can nest within the existing parser size/token/depth budgets. The
innermost query must be an existing native partition or predecessor formulation;
LATERAL, column alias lists, stars and outer computed expressions other than the
supported date_bin subset remain rejected. Outer grouping and windows use the stage bridge described below. Every scope resolves names only against its immediate inner
output labels, with quoted identifiers and ambiguous duplicate labels handled
explicitly. A source alias or hidden column cannot leak through a scope.

The runtime bridge inserts a pure typed Filter after the inner partition or
statistics finalization, before the outer output projection. It never pushes a
rank predicate into source filtering. Thus `row_number <= k` selects SQL row
occurrences in an explicit total order, whereas `rank <= k` preserves peer ties
and can produce more than k occurrences. Projection collisions consolidate signed
multiplicities. This uses bounded affected-partition computation; it is not yet a
specialized asymptotically optimized top-k index.

Nested scopes append separate Filter nodes in SQL scope order. Pure column
projections retain internal fields until the final output: filtering by exposed
values commutes with signed bag projection, including collisions. The binder
restricts visibility even while internal fields remain available to the circuit. This fusion applies only to pure column projection and deterministic predicates;
computed outer expressions beyond the typed date_bin subset require new lowering. Stateful window/group
stages use distinct retained nodes, as described below. Three additional
owned fixtures qualify nested peer/occurrence top-k and grouped count scopes,
renamed quoted labels, optional WHERE, changed predicate rejection and restart.

## Native PostgreSQL LAG/LEAD

Single-source partitions support LAG/LEAD over native bool, integral, text/varchar,
UUID, timestamp/timestamptz and numeric columns. Calls require OVER, one to three
arguments and a SQL ORDER BY containing the complete source primary key (plus the
generated ordinal after expansion). OFFSET defaults to one and DEFAULT to NULL.
Offsets accept signed int4 literals, NULL or native int2/int4 columns; bigint
columns and out-of-int4 literals are rejected. Defaults accept native columns,
NULL, integral/boolean literals and text/varchar string literals. Other input
casts and computed arguments/defaults require further expression lowering.

Navigation counts weighted row occurrences, including NULL-valued rows. Zero
selects the current row and negative offsets reverse direction. A NULL offset
returns NULL without using DEFAULT. DEFAULT is read from the current row only
when the target occurrence is absent; a present NULL remains NULL. Valid supported
ROWS/RANGE/GROUPS frame syntax is checked and ignored for navigation, matching
PostgreSQL. All functions in one SELECT still share one partition/order; mixing
navigation and framed aggregates requires further composition.

The explicit ordered evaluator revises complete affected partitions and emits
old/new bag differences. There is no clock, expiry or late-data exclusion. Weighted unit histories and five
sequential owned PostgreSQL/memory fixtures cover signed/NULL offsets, present
NULLs, native type promotion and values, neighbor-changing updates/deletes, source
filtering followed by outer rank filtering, projection collisions and cold restart.


## Native PostgreSQL FIRST_VALUE, LAST_VALUE and NTH_VALUE

These functions select a physical row occurrence within the current window frame.
They use the existing ROWS, default or explicit peer RANGE, and GROUPS frame
forms, including named windows and the full-partition default without ORDER BY.
ROWS uses the existing complete occurrence ordering requirement. Within a peer
group, SQL leaves relative row order unspecified unless the ORDER BY breaks ties.

The value argument can be a column or supported typed expression. It is evaluated
on the selected row. A selected NULL, an empty frame, or an out-of-frame position
returns NULL. NTH_VALUE evaluates its one-based integer position on the current
row. A NULL position returns NULL and a nonpositive position errors, including
on a row with an empty frame. Results retain the value argument's PostgreSQL
type and collation, with no result type modifier.

Frame navigation shares the bounded affected-partition evaluator with framed
aggregates. It selects by position from the ordered occurrences, so it does not
rescan the frame for each output row. The owned PostgreSQL harness compares
results with both PostgreSQL and an independent frame oracle through updates,
deletes and cold restart.

## Native left predecessor lookup

The frontend accepts this PostgreSQL shape, with native column projections and
an optional outer WHERE from the existing typed three-valued subset:

```sql
SELECT l.label, p.value
FROM source.probes l
LEFT JOIN LATERAL (
  SELECT r.value
  FROM source.history r
  WHERE r.key = l.key AND r.event_time <= l.event_time
  ORDER BY r.event_time DESC, r.id DESC
  LIMIT 1
) p ON true;
```

The two sources must be distinct native publication relations. One to eight
ordinary equality keys must connect them; keys use the current compatible native
bool/integral/UUID/temporal equality types. One `<` or `<=` bound must connect
native integral columns or columns of the same timestamp/timestamptz type.
Reversed comparisons normalize identically. ORDER BY starts with the right bound
DESC and contains the complete right primary key for deterministic ties. Supported
sort columns are native integral/temporal columns, with explicit ASC/DESC and
NULL ordering. There is no hidden Rust tuple-order tie breaker.

The inner local scope resolves unqualified names before the outer source scope.
Outer names see only the left source and the lateral projection's exposed labels;
the inner source alias and hidden right columns cannot leak out. Table qualifiers
must be distinct. Quoted labels and ambiguous names are handled explicitly. Inner
computed expressions, additional predicates, grouping, OFFSET, WITH TIES, alternate
limits, join conditions other than ON true, self joins and other unsupported shapes
fail before slot creation or durable registration. Pure derived projection/filter
layers can consume native lookup results, subject to unfinalized aggregate-operand
boundary.

The explicit runtime bridge tags full source tuples, keys each input, and executes
signed UNION ALL at one synchronized delta tick. The union arrangement retains both
full input bags. A differentiated partition transformation evaluates prior and new
lookup results only for affected keys. Thus late right inserts/updates/deletes
revise existing left matches, and simultaneous input changes include their complete
combined effect. Source primary-key validation remains separate from generic
weighted identity. There is no watermark, expiration, clock or finalization rule.
PUT, atomic PostgreSQL membership/result/progress COMMIT and slot ACK retain their
existing ordering; source LSN remains distinct from logical time.

This initial evaluator sorts right entries and scans them for each left entry in
an affected key, with bounded work and storage budgets. It does not claim a specialized
asymptotic predecessor index or arbitrary partition scale. Weighted histories cover
right duplicates, projection collisions, absent full-tuple retractions, complete
input changes, budget failure and cold restart. Five sequential owned PostgreSQL and
independent memory fixtures cover native timestamp/timestamptz, strict bounds,
post-selection filtering, composite equality keys, exact numeric values, source
updates/deletes, rollback and restart. Existing compiled plan encodings remain
unchanged.

References: [PostgreSQL lateral and left-join semantics](https://www.postgresql.org/docs/17/queries-table-expressions.html#QUERIES-LATERAL),
[PostgreSQL LIMIT ordering](https://www.postgresql.org/docs/17/queries-limit.html),
and the local Feldera `crates/dbsp/src/operator/asof_join.rs` and
`crates/dbsp/src/operator/dynamic/asof_join.rs` implementations.

## Constant VALUES relations

`VALUES` works as a standalone query, a set-operation input, a FROM source,
or a CTE body. Columns default to `column1`, `column2`, and so on. FROM
and CTE aliases can rename them. A single VALUES source supports `SELECT *`.

Each column resolves its type independently across rows using the supported
implicit native casts. Unknown literals take the resolved type, or text
when the entire column is unknown. Common type modifiers and collations
are retained. Duplicate and NULL rows participate in the result bag.
VALUES expressions must be foldable constants, with equal row widths of
up to 64 columns. A multirow constant bag has a 16 MiB value byte budget.
The worker emits the bag once at bootstrap and retains it across restart.

## Composed window and group stages

Aliased derived scopes can now feed another supported GROUP BY or window SELECT.
Each SELECT still has one common window specification, but successive scopes may
use different specifications. For example, grouped COUNT can feed MAX(count)
OVER(), followed by a grouped COUNT of the rows tying that maximum. A native LAG
stage can feed a running COUNT stage; an outer WHERE runs between those stages.
This is ordinary PostgreSQL subquery evaluation, including non-temporal ROWS,
RANGE-peer and GROUPS windows. It introduces no clock or expiration semantics.

The compiler resolves only exposed inner labels, assigns each new stage distinct
node IDs, aggregate fields and computed scalar fields, removes the intermediate output node, and connects
the new stage to its input. The final visible output remains `project`. Existing
source WHERE predicates become explicit source Filter nodes once, and each outer
WHERE becomes a Filter before that scope's map/key/aggregate nodes. Runtime
partition/statistics operators consume synchronized signed delta ticks; affected
partition results are differenced and delivered downstream in the same tick.
Every retained stage participates in the existing checkpoint and atomic durable
publication. Source LSN and circuit logical time remain separate.

Five sequential owned fixtures compare group/window, group/window/group,
window/group navigation/running-window and repeated date_bin map execution against PostgreSQL and an
independent memory oracle after inserts, updates, deletes, NULLs and cold restart.
Signed weighted unit histories separately test duplicate source occurrences,
projection collisions, thresholds, quoted aliases and several retained stages.
The CASE/gap and complete session stages described below build on this composition.

## Join-fed derived scopes

An aliased two-table inner equijoin projection can now feed supported derived
projection/filter, GROUP BY and window stages. The compiler replaces the inner
sink projection with a composable output boundary and removes that boundary when
connecting the next stage. Full joined tuples remain the internal bag identity;
name resolution exposes only the inner SELECT labels. Joined WHERE predicates
become explicit filters after the join, before the outer scope, and move as the
planner rewrites describe. They stay above the join when they read both sources.

Positional ROWS, ROW_NUMBER and navigation stages require explicit ordering by
all fields of the proven logical occurrence identity. Hidden keys cannot satisfy
that proof. Peer rank/frame stages preserve SQL ties without implicit keys.
Every expression must be supported by the in-engine runtime. Joined scopes compose through
explicit boundary maps in the shared graph.

Three owned sequential fixtures compare derived join projection, grouped counts,
MAX and COUNT DISTINCT, and per-auction top-one selection with PostgreSQL and an
independent nested-loop/group/order oracle. Both sources change in complete
transactions; NULL predicates, keys and values, rollback, deleting all inputs,
quoted aliases, changed-query rejection and cold same-slot restart are covered.
Signed unit histories additionally use auction weight two and bid weight three,
so joined multiplicity six must count as six occurrences and top-one must emit
one occurrence. This establishes this bridge, not general derived joins, outer
joins, arbitrary numeric expressions or automatic auction closure.

## Lazy native CASE and timestamp-gap comparisons

Searched CASE in a scalar projection now supports 1..=32 ordered WHEN branches
using the existing predicate subset. Result arms use the supported scalar
types and coercions, including [Boolean and UUID common types](#boolean-and-uuid-common-types).
Integer result promotion follows PostgreSQL int2/int4/int8 resolution (ordinary
small literals are int4). Simple CASE
evaluates its test once and compares each WHEN value in order using the
supported scalar equality rules, including the constant-operand requirement
for text comparisons.
NULL never matches, even when both values are NULL. The result type is resolved
separately across THEN and ELSE, with ELSE considered first and an omitted ELSE
treated as NULL. A scalar subquery used by simple CASE must be computed in
a derived scope. Searched CASE selects a branch when its condition is true. The runtime evaluates conditions in source order, then only the selected
result arm; later conditions are not evaluated. This is a Rust scalar-map stage
because flags may affect subsequent filters, keys and running aggregates.

Predicates may compare `timestamp_column - timestamp_column` to an explicitly
typed positive fixed INTERVAL literal. Both columns must have the same native
timestamp or timestamptz type. Supported literal quantities have at most six
fractional decimal digits, represent integral microseconds, and use microseconds
through days; calendar months and implicit cross-type/timezone casts are rejected.
Days here are interval comparison quantities, not calendar timestamp addition.
NULL operands yield unknown. Finite subtraction uses checked signed 64-bit
microseconds. Infinite differences follow PostgreSQL 17: equal infinities and
finite overflow raise an error; other infinite differences produce signed
infinite intervals, ordered outside finite gaps. This does not replace subtraction
with a timestamp-plus-gap rewrite that could evade PostgreSQL overflow behavior.

A derived LAG stage can feed a lazy boundary CASE, whose int4 flags can feed SUM
with ROWS UNBOUNDED PRECEDING in the next stage. Equality and NULL guards retain
their SQL meaning, including whether subtraction reaches infinity values. The
compiler invents neither guards nor expiration. All searched CASE expressions
use the typed expression evaluator; stage-specific fields prevent map collisions.

Six sequential owned fixtures compare local/instant flags, local/instant running
totals, nullable/promoted result arms and descending order against PostgreSQL and
independent source-bag recomputation. They include exact threshold equality,
NULLs, guarded infinities, late changes, native writer timezone changes and cold
restart. Weighted unit histories separately cover boundary merge/split effects
and duplicate occurrences; native gap tests cover both infinities and finite
range overflow. The complete session slice below adds temporal MIN/MAX and fixed-duration
addition to these flags.

The subtraction rules are grounded in PostgreSQL's
[timestamp_mi implementation](https://github.com/postgres/postgres/blob/REL_17_STABLE/src/backend/utils/adt/timestamp.c)
and saved live PostgreSQL infinity/overflow probes under artifacts/.

## Explicit PostgreSQL sessions and temporal boundaries

MIN/MAX now accept native timestamp and timestamptz columns as well as integral
columns, in supported grouped and window frames. They ignore NULL and filtered
out inputs, return NULL for an empty eligible frame, and compare native instants,
including infinities. The runtime retains the complete weighted input bag and
uses ordered full-identity state for grouped MIN/MAX-only plans. Mixed aggregates
and temporal window frames retain affected-partition evaluation.

Scalar projections accept `timestamp_column + INTERVAL '10 seconds'` and the
corresponding subtraction. Literals must be explicitly typed, positive, integral
microseconds, with one quantity/unit from microseconds through hours. `24 hours`
is a time duration; `1 day` is rejected because calendar-day timestamptz arithmetic
can depend on session timezone across DST. Months, implicit casts, arbitrary
interval expressions and cross-type arithmetic remain rejected before registration. NULL propagates, infinities stay infinite, and finite range overflow fails the
complete staged tick.

A supported session query explicitly composes:

1. Source WHERE time IS NOT NULL, then LAG(time) ordered by time and native identity.
2. Lazy CASE: previous IS NULL starts a session; equal times stay together;
   otherwise a gap >= INTERVAL '10 seconds' starts a session.
3. Running SUM of the int4 flag over ROWS UNBOUNDED PRECEDING.
4. GROUP BY bidder and running session number, with MIN(time), MAX(time), COUNT(*).
5. An outer projection emitting first time and last time + INTERVAL '10 seconds'.

The equality guard is actual query SQL, including its behavior for repeated
infinities. It is not an optimizer rewrite. The MV is the bag produced by that
PostgreSQL query at each committed source boundary. A late bridge insert can merge
sessions; deleting it can split them, changing counts and boundaries. No wall
clock, watermark, expiry, final-only emission or state eviction is introduced.
An unsupported or overflowing PostgreSQL expression is not silently approximated.

Eight sequential owned fixtures compare complete local/instant sessions,
filtered local/instant extrema, empty/all-NULL window frames and both offset
directions with PostgreSQL and direct source-bag connectivity oracles. They cover exact threshold and microsecond boundaries, NULL keys and
excluded NULL times, equal infinities, multi-row transactions, rollback, timezone
changes across DST, bridge insert/delete, moves between bidders, removal to empty,
cold restart and incompatible gap/label rejection. Signed weighted unit histories
restore every retained stage between ticks and include duplicate occurrences;
offset range failures preserve the prior checkpoint and permit valid retry.

## DISTINCT aggregates

Grouped and global COUNT, SUM, AVG, MIN and MAX accept DISTINCT over one
int2, int4 or int8 column or supported scalar expression. Boolean bool_and,
bool_or and every also accept DISTINCT. FILTER admits only true rows and runs
before a computed argument. NULL inputs are ignored. Each distinct value
contributes once, so retracting a duplicate changes the result only if it was
the last qualifying occurrence. MIN, MAX and the Boolean aggregates use their
ordinary evaluators because their results are invariant to duplicates.

COUNT returns zero with no qualifying values. The other aggregates return NULL.
A global group survives empty input. A keyed group disappears when its last
input row is removed. SUM returns bigint for int2/int4 and numeric for int8.
AVG returns numeric for all three widths. Multiple DISTINCT and ordinary
aggregates can share a SELECT and feed a supported outer stage.

DISTINCT with OVER, multiple arguments, aggregate ORDER BY, and nonintegral
arguments to COUNT, SUM, AVG, MIN or MAX are outside this aggregate subset.
The [exact grouped membership path](#exact-grouped-distinct-membership) and
affected-group recomputation both use the same result finalization.

Sequential PostgreSQL fixtures exercise all three integer widths, global
and mixed groups, and nested composition. They compare exact native results,
binary representations and metadata, and independent value-presence oracles
through duplicate threshold crossings, FILTER, NULLs, empty input and restart.

### Named Nexmark q0 and q2 qualification

`queries/nexmark/q0.sql` projects the five native bid payload columns, preserving
full source occurrences even when projected tuples collide. It is qualified with
both timestamp and timestamptz sources, NULLs, infinities, Unicode text and extreme
bigint prices. `queries/nexmark/q2.sql` projects auction/price after the PostgreSQL
`mod(auction,123)=0` filter; int2/int4/int8 inputs include negative multiples,
nonmultiples, zero, NULL and each integer width's extremes. Source IDs remain
source-only identity and are not exposed as an artificial output key.

Five sequential owned fixtures compare every result with PostgreSQL and a raw
Python Counter oracle (integer divisibility for q2). They cover bootstrap,
insert/update/delete, primary-key moves, rollback, complete simultaneous source
transactions, ticks with no relevant input changes, changed-query rejection,
normalized aliases/remainder syntax, cold same-slot restart and final retraction.
These queries add named portfolio evidence using existing runtime operators; no
new expression semantics are introduced.


### Named Nexmark q20 full-payload join

`queries/nexmark/q20.sql` preserves Feldera q20's inner equijoin and category=10
filter, using the native snake_case schema and explicit bid/auction timestamp and
extra labels. All 16 projected payload values retain PostgreSQL native semantics;
there is no auction-time eligibility or implicit closure condition in q20.

Four sequential owned fixtures qualify timestamp/timestamptz, all three integral
category widths and text/varchar payloads against PostgreSQL and an independent
raw-source nested-loop Counter. Histories include NULL join keys/categories/
payloads, timestamp infinities, Unicode, extreme bigint prices/reserves, projected
duplicates, concurrent changes to both inputs, coordinated auction-key/bid-key
moves, new/new insertion and simultaneous deletion, category transitions, rollback,
DateStyle/time-zone changes in source writers, changed-filter/projection rejection,
quoted alias normalization, cold restart and final retraction. This uses the
existing synchronized-input join cross term and durable publication path; no new
runtime operator is introduced.


### Named Nexmark q18 and q19 ordered selection

`queries/nexmark/q18.sql` selects the latest bid per bidder/auction. PostgreSQL
DESC NULLS FIRST means a NULL event time sorts before finite/infinite times; this
is explicit in SQL. Native bid ID ascending resolves otherwise equal times.
`queries/nexmark/q19.sql` selects ten occurrences per auction with their row
numbers, ordered by price DESC NULLS FIRST, event time DESC NULLS FIRST and native
bid ID ascending. The time/identity tie-breakers refine Feldera's unspecified
price ties. Both queries omit the source-only ID from the visible Nexmark payload;
it is a stable ordering input, not an output uniqueness constraint.

Four sequential owned fixtures compare both timestamp variants with PostgreSQL
and an independent Python occurrence-ordering Counter. They include more than
ten equal-price bids, NULL keys/prices/times, infinities, extreme prices, equal-time
ties with different visible payloads, late insertion, winner deletion and backfill,
partition/PK moves, source DateStyle/time-zone changes, rollback, empty query-input
ticks, changed-cutoff/NULL-order rejection, quoted aliases, cold same-slot restart
and final retraction. Existing affected-partition recomputation remains the runtime
algorithm; these fixtures establish correctness, not an optimized top-k index or
arbitrary large/skewed partition cost.


### Named Nexmark q11, q12 and q17 temporal statistics

`queries/nexmark/q11.sql` makes the portfolio's session connectivity explicit:
LAG ordered by recorded event time/stable bid ID, lazy CASE boundaries, running
SUM with ROWS, then temporal MIN/MAX and occurrence COUNT. Adjacent gaps strictly
below ten seconds connect; equality starts another session. NULL times are
excluded, NULL bidders group together, equal infinite times are guarded before
subtraction, and mixed finite/infinite transitions use PostgreSQL 17 interval
semantics. A session ends ten seconds after its last event. Late inserts, updates
and deletes revise sessions; there is no watermark, expiry or implicit clock.

`queries/nexmark/q12.sql` buckets the separate native `processing_time` source
column. It is assigned once by source ingestion and replicated like any other
value; the engine never regenerates it using now(), LSN or logical time. The local
Feldera template uses date_time, while this declared processing-time adaptation
keeps event time separate. date_bin has explicit ten-second stride/UTC origin;
NULL and infinite buckets retain their SQL groups and boundary addition propagates
them. The local-timestamp fixture uses a local origin without an offset.

`queries/nexmark/q17.sql` emits filtered counts, retractable min/max and exact
integral AVG/SUM by auction and UTC midnight `day_start`. As for q15, this explicit
native midnight key replaces the template's date output ABI. The bound bigint
SUM can exceed i64 and is emitted as PostgreSQL numeric; all-NULL measures yield
NULL extrema/AVG/SUM while row count remains positive.

Six sequential owned fixtures qualify both timestamp types against PostgreSQL
and independent adjacency components, integer bucket arithmetic and raw price
statistics with Decimal average rounding. Histories cover exact gap/bucket/day/
price-band boundaries, pre-origin time, NULLs/infinities, session bridge merges
and deletion splits, group moves, all-NULL/non-NULL transitions, extreme integral
sums, source DateStyle/time-zone changes, rollback, empty query-input ticks,
changed-gap/stride/threshold rejection, quoted aliases, cold same-slot restart
and final retraction. q12 additionally asserts that an event-time-only update
leaves its output bag unchanged. These use existing typed relational operators and
checkpoint codecs; no expiration policy is introduced.


## Named local item suggestions (q3)

`queries/nexmark/q3.sql` follows the Feldera local item suggestion query: auction
seller joins person ID, category is 10, and state equals OR, ID or CA. The owned
harness compares the exact SQL through a native PostgreSQL MV and an independent
raw-source nested-loop memory bag. Default deterministic text, C-collated varchar
and deterministic ICU text fixtures cover case/trailing spaces, NULLs, simultaneous
source changes, key moves, empty other-source ticks, rollback and cold restart.
Changed state literals reject reuse; quoted aliases preserve normalized identity.
Text ordering, unresolved column collations and nondeterministic source collations
reject before slot creation or registration.


## Group-only membership

A nonempty `GROUP BY` may project grouping columns without aggregate calls. Each
nonempty group emits one row; NULL keys group together. Omitting a grouping key
from the projection can produce identical output tuples from different groups,
whose weights add. Non-grouping output columns are rejected before registration.
This is group-only SQL, not general `SELECT DISTINCT` or grouping sets.

Lowering uses the existing exact statistics operator with only its hidden row
count, followed by finalization. Inserts and retractions change that count; only
empty/nonempty transitions change membership. No COUNT result is exposed or
narrowed to bigint. Full-tuple input multiplicity remains distinct from group
membership. Derived scopes can consume
these ordinary typed outputs. Group-only output does not supply native occurrence
identity for positional windows.


## Inner joins between derived scopes

Two aliased relational scopes can use explicit `CROSS JOIN`. Each input maps to
a constant navigation key and feeds the same durable weighted join as an
equijoin, including the simultaneous-input cross term. NULL tuples participate
normally. Cartesian fan-out remains subject to existing work and storage limits. No clock behavior is implied:
a persisted clock relation changes computation through ordinary source deltas. The owned native-cross fixture compares exact PostgreSQL materialized-view bags
and a raw nested-loop memory oracle through simultaneous changes and cold restart.

Native tables and independently compiled non-LATERAL subqueries use one join
tree and aggregate/projection builder. Derived inputs require aliases and expose
only their selected labels; ambiguous unqualified names reject. INNER, CROSS,
LEFT, RIGHT and FULL joins share the same key binding, weighted execution, and source
occurrence limits. [ON key extraction and residual predicates](join-predicates.md)
cover numeric/text keys, OR and non-equality conditions. [USING and outer joins](outer-joins.md)
preserve merged names and qualified originals. NATURAL joins infer the common
column names and use the same merge and outer-join rules.
WHERE runs after the join, and the common builder handles outer
projection, grouping, and windows under their existing semantic restrictions.

Scope boundaries, compound key expressions and complete native contracts belong
to normalized graph identity. Equality operand direction, key order and
repeated equivalent keys normalize; changed key sets or inner semantics cannot
reuse incompatible durable state.


## Computed integral predecessor keys

The left operand of a predecessor equality key may be `mod(column, literal)` or
`column % literal`, optionally using `pg_catalog.mod`. The input must be a native
left integral column and the divisor a nonzero integral literal fitting i64.
The right operand is a native column. PostgreSQL promotion is explicit: an int4
literal promotes smallint input to integer; bigint input or an int8 literal
produces bigint. The right key must have that exact result type. Other arithmetic,
right computed keys, casts and zero divisors reject before registration.

A typed scalar map evaluates the remainder before the tagged predecessor input;
it never runs at sink time. NULL stays NULL, signed remainder follows truncation
toward zero, and the minimum integer modulo -1 yields zero. Equivalent MOD/%
spelling, equality orientation and conjunct order normalize. The maintained lookup still selects
one right occurrence by the complete deterministic order, preserves the left
full-tuple bag and revises matches when either source changes.

## Logical occurrence identity and grouped ranking

Derived positional windows use typed graph lineage to prove complete row
identity. Native primary keys seed this proof; filters and maps preserve it,
scoped boundary maps must retain and rename every identity field, and joins
combine disjoint identities. GROUP BY replaces prior source identities with
the complete group key, including its NULL groups. Partition columns and order
columns together must cover the identity. Missing or hidden fields reject before
registration. Untagged unions, predecessor outputs without a separate left-row
proof, and repeated expansion namespaces remain unknown for positional use.
This proof does not replace generic full-tuple weighted identity or source PK
validation; it establishes deterministic occurrence order for admitted SQL.

Logical occurrence proofs are compiler validation, separate from weighted row
identity. They determine whether positional operators are legal; the resulting
order, fields and graph are included directly in plan identity.

The q6 circuit executes eligible auction/bid join, persisted-clock cross join
and closure filter, grouped MAX, ROW_NUMBER per seller, latest-ten filter and
grouped AVG. Numeric AVG finalization executes in the engine. Clock updates drive ordinary synchronized input deltas, preserving
the join cross term and object PUT → PG publication COMMIT → slot ACK order.

## Retained affected-partition output

Plans with native partition/window or predecessor-lookup operators now retain
both operator input and its exact intermediate output. The new runtime computes
current affected partitions once and subtracts that output; it still scans
current affected partitions. Source contracts, full-tuple bag
identity, complete delta ticks and publication/ACK ordering remain binding. See [the Feldera algorithm audit](feldera-audit.md) for the measured callback-work
reduction and remaining ordered-index/distinct work.


## Exact grouped distinct membership

Keyed integral COUNT/SUM/AVG DISTINCT, together with ordinary COUNT/SUM/AVG
and supported FILTER, use exact per-group/value occurrence counts. Membership
changes add or retract one contribution with the value as the SUM/AVG measure.
Full source identities and ordinary rows preserve validity and group presence.
Counts and sums accumulate with BigInt before result finalization. Global and
mixed MIN/MAX or Boolean groups use affected-partition evaluation.

The native SQL subset and PUT/PG COMMIT/ACK boundary are unchanged. The
[Feldera audit](feldera-audit.md) records the controlled block-read measurement,
state tradeoff and remaining ordered-index work.

## Ordered full-identity grouped extrema

Grouped queries whose aggregates are all MIN/MAX and whose grouping keys are
nonnumeric maintain a reusable ordered index over complete weighted rows per
aggregate. Integral, timestamp and
timestamptz values use explicit native ordering, including infinities. FILTER
retains only true; NULL and excluded values use a last sentinel so a nonempty
group with no eligible values still produces NULL. NUL-prefixed internal fields
cannot collide with PostgreSQL identifiers; malformed reserved fields fail closed.
The original full row remains the identity within each value order, including
when multiple source occurrences share the same value.

The executable graph expands each input into per-aggregate ordered identities,
uses OrderedFirst against synchronized prior input and retained selection traces,
then combines the small selected set into the grouped result. Every changed full
identity is checked before an early cursor stop; retracting a nonexistent tail
row cannot be hidden by an unchanged minimum. The generic operator requires a
nonnegative prior input and exact prior selection, established inductively from
empty state and reused only from the bound authoritative checkpoint.

The maintained ordered arrangements are explicit graph state. Mixed aggregate
and window queries retain their appropriate generic operators. A graph/layout
change cannot silently adopt incompatible checkpoints.

COUNT, SUM, AVG and COUNT(DISTINCT) over numeric grouping keys stay additive. A
hidden retained SUM over each key tracks its members' display scales, and the
published key takes the largest retained scale, so `1.50` is not trimmed to `1.5`.
That spelling always belongs to a current member. A retraction can change it. The
extra state doubles a COUNT(*) group's nonzero statistics, so fewer such groups fit
one tick's `output_entries` and `contributions` budgets. A query whose aggregates
and retained scales exceed the statistics width uses the affected-partition evaluator.

MIN/MAX over numeric grouping keys use the existing affected-partition evaluator, including
when the aggregate arguments are integral or timestamp values. Numeric-equal
keys such as `1.20` and `1.2000` share one group while retaining full raw row
identity. The evaluator takes all grouping fields from one retained member;
independent MIN/MAX winners cannot disagree about that member's display scale.
PostgreSQL may choose a different member's scale for the same numeric group,
so SQL numeric equality and retained-member representation are qualified
separately. A representative deletion can change the displayed group scale.
The numeric Rust history covers weighted duplicates, updates, retractions,
last-member removal, cold restore and compaction; three live numeric cases check
MIN, MAX and their combination against a native materialized view and an
independent Decimal oracle.

A controlled 1,000-row/two-extrema update with eight-row blocks and cache disabled
reduces data-block cache misses during the complete prepare operation from 127
to 12, including changed-identity checks and staging reads. New immutable objects
for that tick increase from four to six and from 918 to 1,698 bytes. These exclude
initial bootstrap/cold restore and PostgreSQL metadata/WAL. Full-row copies per
aggregate increase state and write work; deleted prefixes, many retained runs and
compaction policy still affect reads. Qualification covers all three integral
widths and both timestamp types, NULL/FILTER, ties, retractions, simultaneous joined
changes, independent memory/native-MV agreement and cold restart.

## Checked expression execution progression

The circuit scalar tree supports native integral/numeric operands, explicit
integral casts, checked +/−/*, lazy COALESCE and searched CASE. Computed
projections and GROUP BY keys use Map nodes; predicates evaluate typed operands
with SQL three-valued comparisons and NULL tests. All-unknown CASE and COALESCE
results resolve to text. An explicitly typed NULL can supply the intended
native type. CASE result promotion and nullability use the shared typed
representation.

[EXTRACT and date_trunc](temporal-arithmetic.md) now operate in the typed engine
over date, timestamps and interval. EXTRACT emits exact numeric fields; timestamptz
uses the fixed UTC context. Date and interval arithmetic preserve calendar fields
and native bounds. Named-zone/AT TIME ZONE expansion, volatile functions, user
functions and arbitrary casts remain rejected. Source LSN and logical time remain
distinct from temporal SQL values.

## Native and mixed derived join trees

Inner joins execute the maintained incremental join, including simultaneous
input cross terms. LEFT joins tag and union both complete input deltas, then
recompute affected retained key partitions and subtract their prior outputs.
NULL keys on opposite sides cannot match. An unmatched left tuple produces
one null-extended right tuple with its original multiplicity; matched weights
multiply exactly before final narrowing. WHERE runs after null extension, so
filtering a matched row cannot manufacture an unmatched row. Complete tuple
identity survives state maintenance, publication and cold recovery. Left joins
currently do not establish a uniqueness proof for positional window ordering.
RIGHT normalizes physical inputs through this same machinery. FULL additionally
emits unmatched right occurrences with the entire missing scope null-extended;
see [outer joins and USING](outer-joins.md) for names, types and rebuild boundaries.

Source occurrences, compound keys, retained left-join semantics and nested
scope bindings are explicit in normalized IR alongside full native contracts.
Affected-key recomputation and fanout remain bounded by execution budgets; this
does not establish indexed outer-join performance at arbitrary scale.

The sequential native join harness compares eight query shapes against native
PostgreSQL materialized views from identical SQL and an independent raw-row
loop oracle, through NULLs, updates, deletes, simultaneous changes and restart.

## Ordered occurrence top-k specialization

A derived filter directly consuming a partition with only ROW_NUMBER can use
an immutable ordered input and retained top-k output. The predicate must be one
ordinary integral comparison of that row-number column with a literal: `<= K`,
`< K`, `= K`, or an equivalent operand-reversed comparison. The derived cutoff
is at most 1,024 occurrences; nonpositive cutoffs select no occurrences. The
original filter still executes after selection, including equality at the cutoff.
Other predicates, larger cutoffs and mixed window functions retain the existing
partition evaluator. RANK/DENSE_RANK keep peer semantics.
SQL ORDER BY/LIMIT uses the lowering in the [ordering contract](ordering.md).

The existing ROW_NUMBER uniqueness proof remains required: partition and order
fields must include complete logical occurrence identity. Native integral and
timestamp ordering uses explicit fixed-width tokens with signed bias, direction
and independent NULL placement. Full source tuples remain distinct in the input
index. Timestamp infinities retain PostgreSQL order; timestamptz compares instants.
No watermark, wall-clock expiry or historical truncation is introduced.

The generic OrderedTopK operator validates every changed full identity before
reading a bounded ordered prefix, even when K is zero or a changed tuple is
outside the prefix. It merges complete delta coefficients with prior state,
counts positive row occurrences and clips the final tuple's multiplicity at K.
Retained prior output is subtracted exactly; selected occurrences receive their
one-based row numbers. Both sides of a joined source transaction are evaluated
against the same prior boundary before this selection. Deleted prefixes and
cancelling physical runs can still require substantial reads.

The typed order, cutoff, field bindings and maintained arrangements are explicit
in graph identity. Top-k and extrema use their specialized operators inside the
same durable relational engine; changed graphs cannot adopt incompatible state.

The owned top-k harness covers both order directions, NULL placement, reversed
and strict/equality/zero bounds, integral extrema, both timestamp types and
infinities, projected duplicate bags, changes and cold restart. The native
materialized view uses identical SQL; the independent oracle sorts raw source
occurrences. A cache-disabled 128-block fixture compares a complete partition
read with at most 12 reads for an unchanged three-occurrence prefix and a tail
update. This measures that fixture, not worst-case fanout or end-to-end capacity.

## Native DISTINCT and equality-correlated existence

`SELECT DISTINCT` accepts admitted native or computed projections, including
derived scopes, filtered by the existing predicate subset or the existence form below.
It partitions by complete projected values, including NULL groups, while retaining
every complete source row until membership is tested. A nonempty partition emits
one projected occurrence without narrowing its total occurrence count. Supported
output types are boolean, integral, deterministic text/varchar, UUID and timestamp
types. Repeated
output columns remain legal. Single-table DISTINCT ON follows the [ordered selection contract](ordering.md#distinct-on). Unsupported output domains still reject before registration or
slot creation. Grouping after restricted EXISTS/NOT EXISTS now uses the same
partition lowering as other grouped queries; see the [composition matrix](composition-matrix.md).

An outer ordinary source may use exactly `WHERE EXISTS (SELECT 1 FROM schema.table
alias WHERE inner.key = outer.key [AND ...])`, or `WHERE NOT EXISTS (...)`.
Correlations must use qualified operands and 1..16 equalities connecting the
inner and outer inputs. Key types, computations and collations use the shared
[join key binder](join-predicates.md). Aliases expose source
occurrences, permitting self correlation with distinct aliases. Additional outer
or inner predicates, OR/inequality correlations, uncorrelated subqueries, nested
subqueries, aggregates, ordering and limits remain rejected for this EXISTS form.
[IN/NOT IN and scalar aggregate subqueries](subqueries.md) have a separate bounded
shared relational lowering. NOT IN preserves UNKNOWN and differs from NOT EXISTS.

Execution tags both complete full-row input bags, separates NULL keys by side,
unions synchronized transaction deltas and computes current affected key
partitions against retained output. EXISTS emits each left tuple with its original
multiplicity when any right occurrence exists; NOT EXISTS emits it only when none
exists. Right duplicates never multiply left rows. Both inputs change against the
same prior logical boundary. Retractions, empty groups and cold recovery use the
same operators and budgets. This implementation scans affected partitions; it
claims no constant-work bound for popular keys.

Changed SQL cannot adopt incompatible state; existing registrations
whose physical graph changes require explicit rebuild. Object format and
PUT → atomic PG membership/result/progress COMMIT → slot ACK remain unchanged.

The sequential `query_membership_harness.py` compares identical-SQL PostgreSQL
materialized views and independent raw-row Counter calculations across duplicate
projections, NULLs, inserts, updates, deletes, simultaneous input changes and cold
restart. Rust tests additionally exercise weighted left duplicates directly and
restore all maintained object-backed input/output arrangements.
## Bounded exact circuit numeric

Native PostgreSQL numeric can participate in circuit comparisons, equality join
and grouping navigation, computed +, − and *, COALESCE/searched CASE, COUNT,
SUM, MIN and MAX. Finite values use a signed BigInt coefficient and retained
display scale. Addition/subtraction select the maximum operand scale;
multiplication sums operand scales. These operations need no rounding.
Numeric equality and navigation strip redundant fractional zeroes, while source
and computed full tuples retain their display scale. PostgreSQL's numeric order
is −Infinity < finite values < Infinity < NaN; NaN equals NaN for indexing and
comparisons. Infinity arithmetic and zero-times-infinity produce the PostgreSQL
special values. NULL propagates and lazy branches preserve evaluation order.

Circuit input/result text and scale are bounded to 4,096 characters/digits; this
is an explicit circuit resource restriction, far below PostgreSQL's full numeric
range. Intermediate integer powers and products are bounded by operand limits.
This is not an RSS bound. [Exact numeric arithmetic](numeric-arithmetic.md) now
adds division, numeric AVG, rounding and checked integral/precision-scale casts.
Native source typmods have already been applied by PostgreSQL before CDC;
explicit expression casts apply their own modifiers inside the engine.

Numeric SUM and extrema currently use bounded affected-partition recomputation,
retaining all input identities and scales so deleting the highest-scale input
recomputes the correct result scale. Numeric navigation never collapses raw
full-tuple identities. Numeric rendering-only sink maps may cross a derived
scope because their raw circuit values are already usable; final presentation
computations continue through PostgreSQL publication when possible. Numeric
operators required by downstream scopes execute in the circuit.

The revised native
source binding also applies when the captured source contract contains timestamp
columns, even if the query does not consume numeric values. Published object bytes remain unchanged and readable. Feldera's local
`sqllib/src/decimal.rs` provides design evidence for typed decimal arithmetic;
its fixed precision/scale result types and panic-based overflow paths do not
establish compatibility with PostgreSQL's unconstrained numeric semantics.

Constant subexpressions are checked by the local typed binder, including inside
row-dependent lazy expressions. An unqualified constant-error subtree rejects
before activation; the engine does not ask PostgreSQL's planner to evaluate it.

## Computed grouped aggregate inputs

Grouped COUNT, integral SUM/AVG/MIN/MAX, numeric SUM/MIN/MAX and the
DISTINCT aggregate forms above accept locally supported typed scalar arguments. The
[admission matrix](m4-computed-aggregate-inputs.md) records exact boundaries.
Argument maps run after WHERE. For computed arguments, FILTER becomes a lazy
CASE with a typed NULL ELSE; excluded or UNKNOWN-filter rows do not evaluate
the argument. Empty/all-filtered groups remain present, and distinct values
retain full source multiplicity. Repeated identical arguments and filters share
a map within their stage. Scalar grouping calls can also be selected.

Already supported derived sources retain these rules. M6 adds frame-aware
computed window arguments as described below. Ungrouped or
nested aggregates and unavailable scalar overloads continue to reject explicitly.
COUNT(NULL) resolves the untyped NULL to text; other ambiguous NULL arguments
require an explicit cast. This slice adds no PostgreSQL computation path.

## Computed inner-join keys

Native and supported derived INNER/LEFT/RIGHT/FULL inputs accept side-local computed
numeric/text equality keys and residual predicates. Top-level equalities become
navigation keys where compatible; other conditions evaluate over candidate pairs.
The [join predicate contract](join-predicates.md) specifies collation, NULL,
match-transition and resource boundaries. Restricted EXISTS/NOT EXISTS retains
its existing equality-only correlation rules. Hidden relation reads and volatile
or time-dependent expressions remain unsupported.

ON maps evaluate each input row that survives its filters, even when the opposite input
is empty, as a merge join does; see the planner rewrites below. Errors retain the last
sink/progress/ACK boundary.

A row-dependent expression error does not end the worker. It keeps the failed epoch
unpublished, reports `evaluation_blocked`, and merges later committed source transactions
into that epoch. Deleting or correcting the row therefore unblocks the view at a later
boundary, as a later REFRESH would, including when the repair changes the other side of a
join. Retries run only after new input, on a backoff from 100 ms to 30 s, so a repair can
take that long to publish. Progress and the slot stay at the last durable boundary while
blocked, so source WAL grows. An epoch of several transactions first replays them one at
a time, which publishes the valid prefix and holds only from the transaction that failed.
Each `evaluation_blocked` event gives the LSN and relations where the block began, plus
the merged size and ceilings. A merged epoch that reaches its ceiling fails permanently,
because replay would reach it again and a later repair can no longer take effect, and a
single transaction already over the ceiling leaves no room to merge one: repair the row
and rebuild, or raise the epoch limits. An epoch that exceeds a memtable or operator limit
also fails permanently. Internal plan contradictions are not row-dependent and end the
attempt immediately.

AND and OR follow PostgreSQL's evaluation order, so they raise the same errors.
Operands run left to right and stop at the first deciding value, including inside
CASE and FILTER: `10/x<5 AND x>0` raises at x=0 and `x>0 AND 10/x<5` does not. Top-level
WHERE, HAVING and ON conjuncts run as the planner orders them: NOT is pushed through OR,
common OR factors are pulled out, conjuncts run cheapest first with equalities last among
ties, and the first one that is not true stops the row, including NULL. Aggregate-free
HAVING conjuncts of a grouped query join WHERE. An IN list with two or more constants
tests them before its computed elements. A constant operand that decides an AND or OR
removes the rest.

### Planner rewrites

`compiler::plan` applies the rewrites that decide which rows an expression sees, after
binding and before execution, so the same query raises where PostgreSQL does:

- A filter runs at the lowest scope it can: through derived tables, computed columns and
  inner joins to the input it reads, to the preserved side of an outer join, through
  UNION and INTERSECT arms but no set-operation tree containing EXCEPT, and below grouping
  or window partitions when it reads only their keys. A filter on one input raises for its
  rows that never match.
- An ON conjunct on an outer join's nullable input moves to that input. One on the
  preserved input stays a join condition and sees candidate pairs only.
- WHERE and ON conjuncts of inner joins merge, so one guards another whichever clause it
  was written in.
- A strict filter on a nullable input reduces the outer join: LEFT or RIGHT to inner, FULL
  to LEFT, RIGHT or inner.
- Equality with a constant carries across chains of equijoin keys, an IN semijoin probe or
  an EXISTS key. An OR over a join also filters an input when every arm restricts it.
- A top-level `IN` conjunct of WHERE is a semijoin, so filters beside it run first. Any
  other membership test, including `NOT IN`, stays a filter that runs before costlier
  conjuncts on its own input.
- Once movement settles, output maps simplify decided CASE/COALESCE and Boolean branches
  before collecting their live inputs. Unread membership subplans and ordinary aggregate
  inputs can then disappear. Failed simplification retains the original expression for
  final folding; aggregate FILTER arguments retain their separate planning checks.
- Unread computed columns and aggregates are dropped, except a UNION ALL whose arms differ in
  type, which keeps every output because PostgreSQL does not flatten it. Sort expressions and
  the keys of an unread window still run, and filters stay above that window unless they read
  its partition keys. A global aggregate with no result read returns its row without reading
  its input when that input flattens to one relation or a simple UNION ALL, though constants in its
  conditions still fail; over a join or an unflattened subquery it reads the input. A membership
  output nothing reads goes away with its subquery. Computed outputs of a flattened
  scope run above its joins, on joined rows only; a non-strict output over a nullable
  input stays below the join, as a PlaceHolderVar does.
- A LEFT or RIGHT join disappears, with its nullable input's filters, when that input goes
  unread and is one base relation keyed by the join or a subquery grouped or distinct on it.
  Constants in the condition and in a flattened input's filters still fail.
- A CTE that is referenced twice or declared MATERIALIZED is a boundary: no rewrite crosses
  it, so it evaluates every output and filter as written. Any other CTE is inlined.
- Constants fold after these rewrites, so only what survives can fail, and a failure rejects
  the query at registration as it raises during PostgreSQL planning. `false AND 1/0>0`, an
  untaken constant `CASE` branch, and an unread `1/0` output register, unless that output is in
  a UNION ALL arm of one relation with no WHERE; `1/0>0 AND false` and
  `CASE WHEN x>0 THEN 1/0 END` do not. Each written clause folds on its own, so a failure in
  one clause is not spared by a false in another, and an aggregate FILTER does not spare its
  argument. A materialized CTE that is referenced folds even if no reader of it runs. LIMIT 0
  plans its input, so its constants fail, then runs nothing. A strict operator over a constant NULL folds to NULL
  without evaluating its other operand.
- A constant false or NULL filter makes its scope read nothing, along with a join or semijoin
  it empties, so other filters there never run. A constant row beside it still emits. A
  constant false or NULL ON condition of a LEFT or RIGHT join never reads the nullable input.
- `IS NOT NULL` on a NOT NULL base column that no outer join can null is true, and `IS NULL`
  false, as in PostgreSQL 17, including as a whole arm of an OR. An OR rejects NULLs from an
  input when each arm is strict on some column of that input.
- A filter stays above a set operation that casts the column to a common type in any arm.

The planner shares one join view over the existing relational graph. It resolves the
execution tags, count/presence nodes and unions into input attachment points; passes
use the original node for join semantics rather than maintaining a second join-kind IR.
Inner/outer/semijoin filter placement uses one loop, with explicit allowed inputs and
OR-restriction rules. Inferred equalities for all keys attach together, preventing a
second insertion from targeting an edge the first insertion already replaced.

One scope analysis computes append eligibility, flattening, alignment casts and EXCEPT
boundaries. These are separate properties: a materialized subquery can be a simple
UNION ALL member while still preventing flattening. Union pushdown analyzes only its
current input roots; initial provenance includes all bound nodes. Initial flattening
facts stay frozen when rewrites later remove joins or move filters.

The deferred folding and settled-pruning phases remain intentional. Substitution and
unused-output removal decide which expressions PostgreSQL plans, so folding earlier
can report errors from outputs PostgreSQL discards. Planning before physical lowering
would require a coordinated change to binder cardinality/key proofs; the shared views
remove repeated decoding without introducing another execution path.

`scripts/error_order_corpus.json` pins errors and result rows for each rewrite against real
PostgreSQL, under a merge join, the plan that reads both inputs whole as Pgderive does.
Other plans can differ, and these differences remain:

- With an index probe, or an outer join input that is empty or filtered empty, PostgreSQL can
  skip rows of the other input that Pgderive evaluates. An incremental join has to read both
  inputs because either can change, so Pgderive can block even where every PostgreSQL plan
  publishes: `a LEFT JOIN b ON NOT (10/(b.k-1)=0)` raises for `b` while `a` is empty. A join order
  PostgreSQL chooses over the written one changes which pairs a two-sided condition sees.
- PostgreSQL adds an OR restriction only when its statistics call it selective.
- A merge join sorts on an extra computed equality such as `3/a.other=b.id`, so it raises for
  rows Pgderive reaches only as candidate pairs.
- Contradictory constants over a column, such as `x=1 AND x=5`, make PostgreSQL skip the
  query. Pgderive still evaluates what runs before that filter.
- When filters substitute into constants, Pgderive folds them after sorting, so
  `1/0>0 AND 2=1` reached that way empties the scope where PostgreSQL's written order raises.
- Expressions inlined past 16 KiB stay where they were written.
- An unread computed column of a scope with LIMIT or OFFSET still runs, because it is part
  of that scope's total order.
- LIMIT 0 empties a join beside it, where a merge join still sorts the other input and can
  raise.
- A LEFT join whose ON keys are joined with a constant false is removed like any other, so a
  failing constant in a grouped input goes unreported where PostgreSQL keeps the join and raises.
- A subquery under a constant false or NULL filter still folds, so its failing constants reject
  the query at registration where PostgreSQL may never plan that subquery.
- A set operation wrapped in a plain subquery under UNION ALL counts as a nested one, so an
  unread global aggregate over it reads its input where PostgreSQL does not.
- A failing constant output of a UNION ALL arm over a derived subquery, such as
  `SELECT g.id, 1/0 FROM (SELECT DISTINCT id FROM t) g`, goes unreported when unread; PostgreSQL
  flattens that arm and raises.
- A membership output a query does read runs below the joins of its scope, so it can raise for
  rows PostgreSQL's join would have removed.
- COUNT(DISTINCT), and MIN, MAX and prefix-window aggregates lowered to ordered state, are
  not dropped when unread.
- Non-default function costs are not modeled.


## M6 frame-aware and derived expressions

[The M6 composition contract](m6-expression-composition.md) qualifies computed
window aggregate arguments after FILTER and within frame membership. Computed
partition keys use the existing deterministic scalar equality types; computed
ordering results must be integral or timestamp. Positional windows still include
the complete raw logical occurrence key in partition/order. Valid empty frames
return the aggregate's empty value without evaluating their argument. Ranking,
RANGE without offsets and GROUPS retain peer semantics with computed keys.

Intermediate AVG values are rendered by an engine map before a derived filter,
typed scalar projection, group, window or join uses them. Derived scalar arithmetic,
CASE/COALESCE and already supported explicit integral widening/integral-to-numeric
casts use the same typed kernels as native scopes. New function overloads, numeric
AVG inputs, numeric-to-integral conversion and arbitrary subqueries are not added.
Computed side-local integral equality keys now apply to left joins and qualified
EXISTS/NOT EXISTS under the existing eager ON-map error contract.

The twelve-case `expression_composition_harness.py` checks native-MV bags/output
metadata and independent full-source recomputation through committed histories,
rollback and cold restart. Rust tests additionally qualify frame/FILTER overflow
guards, weighted outer/existence membership, independent AVG scopes and failed
prepare checkpoint preservation. M5's blocker inventory retains the promoted
queries under `qualified_compositions`.


## M7 sliding frame evaluation

Eligible ROWS and RANGE/GROUPS partitions containing only COUNT and integral
SUM/AVG reuse transient exact statistics as frame boundaries advance. FILTER and
frame membership still guard argument evaluation, and output width checks remain
at each requested frame. Mixed/ineligible aggregate sets retain scanning. The
persisted graph, checkpoint layout and PUT/COMMIT/ACK order are unchanged; full
affected-partition reads remain. See [M7 evidence and limits](m7-window-work.md).


## M8 destination publication

The engine computes all query expressions. Destination storage decodes bounded
occurrence batches, retracts exact binary identities through a candidate hash
index, then inserts positive multiplicities. Proven [keyed sinks](keyed-sinks.md) use
native UPDATEs for same-key replacements. Index installation and validation
occur during initialization. No weights or occurrence identifiers become public
columns. See [M8 publication details and evidence](m8-sink-deltas.md).


## Mixed integral window extrema

Admitted integral MIN/MAX can now share sliding frame statistics with COUNT/SUM/AVG,
including supported computed arguments and FILTER. Other aggregate sets retain
scan evaluation. Full affected-partition reads remain. See
[extrema evidence and limits](sliding-extrema.md).


## Durable prefix RANGE windows

A single ascending nonnullable integral order column and prefix RANGE frame can
use persistent COUNT/SUM/AVG summaries. The compiler registers ordered raw data
and fused prefix statistics as explicit engine state. Other admitted frames keep
the existing exact evaluator. This changes the plan identity of eligible old
window registrations, requiring explicit rebuild. See [supported forms, evidence
and read/write costs](prefix-windows.md).

## Scalar aggregate subqueries and membership marks

[The subquery contract](subqueries.md) covers scalar global/equality-correlated
aggregates, IN lists and null-aware IN/NOT IN subqueries. It specifies cardinality,
empty/default rows, correlation scope, native types, guarded evaluation and
qualification boundaries.
