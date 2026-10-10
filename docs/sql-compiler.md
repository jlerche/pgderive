# SQL compiler boundary and slices

The compiler executes a deliberately bounded PostgreSQL SELECT through durable
grouped-join, single-source projection, ungrouped inner-join and affected-partition workers. It is not a general SQL planner
or an interpreter for plan declarations. Configure it in place of selectors:

```toml
[worker.query]
sql = """
SELECT a.id AS auction_id, COUNT(*) AS matches, SUM(b.price) AS total
FROM public.auction AS a INNER JOIN public.bid AS b ON a.id = b.auction
WHERE a.category IS NOT NULL
GROUP BY a.id
"""
```

## Legacy grouped-join subset

One SELECT, with exactly three outputs in order: a left column, COUNT(*), and
SUM(right integral column). GROUP BY contains exactly the same left column.
FROM names two distinct publication relations using explicit schema.table names,
connected by JOIN or INNER JOIN and one ON column = column equality. Reversing
the equality operands is accepted. Native join types, modifiers and collations
must match exactly; there are no implicit casts. Join/group keys are boolean,
int2/int4/int8 or UUID. Text/varchar keys are rejected until a collation-aware
runtime exists. All frozen native types can be tested for NULL.

WHERE supports column IS NULL / IS NOT NULL, Boolean columns and TRUE/FALSE/NULL,
and scalar comparisons =, <>, !=, <, <=, >, >= composed with AND/OR/NOT.
Comparisons require at least one native bool, int2/int4/int8 or UUID column.
Integral columns may compare across integral widths and to signed i64 integer
literals; evaluation is exact, without narrowing the literal to the column width.
Boolean columns compare to Boolean literals. UUID columns compare to unknown
string literals using PostgreSQL UUID input forms and byte ordering. Quoted
numeric/Boolean literals, explicit casts, floating/numeric literals, text
comparisons and other coercions are rejected. NULL comparison produces unknown;
WHERE retains only true, including on retractions. No transform_null_equals
session setting is applied. Logical expressions follow PostgreSQL three-valued
truth tables; pure AND/OR trees are flattened, sorted and deduplicated for identity.

Integral comparisons also accept `mod(column, nonzero_integer_literal)` and
`column % nonzero_integer_literal`, including `pg_catalog.mod`. Both forms
normalize to the same typed remainder expression and execute in Rust before the
relational operator consumes the row. The remainder has the dividend's sign;
minimum i64 remainder -1 is zero rather than an overflow. NULL input gives unknown
comparison/qualification. The argument must be a native integral column; nested
calls, column divisors, NULL/zero divisors, function modifiers and computed outputs
remain unsupported. This adds a q2 prerequisite, not complete Nexmark qualification.

ON equality with either key NULL is unknown and yields no match. Grouping preserves
NULL, COUNT(*) includes NULL measures, SUM ignores NULL measures and returns NULL
for an all-NULL group. Empty groups disappear. Zero SUM is distinct from absence.

Table aliases and output aliases support AS or an implicit alias. Output aliases
in grouped queries are presentation-only: the owned destination still uses group_key, row_count,
and total, in that fixed order. group_key is JSONB containing the canonical source
text representation (or JSON null), even for integral/boolean/UUID groups; it is
not a native SQL output-column codec. Aliases are not visible in GROUP BY/WHERE.
Columns may be unqualified when unique, table-qualified, or schema.table-qualified
when the table has no alias. An alias hides the original table name. Ambiguous
columns and duplicate exposed table names are errors. PostgreSQL 17.7 determines
identifier keyword categories, case folding, quoted names and Unicode escapes
(including UESCAPE). Redundant parentheses and PostgreSQL's ISNULL/NOTNULL
spellings are accepted when their AST has the supported semantics. Comments and
terminal semicolons follow PostgreSQL syntax; exactly one statement is required.
PostgreSQL truncates identifiers to 63 UTF-8 bytes, including quoted identifiers;
resolution and plan identity use that native normalized name. This replaces the
v1 frontend's overlong-name rejection. Unquoted non-ASCII identifiers are accepted
with PostgreSQL's folding behavior, rather than requiring quotes.

Input is bounded to 16 KiB, 2,048 scanner tokens, 64 parenthesis/expression levels and 512
WHERE expression nodes; NUL is rejected. Scanner tokens exclude parentheses in strings,
comments and quoted identifiers. These are input budgets, not a general native
parser resource guarantee. Only built-in unqualified COUNT and SUM are supported.

In this legacy grouped-join path, other syntax fails closed: reordered grouped outputs, multiple grouping keys/aggregates, COUNT(column), DISTINCT, aggregate FILTER,
HAVING, ORDER BY, LIMIT, CTEs, subqueries, casts, arithmetic, parameters, windows,
outer/self/multiple joins, set operations and multiple statements. Unsupported
SQL is rejected before a registration row or replication slot is created.
Administrative metadata table installation may precede compilation; it does not
register a query. Resolution uses the explicit frozen publication catalog, not
search_path or live SQL name interpolation.

## Single-source projection/filter subset

A SELECT without GROUP BY supports 1..=64 column references in any order from one
explicit schema.table, with optional table/output aliases and the same WHERE
subset. Repeated columns and output labels are legal. Apart from the terminal built-ins
described below, computed outputs are rejected. Wildcards,
DISTINCT, ordering, limits and all
other unsupported clauses fail before registration or slot creation.

The same output and WHERE subset also supports two distinct source tables joined
by one inner equijoin, with columns selected from either input. Join keys must
have identical native bool, integral, UUID or timestamp contracts; NULL keys never match.
Text keys, implicit integer-width coercions, outer/self/multiple joins remain
rejected. Aliases and reversed equality operands normalize to the same semantics;
output labels and order remain part of durable identity.

Ungrouped joins lower to explicit Source, KeyBy, Join and Project IR nodes. The
worker binds those nodes to a reusable durable acyclic circuit executor, rather
than hand-authored query selectors. Both keyed inputs retain complete native
source tuples. The Join evaluates ΔL⋈R + L⋈ΔR + ΔL⋈ΔR against immutable prior
state; WHERE and final projection operate on signed joined contributions. The
final output arrangement preserves projected bag collisions. Every source receives
one delta per logical tick, including empty deltas. The executor can compose
source/project/join DAGs with maintained join parents and one maintained output;
the SQL frontend deliberately accepts only the two-table shape in this slice.
Aggregate kernels still use the existing grouped bridge.

The explicit projection circuit has one source and one Project node. Its input
is the complete full source row with unit navigation key, including rows with
NULL projected columns. Filtering and projection apply to positive and negative
contributions. Projection collisions consolidate full projected tuples, preserving
SQL bag multiplicity; source primary keys are absent from output identity. The
sole persisted output arrangement supports cold restore and fenced compaction/GC.
Other publication relations still produce complete source transactions and advance
query ticks/progress with empty query deltas.

The owned sink uses `tuple jsonb PRIMARY KEY, weight bigint`: tuple is
`[null, [column_1, ..., column_n]]`. The first null is the unit navigation key.
Boolean/integral columns encode native JSON booleans/i64 numbers; text/varchar and
UUID columns encode canonical source strings; SQL NULL encodes JSON null.
The resolved output layout, names, native types and codec revision bind identity.
This is an explicit weighted bag representation, not a native-column destination
SQL table or general function/type runtime. Frozen supported source types and
deterministic source text equality define its tested domain.

## PostgreSQL terminal expression execution

Single-source outputs may also call `abs(integral_column)` or
`length(text_or_varchar_column)`, unqualified or qualified with `pg_catalog`.
Arguments must be column references; nesting, arbitrary functions, casts and
other function calls in WHERE are rejected before durable registration or slot creation.
The bounded integral remainder predicate described above executes in Rust.
These strict built-ins preserve NULL. ABS uses the native argument width and fails
on its minimum negative value, matching PostgreSQL rather than widening it.
LENGTH counts Unicode characters under the required UTF8 server encoding.

The engine persists and emits the raw operand bag. During the same PostgreSQL
transaction that publishes memberships and source progress, a closed terminal
map evaluates the built-ins on signed deltas, consolidates mapped tuple collisions
with exact numeric weight sums, then checks final coefficients fit i64 before
sink DML. This is a linear map on weighted tuples: insertion and retraction use
identical semantics, including after cold restore. Any evaluation error rolls back
publication and prevents ACK. The existing PUT → COMMIT → ACK order is preserved.

Deferral is permitted only for deterministic tuple-local terminal operations.
Expressions that determine join keys, grouping, qualification or ranking need
execution at the corresponding Rust operator boundary. SQL volatility labels alone
are insufficient: user-defined functions, relation reads, time-dependent functions
and timezone-sensitive functions are outside this slice. It does not yet establish
PostgreSQL expression coverage for the Nexmark portfolio.

Registration binds concrete built-in signatures and catalog fingerprints, server
version and encoding into normalized plan identity. Restart and publication verify
that environment. Function-bearing projections use sql-projection-v2 with the
terminal-builtins-v1 revision; plain projections preserve v1 identity. Upgrades or
changed expression semantics require a new registration/bootstrap. As with source
metadata, privileged concurrent changes to system catalogs or the server executable
are unsupported; environment inspection is not a lock against administrative
mutation. No search_path-dependent function resolution is used.

## Compiler and runtime boundaries

1. Scan and parse with pinned pg_query 6.2.1 (embedded PostgreSQL 17.7), through
   its safe Rust API. Validate supported AST node kinds, clauses and modifiers,
   then lower to the syntax-only grouped or projection SELECT representation. WHERE traversal
   is recursive and bounded to 64 expression levels. Native syntax errors retain PostgreSQL's message;
   the safe wrapper does not expose the native error cursor. AST lowering errors
   identify the rejected construct; precise source spans are deferred. Parsing
   does not perform PostgreSQL catalog resolution or type checking.
2. Resolve relation/column names and aliases against source::Contract. Binding
   errors identify unknown/ambiguous names. No database state is changed here.
3. Type-check the native equality/group/measure contracts, then bind typed
   predicate expressions and projection output layouts. Resolve/type/lower diagnostics are
   distinguished by message prefixes; precise binder source spans are deferred.
4. Normalize into a grouped or projection relational IR: exact source/column selectors,
   normalized typed three-valued predicate expressions and compiler revision. Ungrouped
   joins carry explicit resolved Source/KeyBy/Join/Project graph nodes. Unsupported
   relational shapes are rejected before durable registration.
5. The explicit worker::program bridge binds this IR to GroupedJoin operators,
   connected by typed Stream<T> handles and the executable circuit builder:
   full-row input arrangements, inner join, joined-row WHERE/group projection,
   COUNT/SUM sufficient statistics and fixed destination codec. The predicate
   callback runs on both positive and negative full-row contributions. Projection IR instead binds the dedicated source/Project circuit and native
   weighted-bag codec. Ungrouped join IR binds the generic durable relational circuit
   with explicit maintained join parents and output. Bootstrap and CDC use the same compiled operators.

The existing runtime retains complete full-tuple bag identity, assembles both
inputs from a complete committed transaction, and includes the simultaneous-input
cross term. No source PK is used as generic weighted identity. No durable protocol
is replaced: protected object PUT precedes atomic PG membership/result/progress
COMMIT; only authoritative publication permits slot ACK. Source LSN never becomes
DBSP time. The compiler adds no acknowledgement or publication capability.

COUNT and all final weights/statistics must fit signed i64. SUM uses exact
intermediate arithmetic, but persisted sum statistics and results must fit i64;
SUM(int8) therefore supports a narrower range than PostgreSQL's numeric result.
Overflow fails transaction-atomically without progress/ACK. Hidden sum statistics
also have to fit when the visible sum is NULL. This is an explicit MVP limit, not
full PostgreSQL aggregate arithmetic equivalence.

## Identity and restart

Remainder-bearing predicates add an integral-remainder-v1 compiler revision suffix.
Queries without remainder preserve their prior revision and identity. Function and
operator spellings normalize equally; changing the divisor or predicate is an
incompatible semantic change requiring fresh registration/bootstrap.

SQL plans hash normalized IR together with the entire native source contract,
including OIDs, attribute order, modifiers, nullability, collation, source identity,
publication and physical slot. The compiler revision names the source-row text
codec, object JSON-v2 family, group string codec and bounded sum semantics; engine
plan identity also hashes the validated declarations. Change this revision when
compiler semantics or callback/codec interpretation changes. The revision also
binds pg_query and its embedded PostgreSQL version; parser upgrades require a
semantic audit and revision change. SQL v1/v2 registrations cannot restart under v3;
use a fresh registration/bootstrap. The projection addition preserves grouped
v3 serialization/identity and uses its own sql-projection-v1 revision. Legacy
selector identities are unchanged.
Native parser fingerprints/normalization are never used for durable identity.

Formatting, keyword case, comments, redundant identical conjuncts, conjunct order,
source alias spelling and reversed equality operands normalize equally.
Projection output names/order are durable layout: changing either is incompatible.
Grouped output aliases remain presentation-only. Other semantic
changes bind differently; equivalence is not a general theorem prover. Existing
legacy selector plans retain their exact identity bytes, and switching from legacy
to SQL requires a new registration even if the intended result is equivalent.

Restart recompiles and compares identity before slot creation/reset or adopting
state. Changed SQL, native layout, compiler revision, destination or object prefix
cannot silently reuse state. A mismatch needs a fresh query registration, slot,
owned destination and object namespace/bootstrap; in-place migration is deferred.
Keep the old registration and evidence for diagnosis. No automatic state reset.

The complete q0–q22 target and temporal/runtime prerequisites are specified in
[the Nexmark portfolio contract](nexmark.md). That target exceeds current compiled
execution and is not established by the existing qualification.

## Next coherent slices

1. **This slice:** bounded parser/resolver/typed grouped IR, NULL conjunctions,
   explicit grouped runtime bridge, durable identity and oracle/restart tests.
2. **Typed WHERE (implemented):** native bool/integral/UUID comparisons and
   AND/OR/NOT with three-valued evaluation, truth tables and PostgreSQL/memory
   differential tests through CDC and restart.
3. **Column projection runtime (implemented):** a separate durable bag program and sink mapping
   for single-source projections/filtering, with full-tuple collision semantics;
   selected output order/names and native scalar JSON codecs bind identity.
   PostgreSQL and memory oracles cover collisions, NULLs, empty ticks, cold restart
   after a process kill and a lost COMMIT response.
4. **Broader grouped lowering:** arbitrary supported join-side group/measure
   selections, multiple keys and aggregate output layouts; extend runtime/checkpoint
   types and identity deliberately. Collation-aware text keys remain a separate
   prerequisite. The typed circuit scheduler supplies execution; each new SQL shape still needs
   explicit state, codec and sink lowering.

Each slice gets independent review and the common gate before its own commit.
Recursion, distribution, services and deployment remain deferred.

## Native build dependency

pg_query builds its bundled PostgreSQL parser with a C toolchain and bindgen.
Install libclang (for example `libclang-dev` on Debian/Ubuntu) alongside the Rust
toolchain. The application uses only the safe Rust API; its own unsafe-code ban
remains in force. No PostgreSQL server headers or runtime extension are required.
The pinned parser grammar is PostgreSQL 17.7, not whichever server version happens
to be running; newer syntax remains unsupported until deliberately qualified.

Ungrouped join programs use `sql-relational-v1` and `worker-relational-v1` identities,
including normalized typed graph semantics, full frozen native source contracts,
raw record and native bag codecs, and compiler revision. A changed predicate,
projection, label, native layout or revision requires a fresh registration/bootstrap;
existing grouped/projection identities keep their prior serialized bytes. The live
owned relational harness compares each complete insert/update/delete transaction
against PostgreSQL and an independent nested-loop memory bag, with NULLs,
simultaneous changes, projection collisions, empty output ticks and cold restart.
This evidence qualifies this subset, not complete Nexmark or window support.

## Affected partitions, aggregates and non-temporal ROWS frames

Single-source grouped SELECT supports multiple native bool/integral/UUID/timestamp column
keys and arbitrary output ordering of those keys and COUNT(*), COUNT(column),
SUM(int2/int4/int8), AVG(int2/int4/int8), and integral MIN/MAX. Each aggregate may have a FILTER using the
supported three-valued predicate subset. NULL grouping keys remain present;
COUNT(column) ignores NULL; filters retain only true. Existing groups with no
qualifying non-NULL measures produce COUNT zero and SUM/MIN/MAX NULL. Removing
all input rows removes a grouped result. Ungrouped aggregation, DISTINCT,
numeric input aggregates, nested queries and grouped/window composition remain rejected.

Single-source window SELECT supports the same aggregate functions with explicit
ROWS frames and integral/timestamp column ORDER BY, ASC/DESC and NULLS FIRST/LAST. Partition
keys use the grouped key subset, including NULL. All window calls in one SELECT
must share one specification. Frame boundaries support CURRENT ROW, nonnegative
integer PRECEDING/FOLLOWING offsets and valid UNBOUNDED endpoints. The ordering
must include every source primary-key column, explicitly in SQL, so positional
frames have deterministic occurrence order. The compiler adds no hidden tiebreaker.
RANGE, GROUPS, default frames, exclusions and named windows remain rejected;
ranking/lag/lead and temporal expressions are subsequent slices. Windows preserve
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

Integral-only legacy partition plans retain sql-partition-v1 identity. New numeric
output plans use sql-partition-v2, including exact numeric JSON and terminal
aggregate-statistic codecs, bound PostgreSQL built-ins, and affected-partition-v1
identity covering
typed functions, FILTER, grouping/ordering, NULL placement, frame bounds, source
layouts and codecs. Nonlinear plan identities remain unchanged; optimized grouped
plans append an exact-linear-statistics-v1 revision. Restart cannot
reuse state under changed window/group semantics. Qualification compares grouped
and ROWS outputs with PostgreSQL and an independent source-partition oracle after
inserts, updates, deletes, NULL groups, all-NULL frames, changed neighbors, empty
query ticks and cold restart. This establishes the stated subset only.

The work budget counts materialized occurrences, frame-row visits and aggregate
record visits across affected partitions in one operator tick. It bounds those
operations; it does not measure total CPU instructions or sort comparisons.

## PostgreSQL semantics progression

The next implementation sequence builds on the same typed circuit and durable
publication boundary:

1. Native temporal/numeric values and expression execution: keep timestamp and
   timestamptz distinct, qualify PostgreSQL numeric result rules, and bind types,
   function semantics and relevant session settings into plan identity. A function
   may run at the PostgreSQL sink only when its result cannot affect filtering,
   keys, ordering, grouping or retained operator state, and replay has stable
   semantics. Otherwise use a qualified Rust implementation or reject it.
2. Aggregate and fixed/hopping bucket execution: extend COUNT/SUM to optimized
   sufficient statistics and add exact numeric AVG. Lower date_bin as a scalar
   expression with the SQL's stride and origin. Hopping membership requires an
   explicit bounded relational expansion. Preserve SQL NULL groups, negative
   offsets, type distinctions and errors; timestamptz fixed-duration bins do not
   implement timezone-dependent calendar days. There is no implicit watermark,
   expiration, wall-clock tick or late-data exclusion.
3. Ordered partitions: add PostgreSQL ranking/top-k and additional window frames,
   then qualify temporal lookup and explicit session formulations. Distinguish
   row occurrences from distinct weighted entries, PostgreSQL peer groups from
   deterministic positional ordering, and ROWS from RANGE/GROUPS. Non-temporal
   ORDER BY and PARTITION BY are first-class inputs to the same operator.

Each step requires PostgreSQL and independent memory bag comparisons through
complete insert/update/delete ticks and restart. A changed semantic revision
requires fresh registration/bootstrap; it cannot adopt an incompatible trace.
The affected-partition implementation above is the correctness foundation, with
bounded recomputation costs explicitly exposed rather than a claim that every
window or partition scales linearly. Object-local indexing, batched reads and
compaction optimizations must preserve this result/delta contract.


## Exact integer numeric output at publication

Integral AVG is a terminal finalizer over exact Rust sum/non-NULL-count statistics,
not an approximate Rust division. Rust persists a deterministic decimal
`sum/count` statistic payload in the raw object-backed bag; SUM(int8) persists its
exact decimal integer total. The typed SQL result is numeric, while this internal
payload is explicitly interpreted by the terminal map and is never exposed as a
numeric input to another operator. PostgreSQL's numeric input and numeric_div
functions produce the destination value in the same atomic transaction as object
membership and source progress. All-NULL/empty-frame inputs produce NULL without
division. Filters run in Rust before statistics accumulation.

The terminal binding fingerprints concrete immutable built-in signatures and
server version/encoding; changing that environment rejects restart/publication.
The JSON codec preserves arbitrary-precision numbers through PostgreSQL responses,
so large numeric results never pass through f64. The existing nonnumeric identities
keep their serialized form. Numeric input aggregates/arithmetic, nested
aggregate/window composition and AVG values used by downstream operators require
further typed lowering and remain rejected. This is safe sink deferral for the
currently compiled terminal aggregate plans, not permission to defer relational
keys, predicates or ordering to PostgreSQL.

The owned numeric harness checks int2/int4/int8 AVG and int8 SUM against PostgreSQL
and an independent Decimal oracle, including display-scale rounding, values above
floating-point precision, NULLs, FILTER, signed changes, ROWS neighbors, alias
normalization and cold restart.


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
form; timestamptz constants need explicit numeric offsets or Z. Ambiguous date
formats, timezone names, date-only constants, mixed timestamp/timestamptz coercions,
casts and calendar/timezone functions remain rejected. They require deliberate
PostgreSQL context and expression lowering rather than string ordering.

Numeric projection safely defers native input conversion to PostgreSQL inside
atomic publication, preserving exact decimals and PostgreSQL JSON representations
of NaN/Infinity. COUNT(column) can inspect numeric NULLness in Rust. Numeric keys,
comparisons, arithmetic and numeric-input SUM/AVG remain unsupported; preserving
raw decimal scale in a source row is not a claim of Rust numeric SQL equality.

All compiled/selector plans whose frozen source layout includes these new types
bind native-iso-utc-source-v1, pg-microsecond-timestamp-v1 and
exact-numeric-json-v1 codec revisions. Older source layouts retain their identity.
The owned native harness checks snapshot/CDC/restart projection, timestamp grouping,
ordered windows and same-type joins against PostgreSQL and independent memory
recomputation. It includes BC/min/max dates, infinities, NULLs, decimal precision,
numeric special values, writer-session DateStyle/TimeZone changes and simultaneous
join-input changes. This foundation does not yet claim hopping expansion,
calendar-day truncation, ranking or temporal predecessor execution.


## Fixed-duration date_bin execution

Single-source projection and GROUP BY can evaluate `date_bin(stride, column,
origin)`. The compiler resolves and type-checks a pure scalar map node before
partition state; the runtime evaluates that node on signed full-row contributions.
WHERE runs first, preserving errors only for qualifying input rows. Identical
expressions share a computed field, and grouped outputs must match a grouping
expression. Scalar semantics and the map/date-bin revision bind durable identity.

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
is introduced. Generic signed trace validation continues to permit negative weights.
The circuit retains full-tuple input identity and the exact statistics arrangement,
then projects SQL results from the changed statistics. Both are included in object
membership/checkpoint/recovery/compaction alongside output state. Numeric SUM/AVG
finalizers still use the bound PostgreSQL publication functions. Source LSN, logical
time and the PUT/atomic-publication/ACK order are unchanged. The compiler appends
exact-linear-statistics-v1 to these plans, so an older partition-recompute checkpoint
requires fresh registration rather than silent state adoption.

Qualification includes PostgreSQL and independent full-bag oracles for NULLs,
FILTER, weighted changes, deletion of the last row and cold restart. A memory-store
case bootstraps a 256-row group, restores it under a 16-contribution tick budget,
and updates one row with forced consolidation spilling. This verifies the delta
path under that budget; it does not establish arbitrary group size or throughput.
MIN/MAX and ROWS retain their existing affected-partition costs and limits.
