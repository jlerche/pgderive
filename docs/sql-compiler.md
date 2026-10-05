# SQL compiler boundary and slices

The first compiler slice executes a deliberately bounded PostgreSQL SELECT
through the existing durable grouped-join worker. It is not a general SQL planner
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

## Exact first-slice subset

One SELECT, with exactly three outputs in order: a left column, COUNT(*), and
SUM(right integral column). GROUP BY contains exactly the same left column.
FROM names two distinct publication relations using explicit schema.table names,
connected by JOIN or INNER JOIN and one ON column = column equality. Reversing
the equality operands is accepted. Native join types, modifiers and collations
must match exactly; there are no implicit casts. Join/group keys are boolean,
int2/int4/int8 or UUID. Text/varchar keys are rejected until a collation-aware
runtime exists. All frozen native types can be tested for NULL.

WHERE is absent or a sequence of column IS NULL / column IS NOT NULL tests joined
by AND. These tests produce true or false, including on NULL; WHERE retains only
true. ON equality with either key NULL is unknown and yields no match. Grouping
preserves NULL, COUNT(*) includes NULL measures, SUM ignores NULL measures and
returns NULL for an all-NULL group. Empty groups disappear. Zero SUM is distinct
from absence. Comparisons to NULL (including `= NULL`) are rejected. General
comparisons, OR, NOT expressions and other boolean expressions are later
slices; no two-valued approximation of unsupported expressions is attempted.

Table aliases and output aliases support AS or an implicit alias. Output aliases
are presentation-only: the owned destination still uses group_key, row_count,
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

Input is bounded to 16 KiB, 2,048 scanner tokens, 64 parenthesis levels and 512
WHERE AST nodes; NUL is rejected. Scanner tokens exclude parentheses in strings,
comments and quoted identifiers. These are input budgets, not a general native
parser resource guarantee. Only built-in unqualified COUNT and SUM are supported.

Other syntax fails closed: standalone projection queries, reordered outputs,
multiple grouping keys/aggregates, COUNT(column), DISTINCT, aggregate FILTER,
HAVING, ORDER BY, LIMIT, CTEs, subqueries, casts, arithmetic, parameters, windows,
outer/self/multiple joins, set operations and multiple statements. Unsupported
SQL is rejected before a registration row or replication slot is created.
Administrative metadata table installation may precede compilation; it does not
register a query. Resolution uses the explicit frozen publication catalog, not
search_path or live SQL name interpolation.

## Compiler and runtime boundaries

1. Scan and parse with pinned pg_query 6.2.1 (embedded PostgreSQL 17.7), through
   its safe Rust API. Validate supported AST node kinds, clauses and modifiers,
   then lower to the syntax-only grouped SELECT representation. WHERE traversal
   is iterative and bounded. Native syntax errors retain PostgreSQL's message;
   the safe wrapper does not expose the native error cursor. AST lowering errors
   identify the rejected construct; precise source spans are deferred. Parsing
   does not perform PostgreSQL catalog resolution or type checking.
2. Resolve relation/column names and aliases against source::Contract. Binding
   errors identify unknown/ambiguous names. No database state is changed here.
3. Type-check the native equality/group/measure contracts, then construct typed
   column references for NULL expressions. Resolve/type/lower diagnostics are
   distinguished by message prefixes; precise binder source spans are deferred.
4. Normalize into a grouped relational IR: exact source/column selectors,
   sorted/deduplicated conjunctive typed NULL tests and compiler revision. There
   is no generic arbitrary-graph IR in this slice. Unsupported relational shapes
   are rejected, rather than declared without an executable evaluator.
5. The explicit worker::program bridge binds this IR to GroupedJoin operators:
   full-row input arrangements, inner join, joined-row WHERE/group projection,
   COUNT/SUM sufficient statistics and fixed destination codec. The predicate
   callback runs on both positive and negative full-row contributions. Bootstrap
   and CDC use the same compiled selectors and operators.

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

SQL plans hash normalized IR together with the entire native source contract,
including OIDs, attribute order, modifiers, nullability, collation, source identity,
publication and physical slot. The compiler revision names the source-row text
codec, object JSON-v2 family, group string codec and bounded sum semantics; engine
plan identity also hashes the validated declarations. Change this revision when
compiler semantics or callback/codec interpretation changes. The revision also
binds pg_query and its embedded PostgreSQL version; parser upgrades require a
semantic audit and revision change. SQL v1 registrations cannot restart under v2;
use a fresh registration/bootstrap. Legacy selector identities are unchanged.
Native parser fingerprints/normalization are never used for durable identity.

Formatting, keyword case, comments, redundant identical conjuncts, conjunct order,
alias spelling and reversed equality operands normalize equally. Other semantic
changes bind differently; equivalence is not a general theorem prover. Existing
legacy selector plans retain their exact identity bytes, and switching from legacy
to SQL requires a new registration even if the intended result is equivalent.

Restart recompiles and compares identity before slot creation/reset or adopting
state. Changed SQL, native layout, compiler revision, destination or object prefix
cannot silently reuse state. A mismatch needs a fresh query registration, slot,
owned destination and object namespace/bootstrap; in-place migration is deferred.
Keep the old registration and evidence for diagnosis. No automatic state reset.

## Next coherent slices

1. **This slice:** bounded parser/resolver/typed grouped IR, NULL conjunctions,
   explicit grouped runtime bridge, durable identity and oracle/restart tests.
2. **Typed WHERE:** comparisons for native bool/integral/UUID literals with exact
   coercion rules, then AND/OR/NOT with three-valued typed expression evaluation.
   Add truth-table, PostgreSQL differential and retraction tests before broadening.
3. **Column projection runtime:** a separate durable bag program and sink mapping
   for single-source projections/filtering, with full-tuple collision semantics;
   support selected output order/names and native sink codecs explicitly.
4. **Broader grouped lowering:** arbitrary supported join-side group/measure
   selections, multiple keys and aggregate output layouts; extend runtime/checkpoint
   types and identity deliberately. Collation-aware text keys remain a separate
   prerequisite. General graphs need an explicit executable scheduling bridge.

Each slice gets independent review and the common gate before its own commit.
Recursion, distribution, services and deployment remain deferred.

## Native build dependency

pg_query builds its bundled PostgreSQL parser with a C toolchain and bindgen.
Install libclang (for example `libclang-dev` on Debian/Ubuntu) alongside the Rust
toolchain. The application uses only the safe Rust API; its own unsafe-code ban
remains in force. No PostgreSQL server headers or runtime extension are required.
The pinned parser grammar is PostgreSQL 17.7, not whichever server version happens
to be running; newer syntax remains unsupported until deliberately qualified.
