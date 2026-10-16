# PostgreSQL semantic capability inventory

Baseline: scalar/relational code inspected at commit `867c2a2`; the binding
checks below include the subsequent M0 implementation. Scope is the SQL compiler and continuous worker, not generic engine APIs.
The exact closed signatures and parser guards linked below are authoritative;
a family name is not permission to use all PostgreSQL overloads. Read with the
[compatibility contract](postgres-compatibility.md) and
[SQL subset](sql-compiler.md).

## Scalar and transport capabilities

L means local Rust; S means PostgreSQL terminal/publication execution. Intermediate
network fallback and a vendored PostgreSQL runtime are absent.

| Family and accepted domain | Path; NULL/coercion behavior | Context, limits and failure boundaries | Implementation; qualification |
| --- | --- | --- | --- |
| Native boolean, int2/int4/int8, text/varchar, UUID, timestamp, timestamptz, numeric source values | L transport; SQL NULL preserved, source typmods already applied | Frozen source schema/collation; no generic type codec; JSONB identity needs representation audit | `src/source/contract.rs`, `src/source/copy.rs`; `scripts/native_compiler_harness.py`, `scripts/query_native_join_harness.py` |
| Comparisons and Boolean predicates | L; three-valued comparisons, NULL tests, AND/OR/NOT; type-directed literals | Native scalar domains in `expression.rs`; text limited to equality/inequality with an unknown literal under deterministic collation, not general text ordering | `src/compiler/expression.rs`; `scripts/projection_compiler_harness.py`, `scripts/case_compiler_harness.py` |
| Integral +, subtraction, multiplication and explicit integral casts | L and S; promoted integral widths, NULL propagation; checked narrowing/overflow | Not arbitrary PostgreSQL coercions; local recursion depth <32; terminal budgets below | `src/compiler/typed.rs`, `src/catalog/terminal/program/functions/integral.rs`; `scripts/query_expression_harness.py` |
| Typed CASE and COALESCE | L and S; ordered lazy branches, integral/numeric supported promotions; all-NULL inference differs by accepted form | Constant-fold errors checked through original-query EXPLAIN; no blanket proof of error-equivalence for normalized predicates | `src/compiler/typed/case.rs`, `src/catalog/terminal/program.rs`; `scripts/case_compiler_harness.py`, `scripts/query_expression_harness.py` |
| Integral MOD/remainder | L; NULL propagation, constant nonzero divisor in bounded accepted forms | Divisor/cast/operator forms guarded by compiler; overflow must fail atomically | `src/compiler/remainder.rs`, `src/compiler/expression.rs`; `scripts/sql_compiler_harness.py` |
| Numeric +, subtraction, multiplication, comparisons, navigation | L; exact coefficient/scale and PostgreSQL special-value order; NULL propagation | 4,096-character/digit circuit domain; division, rounding, numeric typmod casts unsupported | `src/numeric.rs`, `src/compiler/typed.rs`; `scripts/circuit_numeric_harness.py` |
| ABS(int2/int4/int8), length(text/varchar) | S; strict, abs minimum-value overflow preserved | Concrete internal built-in signatures and UTF8 attested | `src/catalog/terminal.rs`, `src/catalog/terminal/program/functions.rs`; `scripts/projection_compiler_harness.py` |
| Integral→numeric, numeric multiplication and comparison; numeric AVG finalization | S; exact numeric transport; AVG uses sum/non-NULL-count ratio, not float | Closed signatures; decimal factors restricted to 1..32 whole/fraction digits; final output delta multiplicity must fit i64 | `src/catalog/terminal/program/functions.rs`, `src/catalog/terminal/program/outputs.rs`; `scripts/numeric_compiler_harness.py`, `scripts/nexmark_q1_harness.py`, `scripts/nexmark_q14_harness.py` |
| Text replace, lower, equality, regexp_match and selected array component | S; strictness per bound signature; regex may return NULL; array bounds preserve accepted semantics | Explicit C for collation-sensitive forms; regex closed to channel_id capture; no arbitrary regex runtime | `src/catalog/terminal/program/context.rs`, `src/catalog/terminal/program/render.rs`; `scripts/nexmark_q21_harness.py` |
| string_to_array and array component output | S; built-in non-strict split behavior attested | Closed delimiter/index forms; C context; not general arrays | `src/catalog/terminal/program/outputs.rs`; `scripts/nexmark_q22_harness.py` |
| timestamp formatting, UTC conversion and EXTRACT | S; strict functions; timestamp without timezone or attested UTC conversion | Only YYYY-MM-DD / HH24:MI templates; hour plus fields in `Calendar`; no named-zone/session-sensitive calendar runtime | `src/catalog/terminal/program/functions/calendar.rs`; `scripts/nexmark_temporal_harness.py`, `scripts/query_expression_harness.py` |
| date_bin, fixed timestamp offsets and timestamp gaps | L; distinct timestamp/timestamptz codecs, NULLs and supported infinities | Fixed durations, finite bin origin, checked overflow; not calendar months/days in arbitrary zones | `src/compiler/scalar.rs`, `src/temporal.rs`; `scripts/nexmark_temporal_harness.py`, `scripts/session_compiler_harness.py` |
| Identity, literal and typed NULL terminal outputs | S rendering; constant outputs retain source occurrence counts | 1..64 inputs/outputs, 1..512 program nodes; cumulative rendered-node text ≤262,144 bytes; closed input OIDs; output labels validated separately | `src/catalog/terminal/program.rs`; `src/catalog/terminal/program/tests.rs`, `scripts/query_expression_harness.py` |

`Function` and its `signature`/`call_type` matches enumerate terminal overloads;
`implementation_types` records argument/result OIDs and strictness. `Expression`
enumerates non-call forms. These include Average, Coalesce, And, Or, Not, CollateC,
ArrayElement, IsNull and Case; SQL exposure is narrower than low-level program
construction. Terminal programs can include a final predicate: this is an existing
closed publication path, not permission to defer arbitrary graph filters.

## Relational capabilities

All maintenance below runs locally; numeric/formatting finalizers may run at S.
Every operation preserves complete weighted identities; navigational keys do not
establish uniqueness. See linked SQL-subset sections for exact syntax restrictions.

| Family | Semantics and explicit boundaries | Implementation; qualification |
| --- | --- | --- |
| Source/filter/map/rebind/output | Complete source transactions; projection collisions consolidate; typed scalar consumers run before state | `src/compiler/relational.rs`, `src/worker/program.rs`; `scripts/relational_compiler_harness.py` |
| INNER/CROSS/self/multiway joins, tagged union | Full-tuple multiplicity; simultaneous-input cross term; matching typed equality keys | `src/compiler/bind/native_join.rs`; `scripts/query_native_join_harness.py`, `scripts/join_composition_harness.py` |
| LEFT joins | NULL extension only for unmatched left occurrences; post-join WHERE does not manufacture unmatched rows; retained affected-key recomputation | `src/compiler/left_join.rs`; `scripts/query_native_join_harness.py` |
| SELECT DISTINCT, EXISTS / NOT EXISTS | NULL grouping distinct from NULL join equality; right duplicates do not multiply existence output; restricted equality correlation | `src/compiler/bind/membership.rs`; `scripts/query_membership_harness.py` |
| GROUP BY and group-only membership | Retractions, NULL groups, complete identity retained; accepted key families depend on compiled query shape | `src/compiler/partition.rs`; `scripts/group_membership_harness.py`, `src/worker/tests/grouped_occurrence.rs` |
| COUNT, SUM, AVG, MIN, MAX, FILTER and distinct aggregate membership | Empty/all-NULL semantics; integer exact statistics or retained affected partitions; result type bounds vary; numeric display scale retained | `src/compiler/partition.rs`, `src/compiler/bind/partition`; `scripts/numeric_compiler_harness.py`, `scripts/group_membership_harness.py`, `scripts/circuit_numeric_harness.py` |
| ROW_NUMBER, RANK, DENSE_RANK, top-k | Occurrences versus peers; NULL placement and direction; uniqueness proof where needed; specialized cutoff ≤1,024 | `src/compiler/topk.rs`; `scripts/ranking_compiler_harness.py`, `scripts/query_topk_harness.py` |
| ROWS/RANGE/GROUPS aggregate frames and LAG/LEAD | Bounded accepted frame/offset forms; peers distinct from occurrences; no generic SQL-window promise | `src/compiler/partition.rs`, `src/compiler/navigation.rs`; `scripts/peer_frames_compiler_harness.py`, `scripts/navigation_compiler_harness.py` |
| Left predecessor lookup | Explicit typed partition/order and supported offset/key forms; retractable retained inputs | `src/compiler/lookup.rs`; `scripts/lookup_compiler_harness.py`, `src/worker/tests/lookup_remainder.rs` |
| Bounded hopping expansion and revisable sessions | Explicit finite expansion; sessions revise historical boundaries; no watermark or wall-clock expiry | `src/compiler/expansion.rs`; `scripts/hopping_compiler_harness.py`, `scripts/session_compiler_harness.py` |
| Derived scopes and composed windows/groups/joins | Only bound combinations; terminal values cannot become intermediate operands without a qualified local path | `src/compiler/bind/derived.rs`, `src/compiler/bind/scoped_join.rs`; `scripts/composition_compiler_harness.py`, `scripts/native_cross_harness.py` |

## Existing binding checks and gaps

| Dependency | Existing enforcement | Remaining M0 work |
| --- | --- | --- |
| Sources/publication/slot | Frozen native contracts now include shared server/catalog fingerprints; registration/restart verification; activation/publication take ACCESS SHARE locks and re-verify layouts only | No general lock against publication/catalog/environment mutation |
| Terminal built-ins | Bind and inspect at initialization/restart concrete pg_proc attributes including implementation, language, volatility, strictness, argument/result types | Not an administrative mutation lock; implicit rendering/coercion helpers need dependency audit |
| Terminal environment | Shared native codec/helper/catalog fingerprint, exact server_version_num, UTF8, explicit C collation, UTC abbreviation context | Runtime libraries/timezone file contents and administrative races remain outside attestation |
| Execution settings | SQL worker and low-level terminal initialization set pg_catalog search_path, UTC, ISO/MDY and ISO intervals; source COPY/replication set canonical text formats | Consumers of the public SQL view still have their own presentation settings |
| Durable semantics | Normalized resolved IR, full source contract and bound terminal environment participate in plan identity | No separate global engine-version attestation; incompatible IR changes require rebuild, not migration |
| Public result representation | Typed expanding SQL view over JSONB weighted sink | Ordinary typed table, representation-preserving identity and logical decoding remain M1 |

No general execution-placement proof, automatic dependency invalidation, arbitrary
function execution, named-zone runtime, floating-point aggregate fidelity, or
intermediate network fallback is established by this inventory.

## Initialization and steady-state check schedule

- `src/worker/spec.rs::compile_bound` binds terminal operations and plans the
  validated original SELECT with EXPLAIN before durable registration.
- `src/worker/registration.rs` recompiles, compares destination/plan identity and
  verifies sources when reopening registered work.
- `src/source/contract.rs::inspect` and `verify` inspect source layouts and semantic
  environments during initialization, restart and snapshot setup.
- `src/catalog/bootstrap.rs` locks and verifies source layouts in activation.
- `src/catalog/publication.rs::publish` locks and verifies registered source layouts
  inside the result/progress transaction before applying destination deltas.
- `src/catalog/publication.rs::claim` and binding initialization verify source and
  terminal environments, including low-level cold reopen on a fresh SQL client.
- `src/catalog/terminal.rs::evaluate` uses the bound program and initialized session
  without catalog inspection or repeated SET commands.

This list is a code-path inventory, not a completed audit of every lifecycle path.

## M0 binding-validation baseline and remaining work

The shared manifest in `src/semantics/catalog.sql` now binds the supported native
codec/catalog baseline, locally executed function families (including aggregate and
planner helpers), generated-SQL helpers and source collation/provider metadata.
Interval/float8 metadata covers implicit hopping operators/coercions without
expanding the accepted source type set.
Source inspection and terminal inspection each use that baseline. The owned-database
`scripts/query_binding_harness.py` covers quiesced catalog drift, incompatible restart without progress changes,
restored replay and controlled connection settings. Protocol observation verifies
zero semantic inspections during plain/terminal publication, including cold resume.

The implementation is conservative rather than a minimal per-query dependency set.
Remaining work includes stronger administrative fencing, binary/library/timezone-file
attestation and future capability-specific dependencies. These remain explicit
operational restrictions; the implementation does not claim automatic invalidation.
