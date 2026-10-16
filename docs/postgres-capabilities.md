# PostgreSQL semantic capability inventory

Baseline: scalar/relational code inspected at commit `867c2a2`; the binding
checks below include the subsequent M0 implementation. Scope is the SQL compiler and continuous worker, not generic engine APIs.
The exact closed signatures in `src/compiler/capabilities.rs` and parser guards
linked below are authoritative;
a family name is not permission to use all PostgreSQL overloads. Read with the
[compatibility contract](postgres-compatibility.md) and
[SQL subset](sql-compiler.md).

## Scalar and transport capabilities

All execution is in-engine. There is no sink evaluator, mapped sink, connected
PostgreSQL computation fallback, or placement report.

| Family | Implemented path and limits |
| --- | --- |
| Native scalar source codecs | Boolean, integral, text/varchar, UUID, timestamps and bounded exact numeric; source modifiers and environment captured |
| Integral casts and +, -, * | Checked native widths in Rust; unavailable casts reject |
| CASE/COALESCE and predicates | Local lazy branches and SQL NULL behavior in supported typed domains |
| Numeric arithmetic | Exact bounded coefficient/scale +, -, *; unsupported division/rounding reject |
| Integer AVG | Local arbitrary-precision sum/count finalization with PostgreSQL scale selection and rounding; no SQL numeric_div call |
| Fixed timestamp bins/offsets and integral MOD | Local qualified bounded implementations |
| Pure integer/text functions | Checked integral ABS/to_hex; UTF8 length, trim, slice, replace, translate, reverse, split, repeat, code points and prefix checks; exact signatures in `src/compiler/capabilities.rs`; expansion budgets and right/MIN boundary in the roadmap |
| lower, upper, regexp, array extraction, calendar formatting/extraction | Not available in-engine; compile-time errors, no fallback |

The scalar architecture uses one bound IR in `src/compiler/typed.rs` for maps and
predicate operands. `src/compiler/value.rs` owns native decoding and literal
normalization. Function/operator dispatch resolves at binding time. Existing
computed key/window maps use the same IR; specialized temporal/MOD kernels retain
bounded parser/context guards. These structures do not enable computed aggregate
arguments or arbitrary computed join/window keys.

## Relational capabilities

All maintenance and finalization below run locally.
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
| Derived scopes and composed windows/groups/joins | Only bound combinations; unfinalized AVG encodings cannot become intermediate operands | `src/compiler/bind/derived.rs`, `src/compiler/bind/scoped_join.rs`; `scripts/composition_compiler_harness.py`, `scripts/native_cross_harness.py` |

## Existing binding checks and gaps

No general execution-placement proof, automatic dependency invalidation, arbitrary
function execution, named-zone runtime, floating-point aggregate fidelity, or
connected computation fallback is provided by this inventory.

## Initialization and steady-state check schedule

This list is a code-path inventory, not a completed audit of every lifecycle path.

## M0 binding-validation baseline and remaining work

The shared manifest in `src/semantics/catalog.sql` now binds the supported native
codec/catalog baseline, locally executed function families (including aggregate and
planner helpers), generated-SQL helpers and source collation/provider metadata.
Interval/float8 metadata covers implicit hopping operators/coercions without
expanding the accepted source type set.
Source inspection uses that baseline. The owned-database
`scripts/query_binding_harness.py` covers quiesced catalog drift, incompatible restart without progress changes,
restored replay and controlled connection settings. Protocol observation verifies
zero semantic inspections during publication, including cold resume.

The implementation is conservative rather than a minimal per-query dependency set.
Remaining work includes stronger administrative fencing, binary/library/timezone-file
attestation and future capability-specific dependencies. These remain explicit
operational restrictions; the implementation does not claim automatic invalidation.
