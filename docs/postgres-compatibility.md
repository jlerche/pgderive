# PostgreSQL compatibility contract

Status: M0 specification and implementation inventory. This document defines the
target contract; the [capability inventory](postgres-capabilities.md) distinguishes
existing behavior from missing qualification and enforcement. It does not certify
arbitrary PostgreSQL SQL or declare M0 complete. See the
[implementation roadmap](postgres-fidelity-roadmap.md).

## Reference result

For registered query Q, source state S at a complete committed source boundary,
and registered execution environment E, the successfully published result must
be the typed bag produced by native PostgreSQL evaluation of Q over S under E.

S includes every participating input from that boundary, not a mixture of source
transactions. A published result may lag the current source database. Comparing it
with a fresh SELECT over newer tables is not a correctness oracle. Qualification
fixtures must control writes or reconstruct the matching source state separately.
Source LSNs identify source progress; DBSP ticks describe computation order, not
wall-clock time or PostgreSQL snapshots available to arbitrary later connections.

Match column names/order, type OIDs and modifiers within the registered database,
collations, NULLs, values, observable value representations and row multiplicities.
Physical storage, row order without a sufficient ORDER BY, relation OIDs and index
layout are not result equivalence. Where SQL equality ignores a representation
that remains observable (for example numeric display scale), equality-only bag
comparison is insufficient. Intermediate navigation equality must remain separate
from full-tuple identity and physical occurrence identity.

A regular public table must contain one row per positive result occurrence,
including identical duplicates, with only query output columns. The current
weighted internal table plus expanding public view does not satisfy that storage
requirement. Source primary keys never imply result uniqueness. External writes
to owned results are unsupported. Logical-replication consumers own delivery;
Pgderive must document replica identity, feedback exclusion and rebuild boundaries.

## Supported query domain

Acceptance is conditional on explicit syntax, type, context and resource limits.
Reject unsupported semantics before durable activation; do not substitute an
approximation. A resource failure preserves the last committed result and progress.
No promise requires completing computations beyond declared limits. Existing
bounded numeric and temporal domains need individual qualification; they do not
imply coverage of the corresponding complete PostgreSQL types.

Ordering-sensitive selection needs a reproducible result. Reject an ambiguous
ROW_NUMBER/top-k selection unless the existing supported form proves sufficient
occurrence ordering. Peer-aware ranks and frames have distinct contracts. Do not
invent tie breakers that change query semantics. Float aggregation and other
plan/order-sensitive operations require a separate support decision.

The closed built-in allowlist is not an arbitrary function runtime. User-defined
functions, relation-reading functions, volatile operations, wall-clock predicates,
sequences and untracked session/role dependencies are outside the current domain.
A declared IMMUTABLE or STABLE attribute alone does not establish replay safety.
Time-based windows over stored timestamps are not wall-clock expiration.

## Execution placement

There are three intended paths, in preference order:

| Path | Required proof | Current status |
| --- | --- | --- |
| Sink-deferred | No pre-publication consumer needs the result; preserve cardinality, evaluation domain, context, errors and retractions | Closed terminal programs exist; no general graph placement proof |
| Local | Qualified semantics for the exact type/signature/domain and context | Bounded Rust evaluators exist; no vendored PostgreSQL runtime |
| Connected-PostgreSQL fallback | Same expression proof plus typed batching, source-boundary independence and durable retry/retraction behavior | Not implemented |

Backward dependency traversal must include predicates, join/group/distinct keys,
aggregate arguments/filters, ordering, frames and consumers in enclosing scopes.
Being outside that closure identifies a candidate, not sufficient proof: moving
CASE branches or filtering can change errors and evaluation. Constant-only output
still retains source multiplicity. Set-returning functions need a cardinality-aware
path, not a scalar renderer. Aggregate finalization requires exact sufficient
statistics and no intermediate consumer of the finalized value.

A fallback may evaluate explicit captured tuples without reading current sources.
It cannot infer a historical snapshot from an LSN. Hidden table reads are rejected
until their dependencies and boundary-consistent execution are implemented.
Network failure, partial evaluation and downstream failure must not publish a
partial result or advance source progress.

## Environment and invalidation

The environment includes the resolved type/operator/function/cast identities and
implementations actually used, parser/compiler semantics, server/runtime versions,
encoding, collations and relevant session settings. Include role/security or
extension dependencies before accepting operations that depend on them. Record
only supported contexts, but do not assume an unrecorded dependency is harmless.
OIDs alone do not prove implementation stability. Version strings do not attest
external timezone, locale or shared-library contents.

Minimum deployment contract until stronger fencing exists:

- Administrators must quiesce workers before changing participating schemas,
  built-ins, type definitions, relevant collations, timezone files or runtime
  implementations. Revalidate and rebuild incompatible registrations before resume.
- External writers must not modify owned sink/catalog relations, slot progress or
  object storage. Use documented least-privilege roles.
- Worker-controlled settings apply to actual execution transactions, not merely
  validation. Arbitrary session defaults are not part of accepted query semantics.

These are operational preconditions beyond existing relation locks. Publication
already takes ACCESS SHARE locks on registered sources and re-verifies their
contracts inside the transaction, fencing conflicting relation DDL. Catalog inspection
before execution can detect drift but cannot prevent a concurrent administrative
mutation between inspection and use. Existing source/terminal checks therefore do
not establish unconditional fencing. The next M0 slice must document and test the
check schedule, bind missing supported dependencies, and fail before affected
publication when drift is detectable. M6 broadens supported invalidation behavior.

## Failures, retry and durability

Maintain complete transactions and simultaneous-input join cross terms. Preserve
PUT → atomic PG membership/result/progress COMMIT → slot ACK. Retain enough raw
or evaluated state to retract previous outputs exactly. Publication errors roll
back all visible deltas and progress; source errors do not get skipped. Uncertain
COMMIT requires reconciliation, not blind application of result deltas again.

Match values for successful evaluations. Do not claim identical PostgreSQL error
messages or optimizer-specific expression order. Explicit lazy branches and
unavoidable type/arithmetic errors are part of qualification. PostgreSQL planning
can raise constant-folding errors even in branches unused at runtime; existing
original-query EXPLAIN checks cover that boundary. Predicate normalization and
local short-circuit choices need tests where error-producing operands occur;
no blanket equivalence follows from three-valued truth tables alone.

## Required evidence

Each supported capability needs implementation ownership, exact accepted domain,
placement, coercion/NULL rules, environmental dependencies, resource/error bounds,
and qualification references. Entries may group overloads only when their exact
signatures remain enumerated by the linked closed implementation.

Compare identical SQL with native materialized views at controlled boundaries.
Use bidirectional EXCEPT ALL where supported, plus metadata and representation
checks. Exercise NULLs, duplicates, collisions, special values, updates/deletes,
simultaneous inputs, rollback, evaluation errors, restart and failure recovery.
Retain independent memory oracles for weighted relational behavior. A harness
reference is evidence scope, not proof that every input or context is covered.

Before adding a capability, update its inventory entry and qualify all enabled
execution paths. Before changing durable semantics, verify restart rejection or
explicit rebuild; unreleased IR refactors need not preserve historical formats.

## Implemented M0 binding validation

Source contracts now include a SHA-256 catalog fingerprint and exact server version
and encoding, including queries without terminal expressions. The closed dependency
manifest in `src/semantics/catalog.sql` covers supported native type definitions and
I/O functions, casts between those types, catalog comparison/index support,
the local date_bin/mod, aggregate, ranking/navigation and series families, their
aggregate transition/finalization and planner-support dependencies, and generated-SQL
transport helpers. Interval/float8 codecs and casts cover implicit hopping-expression
operations; this does not enable those types as source columns.
It includes registered source collation definitions/provider versions and database
locale metadata. Terminal bindings independently include the same shared baseline,
in addition to their explicit expression-function and UTC/C checks. Explicit
functions retain full pg_proc/language definitions in the binding and must remain
scalar, non-variadic, security-invoker built-ins without default arguments.

Source verification compares this fingerprint during registration/restart and under
source relation locks before snapshot activation and transactional publication.
Terminal evaluation rechecks its environment inside the publication transaction.
Workers set search_path to pg_catalog, UTC, ISO/MDY DateStyle, ISO interval format
and extra_float_digits=3 on their SQL connections. Terminal evaluation also sets
those locally so low-level publication callers do not inherit arbitrary defaults.

These are conservative dependencies: a change to a supported built-in that an
individual query does not use can require a rebuild. The catalog is hashed inside
PostgreSQL, so each baseline check returns only version, encoding and a digest in
one SQL request, not a full catalog transfer. Existing explicit terminal signature
checks still inspect each distinct signature. This does not introduce
intermediate network expression evaluation.

An incompatible old serialized source/terminal binding is rejected; the unreleased
format has no automatic migration. Registrations from before this shape change
need retirement with a compatible older binary before upgrade, followed by fresh
registration/bootstrap. New-format registrations with compatible structure but
changed attested semantics require explicit rebuild after restoring or qualifying
the supported environment. Fingerprints attest catalog
metadata, not the contents of backend binaries, shared libraries or timezone files.
They do not prevent concurrent administrative mutation. The quiescence policy above
continues to apply. This slice establishes the baseline check schedule, not general
function support or complete dependency invalidation.

The owned-database `scripts/query_binding_harness.py` tests codec, local-function, aggregate/planner-helper and interval/coercion drift,
incompatible restart, helper drift inside publication, unchanged durable state and
ACK on failure, successful replay after restoration, and hostile database defaults.
It is part of the sequential common gate; catalog changes never touch the shared
test database, and failure databases/logs remain available for investigation.
