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
including identical duplicates only when produced by the query, with only query
output columns. The worker now publishes this ordinary typed table directly;
weighted arrangements and retained operands stay in the engine/object store. Source primary keys never imply result uniqueness. External writes
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

## In-engine execution only

Every accepted computation must have a qualified in-engine implementation.
PostgreSQL supplies source CDC, schema/semantic metadata, durable progress and the
ordinary typed result table. It never evaluates source operands on the engine's
behalf: sink expression programs and batched network fallback are unavailable.
Unknown functions and unsupported overloads/casts/features reject with a reason
before activation. Test oracles deliberately execute native queries for comparison.

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
still takes ACCESS SHARE locks and verifies source layouts and publication scope
inside the transaction, fencing conflicting relation DDL. Semantic catalog
fingerprints  are inspected only during
initialization, restart and rebuild. Running workers assume those semantics remain
stable: there is no live semantic-drift detection or per-publication catalog hash.
Administrators must stop workers before semantic changes and revalidate before
resuming. M6 may broaden invalidation support without introducing routine catalog
rescans on the publication path.

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
and encoding, for every accepted query. The closed dependency
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

Source verification compares this fingerprint during initialization, restart and
rebuild, including snapshot setup. Activation/publication retain layout-only
source locking and verification. Terminal environment inspection and canonical
session configuration happen during binding and sink initialization. Evaluation
uses the bound program without semantic catalog queries or repeated session SETs.
Workers set pg_catalog search_path, UTC, ISO/MDY
DateStyle, ISO intervals and extra_float_digits=3 during initialization; callers
must keep that session context stable for the worker lifetime.

An incompatible old serialized source binding is rejected; the unreleased
format has no automatic migration. Registrations from before this shape change
need retirement with a compatible older binary before upgrade, followed by fresh
registration/bootstrap. New-format registrations with compatible structure but
changed attested semantics require explicit rebuild after restoring or qualifying
the supported environment. Fingerprints attest catalog
metadata, not the contents of backend binaries, shared libraries or timezone files.
They do not prevent concurrent administrative mutation. The quiescence policy above
continues to apply. This slice establishes the baseline check schedule, not general
function support or complete dependency invalidation.

## Result-table lifecycle and replication

`worker.sink_table` is the sole public destination. PostgreSQL holds actual native
result occurrences, registration/ownership metadata, object memberships and durable
progress. It holds no internal weighted result table or presentation view. The
low-level publication API also supports ordinary JSON-result occurrence tables;
these are explicit destinations, never coefficient stores.

FULL replica identity supplies old tuples for logical replication for the bounded
output types. Destination tables must not enter the source publication. Consumers
may follow a separate publication. Real pgoutput tests cover duplicate/NULL result
inserts, exact deletes, rollback and cold restart. Compatible rebuilds replace
contents using DELETE/INSERT in the cutover transaction while preserving the public
OID; consumers observe that committed replacement. Incompatible schemas require
explicit drop/create and consumer resnapshot. Rebuild staging tables contain only
replacement result rows and are retired after cutover.

The first publisher rejects over 1,000,000 physical changes in a bootstrap or
incremental delta publication. Compatible rebuild cutover separately rewrites
the complete result with DELETE/INSERT. It
uses binary native-value comparison and transaction-local CTIDs for exact occurrence
deletion; no persistent per-occurrence SQL metadata is created. Deletions scan the
result table. Engine weights, keys, indexes and retraction operands remain
object-backed. DML and source progress roll back together on a missing occurrence,
resource limit or evaluation error. External writes and runtime administrative
changes are unsupported; consumer roles must have read-only access.

Old `result_view` configurations and weighted sink bindings require explicit
migration using the prior binary to retire the old registration and a fresh owned
namespace with this version. There is no silent adoption or conversion of existing
relations. A replaced relation requires downstream publication setup and resnapshot.
