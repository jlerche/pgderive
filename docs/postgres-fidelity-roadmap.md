# PostgreSQL fidelity and regular-table roadmap

This is the implementation roadmap for PostgreSQL-faithful incremental view
maintenance. It complements the operational work in [release-plan.md](release-plan.md)
and supersedes its application-facing result-view target: the destination is a
regular, typed PostgreSQL table containing actual result row occurrences. Other
processes may consume its logical replication stream. Subscription delivery,
client synchronization and fanout are outside Pgderive's scope.

## Contract and current baseline

For every accepted query and successfully published source transaction boundary,
the destination must match native evaluation of the original query over that
source state under the registered execution environment. Match column names,
types, modifiers, collations, NULLs, values and duplicate multiplicities. Physical
row order is not a contract. Ambiguous ordering/selection must be rejected where
it prevents a reproducible result. Resource restrictions must be explicit.

This is a growing supported-query contract, not a claim to support arbitrary SQL.
Unsupported semantics fail before activation. An evaluation failure preserves the
last complete result and durable progress; no source acknowledgement advances.
Error qualification must distinguish observable query errors from incidental
PostgreSQL planner/evaluation order, and reject cases without a justified contract.

The worker publishes an ordinary typed result table directly. Weighted arrangements,
keys, retained operands and retraction state stay in the engine and object storage;
PostgreSQL holds source data, result occurrences and agreed coordination metadata.
Existing foundations include complete transaction CDC, object-backed state, atomic
publication, bounded SQL compilation, terminal PostgreSQL expressions and native-MV
harnesses. Existing features remain bounded; this roadmap does not retroactively
certify every type/expression domain.

Execution has three paths:

1. Sink-deferred: proven-safe final expressions execute in the publication transaction.
2. Local: qualified Rust implementations, or a qualified local PostgreSQL runtime.
3. Network fallback: batched evaluation on the connected PostgreSQL instance when
   an intermediate expression cannot execute locally.

Prefer safe sink deferral, then local execution, then fallback. Never silently
approximate. Source LSN and DBSP logical time remain separate. Preserve object PUT
→ one PG membership/result/progress transaction → slot ACK throughout every step.

## M0 — Freeze the semantic contract

The [compatibility specification](postgres-compatibility.md) and
[capability inventory](postgres-capabilities.md) establish the first documentation
slice. The shared binding-validation baseline and owned-database drift tests are
implemented; see the specification for scope and remaining administrative fencing
and external-runtime attestation limits.

Deliver a compatibility specification and an inventory of current operations.
Record supported signatures, type/coercion rules, NULL/strictness behavior,
collations, error cases, resource bounds, execution paths and evidence. Bind
resolved catalog identities, relevant settings, PostgreSQL/runtime versions and
semantic revisions into registration/restart validation. OIDs alone do not prove
that a function definition or environment is unchanged. Establish the minimum
supported-environment policy here before M1–M5 ship: validate semantics during
initialization/restart/rebuild and require administrators to quiesce workers before
semantic changes. Publication retains structural source checks but does not rescan
semantic catalogs.

Initially reject volatile/time-dependent expressions and hidden relation reads
unless an existing qualified feature supplies an explicit equivalent contract.
Function volatility declarations alone are not a proof of replay safety.

Acceptance: every currently accepted operation has an inventory entry, known
qualification gaps are visible, and changed semantics require explicit rebuild.
Implement as separate contract/inventory and binding-validation commits.

## M1 — Publish an ordinary typed result table

Implementation baseline: native-column tables, physical occurrences, binary-value
retractions, atomic publication, compatible stable-OID cutover and real pgoutput
qualification replace the old weighted SQL sink and expanding view. Internal
weights and retraction state remain engine/object-store owned; no SQL-side
occurrence map is introduced. Incompatible schemas currently require explicit
drop/create plus consumer resnapshot. Retractions scan the destination and a
publication is bounded to 1,000,000 row changes. Further performance and broad
representation-domain qualification remain incremental work.

Add a public result-table configuration and migration/rebuild path from existing
result views; do not silently change an existing registration. Keep weighted
storage internal. Materialize one physical row per positive result occurrence,
including identical duplicates, without adding visible bookkeeping columns or
inventing a primary key. Design exact occurrence deletion using engine-owned identity
state if necessary; SQL equality alone must not erase representation distinctions.

Apply public table changes in the same transaction as membership and source
progress. Never retain a separate internal sink table in PostgreSQL. Reject negative final multiplicities. Preserve ownership
checks and prohibit external writes. Qualify native types, scales, NULLs and
output collisions rather than assuming JSONB equality preserves every type.

Define replica identity and publication eligibility for supported output types;
where FULL identity or type equality support is insufficient, reject or document
an explicit supported alternative. Exclude destination tables from source capture
and reject feedback loops. Specify bootstrap and rebuild behavior for existing
logical-replication consumers: replacing a table OID must not be presented as a
transparent continuation. Prefer a stable public relation where compatible;
otherwise require a documented consumer resnapshot boundary.

Acceptance: native-MV bag equality; real logical decoding observes committed
inserts/deletes of duplicate and NULL rows; rollback exposes no partial output;
cold restart, uncertain COMMIT, rebuild and drop preserve ownership/progress.
Use separate schema/config, DML, lifecycle and replication-qualification slices.

## M2 — Prove expression placement

First reporting slice: `query validate` now returns a versioned, report-only
placement inventory. It follows maintenance consumers backward through operator
edges and records exact typed dependencies/consumer reachability within existing
terminal programs. Local Map expressions expose native types and definitions.
Operator dependencies are conservative across fields and scopes; this is not a
complete field-lineage or general movement proof. Candidates list outstanding
liveness, evaluation-domain/error, lazy-branch, cardinality, context, retraction
and runtime-equivalence obligations. Diagnostics do not affect executable plan
identity or pending rebuild signatures. No new expressions move or become accepted.
Field-lineage refinement now reports exact local scalar operands and separately
traces field consumers through row-preserving maps/rebinds, including predicates,
keys, aggregate input/filter and window/navigation operands. Packed state remains
an explicit conservative boundary; empty field-consumer lists do not prove
movability. Full-tuple arrangement identity and evaluation/error obligations remain
blocking. Additive finalizers now expose physical value dependencies separately from whole-row
validation, SQL types separately from AVG ratio encoding, and lineage through
statistics into local operands. Codec positions are shared with runtime. Tests
cover shared grouping/aggregate operands, finalizer overflow/NULL/retraction checks,
and live PostgreSQL CASE/COALESCE/cast error domains plus a filter-crossing
counterexample. Other packed state remains conservative. The executable output layout now provides the retained-operand/reuse contract,
reported by placement revision 4. A first qualified compiler rule defers only
direct lossless integral widening in a final Map immediately before Output, when
no terminal program already exists. Raw typed operands remain object-backed;
visible uses share slots/definitions, both delta signs use the bound publication
program, and maintenance-used values remain local. No predicate/state boundary is
crossed. Next: qualify further total operations or introduce explicit error-domain
and retained-identity proofs before attempting fallible or cross-boundary movement.

Introduce typed expression nodes and consumer dependencies in the executable plan.
Mark maintenance consumers (predicates, join/group/distinct keys, aggregate
arguments/filters, ordering, windows and intermediate computations). Traverse
backward to find expressions whose values are required before publication.
Only expressions outside that set are terminal candidates.

Then check contextual determinism, lazy CASE/COALESCE behavior, errors, cardinality
and retraction reproducibility. Preserve original expression evaluation domains;
unused values alone do not prove safe movement. Do not defer set-returning
expressions through a scalar path. A shared expression needed both internally and
in final output must have an explicit reuse/evaluation plan.

Extend query validation with per-expression placement and reasons, unresolved
capabilities, fallback stages, context and semantic revision. Initially report
placement without changing execution, then enable proven cases in a second slice.

Acceptance: fixtures distinguish final `lower(email)` from grouping by it;
filter/CASE error guards, shared expressions, derived scopes and aggregate
finalizers retain semantics. Unsupported placements reject before slot creation.

## M3 — Generalize sink-deferred execution

Extend the existing closed terminal program to qualified typed expression trees.
Retain operands or evaluated outputs sufficient for exact retractions. Consolidate
post-expression collisions and preserve occurrence multiplicity. Bind execution
context during initialization and keep it stable for the worker lifetime; changed
semantics require revalidation and may invalidate retained state.

Allow aggregate finalizers only when their sufficient statistics are exact and no
downstream maintenance needs the finalized value. Preserve cast errors, NULLs,
collations and type modifiers. Budget publication work and duplicate expansion.

Acceptance: identical SQL matches native materialized views through collisions,
deletions, lazy branches, failures, retry and cold restore. Each added function
family is a separately qualified slice, not a blanket arbitrary-function feature.

## M4 — Qualify and extend the local semantic runtime

Put equality/order, casts, arithmetic and expression evaluation behind typed
interfaces used by relational operators. First inventory and wrap existing
implementations without changing semantics. Then close correctness gaps before
adding operation families: coercions/NULLs, numeric scale and special values,
temporal/calendar/timezone operations, then locale-sensitive text.

For each family choose Rust implementation or local PostgreSQL code reuse using
measured engineering cost and fidelity evidence. Run a bounded feasibility spike
for local reuse: runtime initialization, memory/error isolation, context, type
transport, batching, packaging and versioning. Respect the unsafe-Rust prohibition;
any required FFI policy change needs an explicit design decision. A subprocess is
an option to evaluate, not an assumed solution. Vendoring source is not itself a
usable expression runtime.

Acceptance: each operation is differentially qualified against the pinned server;
unsupported domains reject. Equality/navigation never collapse full-tuple identity.
Floating-point aggregate order and other plan-sensitive results get an explicit
support decision; scalar compatibility alone does not certify aggregate fidelity.

## M5 — Batched network fallback for intermediate expressions

Build on M0/M2's binding and placement contracts. Support only qualified operations
over explicitly supplied typed engine operands initially. Evaluate compatible
expressions per stage in bounded batches, never one intentional round trip per row.
Preserve occurrence mapping, lazy evaluation, context and exact type transport.
Reject hidden source-table reads: an asynchronous connection can see a newer
snapshot than the source LSN being maintained.

Define retry behavior and durable ownership of evaluated intermediate values;
retractions must not depend on reevaluating an old input under changed semantics.
Persist reusable evaluations consistently with derived state before acknowledging
source progress. Inspect actual generated SQL to prevent eager evaluation of
branches that the original query would not execute.

Expose fallback batch count, rows, latency and byte budgets. Permit operators to
disable fallback or reject plans exceeding configured stage/call budgets. Do not
silently switch runtime implementations under an incompatible registration.

Acceptance: mixed local/fallback/deferred plans match native MVs; connection loss,
partial batch failure, downstream failure and cold retry preserve result/progress.
Compare call counts for growing row batches and independent expression stages.

## M6 — Expand relational coverage and invalidation

Use the capability inventory to select concrete workload queries and publish a
coverage matrix before implementation. Prioritize computed predicates/keys,
computed aggregate inputs, composed derived queries, then additional joins,
subqueries and windows. Every operation must resolve to one of the three paths.
No new SQL syntax is considered complete without retractions and recovery evidence.

Broaden M0's dependency tracking and fencing to support more changes: DDL, function/type
changes, extensions, settings, collation providers and timezone data. Merely
checking once at startup is insufficient if concurrent administrative changes can
invalidate a running plan. Unsupported changes stop publication and require rebuild.

Keep time-dependent queries rejected until explicit refresh epochs/invalidation
are implemented. Advancing wall time is not a source-table change; network fallback
does not fix that. Hidden relation reads require explicit tracked dependencies and
a correct source-boundary evaluation design before support.

Acceptance: supported environment changes safely reevaluate or rebuild; unsupported
ones cannot silently reuse old keys/results. Each new query shape has native-MV,
independent relational-oracle and recovery coverage.

## Qualification and sequencing

Start with M0 and M1: the regular-table result is a product requirement, independent
of broader expression coverage. Follow with M2 and M3. M4 and M5 share M0/M2's
interfaces and can then progress independently; neither blocks retaining the other
as an execution path. M6 expands coverage only after those foundations are qualified.
This is a dependency sequence, not a calendar estimate or an instruction to run
mutating integration harnesses concurrently.

For every slice, compare identical SQL against native materialized views at
controlled source boundaries and settings. Use bidirectional duplicate-sensitive
comparison plus metadata and representation checks where SQL equality is too weak.
Include NULLs, special values, inserts/updates/deletes, simultaneous input changes,
errors, crashes and cold recovery. Retain the independent memory oracle for
relational algebra and preserve failure evidence. Keep existing PoC artifacts frozen.

Before each commit obtain independent review and run `./scripts/check.sh`, including
80% production Rust line coverage. Run shared-PostgreSQL mutation harnesses
sequentially with isolated owned fixtures. Performance evidence must include
fallback calls, publication work, retained-state growth and skew, not just throughput.
WAL retention remains an operational concern separate from crash consistency.
