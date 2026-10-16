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
publication, bounded SQL compilation, in-engine expressions and native-MV
harnesses. Existing features remain bounded; this roadmap does not retroactively
certify every type/expression domain.

There is one execution path: qualified implementations inside the engine. A local
PostgreSQL runtime could be considered only as an in-process implementation with
its own qualification; the connected database is never a computation fallback.
Unsupported functions, casts, collations or query shapes fail before activation
with an explicit reason. The sink receives complete result values and performs
ordinary typed table DML. Keep source LSNs distinct from logical time and preserve
object PUT → one PG membership/result/progress transaction → slot ACK.

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

## M2 — Qualify the in-engine semantic runtime

All scalar operations and aggregate finalization execute in Rust. The former
sink-deferral pass, placement/retention reports, terminal SQL renderer, connected
function binding and mapped sink APIs have been removed. Supported native casts,
checked arithmetic, CASE/COALESCE, predicates, date_bin and sufficient statistics
remain in-engine. Integer AVG scale selection and rounding are finalized locally,
qualified against PostgreSQL and the independent Decimal oracle. Unimplemented
text, calendar, regex, formatting and extension functions fail compilation.

Expand supported functions one small, independently tested implementation at a
time. Bind exact types, coercion, NULL/strictness, lazy evaluation, collation,
error and retraction behavior. Add memory and native-materialized-view fixtures
covering source transactions and cold restart. Never substitute an approximate
implementation or send operands to PostgreSQL for evaluation. Registration still
captures the source semantic environment during initialization.

## M3 — Improve engine scalar diagnostics and coverage

Inventory missing functions and supported overloads. Give rejected calls a useful
function/type/feature reason before slot creation. Generalize expression coverage
only when implemented locally, with explicit budgets. Qualify constant-folding,
errors in lazy branches, aggregate finalization, and shared evaluation/retraction
behavior without an execution-placement system.

## M4 — Expand relational coverage and invalidation

Use the capability inventory to select concrete workload queries and publish a
coverage matrix before implementation. Prioritize computed predicates/keys,
computed aggregate inputs, composed derived queries, then additional joins,
subqueries and windows. Every operation requires an in-engine implementation.
No new SQL syntax is considered complete without retractions and recovery evidence.

Broaden M0's dependency tracking and fencing to support more changes: DDL, function/type
changes, extensions, settings, collation providers and timezone data. Merely
checking once at startup is insufficient if concurrent administrative changes can
invalidate a running plan. Unsupported changes stop publication and require rebuild.

Keep time-dependent queries rejected until explicit refresh epochs/invalidation
are implemented. Advancing wall time is not a source-table change; a connected database is not an evaluation path. Hidden relation reads require explicit tracked dependencies and
a correct source-boundary evaluation design before support.

Acceptance: supported environment changes safely reevaluate or rebuild; unsupported
ones cannot silently reuse old keys/results. Each new query shape has native-MV,
independent relational-oracle and recovery coverage.

## Qualification and sequencing

Prioritize native fidelity and growing in-engine SQL coverage. Every slice requires
independent review and the common gate, including sequential live PostgreSQL and
object-backed restart/durability fixtures. PostgreSQL is the reference oracle in
tests, never the production evaluator. Changed executable identities require an
explicit rebuild; old state must not be silently reinterpreted. Live delivery
remains outside scope.
