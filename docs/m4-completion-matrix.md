# M4 completion matrix

M4 completes a bounded architecture baseline. Individual SQL functions and further
query shapes remain the separate SQL support backlog. The existing computed
aggregate matrix is the first slice. This completion scope was published before
implementation and is now implemented and qualified.

| Boundary | Implementation and acceptance evidence |
| --- | --- |
| Live semantic maintenance | Database-wide shared worker lease; exclusive maintenance begin refuses active workers. Durable pending state rejects startup/rebuild; finish releases the pending state and preserves a monotonically increasing revision. Existing bindings require explicit rebuild, even if catalog contents were restored. No semantic polling on the computation/publication path. |
| DDL/function/type/extension/settings/collation/timezone changes | Stop all workers in the database, begin maintenance, change/attest the environment, finish, then rebuild. Source-layout DDL remains transactionally guarded. Catalog fingerprints are rechecked at initialization; external runtime files require operator attestation through the revision protocol. Uncoordinated privileged mutations remain outside the deployment contract. |
| Maintenance durability | Owned-database tests: active-worker refusal, pending-state startup rejection, idempotent begin/finish, process restart, stale registration refusal with unchanged sink/progress/ACK, explicit rebuild, and native-MV equality after resume. |
| Computed inner equijoin keys | Side-local typed integral expressions, exact matching result OIDs, at most 16 equalities. Preserve source tuple identity, NULL exclusion, collisions and simultaneous input cross terms. Native-MV and independent source oracle through retractions and restart. |
| Derived compositions | Computed aggregate results and keys can participate in already supported derived joins; state and generated field identities remain scope-local. Qualify through CDC and recovery. |
| Outer/existence computed ON expressions | Keep rejected until unmatched-row semantics and evaluation domains are separately qualified. Existing raw-column forms remain supported. |
| Computed window arguments/keys and arbitrary subqueries | Remain rejected where frame-aware evaluation, occurrence identity or dependency tracking is unproven. Add explicit rejection evidence; no approximate execution. |
| Wall clock, volatile operations, hidden relation reads | Rejected; no refresh-epoch or hidden dependency evaluator is introduced. |

Maintenance does not rewrite old arrangement state. Rebuild takes a fresh source
snapshot and uses the existing atomic compatible-table cutover. Incompatible
layouts retain the documented consumer resnapshot boundary. PostgreSQL remains
metadata/source/destination infrastructure, never a computation fallback.

The lease and revision are database-local. For shared backend binaries, libraries,
timezone/locale files or cluster-wide settings, quiesce workers and begin
maintenance in **every affected database** before changing shared resources. Finish
and rebuild each database afterward. A single database barrier cannot protect
other databases or hosts. An interrupted rebuild from before maintenance can
have an incompatible recorded signature; explicitly drop/recreate that pending
query if ordinary rebuild cannot resume. Do not edit its journal to bypass this.

Computed ON operands evaluate eagerly on each side, including when the opposite
input is empty. The conservative local-map error contract applies; later WHERE
clauses do not guard ON maps. The Rust history test empties both inputs, then
proves that an overflowing unmatched operand fails without changing its checkpoint.

Qualification: `scripts/environment_harness.py` covers the maintenance protocol;
`scripts/computed_join_harness.py` covers six new join shapes against native
materialized views and independent source recomputation. Both run in the common
core gate. `src/worker/tests/computed_join.rs` adds weighted histories, simultaneous
input changes, cold object-state restart, rejected shapes and failure atomicity.
The existing native-join, join-composition and native-cross live harnesses supply
additional regression evidence for unchanged outer/self/multiway/scoped paths.

Use one maintenance coordinator for an affected environment. `finish` is the
operator's explicit attestation that all coordinated mutations have completed;
it must not race another administrator's changes or another maintenance cycle.
The command serializes database state changes, not arbitrary administrator actions.

Maintenance does not qualify a new function implementation, overload or runtime
by recording its metadata. Restore qualified definitions or first qualify the
corresponding engine implementation before finishing and rebuilding. The closed
capability registry and supported-environment contract still govern admission.
