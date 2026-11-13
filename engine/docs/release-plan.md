# Controlled production release work

The [PostgreSQL fidelity roadmap](postgres-fidelity-roadmap.md) completed the
M0–M4 structural baseline with ordinary typed result tables and in-engine
computation. The [application qualification roadmap](application-qualification-roadmap.md)
continues with M5–M9. The requirements below describe the release contract and
remaining deployment qualification; they are not a list of wholly unimplemented features.

Bounded implementations and fixture qualification already cover query lifecycle,
health and failure handling, TLS/storage configuration, deployment and recovery,
and the Feldera audit. See [deployment](deployment.md), [health](worker-health.md),
[backup/failover](backup-failover.md), [storage](storage-configuration.md), and the
[pinned audit and implemented improvements](feldera-audit.md). M5 adds application
composition evidence and a local release baseline. M6–M8 expand composition and
measured scalability; M9 must qualify the actual provider, deployment and workload.
Neither local fixture success nor a small release baseline establishes production
capacity, cloud-provider durability or solved WAL retention.

The release target remains a containerized worker per query on PostgreSQL 17,
using the documented SQL subset, fixed source layouts and engine-owned results.
The following requirements remain binding as that supported envelope expands.

## 1. Query lifecycle and application results

Add a configuration-backed `pgderive query` CLI: validate, create/run, status,
restart, rebuild and drop. Validation resolves the actual catalog and checks in-engine support without installing metadata or creating a slot. Report native
output names/types and the executable plan identity. Creation and restart reuse
the existing journal and worker. Results expose correctly typed named PostgreSQL
columns with physical query-produced occurrences; weighted state remains in the
engine/object store.
Reject ambiguous/duplicate public column labels with actionable diagnostics.

Rebuild must bootstrap a fresh query generation and object namespace, then switch
the public result table contents atomically after activation. Preserve the old result until
the replacement is ready; interrupted operations must be restartable. Drop must
take the same exclusive ownership lock, validate journaled resources and respect
reader/upload pins. Retire resources in recoverable phases rather than deleting
unverified object prefixes. Never reset source/application tables.

Qualify command errors, quoted output names, all supported output scalar/array
types, NULL, duplicate visible rows, concurrent ownership, incompatible restart,
interrupted rebuild/removal, native-MV equality and cold restart in owned fixtures.

## 2. Operational protection and failure handling

Expose stable machine-readable status and health: activation/readiness, exact
durable source progress, slot validity, retained WAL/backlog, retry/failure state,
resource usage and maintenance. Freshness is a source progress observation, not a
claim that logical time is elapsed wall time. Add configurable lag/WAL alerts and
document PostgreSQL retention caps and slot-loss recovery. Exhausted budgets,
oversized transactions, scratch exhaustion and object outages fail without ACK;
operators need concrete diagnosis and a supported recovery/rebuild path.

Test monitoring with a deliberately stalled worker, invalidated/missing slot,
oversized transaction and resource/storage failures. Pausing consumption alone
does not protect PostgreSQL disk. No automatic skipping or dropping active slots.

## 3. Deployment and release qualification

Support explicit remote HTTPS S3-compatible endpoints, region and secure credential
configuration, preserving create-only objects and failure reconciliation. Verify
PostgreSQL TLS hostname/CA validation and least-privilege provisioning. Supply a
reproducible packaging and container runtime guidance with graceful shutdown.

Run sequential owned-fixture sustained/skewed workloads, prolonged storage outages
and supervised restart checks; retain capacity/resource evidence and scope claims
to measurements. Exercise a remote TLS endpoint through the real object-store
client. Document backup/restore of authoritative PostgreSQL metadata plus immutable
objects and an explicit failover/rebootstrap policy. Do not claim a specific cloud
provider was qualified without exercising that provider.

## 4. Feldera incremental implementation audit

Pull the existing local Feldera checkout without discarding user changes. Record
the inspected revision and compare our recomputation paths with its aggregate,
distinct, top-k/ranking, navigation, sessions and rolling-window implementations.
Separate incremental algorithms from affected-group scans, insertion-only
optimizations and watermark-dependent state eviction. PostgreSQL retractability,
NULL/peer/tie semantics and the absence of a watermark remain binding constraints.

Implement applicable incremental improvements, with independent memory and native
PostgreSQL oracles, restart coverage and measured work/read reduction. Record why
remaining scans are necessary or require a separate storage/index design; do not
claim full incremental execution from an API name or insertion-only benchmark.

Each coherent implementation commit requires independent review and the complete
`./scripts/check.sh` gate, including production coverage >=80%. Mutating fixtures
run sequentially on owned tmpfs PostgreSQL; preserve failures and frozen PoC data.
