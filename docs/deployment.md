# Controlled production installation and supervision

The supported release envelope is one supervised worker per query on PostgreSQL
17, fixed source contracts and the documented SQL subset. Capacity must fit the
measured workload and configured engine/storage budgets. This is not an automatic
HA service or arbitrary PostgreSQL SQL implementation.

## Build and inspect

Use the repository's pinned Rust toolchain, a C toolchain/Clang for pg_query,
OpenSSL development libraries and Python3. Build a host-native package:

```sh
./scripts/package_release.sh artifacts/package-my-release
```

The output directory must be new; build logs survive failure. Inspect manifest.json
for the exact revision, dirty-tree status, input hashes, binary hashes and toolchain.
Use a clean reviewed commit for deployment. runtime-libraries.log records linked
libraries; the executable is host-native, not a universal/static Linux binary.
Build and run on a compatible architecture, libc and OpenSSL environment. The
package script clears inherited Rust instrumentation flags, compiler wrappers,
target and release-profile overrides and uses a separate target/package-release
build directory with an explicit host target. Its manifest records this policy; gate package measurements
therefore use an uninstrumented release binary. A successful build alone does not establish the live PostgreSQL/object-store gate.

## Provision

An administrator provisions PostgreSQL TLS, source primary keys, REPLICA IDENTITY
FULL, an explicit publication and an empty dedicated engine schema. Follow
postgres-security.md for restricted role grants and storage-configuration.md for
HTTPS credentials. Avoid using the source schema as the engine schema in production.
Source DDL/layout changes require a deliberate query rebuild or fresh bootstrap.

## Container runtime contract

Deploy one worker per query in a container using an image built from a clean,
reviewed commit. Run the packaged executable on a compatible architecture, libc
and OpenSSL environment; the host-native package is not itself a container image.
Run as an unprivileged UID/GID and mount the query configuration and CA files
read-only. Inject secrets through the deployment platform's secret mechanism.
The S3 credential JSON must be a private regular file readable by the worker,
mode0600, selected by `PGDERIVE__OBJECT_STORE__CREDENTIALS_FILE`. PostgreSQL uses
`PGDERIVE__POSTGRES__PASSWORD`. Never put credentials on the command line or in
an image layer, repository or shared logs. Verify the actual mounted secret's
permissions against the credential-file checks.

Give the worker a private writable scratch directory and set TMPDIR to it.
Choose disk-backed scratch or account for its memory usage when using tmpfs.
Preserve failed spill evidence across container restarts using an explicitly
managed scratch mount, with filesystem capacity and operator cleanup limits.
Engine serialized byte budgets are not container memory limits or RSS ceilings.
Keep source database storage outside the worker container. Disposable Docker
PostgreSQL test fixtures must continue to use tmpfs.

## First creation and normal restart

For first creation, run the executable directly as the container's main process:

```text
pgderive query create /etc/pgderive/query.toml
```

Creation remains a running worker after activation. Inspect its JSON ready event
and status; process startup alone does not establish readiness. After the initial
activation, configure subsequent container starts to run:

```text
pgderive query restart /etc/pgderive/query.toml
```

Restart deliberately refuses an unregistered query. If creation fails, inspect
status and preserved logs before deciding whether to retry creation or restart.
Do not delete metadata or replication slots to force startup. Only one container
may own a query; use a rollout policy that respects that exclusive ownership.

Configure the container runtime to send SIGINT to the worker on shutdown, with a
termination grace period sized from the qualified workload. Execute the binary
directly or use an entrypoint that forwards signals. SIGINT drains the current
publication; an incomplete source transaction can be cancelled safely and replays
from durable progress. A forced kill after the grace period relies on crash
recovery. Configure bounded restart backoff and operator attention after repeated
failures, alongside the worker's internal retry_attempts bound.

Publication and maintenance legitimately report busy. Do not use every
readiness=false observation as a liveness failure or automatic restart trigger.
Alert on failure, stale heartbeat, slot validity and WAL retention according to
worker-health.md. Apply container CPU, memory and scratch limits using measured
workloads and verify failure/recovery with those limits in the selected platform.

Resource exhaustion fails without ACK. Inspect the exact category, preserve
artifacts, adjust a verified budget or remove the cause, then restart. Slot loss
requires the explicit rebuild path on the same source identity. Read the
backup-failover.md policy before restore or promoted failover. No container is built or deployed by the package script or qualification harness.

## Qualification scope

The workload harness tests an external process restart policy, SIGINT,
unavailable storage and recovery. It does not qualify a container image,
orchestrator, mounted secrets or runtime resource limits. Exercise those with
the selected deployment platform before production. Native PostgreSQL and
independent memory oracles check results.
Read the retained measurements before choosing resource limits; neither one
passing run nor a byte budget establishes universal throughput or an RSS bound.

The retained optimized-package candidate run used 64 auctions and 10,000 bids,
including 9,531 bids on one auction and 2,000 NULL prices. It applied 100 complete
source transactions of 32 bid updates each, with simultaneous auction changes
every tenth transaction. On this local PostgreSQL/SeaweedFS fixture, observed
transaction submission-to-publication latency, including psql startup and SQL
execution, was 62 ms median, 583 ms p95 and 800 ms maximum; sampled peak RSS was 91.3 MiB. The workload took 28.4 seconds including
periodic native-MV and independent-memory comparisons. These are observations,
not throughput guarantees or enforceable memory ceilings.

The same run preserved durable progress and left slot acknowledgement at or
before that boundary during a 60-second storage outage, failed its bounded
external restart attempt, then recovered the queued transactions with both
oracles matching. Cold restart and SIGINT shutdown also passed. After recovery
and worker shutdown, the owned prefix contained 171 physical objects totaling
583,172 bytes, including retained/unpublished keys. PostgreSQL's eight coarse
membership roots totaling 52,286 bytes were measured earlier at workload end;
that root count is not the reachable object closure or total state footprint.
Evidence is retained under artifacts/release-capacity/packaged, with its candidate
package manifest under artifacts/package-release-candidate. The common gate
repeats this qualification with its freshly packaged executable.

The initial run with a 128 spill-run budget failed on the first hot-auction
change; its owned fixture, logs and scratch evidence were preserved under
artifacts/release-capacity/first. Durable progress and confirmed slot flush both
remained at the preceding boundary. The successful runs explicitly configured
512 spill runs while retaining the 128-entry resident budget. This observed
limit is part of choosing the operating envelope, not evidence of unlimited skew.
