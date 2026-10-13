# Controlled production installation and supervision

The supported release envelope is one supervised worker per query on PostgreSQL
17, fixed source contracts and the documented SQL subset. Capacity must fit the
measured workload and configured engine/storage budgets. This is not an automatic
HA service or arbitrary PostgreSQL SQL implementation.

## Build and inspect

Use the repository's pinned Rust toolchain, a C toolchain/Clang for pg_query,
OpenSSL development libraries and Python3. Image qualification also requires
Docker with BuildKit and support for local tmpfs-backed scratch volumes. Build a host-native package:

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

## Build the container image

The runtime image uses a scratch base and bundles the release executable, its
host ELF loader/library closure and the host CA bundle. No package repository or
network is consulted during Docker build. The image manifest binds the package
manifest, every runtime input hash and the resulting image ID. Context and BuildKit image timestamps
are normalized to the source commit timestamp through SOURCE_DATE_EPOCH. Two
clean no-cache builds must produce the same immutable image ID; both IDs and the
epoch are recorded in the manifest. The Rust package remains host-native and does not promise identical
bytes across arbitrary build hosts. Build on the target architecture and inspect
both manifests before release:

```sh
./scripts/package_release.sh artifacts/package-my-release
python3 scripts/build_container.py artifacts/package-my-release \
  artifacts/image-my-release --tag pgderive:my-release
```

Build from a clean reviewed commit for deployment. Qualification may use an
explicitly attested dirty tree before its commit; that image is a test artifact.
The scratch image has no shell, runs UID/GID10001, starts the executable directly
as PID1 and declares SIGINT as its stop signal. It contains no query configuration
or credentials. Supply `query create /etc/pgderive/query.toml` for first activation
and `query restart /etc/pgderive/query.toml` for subsequent starts.

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
backup-failover.md policy before restore or promoted failover. The package script builds binaries; the separate image builder creates the runtime image. Neither deploys a service.

## Qualification scope

The container harness runs the actual image against newly owned tmpfs PostgreSQL
with restricted TLS worker roles and a local HTTPS proxy to SeaweedFS. It mounts
configuration/CA read-only and a mode0600 S3 credential file owned by UID10001.
Workers have a read-only root filesystem, dropped capabilities, no-new-privileges,
a 512MiB container memory limit and two CPUs. Scratch uses a separately owned
256MiB tmpfs-backed volume held mounted by an evidence keeper across worker
restarts; the engine scratch byte budget is 128MiB. A marker proves that the
mount survives actual container stop/start. Failed runs archive scratch and retain
the owned containers/volume for inspection. The database never uses that volume;
its storage remains disposable tmpfs. These limits are specific test inputs,
not general sizing recommendations.

Run sequentially with the other mutating PostgreSQL harnesses:

```sh
python3 scripts/query_container_harness.py artifacts/container-soak-my-release \
  --image pgderive:my-release --image-manifest artifacts/image-my-release/manifest.json \
  --duration-seconds 1800 --interval-seconds 5
```

The default thirty-minute workload maintains three queries: a grouped inner join,
grouped COUNT/SUM/AVG/integral COUNT DISTINCT and grouped integral MIN/MAX. Transactions repeatedly insert,
update and delete skewed source rows, include NULLs and simultaneous join-input
changes, and are checked against native PostgreSQL materialized views and an
independent source-row memory calculation. It forces SIGKILL and starts the same
restart container every thirty iterations and at the end, then checks recovered
results. SIGINT shutdown is checked separately. The harness resolves the image to its immutable ID before execution and checks
every worker container against that ID. It verifies every Python qualifier/helper
hash against the image package source manifest at start and finish, preventing
mutable test code from being attributed to an earlier image. Progression samples
record cache hits/misses, RSS,
scratch usage, coarse membership roots, protection-ledger growth and slot WAL
backlog; event/request logs witness compaction/collection and signed conditional
PUTs. Final physical inventories include retained/unpublished objects. A short
run is runtime acceptance, not sustained-capacity evidence; inspect requested
and actual elapsed duration in qualification.json before calling a run a soak.

This qualifies the local Docker runtime and local SeaweedFS TLS fixture, rather
than a cloud provider, orchestrator or arbitrary workload. The production memory,
scratch and WAL budgets still require representative deployment sizing. Neither
one passing run nor an engine byte budget establishes universal throughput or
an RSS ceiling.

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

## Joined top-k storage qualification

The storage workload uses an owned source fixture with 10,000 bids, eight auctions
and a heavily skewed join key. It maintains the first three row occurrences per
auction with explicit PostgreSQL ordering. Each complete source transaction is
checked against a native materialized view defined by identical SQL and an
independent raw-row memory oracle. Updates, delete/insert replacements, NULLs and
simultaneous changes to both join inputs exercise retractions and join fanout.
Two graceful stops and fresh-process restarts check recovery from durable state.

Build the optimized worker, then run this harness sequentially with all other
mutating PostgreSQL checks. Supply the port of an independently owned PostgreSQL
fixture configured for logical replication; Docker database storage must be tmpfs.

```sh
cargo build --locked --release --bin pgderive_worker
mkdir -p artifacts/storage-soak-my-run
PGDERIVE_TEST_POSTGRES_PORT=32835 PGPASSWORD=postgres \
  PGDERIVE_S3_LATENCY_SCALE=0.1 PGDERIVE_HARNESS_TIMEOUT_SECONDS=7200 \
  python3 scripts/run_with_s3_proxy.py artifacts/storage-soak-my-run \
  python3 scripts/query_storage_soak_harness.py \
  --duration 3600 --transactions 1000 --rows 10000 \
  artifacts/storage-soak-my-run -- ./target/release/pgderive_worker
```

The run finishes only after both the requested duration and transaction count
have been reached. The common gate runs a six-transaction version as harness
acceptance; that short run does not establish sustained behavior. Retain the
binary and source hashes alongside the longer run's evidence.
The proxy wrapper defaults to a 600-second harness timeout. Longer workloads must
set a finite positive timeout that allows for bootstrap, recovery and checking
in addition to the workload duration; the value is recorded in storage-profile.json.

`qualification.json` records publication latency, cold restarts, protection
records, sampled RSS/cache usage, final physical object inventory and HTTP body
bytes/request counts excluding S3 listing requests. Interval inventories can
overlap maintenance; the final
inventory follows worker shutdown. HTTP byte counts exclude headers and framing,
and any unaccounted chunked requests are reported separately. The example applies
10% of the recorded proxy latency profile to local SeaweedFS. These measurements
qualify that fixture and do not establish cloud throughput, arbitrary SQL scale
or an enforceable RSS ceiling. Fenced uploads whose PUTs may still finish retain
their protection records; this workload cannot prove a universal ledger bound.

The retained optimized-worker run under artifacts/storage-soak-hour-second
completed 1,000 transactions over 3,608.11 seconds with cold restarts at
transactions 333 and 666. The native materialized view and independent memory
oracle matched after every transaction and restart. Logs record 1,000 published
transactions and 333 maintenance events. The 341 recorded input hashes matched
at completion; the PostgreSQL fixture used tmpfs with no database volume mounts.

Transaction submission-to-publication latency, including psql startup and source
SQL execution but excluding the subsequent oracle comparisons, was 158 ms median,
4.09 seconds p95 and 11.64 seconds maximum. Sampled peak worker RSS was 139.7 MiB;
protection records ranged from two to five, coarse membership roots from ten to
twenty, and the protection relation occupied 64–96 KiB. After graceful shutdown,
two protection records remained and the owned prefix contained 590 physical
objects totaling 7,645,107 bytes, including retained/unpublished objects.

Excluding listings, the proxy recorded 179,843 GET, 111,689 PUT, 111,099 POST and
8,350 HEAD requests, with no DELETE requests. POST includes object-store protocol
operations, so these counts are not a count of logical result writes. Accounted
HTTP request bodies totaled 1,105,730,208 bytes and response bodies totaled
2,073,763,950 bytes, with no unaccounted chunked requests. The observed tail
latency and storage traffic remain substantial despite bounded-prefix selection
and selective compaction; this evidence does not establish low storage cost or
universal capacity. The first attempt under artifacts/storage-soak-hour-first
was terminated by the wrapper's former ten-minute timeout. Its source snapshot,
logs and owned PostgreSQL fixture dump are retained as failure evidence; it is
not a completed sustained qualification.
