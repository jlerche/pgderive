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

Create an unprivileged pgderive OS user/group and install bin/pgderive to
/opt/pgderive/bin/pgderive. Install the supplied template to
/etc/systemd/system/pgderive@.service and run systemctl daemon-reload. Put a query
configuration at /etc/pgderive/myquery.toml, readable by the pgderive user but not
world-readable. Use an instance name containing only letters, digits, underscore
or dash; it is an operator-selected filename, not SQL text.

Place the S3 JSON document at /etc/pgderive/myquery.s3.json, owned by root, mode0600.
The unit's LoadCredential copies it privately; the injected
PGDERIVE__OBJECT_STORE__CREDENTIALS_FILE points to that private copy. Omit inline
S3 credentials from the TOML. Put the PostgreSQL password in the root-owned
mode0600 EnvironmentFile /etc/pgderive/myquery.env:

```text
PGDERIVE__POSTGRES__PASSWORD=your-secret
```

Provision secrets out of band; never commit them, echo them into shared logs or
put them on the command line. Follow systemd EnvironmentFile quoting rules for
special characters. TLS CA files must remain readable to the service. Scratch
uses the unit's private /run/pgderive-myquery directory, retained across service
stops/restarts so failed spill evidence survives. Inspect and archive failed
work before removing only verified stale execution directories while the service
is stopped. Retention is bounded by operator cleanup and filesystem capacity;
/run does not survive a host reboot. Size its filesystem and
engine scratch_bytes deliberately. /run is commonly tmpfs, so scratch usage may
consume host memory. Engine serialized byte caps are not RSS ceilings.

## First creation and normal restart

The normal service runs `query restart`, which deliberately refuses an
unregistered query. For first creation, add only this temporary drop-in at
/etc/systemd/system/pgderive@myquery.service.d/bootstrap.conf:

```ini
[Service]
ExecStart=
ExecStart=/opt/pgderive/bin/pgderive query create /etc/pgderive/myquery.toml
Restart=no
```

Reload systemd and start pgderive@myquery. Inspect its journal for the JSON ready
event; Type=simple means process startup, not query readiness. Readiness requires
successful activation and the published health checks. Stop the service after
ready, remove only bootstrap.conf, reload systemd and enable/start the normal
service. The stop sends SIGINT and waits for the current engine publication. An incomplete
source transaction still being assembled can be cancelled safely and will replay
from durable progress;
normal restart resumes the registered query. If creation fails, inspect status
and preserved logs before retrying: never delete metadata or slots to force it.

The template uses Restart=on-failure with a delay and bounded start bursts.
Worker retry_attempts is an additional internal bound. Both bounds require operator
attention after exhaustion. KillSignal=SIGINT invokes the current graceful path;
the 900-second stop timeout is an example, not a proved upper bound. Size it from
qualification; expiry becomes forced termination and relies on durable recovery.
Do not use a readiness=false observation as an unconditional restart trigger:
publication and maintenance legitimately report busy. Alert on failure/stale
heartbeat/slot validity and WAL retention according to worker-health.md.

Resource exhaustion fails without ACK. Inspect the exact category, preserve
artifacts, adjust a verified budget or remove the cause, then restart. Slot loss
requires the explicit rebuild path on the same source identity. Read the
backup-failover.md policy before restore or promoted failover. No production
service is installed by the package script or qualification harness.

## Qualification scope

The template is statically checked with systemd-analyze against the built
executable. The workload harness tests an external process restart policy,
SIGINT, unavailable storage and recovery; that is not a live systemd deployment
qualification. Native PostgreSQL and independent memory oracles check results.
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
