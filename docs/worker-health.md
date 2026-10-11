# Worker health observations

`pgderive query status` includes a versioned `health` object. The controlled
release qualification exercises this interface through the owned-fixture checks
below. Its observations do not acknowledge source WAL or advance authoritative
progress.

`ready` requires activated progress, an active compatible slot, a recent heartbeat,
current writer fence and live ownership lock, and the worker's `ready` phase.
Publication and maintenance report busy phases and therefore temporarily report
`ready=false`. Supervision must distinguish busy from failed rather than restart
a worker on every false observation. The final stopped/failed state is written
before releasing ownership. If diagnostics themselves fail, ownership and fence
observations still prevent a stale heartbeat from establishing readiness.

Configure `[worker.health]` with `heartbeat_secs` (default 5), `stale_secs`
(default 30), `operation_timeout_secs` (default 300), and optional positive
`retained_wal_alert_bytes` and `backlog_alert_bytes`. Publication and maintenance
have a timeout. Cancellation never authorizes ACK; subsequent recovery reconciles
an uncertain PostgreSQL COMMIT against authoritative progress. Protected uploads
remain subject to the existing recovery and garbage-collection protocol.

WAL byte distances are physical PostgreSQL WAL distances. They include unrelated
transactions and Pgderive metadata writes; they are not source row counts or
elapsed processing latency. Logical time counts engine ticks, not wall time.
Missing observations remain null. `wal_status=unreserved` or `lost`, incompatible
slot identity, invalidation and feedback beyond durable progress are explicit
alerts and cannot establish readiness.

Heartbeat and alert settings do not cap PostgreSQL disk consumption. Operators
must size WAL storage and configure PostgreSQL retention policy, including
`max_slot_wal_keep_size`, for their workload. A cap can invalidate a lagging slot.
Pgderive does not skip WAL or automatically drop a slot to recover. Inspect status,
retain failure evidence, correct the operational cause, and use explicit query
rebuild when source history is unavailable. Rebuild takes a new consistent
snapshot and replaces the active generation through the lifecycle cutover.

Diagnostics report sampled logical time, run count and shared encoded-block cache
usage, together with configured execution and source transaction budgets. These
are serialized-record limits, not a bound on process RSS. Failure categories are
coarse (`resource_limit`, `operation_timeout`, `diagnostics`, `execution_or_io`);
full failure chains and retained scratch paths remain in the worker logs. Health
does not copy arbitrary transport errors or credentials into the catalog.

Owned-fixture qualification covers whole-process staleness, diagnostic connection
termination, oversized source transactions, unavailable scratch directories and
stalled object requests. Failure cases check unchanged authoritative progress and
slot feedback, then native materialized-view and independent memory equality
following restart. These checks do not establish every possible I/O or COMMIT
failure window.

Status also includes `worker_attempt`, keyed by the logical query and guarded by
its exclusive ownership lock. It records starting/running/stopped and failed,
backoff or exhausted phases, consecutive failures, retry limit and coarse failure
category. This covers failures before the heartbeat starts, such as a missing
slot or failed bootstrap. Database unavailability can prevent a new observation;
a historical observation must not be interpreted as a current running worker.

Scratch-cap qualification grows one join key through successful transactions,
then changes a single source tuple so join fanout exceeds the configured scratch
budget. Its failure leaves the complete source transaction unpublished and
unacknowledged; recovery with a sufficient budget matches both independent oracles.
