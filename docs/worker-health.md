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
slot identity, invalidation and feedback beyond the object checkpoint LSN are explicit
alerts and cannot establish readiness.

Heartbeat and alert settings do not cap PostgreSQL disk consumption. Operators
must size WAL storage and configure PostgreSQL retention policy, including
`max_slot_wal_keep_size`, for their workload. A cap can invalidate a lagging slot.
Pgderive does not skip WAL or automatically drop a slot to recover. Inspect status,
retain failure evidence, correct the operational cause, and use explicit query
rebuild when source history is unavailable. Rebuild takes a new consistent
snapshot and replaces the active generation through the lifecycle cutover.

Status exposes published `logical_time`/`end_lsn` and separate
`checkpoint_time`/`checkpoint_lsn`. Published progress may be ahead of immutable
objects; WAL after the checkpoint remains required for replay. Diagnostics include
charged memtable bytes and the configured memtable cap.

Diagnostics report sampled logical time, run count and shared verified decoded
block cache usage, together with configured execution and source transaction
budgets. Cache `bytes` charges retained vector capacity plus encoded payload as
a heap estimate; `live_bytes` includes retained and evicted blocks still alive,
and `pins` counts reader references excluding cache ownership. Retained and live
charges overlap. Active pins may exceed the retention cap; indexes, tracking
metadata, allocator overhead and custom serde allocations are outside that cap.
These diagnostics and serialized-record limits do not bound process RSS. Failure categories are
coarse (`resource_limit`, `operation_timeout`, `diagnostics`, `execution_or_io`);
full failure chains and retained scratch paths remain in the worker logs. Health
does not copy arbitrary transport errors or credentials into the catalog.

Owned-fixture qualification covers whole-process staleness, diagnostic connection
termination, oversized source transactions, unavailable scratch directories and
stalled object requests. Failures before sink publication leave published progress
unchanged. Checkpoint failures may leave published progress ahead of the object
checkpoint, while feedback stays at that checkpoint. Recovery checks native
materialized-view and independent memory equality. These checks do not establish every possible I/O or COMMIT
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

## WAL protection policy and operator response

Provision retention on the PostgreSQL server, independently of the worker.
For example, an administrator can select an 8 GiB per-slot retention allowance
and configure PostgreSQL with `max_slot_wal_keep_size = '8GB'`. Set corresponding
worker warning thresholds before that allowance; worker.example.toml illustrates
1 GiB retained/backlog warnings. These are sizing examples, not universal safe
values. Verify the effective setting with `SHOW max_slot_wal_keep_size` and the
status observation before starting a workload. A negative value means unlimited
slot retention.

Choose the allowance from the peak **whole-cluster WAL generation rate** times
an acceptable outage/recovery interval, plus operational headroom. Set warnings
low enough that alert delivery and operator intervention can finish before the
remaining allowance is consumed. Include all slots, archive failures, checkpoint
behavior and other disk consumers in the database disk budget. Monitor actual
pg_wal/filesystem usage as well as per-slot distances. PostgreSQL applies this
setting at checkpoint time; it is not a strict instantaneous disk ceiling.
See [PostgreSQL slot retention configuration](https://www.postgresql.org/docs/17/runtime-config-replication.html).

Deliver `wal_retention`/`wal_backlog` alerts from periodic query status polling to
the deployment's monitoring system. A warning calls for diagnosis of worker,
object-store and database availability; fix the cause and restart/catch up while
the slot remains valid. Pausing the worker does not reduce retention. Do not
increase the allowance without verifying disk capacity. An unsafe, lost or
missing slot requires preserving logs/status, fixing the cause, stopping the
query container, then explicit `query rebuild CONFIG` on the same source identity.
Check the identical native materialized view, then `query restart CONFIG` and
verify progress advances. Never manually advance feedback or reset the journal.

The sequential owned-fixture slot harness now uses a **positive 64 MiB cap** and
**8 MiB warnings**. It records warning before invalidation, unchanged progress
and feedback, successful catch-up, checkpoint-driven slot loss, rejected restart,
and measured explicit rebuild with PostgreSQL/memory equality. The focused run
warned at 23,451,960 retained bytes and rebuilt in about 0.55 seconds for its
32-bid/8-auction fixture. Segment-boundary granularity explains overshooting the
warning threshold. This is a local observed recovery time, not a production RTO
or proof that total PostgreSQL disk stays below 64 MiB. The full gate repeats the
drill and retains the actual timings under query-health-slot/qualification.json.
