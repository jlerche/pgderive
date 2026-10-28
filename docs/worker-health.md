# Worker health observations

`pgderive query status` includes a versioned `health` object. The controlled
release qualification exercises this interface through the owned-fixture checks
below. Its observations do not acknowledge source WAL or advance authoritative
progress.

`ready` requires activated progress, an active compatible slot, a recent heartbeat,
current writer fence and live ownership lock, and the worker's `ready` phase.
Publication and forced checkpoint drains report busy phases and temporarily
report `ready=false`. Background checkpoint uploads and GC can run while the
foreground phase remains `ready`; upload deadlines and maintenance debt still
bound admission. Supervision must distinguish busy from failed rather than
restart a worker on every false observation. The final stopped/failed state is written
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

## Shared worker admission

`execution.workspace_slots` (default 8, range 1..=256) limits active resource
phases through one pool owned by the query execution. CDC/replay input conversion,
foreground operator evaluation, arrangement staging, snapshot chunk/final
processing, output materialization and sink-delta encoding use this pool.
Recovery/adoption, detached compaction, captured checkpoint arrangement uploads
and the background collector share it too. The snapshot execution clone retains
the original pool even when its per-operator limits are reduced.

Checkpoint writers and GC additionally share a maintenance quota of
`max(1, workspace_slots - 1)`. At capacities above one this leaves a slot for
foreground work while an upload or collection is stalled. Capacity one executes
phases serially. An operation acquires admission before opening its private
workspace and releases it before invoking another admitted phase. Evaluation
therefore releases its permit before parallel arrangement staging and reacquires
one for final output materialization. GC acquires before its SQL mutex/transaction.
Cursors and cache pages do not own permits that they would need to acquire again
to make progress. Cancellation releases admission through owned RAII permits.

`execution.object_requests` (default 16, range 1..=256) configures one
object-store API limiter around the query's scoped store. Foreground, bootstrap,
recovery, checkpoint and GC share the same wrapper Arc. GET-body and LIST-stream
lifetimes retain admission until consumed or dropped; bulk deletion still uses
the underlying store's batched implementation. One API operation can issue more
than one HTTP request internally. Pgderive's collector continues to bound each
delete batch. These settings alter admission, leaving data packing, checkpoint
cadence, publication ordering and object-store retry policy unchanged.

Published events and heartbeat diagnostics report `workspaces`: capacity, active
and peak global ownership, global waits, background quota/ownership and background
waits. A background quota holder may still be waiting for global admission.
Heartbeat `budgets` includes both shared limits alongside existing per-operation,
cache and memtable limits. Queued phases have no new reader/writer workspace;
existing captured snapshots, sealed values and proof handles can remain alive.

These are active-phase and API-operation bounds. Aggregate retained-byte/RSS
qualification remains separate: bootstrap folds, sealed results, decoded and
compressed cache pins, native caches, transport/runtime buffers and allocator
retention have lifetimes beyond an active permit. Existing encoded-record,
contribution, scratch, cache and memtable limits remain necessary. This slice
controls concurrency amplification without claiming a whole-process byte cap.


## Incremental physical collection

Each collection turn requests one global `pgderive/` page, at most 1,000 objects
by default (`GcLimits::page_objects`, valid 1..=1000). It never lists each upload
namespace separately. The pinned S3 adapter supplies an opaque continuation token;
no filename ordering is assumed. Ordinary object requests and stateless pages use
one scoped semaphore because the upstream `LimitStore` does not expose paginated
listing. GET/LIST/delete streams retain admission; a completed page releases it.
Coalesced range reads and batched deletes still delegate to the underlying store.
Multipart uploads are explicitly unsupported; the engine uses bounded ordinary PUTs.

The coarse `pgderive_gc` row records the sweep generation, continuation token and
whether a pass remains open. It is physical metadata, unrelated to source LSN or
DBSP logical time. Every turn marks a fresh authoritative checkpoint/pin snapshot.
It deletes only unmarked canonical objects in sealed upload namespaces, then
commits cursor and survivor metadata together. A failed or cancelled page can
repeat already completed deletes. Cursor state never advances on a failed delete.
The collector holds its advisory transaction lock throughout, releasing the
publication lifecycle lock before object I/O and reacquiring it for metadata.

Retirement candidates are exactly the namespaces already sealed and proven
quiescent at pass start. Namespace-level survivor markers accumulate through all
pages. Only a completed pass can retire candidate ledgers with no survivors.
Unknown objects retain their ledger. An upload closing during the pass, or a
fenced upload receiving its writes-finished proof later, must wait for a new
pass. Fencing alone never proves writes have finished. Late writes behind a
cursor remain collectible on later passes. The S3-compatible service must provide
complete listing/continuation for objects stable throughout a pass, including
when already returned objects are deleted; the live SeaweedFS adapter exercises
that behavior. There is no token-expiration reset fallback: service errors remain
visible and the durable token stays unchanged.

Collection events expose structured `collection` status with `scanned`, `deleted`,
`pass_complete`, `generation`, `retired_namespaces` and `retained`. The current
cadence remains one collection turn after checkpoint installation. Cursor progress
alone does not establish that GC service can keep up with an offered workload.

The physical inventory no longer has a total-object listing ceiling. The mark and
coarse ledger snapshot remain bounded by 4,096 roots, 262,144 reachable objects or
sealed ledger entries, and 16 MiB of encoded catalog/protection/namespace metadata.
A continuation token is capped at 16 KiB separately. This is not a total RSS cap,
and mark work still depends on current live dependencies. See
[production qualification](production-qualification.md) for the remaining gates.

This is a development catalog hard cut. Existing catalogs lack the new GC columns
and are rejected with a rebuild instruction. Recreate the owned query catalog and
rebuild its views before running this version against old state. No migration,
legacy collector or automatic destructive reset is provided. The paused demo has
not been changed.

Explicit `query drop` drains the current sweep and one fresh complete pass using
these same bounded turns, sharing one dependency cache and scoped store. This
also applies to repeating drop after the logical state is already `dropped`.
Rebuild performs one opportunistic turn because its resumed worker continues GC.
`collection_passes_complete` is false if another collector or unresolved active
upload prevents draining; repeat drop after resolving that condition. Completed
passes do not imply unknown objects or live reader pins were removed, and fenced
nonquiescent ledgers still need a writes-finished proof. The returned `collection`
statistics describe the final turn, not the command's aggregate deletion count.
Only individual turns are bounded; concurrent namespace growth can extend total
drop duration. Cancelling the command preserves committed per-page progress.


Physical maintenance service and object-request admission diagnostics are described
in [maintenance service budgets](maintenance-service.md). These are work/payload
measurements, not retained-memory, HTTP billing or production capacity guarantees.
