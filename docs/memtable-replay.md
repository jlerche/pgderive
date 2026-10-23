# WAL-backed memtables and recovery

The source database, sink and authoritative progress live in the same PostgreSQL
instance. Ordered groups of complete source transactions are evaluated against committed
engine state. Each group is an epoch and advances DBSP logical time once. Its arrangement deltas remain as immutable memory runs; sink deltas and
published source progress commit atomically with `synchronous_commit=on` before
local engine visibility advances. No new object PUT is required for that commit.

`pgderive_progress` records two distinct durable boundaries:

- `checkpoint_lsn` (C) is covered by the object membership and logical time in
  `pgderive_queries`. Logical replication feedback never advances beyond C.
- `end_lsn` (P), `commit_lsn`, `xid` and `published_time` describe the complete
  transaction already reflected in the sink. P may be ahead of C.

Checkpointing captures immutable arrangements and their exact published epoch,
then uploads them in one background task under an upload protection. Foreground
epochs continue publishing. Independent arrangement uploads have concurrency eight
and drain before returning an ordinary error. One PostgreSQL transaction publishes
the captured object membership, resolves its exact LSN from the epoch journal, and
advances C to that captured boundary. It preserves newer P and journal entries.
No checkpoint repeats sink DML. Local adoption replaces only the captured run
prefix, retaining all newer memory runs and their logical time. Only then may slot
feedback advance to C. A failed or uncertain publication causes authoritative
recovery; cached bytes and received messages never authorize acknowledgement.

The coordinator owns at most one upload. A checkpoint contains at most 128 object
roots per arrangement. Adjacent memory runs consolidate by binary carry over equal contiguous epoch spans.
Capturing a checkpoint seals its prefix and invalidates outstanding preparations;
subsequent merges affect only the newer suffix. Empty consolidated runs retain
their spans. This bounds normal suffix fan-in logarithmically rather than keeping
one run per epoch. Subsets whose weights exceed i64 retain separate exact runs
until checkpointing resolves their cancellation with retained objects.
Backpressure applies at 120 actual suffix runs while uploading (leaving room
for a 128-root replacement), at 248 resident runs within the 256-run input bound, or at
4,096 total epochs beyond durable C, independently of when the upload started. Memory
pressure drains the in-flight checkpoint and checkpoints the remaining suffix
before retrying the unpublished input. Frozen and active snapshots share immutable
batches; frozen memory remains charged until physical adoption. Cancellation is
awaited before a new worker attempt; failed/cancelled uploads retain their durable
non-quiescent protection. Timeout never releases a namespace as safely collected.

On restart, the engine claims and fences the query, pins and cold-validates C,
validates the registered cluster/timeline/database/publication/slot, then starts
logical replication at C. Transactions through P rebuild memory state without
sink writes or progress advancement. Transaction end LSN orders replay; commit
LSN and xid validate conflicting identities at equal boundaries. Repeated source
transactions are skipped, while legitimate duplicate tuples retain their weights.
At P, the exact transaction identity and reconstructed logical time must agree
with PostgreSQL before readiness. New transactions then publish normally.

The worker starts an upload when retained arrangement run count reaches
`maintenance_ticks` or after `checkpoint_interval_ms` (default 5000 ms).
This is a scheduling interval, not a hard bound on checkpoint age during slow I/O.
Memory/run pressure waits for checkpoint completion. Graceful stop drains the
in-flight checkpoint and then any newer suffix until C=P. `memtable_bytes`
(default 64 MiB) caps retained serialized payload plus structural charges across
arrangements. It is an estimate, not an RSS limit; temporary computations,
allocator overhead, reader pins and immutable caches have separate costs. An
oversized single transaction is rejected without advancing its source progress.
Restarting with a cap smaller than the published suffix requires restoring the
previous cap; recovery fails closed rather than skipping state or republishing.
Recovery has a startup deadline and responds to shutdown requests.

WAL after C is part of the recovery data, not disposable backlog. Advancing or
losing the slot past C rejects startup even when P is newer. WAL retention limits,
storage outages, large unrelated database write volume and idle long-running
source transactions remain operational concerns. Health reports distinguish
published backlog from retained WAL and flag acknowledgement beyond C.

The live memtable harness witnesses two crashes with C<P, proves unchanged sink
row versions and physical tuple identities during replay, compares joined aggregates with native
PostgreSQL, checks timer checkpointing, and rejects an externally advanced slot.
The asynchronous checkpoint harness additionally holds a witnessed PUT while 160
newer epochs publish, kills the worker during upload and after the older checkpoint
commits, checks unchanged sink row identities on replay, exercises memory pressure
with an upload in flight, drains a suffix during graceful shutdown, and verifies
that upload timeout retains non-quiescent protection and safe acknowledgement.
Existing COMMIT, object upload, recovery and GC crash probes remain enabled.

Epoch publication also inserts a coarse boundary into `pgderive_epoch_boundaries`
in the same PostgreSQL transaction as sink deltas and progress. Each entry records
logical time, preceding end LSN, final transaction identity and source transaction
count. Recovery reconstructs these exact groups from WAL, validates the complete
boundary chain and fails closed if a boundary is missing or inconsistent. A
checkpoint atomically removes the entries it covers. No tuple indexes live in
this journal.

All registrations use format 3 and record complete epoch boundaries. Recovery
requires the journal to cover the exact durable suffix; older catalog formats
are rejected and require a fresh registration. There is no automatic upgrade or
transaction-by-transaction replay fallback.

The adaptive scheduler learns arrival intervals, fixed publication cost, marginal
work cost and prediction error. It drains ready transactions or briefly waits for
predicted arrivals when doing so can amortize publication within the configured
freshness objective. Sparse inputs publish immediately. The objective is not a
latency guarantee; storage, PostgreSQL and computation can exceed it. Byte,
transaction and existing execution limits bound groups, and transactions are never
split. Intermediate source transaction states may be skipped in the destination;
every published state represents a complete ordered source prefix.

`[worker.epoch]` defaults to `enabled=true`, `freshness_target_ms=50`,
`max_bytes=8388608` and `max_transactions=4096`. Disabling grouping retains the
format 3 journal and publishes one source transaction per epoch. Bounded read-ahead
receives complete transactions while publication runs; receiving never acknowledges
WAL. The checkpoint policy and its durability boundary remain independent of
epoch selection. Epoch events report closure reason, source transaction count,
source bytes, predicted service time and observed source age. Source age uses the
PostgreSQL commit timestamp and therefore depends on clock synchronization.

Freshness pressure stops intentional waiting for future arrivals. It does not
prevent combining already queued complete transactions: fixed cost or prediction
uncertainty above the objective must not force singleton epochs and amplify
checkpoint stalls. The learned exploration bound applies even when already late
or arrival estimates are unavailable, in addition to the hard resource ceilings.

`epoch.received_to_sink_ms` measures the oldest complete transaction in the epoch
from the source reader's completion of transaction decoding through durable sink
publication, including reader queueing and coalescing. Its monotonic receipt clock
is runtime-only and never enters persisted transaction identity or replay metadata.
This differs from source-commit age (which also includes upstream CDC/decode delay)
and `published.elapsed_ms` (execution and publication after epoch collection).
These are per-epoch samples; they are not per-row or per-source-transaction latency
percentiles when multiple transactions share an epoch.


Steady-state publication compares the complete claimed query and progress boundary
inside PostgreSQL, locking the query row before the progress row. The comparison
includes the plan definition, checkpoint body and digest, binding, ownership,
fence, clocks and source transaction identity. It avoids reconstructing and
validating the same immutable metadata in Rust on every transaction. Claims,
recovery and checkpoints still perform full decoding and validation. Publication
retains source DDL exclusion and layout verification, lifecycle protection,
synchronous COMMIT and one atomic sink/progress transaction. Slot feedback still
uses only the immutable checkpoint boundary C.

With `phase_metrics=true`, published events include `preparation_us`: sequential
operator timings, arrangement staging, memory accounting and candidate validation.
Repeated stages accumulate across a memory-pressure retry; `failed_preparation_tail`
and `forced_maintenance` separate the failed attempt from its checkpoint. The
`publication_us` fields `boundary_guard`, `source_layout` and `progress` subdivide
`membership_layout_progress`, so do not add that aggregate to its children.
Disabled preparation profiling does not sample clocks or allocate operator labels.

`maintenance.elapsed_us` is background checkpoint wall time, including time until
foreground completion. It is not publication-blocking duration. The maintenance
event reports captured `time` separately from current `published_time`; their gap
shows foreground epochs that completed while the upload was outstanding.
