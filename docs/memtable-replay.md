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

Checkpointing uploads equivalent consolidated arrangements under an upload
protection. Independent arrangement uploads have bounded concurrency and drain
before returning an error. One PostgreSQL transaction publishes the new object
membership and sets C=P without repeating sink DML. Only then may slot feedback
advance. A failed or uncertain publication causes authoritative recovery; cached
bytes and received replication messages never authorize acknowledgement.

On restart, the engine claims and fences the query, pins and cold-validates C,
validates the registered cluster/timeline/database/publication/slot, then starts
logical replication at C. Transactions through P rebuild memory state without
sink writes or progress advancement. Transaction end LSN orders replay; commit
LSN and xid validate conflicting identities at equal boundaries. Repeated source
transactions are skipped, while legitimate duplicate tuples retain their weights.
At P, the exact transaction identity and reconstructed logical time must agree
with PostgreSQL before readiness. New transactions then publish normally.

The worker checkpoints when retained arrangement run count reaches
`maintenance_ticks`, after `checkpoint_interval_ms`
(default 5000 ms), on memory pressure, and on graceful stop. `memtable_bytes`
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
Existing COMMIT, object upload, recovery and GC crash probes remain enabled.

Epoch publication also inserts a coarse boundary into `pgderive_epoch_boundaries`
in the same PostgreSQL transaction as sink deltas and progress. Each entry records
logical time, preceding end LSN, final transaction identity and source transaction
count. Recovery reconstructs these exact groups from WAL, validates the complete
boundary chain and fails closed if a boundary is missing or inconsistent. A
checkpoint atomically removes the entries it covers. No tuple indexes live in
this journal.

New registrations start in format 2 and activate format 3 at a fully checkpointed
boundary. Existing format 2 suffixes first replay with their original transaction
boundaries, then checkpoint before activating epochs. Older binaries reject format
3. This is a forward metadata upgrade, not a reversible configuration change;
roll forward or explicitly rebuild from a fresh source snapshot. Immutable trace
codecs remain version 1.

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
