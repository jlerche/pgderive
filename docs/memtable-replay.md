# WAL-backed memtables and recovery

The source database, sink and authoritative progress live in the same PostgreSQL
instance. Each complete source transaction is evaluated against committed engine
state. Its arrangement deltas remain as immutable memory runs; sink deltas and
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

The worker checkpoints at `maintenance_ticks`, after `checkpoint_interval_ms`
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

Installing this version migrates all registrations in the catalog schema to format 2, so older
binaries reject them before claiming or publishing. This is a forward metadata
upgrade; retain the previous image for evidence, but do not treat restarting it
as a rollback procedure. Roll forward on failure, or explicitly rebuild using a
compatible engine and fresh source snapshot. Immutable trace codecs remain at
version 1; all workers sharing a catalog schema must upgrade together. Catalog compatibility and trace serialization are separate versions.


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
