# Shared checkpoint data packs

Relational checkpoint flush and compaction now drive the same streaming writers
through one checkpoint-local packing queue. Independently typed logical
arrangements retain separate roots, indexes and schemas; their compressed data
payloads share physical content-addressed objects. The existing authenticated
block addresses already describe global pack paths and byte offsets, so neither
a second storage format nor a compatibility reader is needed. Every caller of
the streaming data upload boundary uses the same code.

The queue combines ready payloads up to the existing 4 MiB object target. A single
larger encoded block remains valid up to the existing stored-block hard limit.
Each block offset is rebased into the combined pack; its block checksum and
logical full-tuple identity stay unchanged. Root metadata remains independent:
those small root objects are the catalog's integrity anchors. Sharing them would
require another root-extent format and GC authentication boundary; this change
reuses the existing range-addressed data format instead.

Packing is bounded by the query workspace count and a queue of that same bounded
capacity. Each admitted streaming writer retains its existing bounded payload,
block fences and index budget. The pack driver retains at most one target-sized
assembly (or one oversized legal block), one carry payload, and bounded reply
addresses. It never buffers an entire checkpoint. Batching depends on which
writers have ready payloads; a one-workspace query and a solitary large batch
have no cross-arrangement savings. Data PUTs are serialized within a checkpoint,
while root PUTs remain concurrent. No upload task is detached: cancellation drops
the queue driver and all sibling futures together. Upload protection closes only
after the normal atomic PostgreSQL checkpoint publication; errors preserve its
nonquiescent reservation. Error fanout preserves the source chain, including
permanent operation-budget exhaustion.

GC continues marking authenticated roots and every referenced pack path across
current memberships and pinned reader protections. A shared pack is retained
while any marked root names it, including a partial run extent or a compacted
root reusing a large block. Physical deletion remains object-granular; obsolete
bytes in a shared pack may survive until the last reference disappears. Existing
compaction repacks tiny fragments and may reuse large disjoint blocks. It cannot
adopt arbitrary unprotected old objects. A complete later bounded GC sweep can
reclaim a pack only after all committed and protected references disappear.

The deterministic test fixture compares arrangement-local production flushes
against relational checkpoint uploads, cold-opens every independent arrangement,
checks disjoint range addresses and continues incremental execution after
recovery. The sequential common gate exercises PostgreSQL atomic publication,
upload protection, crash recovery, pinned readers and GC on this production path.
See request-cadence.md and operation-budget.md for independent cadence controls,
operation enforcement and deployment boundaries. No live demo was used.

Measured packing alone uses the same production traces and arrangement writers
for its control. Two independent eight-arrangement queries make 32 PUT attempts
with arrangement-local flushes and 20 with shared checkpoint packs (37.5% fewer).
A two-arrangement codec/range fixture makes 4 versus 3 PUTs. These include root
metadata PUTs; metadata sharing is not claimed. The cadence comparison in
request-cadence.md holds this implementation constant to separate timer savings
from packing savings. Budget tests include conditional attempts rather than
counting only successful uploads. Failure evidence and focused-test output are
preserved under `artifacts/request-budget/`.
After recovering and applying three more incremental epochs, full production
compaction of those two queries made 20 additional PUT attempts; a further cold
reopen preserved their exact bags. This is a separate explicit-compaction
measurement, not a predicted steady-state compaction rate. The sampled cadence
run reports its own selected-compaction work independently.
