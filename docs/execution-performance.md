# Reusable execution and checkpoint optimizations

These mechanisms depend on immutable batches, codec identity and catalog
publication rules. They do not inspect SQL text, arrangement names, demo data or
a particular graph topology.

## Immutable batch sizing

A finalized `ZSet` shares its exact encoded byte count, largest record and entry
count with clones. Edge validation and memtable charging reuse that summary.
Serialization writes to a bounded counter instead of allocating a temporary byte
vector. Uncached validation stops on the first exceeded limit; failed or partial
summaries are never cached. Successful mutation replaces the summary, while a
failed mutation leaves both entries and summary intact.

## Native values

`Value<T>` retains canonical data inline. Equality, ordering, debugging and
serialization use that data exclusively. An optional immutable native view can
avoid repeated parsing of an engine-produced value. Mutating canonical data
removes its view; clones keep independent canonical data and share their view.

One `NativeBudget` is shared by all bound operators in a relational query.
`execution.native_cache_bytes` defaults to 4 MiB; zero disables retention. The
budget includes native payload capacity, the view and decoded-owner allocations,
codec storage, and allocation alignment. The extra inline wrapper size is covered
by existing tuple-size accounting. A decoded handle owns its reservation, so
removing a row or evicting state cannot release accounting while that handle lives.
This is an allocation charge, not an RSS limit: transient decoding, canonical
state, active block pins and allocator overhead have separate accounting.

Codec IDs include their domain and decoding parameters. A view is reused only
with its original budget. Cold values and values from another budget decode
transiently without an extra wrapper allocation; persisted data contains no native views. After recovery, cold state
can decode repeatedly until execution replaces it with a newly produced value.
Budget exhaustion follows the same canonical in-engine computation.

Grouped sufficient statistics, prefix-window statistics and nullable grouping
keys use this facility. Integer vectors are cloned into fresh immutable storage
before charging, discarding capacity retained by earlier large arithmetic.
String-key charges include retained string and vector capacities. New codecs
must preserve canonical identity, use immutable payloads, and conservatively
charge every retained allocation they own.

## Delta flushing

Ordinary checkpoints consolidate memory deltas and upload them without reading
or rewriting retained object runs. Signed weights and duplicate object
memberships retain their full-tuple additive meaning. If a memory-only sum cannot
fit the persisted coefficient type, full-trace compaction handles cancellation
against object state before narrowing.

Uploads use byte-aware blocks and bounded root metadata. A nonzero delta triggers full compaction at eight existing
object runs or, for nonempty retained state, when its distinct entry count exceeds
half the retained physical row count. Counts are cached from resident fences at upload/open, including every
repeated membership; the decision needs no object reads or fence scans. Canceling
memory deltas simply disappear without reading or rewriting retained objects,
even at eight runs. The complete candidate is bounded by the 128-run limit. A wide delta may produce multiple roots. Consolidating the
memory delta requires additional memory proportional to retained memory input;
this is bounded by the worker memtable setting, not a fixed-memory streaming
merge. Periodic full compaction still runs in the checkpoint path.

Publication remains: protected immutable PUTs, PostgreSQL checkpoint transaction,
local installation, protection release, then acknowledgement of the durable
source position. Native caches and sizing summaries are disposable.

## Physical collection

A nonblocking schema-wide collector lock coalesces concurrent requests. A
read-only savepoint captures metadata under the lifecycle lock; rolling it back
releases that lock while keeping collector coordination until sweep completion.
The collector captures all committed and protected roots and the set of sealed
upload namespaces under the catalog lifecycle barrier. Marking, listing and
object deletion run after releasing that barrier. Retirement reacquires it and
only retires captured namespaces with a completed-writes proof.

This relies on existing publication validation: a new membership can retain an
exact current root or introduce a root from an active owned upload reservation.
It cannot resurrect a sealed orphan. Dependencies remain within the root's
namespace, and new uploads use fresh namespaces outside an older sweep's set.
Failed marking prevents deletion; nonquiescent fenced reservations remain in the
ledger so delayed PUTs stay collectible.

A worker owns at most one collection task and a dedicated SQL connection
configured at initialization. Waiting for the task is cancellation-safe, and an
attempt drains it before retry. Object-store GC work no longer holds the
publication barrier or runs inline with foreground checkpoint maintenance.
