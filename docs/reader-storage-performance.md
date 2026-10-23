# Immutable reader and storage performance

The selected production change shares checksum-verified canonical decoded blocks
across readers, retains them by decoded charge, borrows uncontended block prefixes
through the real merge/streaming compaction path, and batches GC deletion through
the actual object-store adapter. It retains canonical JSON rows and v1/v2 durable
objects. Ordinary restart reads existing committed objects without migration;
no published object is rewritten in place. New cache entries bind row-codec
revision, schema, Rust key/value types and content hash. Every caller still
checks its own row count and full-identity fences. Cold manifest recovery bypasses
shared retention while checking every committed block and consolidated weight.

## Controlled experiment

The retired format experiment used independent temporary snapshots and release
executables, with seven shuffled samples per case (seed 712). Its executable
harness and candidate codecs have been removed. The measurements below remain
historical evidence; production uses the bounded canonical JSON+Zstd codec.

The actual reader/execution fixture uses `object_store`'s `InMemory` adapter,
4,096 canonical `(i64, String)` rows, 128-row blocks, a shared 8 MiB/128-entry
cache, four values per navigation key, and a repeated 56-byte payload within each
string. Point seeks number 2,000; bounded prefixes number 200 with 20 rows each;
full scans and two-input merges number 20; streaming rewrites, full compactions
and cold recoveries number 10. The second run overlaps the first half of the
first run and places its remaining rows beyond the first run, exercising both
full-identity consolidation and uncontended borrowed spans. Compaction and
recovery compare complete materializations with the preceding pinned state.
These are synchronous weighted-engine workloads, not the live PostgreSQL
qualification or a universal capacity claim.

The format experiment compares current JSON with compact typed bincode 1.3
varint rows, typed columns `(keys, values, weights)`, and zstd 0.13 level 3 for
each. Candidates replace actual block writer/reader serialization while keeping
resident JSON fences, checksum verification, cache, seeks, merged execution,
compaction and recovery. The temporary envelope explicitly selects row/column
and compression flags. Its three-byte header and owned-row construction add
small overhead to the experiment JSON control. It is not a production format
version or a supported migration path. Generic custom serde types are not
qualified for bincode by this fixture.

The adapter has no network latency, cloud request charge, external bandwidth,
service throttling or persistent cold page cache. Cold recovery means reopening
all manifest state with decoded retention disabled and re-materializing it;
it does not claim a cold operating-system cache. Timings combine verified
serialization, allocation, hashing, cursor work and adapter execution. The phase timers do not separately attribute CPU or allocations. An aggregate
CPU/allocator supplement appears below; no RSS bound is inferred. Object bytes/count describe the
final deduplicated fixture, not total PUT traffic. Candidate packing is held
fixed; compressed and columnar candidates do not reduce request count.

## Accounting and selected behavior

Decoded cache `bytes` is vector capacity times entry size plus encoded payload
length as a heap estimate. `live_bytes` counts each decoded allocation once,
including retained entries and evicted/oversized allocations still pinned by
readers. `pins` counts reader strong references excluding cache ownership; clones
share the same allocation. These counters overlap and must not be added together.
Dead weak tracking records are pruned on insert/stats; active-pin metadata,
resident indexes, allocator overhead and arbitrary custom serde allocations lie
outside the retention budget. An encoded-size estimate cannot establish an RSS
bound. Streaming writer buffers and output limits remain separate budgets.

Cache hits avoid repeated JSON decode, row-order validation and checksum hashing
of immutable resident data, while rechecking object-specific fences/counts.
Uncontended merge spans stop strictly before every other run's current full
identity. Overlap falls back to exact scalar BigInt consolidation. Advancing a
span stages cloned cursor positions and the next consolidated head; a failed
GET, checksum, fence or coefficient check leaves the prior position retryable.
Tests include restricted keys spanning blocks, sharing/pin eviction, type/schema
isolation, cached fence mismatch, simultaneous cancellation and real compaction,
and a failed merged-span load followed by retry.

Typed binary cannot silently replace the existing generic `BatchData` contract:
an ordered serde untagged domain value roundtrips through JSON but the measured
bincode candidate returns `DeserializeAnyNotSupported`. The retired
experiment included that contract regression. A production typed codec would need
an explicit supported-type boundary, durable codec/version identity, bounded
compression decoding and separate native-type qualification. It would replace
the current format explicitly, without compatibility decoders.

The memory-batch cursor still constructs owned row storage from its tree; this
change does not remove that initial clone. Streaming output still owns each row
until flush, and caches do not eliminate its serialization, hashing or PUT cost.
Compression's wire-byte savings may matter with external bandwidth constraints;
no cloud-latency or compression break-even measurement is implied here.

## Release measurements (2026-10-13)

Seven shuffled samples per case; elapsed microseconds for each complete workload.
The after case includes verified decoded sharing, borrowed-minimum merge and
production uncontended spans. Values are median [minimum–maximum].

| Workload | Baseline d7368d1 | Selected reader/execution |
|---|---:|---:|
| Initial write | 846 [830–922] | 898 [844–954] |
| 2,000 point seeks | 65,155 [62,140–69,506] | 1,123 [1,023–1,210] |
| 200 bounded prefixes | 3,028 [2,980–3,548] | 42 [41–54] |
| 20 full scans | 10,294 [10,039–10,904] | 321 [308–370] |
| 20 two-run merges | 54,611 [54,053–56,272] | 19,559 [19,207–20,062] |
| 10 streaming rewrites | 18,008 [17,198–19,444] | 14,169 [13,469–15,227] |
| 10 full compactions | 130,025 [124,275–134,764] | 57,777 [54,138–60,302] |
| 10 cold recoveries | 72,421 [69,286–76,674] | 46,934 [44,772–51,254] |

Production durable bytes are unchanged: 1,051,332 bytes in 69 final deduplicated
objects for both cases. The full samples are preserved in
[reader/storage samples](reader-storage-performance-samples.json).

Candidate-control medians below use the same decoded cache and engine. Stored
bytes include resident JSON manifests and the final fixture objects.

| Codec | Write | Point seeks | Prefixes | Scans | Merges | Rewrites | Compactions | Recovery | Stored bytes |
|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| json | 822 | 1,067 | 37 | 312 | 19,851 | 12,436 | 55,259 | 48,856 | 1,051,527 |
| binary | 632 | 876 | 39 | 318 | 20,481 | 13,079 | 53,004 | 38,904 | 949,169 |
| column | 650 | 899 | 36 | 329 | 20,197 | 13,019 | 56,794 | 41,019 | 949,303 |
| json-zstd | 1,622 | 1,086 | 45 | 309 | 19,497 | 12,265 | 59,409 | 47,731 | 68,883 |
| binary-zstd | 1,540 | 898 | 45 | 324 | 19,798 | 10,563 | 57,693 | 39,745 | 70,096 |
| column-zstd | 1,497 | 831 | 37 | 327 | 19,047 | 10,567 | 57,466 | 41,777 | 68,722 |

All candidates retained 69 final objects. Typed rows cut cold recovery from
48,856 to 38,904 microseconds and initial writing from 822 to 632, but increase
stream rewrites from 12,436 to 13,079 and merged scans from 19,851 to 20,481.
Columns do not show a consistent advantage over rows. JSON+zstd cuts final
storage from 1,051,527 to 68,883 bytes, while initial writing grows from 822
to 1,622 microseconds and compaction from 55,259 to 59,409. Compression therefore
has clear byte savings but no demonstrated whole-workload CPU/request win in
this adapter. Typed columns+zstd improve streaming rewrites but cost more initial
writing and do not improve cold recovery over uncompressed typed rows.

Retaining JSON is deliberate: the selected cache/merge changes improve the
actual generic execution paths without changing durable codecs, while the
typed candidate cannot satisfy the existing generic serde contract and neither
columnar layout nor compression demonstrates a consistent whole-workload win.
The storage-byte benefit remains relevant for a future bandwidth-constrained
adapter qualification. No cloud throughput or process-RSS bound follows from these measurements.


## Separate CPU and allocator profiles

Whole-fixture CPU was measured with GNU `/usr/bin/time -v` in 30 shuffled
uninstrumented release process executions (15 per side, seed 713). This includes
fixture construction, reader/writer work, merge, compaction, recovery and oracle
checks, rather than attributing CPU to individual phase timers. Median user CPU
was 0.38 seconds [0.36–0.42] before and 0.16 [0.15–0.19] after. Aggregate user plus
system CPU across the 15 runs was 5.81 versus 2.43 seconds. GNU time rounds CPU
to hundredths of a second. Observed maximum-RSS medians were 17,100 versus
17,348 KiB, with ranges 16,128–17,664 and 16,128–18,176 respectively; this does
not demonstrate a memory reduction or establish an RSS bound.

`perf stat` was denied by the host's `perf_event_paranoid=4` policy. Its failure
was retained without changing system permissions. Heaptrack and Valgrind were
unavailable on PATH. Actual aggregate Rust allocator counters were therefore
collected using pinned `dhat=0.3.3` in independent temporary source snapshots and
release targets. A safe global declaration uses the library's allocator; no
unsafe allocator implementation or production dependency was introduced.

The profiler starts at benchmark entry, after Tokio runtime construction, and
samples at the end with the fixture store/batch/cache still alive. Three runs
per side (shuffled with seed 714, one test thread) produced identical counters:

| Allocator measure | Baseline | Selected |
|---|---:|---:|
| Allocation/reallocation requests | 8,740,466 | 4,815,527 |
| Requested bytes, including reallocations | 1,469,303,981 | 521,763,373 |
| Maximum tracked live bytes | 4,628,611 | 4,858,310 |
| Blocks at tracked byte peak | 24,128 | 32,449 |
| Tracked live bytes at final sample | 2,244,236 | 2,473,988 |

Allocation requests decrease 44.9% and requested bytes 64.5%; tracked peak live
bytes increase 5.0%. This is lower allocation churn with higher retention, not
a retained-memory reduction. Reallocation requests count new requested sizes,
even when resize may happen in place, so these bytes are not memory-copy traffic.
Profiler-internal allocations, allocator bookkeeping, external allocations and
allocations preceding the profiler are excluded. Instrumented timings are not
production performance timings. The existing clone-count test measures only
minimum-head selection; aggregate allocation counters do not attribute work to
cloning versus decoding, hashing, serialization or operators.

These supplemental measurements came from the retired experiment's temporary
snapshots, GNU time and a dhat allocator confined to the experiment. Raw compact
measurements, summaries, tool availability and executable checksums are preserved
in [CPU/allocator samples](reader-storage-profile-samples.json).

## Shared immutable memory cursors

Finalized memory batches store their canonical full-tuple weighted rows in a
shared immutable array. Consolidation still sums exact intermediate coefficients,
rejects final overflow and removes zero weights before publishing the array.
Opening or cloning a memory cursor pins the same rows without copying or
revalidating the run. Cursor positions and key bounds remain independent, and
key/range navigation uses binary search. Existing cursors continue reading their
original snapshot when a batch is replaced or its owner is dropped.

Applying a delta builds a fresh sorted array by merging the two canonical inputs.
Overflow leaves the old array and every reader unchanged. Full-tuple identity,
signed multiplicities, object formats, checkpoint membership, source progress and
acknowledgement rules are unchanged. The existing conservative memory accounting
remains in force; shared rows are not charged less merely because cursors share them.

Regression tests count tuple clones and comparisons across increasing run sizes:
opening a cursor performs neither, and bounded seeks do not scan the whole run.
They also check independent positions, pinned lifetimes, signed weights, atomic
failed integration and matching memory/object cursor behavior.

## Incremental resident-memory accounting

Trace snapshots retain a checked total for the serialized payload and structural
charge of memory runs. Appending a run serializes only that new run; unchanged
rows are not revisited during a memtable limit check or metrics reporting.
Cloned snapshots keep their original total. Physical run replacement recomputes
the total for the replacement membership; object-only cold recovery starts at zero
because immutable block caches have separate accounting and limits.

The charge formula and configured cap are unchanged. Serialization or arithmetic
failure rejects the unpublished candidate without changing its predecessor.
Only new-row sizing moves into arrangement staging; the final accounting phase
now adds cached arrangement totals. Checks for full-tuple identity, multiplicity,
logical equivalence during compaction and durable publication remain independent.

## Batched affected-tuple and source-layout probes

Full-tuple delta validation visits each immutable run once for the affected batch.
Object probes retain the selected verified block while checking its affected
identities, then release it on a block change. Exact coefficients are summed
across all runs before narrowing to i64, followed by the unchanged delta overflow
and bag nonnegativity checks. Aggregate group probes reuse one run head per run
across keys; current and returned cursors can pin additional blocks. Returned
cursors remain independent and failed reads leave the
previous probe boundary intact. Neither path scans unrelated groups or treats a
navigation key as tuple identity.

Source publication still acquires ACCESS SHARE locks on every bound table and
holds them through sink/progress commit. One typed catalog query then reads
cluster/timeline/database identity, publication flags and membership, table
properties, and all column contracts from the same statement snapshot. All
previous source restrictions remain; semantic catalog verification remains an
initialization operation. Immutable object publication, WAL replay boundaries,
synchronous PostgreSQL commit and checkpoint-only acknowledgement are unchanged.

## Ordered aggregation and resident finalization

Linear statistics consume sorted input groups directly when the bounded
accumulator vector fits the resident byte and entry budgets. One accumulator is
reused across groups; no intermediate per-column tree or grouped map is built.
Contribution, record and finalized intermediate-output budgets still apply, and
coefficients remain arbitrary precision until the existing SQL finalizer narrows
them. The spillable implementation remains available for smaller resident budgets.

A consolidator that has never spilled finalizes its canonical resident entries
without a scratch write/read cycle. Zero coefficients are removed and the same
final record, entry, byte and i64 checks apply. Once spilling starts, the existing
checksummed scratch merge and failure retention remain in use. Scratch byte
limits now apply only when scratch is actually written.

Sorted full-tuple memory probes advance from their previous lower bound with
exponential skipping. Reusable, unconsumed key-probe heads similarly retain their
positions; public cursors retain their arbitrary reseek semantics. Full-tuple
identity, backward probes and read failure atomicity remain intact.

Statistics inputs can carry an internal successful bag-validation proof through
one evaluation. It owns the exact immutable batch and borrows its immutable trace;
staging checks both identities before reusing the stronger bag check in place of
an additional signed-coefficient lookup. Other operators retain their checks.
Proofs cannot cross evaluation lifetimes or authorize a different batch/root.
Object publication, atomic PostgreSQL sink/progress commit, source replay and
checkpoint-only WAL acknowledgement are unchanged.
