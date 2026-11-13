# Bounded compressed range reuse

SF10 exposed 7,945 range GETs for 652 distinct ranges. The decoded working set
exceeded the 16 MiB decoded-cache budget, so statistics and full-tuple staging
validation repeatedly fetched small (~4 KB) compressed pages. The worst timed
epoch added 733 decoded misses and took 17.4 seconds, with only 8 ms in PostgreSQL
publication. Arithmetic remained incremental; physical access amplified requests.

The shared block cache now also retains compressed windows. A foreground page
miss fetches aligned windows of at most 256 KiB, clamped to the trusted pack size.
Neighboring pages and subsequent validation reuse those bytes. A page larger than
the compressed budget uses its original exact range. A disabled cache retains
nothing and also uses exact range reads. No object format or write path changes.

Compressed payload retention is bounded separately at one quarter of the decoded
byte budget and the same entry cap: a 16 MiB decoded cache adds at most 4 MiB of
retained compressed payload. Keys/entry metadata are bounded by entry count.
This is an explicit resource addition, not part of CacheStats.bytes (which still
reports decoded rows). Sixteen striped async locks coalesce matching requests;
different stripes can fetch concurrently. There is no cache-state lock across
I/O. Transient fetched windows and copies, caller block buffers, decoded cursor
pins and the existing optional Foyer tier remain separate resource allowances.
Stripes bound concurrent window fetches to sixteen, not all active caller buffers.

Scans and checkpoint writers share the window cache without admitting cold scan
bytes or promoting scan hits. Generated-output staging does not allocate another
compressed tier. The existing optional Foyer block cache remains usable beneath
the decoded tier; a Foyer hit avoids a range fetch entirely.

Window identity includes qualified pack path, content hash, trusted pack length
and aligned offset. Windows can contain not-yet-used bytes, so they are not a
semantic integrity boundary. Each selected page still passes its exact length,
SHA-256, decompression limit, canonical order, weights, fences and row-count checks
before exposure. A failed page checksum invalidates overlapping windows so a
repair/retry can refetch. Failed or canceled GETs release their flight lock and do
not admit partial windows. Valid immutable cached bytes may survive a later
external storage corruption; cold recovery still authenticates storage anew.

No membership, publication, source progress, LSN or acknowledgement behavior
changes. Full-tuple staging validation is retained. The optimization reduces
remote reads, not the cost of repeated decoding or validation; those remain
possible follow-up bottlenecks.

Fast regression uses production Statistics plus staging coefficient validation on
100,000 wide states and 513 distributed changed groups, with an independent exact
delta oracle. It compares the same decoded cache with compressed retention
disabled/enabled and requires >10x fewer GETs (observed 1,026 versus 3). No real
network delay is needed. Boundary, eviction, scan policy, disabled/oversized
requests, concurrent loads, cancellation, corruption and repaired-storage retries
are covered separately. Existing full application replay, native PostgreSQL and
crash/publication checks remain required.

## Validation and SF10 result

The common gate passed: 476 unit tests (three ignored), 147 core live worker
results plus rebuild, 97.37% production line coverage, strict Clippy and dependency
checks. Independent implementation review approved the bounded cache, integrity,
scan isolation and failure behavior before SF10 began. The release request-count
regression took 0.50 seconds including fixture construction; its measured
statistics/validation work took 42 ms without windows and 41 ms with them on the
zero-delay store. This proves request reduction, not local CPU improvement.
Independent application replay passed all 18 exported cases. Fast growth replay
passed at 10k/100k groups, with median measured preparation 0.96/1.56 ms and 4/11
GETs over 32 updates, zero measured PUTs. It does not model dense SF10 epochs.

The real SF10 rerun used 59,986,052 rows, a 10,371,596,288-byte heap and
12,265,627,648 bytes including indexes. Same query, 100k groups, decoded cache,
source-key hot set, varied transaction sizes and injected-latency profile as the
prior run. Both offered 30,000 changes / 300 transactions over 30 seconds.

| Metric | Prior SF10 | Compressed windows |
| --- | ---: | ---: |
| Changes completed in timed interval | 687 | 29,479 |
| Transactions completed in interval | 10 | 298 |
| Completed epochs in interval | 6 | 127 |
| Processing p50 / p99 | 564.5 / 17,388 ms | 110 / 498 ms |
| Processing maximum | 17,388 ms | 504 ms |
| Observed-to-sink p50 / p99 | 841 / 22,938 ms | 162 / 610 ms |
| CDC plus drain GETs | 6,765 | 81 |
| CDC plus drain response bytes | 33,483,922 | 5,975,695 |
| CDC plus drain PUTs | 16 | 24 |
| Whole-run GET / PUT / HEAD | 7,949 / 20 / 40 | 1,404 / 28 / 70 |
| Sampled peak process RSS | 437,264,384 B | 334,925,824 B |
| Sampled bootstrap peak RSS | 265,203,712 B | 264,908,800 B |
| Bootstrap elapsed | 2,871.60 s | 2,913.42 s |
| Sampled peak temporary scratch | 91,985,484,928 B | 92,394,896,638 B |
| Initial physical objects / bytes | 4 / 2,987,239 | 4 / 2,987,239 |
| Post-CDC physical objects / bytes | 20 / 3,483,725 | 28 / 4,090,085 |

All results match native PostgreSQL after bootstrap, CDC and cold restart. No
worker retries. The final 521 changes published 0.133 seconds after the timed
interval ended; total drain plus native full-table comparison was 6.678 seconds.
Do not mistake that full comparison time for engine backlog. All changes were
ultimately applied. Restart ready took 1.571 seconds with added delay disabled.

Percentiles above are per published epoch. Prior SF10 had just six completed
epochs and much unfinished work, so its percentile sample is censored; this is
not an equal-load steady-state percentile comparison. Adaptive epochs and
checkpoint opportunities change when the engine stops stalling. Delay is enabled
only for the 30-second interval, then disabled for drain/restart, identically in
both fixtures. Requests are counted by completion phase, so some drain work
started under the prior latency setting. The new result completes 98.3% of offered
changes in-window, not a long-term throughput or freshness guarantee.

GETs fall 98.8% and transferred bytes about 82% across the complete CDC plus drain
workload. PUT count is higher because the faster engine completes six background
checkpoints rather than four; the reader change itself adds no PUT path. Both
still write four objects per such checkpoint. Final physical state includes
retained suffix generations awaiting compaction/GC, so the differing inventories
are not equivalent generation snapshots or a storage-format expansion.
Whole-run counts include bootstrap/recovery verification. They are proxy attempts,
not cloud invoices. Peak RSS combines the resource monitor, bootstrap/traffic samples and post-CDC sample.
Lower RSS is observed whole-process behavior, not a promise
that adding a compressed tier always lowers memory. SF10 bootstrap overlapped the
common gate in the new run; its elapsed time is not an isolated comparison. The
spill/merge bootstrap bottleneck is unchanged.

Remaining work is clear: the worst new epoch spent 179 ms in statistics and
228 ms in staging, versus 77 ms in publication. Compressed reuse avoids remote
GETs but still permits repeated decoding/full-tuple validation. A bounded reuse
or producer-proof design can address that separately while keeping generic
weighted operators checked. When the compressed working set exceeds its own
budget, windows may be fetched again; this is not a claim that all larger-state
access costs are solved.

Evidence: `artifacts/benchmarks/sf10-range-read/` retains source snapshot, binary
hash, raw event/proxy/resource traces and `comparison.json`; prior SF10 evidence
is `artifacts/benchmarks/checkpoint-handoff-sf10/`. The measured binary hash is
`b2d7bb785e41db03e84cd575be55eba04778f242404e6ba1321f314272ddcd15`.
Fast evidence is under `artifacts/fast-validation/sf10-read-correctness/` and
`sf10-read-growth/`; gate evidence is `sf10-read-gate/` in the same parent.
Both owned benchmark containers and the gate container were removed. No Docker
volumes were created; PostgreSQL/object storage used tmpfs. Scratch was removed.
Datasets and all results remain ignored and retained.
