# Statistics lookup page geometry

A statistics point lookup previously decoded and validated a complete 64 KiB
canonical JSON page. Compressed-window reuse avoided repeated network requests,
and the producer handoff removed the second statistics validation traversal, but
the first traversal still decoded many unrelated rows.

The shared streaming writer now targets 16 KiB independently compressed pages,
still packed into the same bounded 4 MiB objects. Fixed-batch output, bootstrap,
flush and compaction use that writer. The soft target preserves the existing
single-large-row exception and hard 8 MiB decoded limit. Each selected page still
passes its complete checksum, canonical ordering, nonzero weights, row-count and
fence checks. No selective deserialization bypass or new storage format is used.
Existing larger pages remain readable; new geometry requires rewriting state to
benefit. No migration adapter or second execution path is introduced.

Decoded-cache bytes/entries, compressed-window budgets, index-byte/block/root
limits and publication ordering remain unchanged. Direct Arrangement staging now
uses the existing streaming upload/append path, removing its single-root helper.
More fences can therefore split large writes under existing limits rather than
silently dropping membership or raising those limits. Tests that intentionally
need several roots use the same bounded writer. The packing split fixture uses
eight pages per root instead of three to keep its many forced splits inside the
unchanged 128-root limit.

Additional cold decoded-byte and decoded-row counters measure successful page
payload deserialization, including unrelated rows. Cache hits and locally
generated output do not count as decoding. Scans account their own work rather
than mutating foreground counters. Worker health and fast replay expose them;
fast report validation rejects missing/negative fields. Miss counts alone cannot
show improvement when more, smaller pages are fetched.

## Fast trials

A production statistics/staging fixture contains 100,000 wide states and changes
513 distributed groups. At 64 KiB it decoded 33,420,081 bytes / 42,579 rows; at
32 KiB, 16,508,989 / 21,033; at 16 KiB, 8,053,423 / 10,260. Cold resident-input
lookup time was 17.91 / 10.00 / 7.18 ms in the recorded release trials. Spilled
input includes scratch I/O and is reported separately in raw logs; its variation
is not attributed solely to page decoding. Staging still performs zero repeated
statistics decodes. The regression now requires no more than 12 MiB / 16,000 rows
for the 513-group lookup, with an independent exact-delta oracle.

The wide fixture's physical state grows from 2,655,202 bytes / two objects at
64 KiB to 9,384,300 bytes / four at 16 KiB. Full-tuple fences duplicate wide values,
and the smaller pages reach existing per-root index limits. That extra PUT cost is explicit;
this policy does not guarantee unchanged writes for every state shape. The old
single-root fixture failure was retained before switching to bounded multi-root
output. At 32 KiB the same fixture used two objects / 4,862,125 bytes.

A second replay uses the existing fast engine/oracle transport with 100,000 SQL
COUNT and SUM(amount::numeric) groups, cold restore, and eight 512-group update
batches. The generated source rows come from the existing preloaded workload;
expected results are independently computed integer counts/sums. It is a compact
numeric-state workload, not the full 60-million-row SF10 bootstrap.

| Page target | Median prepare+commit | Decoded bytes | Decoded rows | Measured GETs | Total PUTs | Stored bytes |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| 64 KiB | 245.75 ms | 393,872,358 | 2,380,998 | 6 | 6 | 1,760,410 |
| 32 KiB | 220.87 ms | 304,616,736 | 2,007,661 | 6 | 6 | 2,379,487 |
| 16 KiB | 147.10 ms | 184,437,953 | 1,417,859 | 8 | 6 | 3,532,886 |

All replay oracle/snapshot checks passed. The eight-batch medians are local
component observations without injected network delay; native/generic input
validation and other arrangements also contribute. PUT totals include initial
state/checkpoint construction; measured updates use the memtable and no PUTs.
The 16 KiB choice gives about 40% lower median and 53% fewer decoded bytes in this
numeric workload, spending more index/compressed storage and two extra cold GETs.

A smaller page does not remove all physical access costs. The entry cap can limit
retained decoded bytes; at the time of this measurement, larger indexes consumed
resident metadata; compressed
frames can grow and exceed the compressed-cache working set. Cold recovery and
uncached scans can issue more page reads. Those costs must be reported separately
from timed CDC. The current [paged-index design](steady-growth.md#continuous-operation-qualification)
bounds navigation retention separately. Bootstrap spill/merge work is unchanged. Evidence, trial binaries,
raw logs and the numeric suite generator are retained ignored under
`artifacts/benchmarks/statistics-page-decode/`.

## SF10 verification

The final binary (`aea05333999f4adcc5d2f9459dfe8f32040addf059abbdb4956abc4c0a03e7f8`)
was compared with `0a00be7`, using the same 59,986,052-row source, 100,000 supplier
groups, cache budgets, query, deterministic transaction-size mix and seeded
object-storage latency proxy. PostgreSQL heap was 10,371,596,288 bytes and total
relation size including indexes was 12,265,627,648 bytes. Bootstrap and cold
verification use zero injected delay; only the 30-second CDC interval uses the
proxy's full latency distribution. Both runs completed all 30,000 changes in
300 transactions inside the offered interval, with no CDC publications left for
drain. These are single short runs, not a steady-state latency guarantee.

| Measurement | 64 KiB baseline | 16 KiB |
| --- | ---: | ---: |
| Published CDC epochs | 169 | 152 |
| Statistics operator total | 7,979.901 ms | 5,087.929 ms |
| Arrangement staging total | 4,716.491 ms | 3,200.255 ms |
| Preparation total | 13,133.165 ms | 8,743.655 ms |
| Preparation p50 / p95 / p99 | 37.72 / 247.95 / 285.79 ms | 40.28 / 180.75 / 218.86 ms |
| Processing p50 / p95 / p99 | 48 / 262 / 308 ms | 73.5 / 245 / 273 ms |
| Observed-to-sink p50 / p95 / p99 | 67 / 322 / 426 ms | 106 / 280 / 604 ms |
| Publication total | 1,634.452 ms | 4,610.306 ms |
| CDC + drain GET / PUT / HEAD | 81 / 24 / 54 | 82 / 24 / 54 |
| Cold restart GET / HEAD | 800 / 14 | 3,140 / 14 |
| Initial stored bytes / objects | 2,987,239 / 4 | 4,142,528 / 4 |
| Stored bytes / objects after CDC | 4,129,542 / 28 | 5,800,476 / 28 |
| Whole-run sampled peak RSS | 376,975,360 B | 341,413,888 B |
| Sampled peak scratch | 91,868,462,334 B | 91,871,166,235 B |
| Bootstrap | 2,867.74 s | 2,792.58 s |

The component improvement survives the real workload: statistics time decreases
36% and total preparation time decreases 33% for the same source changes. Adaptive
epoch grouping differs between runs, so totals per input and tail percentiles
are reported alongside medians. The larger publication times in this run prevent
an overall median or freshness improvement claim; the benchmark does not isolate
the cause of that publication variation. Faster decoding alone is insufficient
to guarantee lower observed-to-sink latency.

Smaller pages also expose a cold-scan request cost: scan readers deliberately do
not admit compressed windows and therefore fetch each page separately. Cold
restart GETs almost quadruple even though timed CDC requests and PUTs barely
change. Decoded-cache entry limits now bind before its byte limit (about 8 MiB
resident versus about 16 MiB previously). Bounded scan-local range coalescing
and cache geometry are follow-up bottlenecks; this change does not silently raise
budgets or alter scan admission. More compressed/index bytes are an explicit
tradeoff, not an object-request optimization.

Native PostgreSQL results matched after initialization, all updates and cold
restart. Six maintenance checkpoints completed and retries were zero. The common
`./scripts/check.sh` gate passed, including 481 unit tests (three ignored), the
sequential core live-worker/rebuild checks, strict lint/dependency checks and
97.39% production line coverage. Independent review approved implementation and
fast evidence. The Docker fixtures and scratch were removed, no Docker volumes
were created, and both datasets and raw evidence were retained under
`artifacts/benchmarks/statistics-page-decode/`. Final cleanup verification records
actual absence separately from the runner's earlier cleanup record.
