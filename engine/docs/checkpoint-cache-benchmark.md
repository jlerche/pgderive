# SF1 checkpoint-page cache handoff validation

The generated-output handoff removes the checkpoint cold-page stalls observed in
99d5a77. Independent implementation review found no actionable correctness or
durability issues. The common `./scripts/check.sh` gate passed, including live
PostgreSQL recovery/publication checks and 97.41% production line coverage.

## Workload and comparison

Both runs use the retained TPC-H SF1 lineitem dataset: 6,001,215 rows,
955,973,632 PostgreSQL heap bytes and 1,145,487,360 bytes including indexes.
The view groups by supplier and maintains COUNT and numeric SUM across 10,000
groups. CDC offers 30,000 changes in 300 transactions over 30 seconds, about
1,000 rows/s, with transaction sizes 1–1,000 and a 4,096-key hot set.
The same seeded object-storage latency proxy is enabled for timed CDC in both
runs. Bootstrap, drain and restart use zero added latency. Cache is 16 MiB/256
entries; checkpoints run every five seconds. Timed CDC waits for the common gate
to finish; bootstrap overlaps validation, so its timing is not isolated.

| Metric | Numeric SUM baseline | Cache handoff |
| --- | ---: | ---: |
| Processing p50 | 9 ms | 8 ms |
| Processing p95 | 91 ms | 48 ms |
| Processing p99 | 1,276 ms | 63 ms |
| Processing maximum | 1,456 ms | 80 ms |
| Observed-to-sink p50 | 14.5 ms | 13 ms |
| Observed-to-sink p95 | 1,144 ms | 51 ms |
| Observed-to-sink p99 | 1,480 ms | 65 ms |
| Observed-to-sink maximum | 1,521 ms | 83 ms |
| CDC completed changes / transactions | 30,000 / 300 | 30,000 / 300 |
| Published CDC epochs | 168 | 221 |
| CDC GET / PUT / HEAD / POST attempts | 280 / 20 / 10 / 5 | 15 / 20 / 10 / 5 |
| Whole-run GET / PUT attempts | 393 / 28 | 128 / 28 |
| Sampled peak process RSS | 201,355,264 B | 241,197,056 B |
| Sampled bootstrap peak RSS | 60,428,288 B | 60,538,880 B |
| Sampled peak scratch | 8,654,167,695 B | 8,544,279,583 B |
| Bootstrap elapsed | 241.148 s | 237.738 s |

These are per-published-epoch distributions, not per-row percentiles. Adaptive
epoch sizes differ because removing stalls changes scheduling. Observed-to-sink
starts when the engine receives the source transaction; it excludes earlier WAL
or source backlog. GET counts include maintenance metadata/listing requests;
they are proxy attempts, not a cloud billing invoice. No PUT increase occurred.

Native PostgreSQL comparison passed after bootstrap, CDC and cold restart in both
runs. Physical object inventory was unchanged: four objects / 298,001 bytes after
bootstrap, four / 298,055 bytes after CDC. Drain plus native comparison took
0.331 seconds versus 0.325 seconds. All CDC completed during the timed interval.

## Interpretation and limits

The prior trace linked a 1.456-second epoch to 51 cold GETs after checkpoint
maintenance. Processing p99 improves about 95% in this repeat, and CDC GETs fall
about 95%; the new maximum is 80 ms. The mechanism is covered separately by a
changed-content-hash regression requiring zero post-compaction GETs.

The additional private workspace is bounded by one decoded cache budget per
in-flight maintenance operation, shared across sibling uploads. This is an
explicit memory tradeoff. Whole-process sampled peak RSS increased about 40 MB;
RSS includes allocator retention, native views and other allocations, so these
runs do not establish a cache-only RSS delta. Oversized generated output abandons
the workspace and preserves foreground cache contents; it can still incur cold
reads. This 30-second SF1 test does not establish long-term behavior or SF10
performance.

Object PUT completion, PostgreSQL atomic membership/results/progress publication
and subsequent replication acknowledgement retain their existing ordering.
No object format or persistent catalog change is introduced.

Evidence remains ignored under `artifacts/benchmarks/checkpoint-handoff/`:
`comparison.json`, `sf1/result.json`, event/proxy/resource histories, exact source
patch, new-source archive and binary hash. Baseline evidence is under
`artifacts/benchmarks/numeric-sum/`. The measured handoff binary SHA-256 is
`f0dd2abfe2dd9c45be65de611ab6c2c78f9ac478e949f637b397ee65ea053b5f`.
The common gate evidence is in
`artifacts/fast-validation/checkpoint-handoff-gate/`.

Both owned Docker fixtures and the gate fixture were removed. Their database and
object storage used tmpfs, with no persistent Docker volumes. Scratch was removed;
the datasets and results were retained.
