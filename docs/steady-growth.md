# Growing-state qualification

The target is stable cost for a fixed stream of changes as retained data grows.
State bytes can grow with the data. Processing cost, backlog, freshness and
memory must be assessed separately; a finite test qualifies a declared range,
not arbitrary unbounded datasets. Hold query shape, affected output fan-out,
operation mix and transaction sizes fixed: growing historical state alone must
not force work proportional to the complete dataset.

## Checkpoint work

The previous policy compacted the complete trace whenever eight physical roots
accumulated. A fixed number of tiny checkpoints therefore caused a rewrite
proportional to retained state. Root count also confused a multipack upload with
several independently accumulated deltas.

Trace manifest v2 stores coarse logical-run grouping as consecutive root counts.
Eight similarly sized logical runs can be merged without reading larger runs.
Resident row counts select tiers without storage requests. Restarts and rebasing
preserve grouping, full-tuple signed weights and duplicate root occurrences.
There is no old-manifest fallback: existing views require a rebuild on upgrade.

The size-tier policy is amortized. Promotions have larger occasional costs; the
128-physical-root safety cap still permits complete compaction. This does not
establish strictly constant costs at every size or remove that capacity boundary.

Run the fast production-path qualification with:

```sh
python3 scripts/fast_validation.py --mode storage-growth --release \
  --output artifacts/fast-validation/storage-growth-new
```

It runs missing-base-block, multipack restart and selected-subset overflow
regressions, then applies 256 single-identity growing inserts with one flush each
against 2k, 8k, 32k and 128k retained identities. Cache retention is disabled.
Only maintenance is timed; bootstrap, independent weighted-bag integration and
final cold restore are excluded. Every final bag and reopened state must match.
Object-store counters include maintenance scans, which foreground cache counters
intentionally exclude. Bounds cover reads, bytes downloaded/uploaded, PUT count
and merge fan-in. This is an unpaced component check, not a freshness benchmark.

The checkpoint scheduler counts unsealed memory runs for its soft upload trigger.
Durable roots do not represent pending upload work. Total run count, replay age
and memory capacity still enforce hard drain limits.

## Publication

Progress advancement and epoch-journal insertion now share one typed SQL
statement. Sink JSONB execution supplies its parameter type directly. This removes
redundant protocol round trips while retaining source layout checks, writer
fencing, affected-row checks and synchronous commit. The durable sequence remains
object PUT, atomic PostgreSQL publication, then source acknowledgement.

## Paced integration

The paced query is:

```sql
SELECT book_id, COUNT(*) AS positions, SUM(quantity) AS units
FROM public.positions GROUP BY book_id;
```

Reuse `scripts/performance/soak.py` for 1k changes/s with mixed updates, inserts
and deletes, half autocommit transactions and sizes through 1,000 rows. Its source
and aggregate cardinality grow. Record processing and commit-to-sink distributions,
backlog, CPU, RSS, physical object size, request counts and WAL retention. Native
PostgreSQL comparisons and two witnessed crash/replay checks are mandatory.

This short qualification is a regression check. A longer constant-input run and
larger cold-state ranges are still necessary before claiming a long-term plateau.

## Results, 2026-10-24

The baseline is `ffaca16f129b0fefc369c5fea9d15d52ac2eb211`. Both versions
ran the identical storage harness. Each row measures 256 tiny checkpoints:

| Retained identities | Bytes read, before → after | Bytes uploaded, before → after | Checkpoint p99 ms, before → after |
| ---: | ---: | ---: | ---: |
| 2,000 | 145,382 → 7,617 | 250,478 → 104,743 | 0.692 → 0.071 |
| 8,000 | 441,030 → 7,617 | 575,088 → 104,743 | 2.057 → 0.053 |
| 32,000 | 1,692,996 → 8,076 | 1,970,432 → 106,279 | 7.365 → 0.058 |
| 128,000 | 6,556,367 → 8,584 | 7,426,101 → 107,842 | 30.741 → 0.068 |

PUT count changes from 512 to 518, independent of starting size. The fast
qualification completes in about 0.8 seconds after compilation. Absolute timings
are local observations; request/byte work bounds are the regression gate.

The 180-second paced crash/replay comparison sustained 1,000 changes/s and grew
source rows from 12,001 to 71,949 (59,950 final aggregate groups). Both versions
passed native equality, both witnessed crash boundaries and acknowledgement
ordering; the final replay journal was empty. Checkpoints stayed at 32, PUTs
changed 135 → 136 and estimated Class A attempts changed 167 → 168. Uploaded
request bodies fell 1,762,902 → 1,015,074 bytes. An intermediate scheduling
feedback bug produced 141 checkpoints/575 PUTs; its failed cost evidence is
retained, and the corrected memory-only trigger removes that feedback.

The tradeoff is metadata fan-in: GETs changed 334 → 613 and HEADs 272 → 518;
returned bytes changed 6,661,777 → 6,391,040. Counts cover bootstrap, recovery,
drain and GC too, not only foreground processing. Class estimates are not invoices.
Processing p50 stayed 3 ms; p95/p99 changed 9/13 → 11/15 ms. Checkpoint p99
fell 424 → 278 ms. These short local runs do not establish a remote freshness SLO.

An additional uninterrupted paired run applied exactly 180,000 changes to each
version at 1,000/s. After the first 60 seconds, every 20-second processing window had
a 3 ms median for both versions. Candidate commit-to-observed-sink p50/p95/p99
were 11.70/26.06/29.43 ms, including the polling observer. Native results and
acknowledgement ordering matched. CPU consumption was 7.38 → 7.24 seconds.

Memory has **not** demonstrated a plateau. In that uninterrupted run, candidate
RSS grew 37 → 186 MiB, versus 35 → 194 MiB before. Decoded cache charge settled
around 7 MB/256 entries and final charged memtable usage was about 0.5 MB.
A four-thread control also showed rising RSS, so a large default thread pool
does not fully explain it. Heap/allocator and retained-object profiling is the
next investigation; more wall-clock soaking alone is not a diagnosis.

Next storage work should reduce retained-root metadata GET/HEAD overhead and
address the physical-root capacity boundary. Latency is stable in the measured
range; whole-process memory stability and larger-state behavior remain open.

Aggregates are in [the recorded samples](steady-growth-samples.json). Detailed
traces, failed attempts and cleanup records remain under ignored
`artifacts/benchmarks/steady-growth/` and `artifacts/fast-validation/steady-storage-accepted/`.
The live demo was not changed by this work.
