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
PostgreSQL comparisons and acknowledgement checks are mandatory. Select
`--qualification growth` for an uninterrupted worker and `recovery` for the
separate two witnessed crash/replay checks. Restarted workers cannot qualify RSS
stability. The query uses bigint quantities, including signed non-NULL values,
and updates retained historical groups as new groups appear.

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


## Retained-run probe overhead

A later live observation showed processing p50 rising from 64 to 79 to 104 ms
while achieved input fell from 831 to 778 to 655 changes/s. Preparation p50
rose from 20 to 28 to 44 ms; publication stayed around 31–35 ms. This identifies
preparation for investigation, not proof that growing table cardinality caused it.

A controlled production `Statistics::evaluate` workload keeps 256 groups and
256 contributions fixed while increasing retained update runs. Each run retracts
the preceding state and adds its replacement, so probes must cancel complete
weighted identities to find the one current statistic. The old path rebuilt its
reader deduplication map for every key and cursor clones copied immutable object
reference strings. Cursors now share their immutable reader descriptor, and probe
construction builds the reader/occurrence mapping once per snapshot. Separate
cursor positions, repeated run multiplicity, store/cache isolation, failed-read
retry and cancellation boundaries remain unchanged. No state-format or durable
publication change is required.

Three alternating baseline/candidate repetitions, excluding the first timing in
each case, gave these local medians:

| Retained runs | Before ms | After ms |
| ---: | ---: | ---: |
| 1 | 0.327 | 0.235 |
| 8 | 1.578 | 0.807 |
| 32 | 8.028 | 3.389 |
| 64 | 18.774 | 8.906 |

Run the component measurement with:

```sh
cargo test --release --lib engine::trace::growth_tests::retained_run_statistics_churn -- --exact --ignored --nocapture
```

Every evaluation must match the exact expected weighted replacement and issue
zero GETs/PUTs. This measures cached probe CPU/ownership cost, not remote freshness
or a plateau as the whole dataset grows. Remaining cost still depends on retained
run fan-in. An experimental tournament merger was slower and was removed rather
than retained as another path. Detailed paired samples, the rejected experiment
and live evidence are retained under ignored
`artifacts/benchmarks/live-latency-debug/`. The candidate requires a separate live
deployment comparison before claiming any live latency improvement.


## Uninterrupted local qualification, 2026-10-24

The stronger growth qualification **fails** on Rust commit `4a20a2f`.
The worker applied 599,614 changes in 5,908 transactions over ten minutes at
approximately 1,000 changes/s. Source rows grew from 12,001 to 211,871 and output
groups to 199,872. The worker never restarted. PostgreSQL CDC, numeric SUM,
keyed sink publication and the existing latency-injected object store were real;
this is wall-clock integration, separate from accelerated component replay.
The workload mixes inserts, deletes, signed/NULL quantities and historical updates,
with half the transaction schedule using single-statement autocommits and sizes
through 1,000 statements. Query shape, rate and cache limits remain fixed. This local profile uses the
example configuration: a 16 MiB decoded-cache charge budget with a 256-entry cap,
a compressed-window budget derived from it, and no optional Foyer encoded cache.
It is a cold-working-set qualification, not an identical live deployment profile.

After 120 seconds of warmup, the declared early interval is 120–270 seconds and
the late interval is 450–600 seconds. Source rows more than double between their
last samples (97,403 → 207,979). Metrics use raw publications and observed marker
commits, rather than quantiles of sampled quantiles:

| Metric | Early | Late | Growth gate |
| --- | ---: | ---: | --- |
| Accepted changes/s | 1,001.3 | 999.0 | Pass |
| Processing p50 / p95 ms | 7 / 118 | 8 / 150 | Pass |
| Commit-to-observed-sink p50 / p95 ms | 25.8 / 142.0 | 27.8 / 183.1 | Pass |
| CPU ms/change | 0.193 | 0.244 | **Fail** |
| Preparation µs/change, including object I/O | 171.7 | 223.9 | **Fail** |
| Publication µs/change | 21.4 | 21.8 | Pass |
| Peak RSS MiB | 224.8 | 230.5 | Pass |
| Maximum changes after latest visible marker | 1,001 | 1,001 | Pass |
| Estimated Class A requests/million changes | 905 | 934 | Pass |
| Estimated Class B requests/million changes | 6,838 | 7,601 | Pass |
| Uploaded bytes/change | 21.3 | 20.3 | Pass |
| Downloaded bytes/change | 111.5 | 226.0 | **Fail** |

The run passed native SQL equality and final WAL-fence drain before assessment;
final durable and published logical times both reached 4,544 with no replay journal
rows. ACK bounds held. No transport errors or unknown request sizes occurred.
Final physical objects occupied 4,809,647 bytes. Owned PostgreSQL and object-store
containers were removed, no Docker volumes were created, and failure evidence was
retained under ignored `artifacts/benchmarks/steady-local/non-null-growth/`.
A stopped NULL-only preflight is retained separately and is not qualification.

The passing endpoint latency check is insufficient evidence of a plateau. The
15-second processing medians rose as high as 17 ms between larger merges, then
fell to 6–8 ms after a merge. CPU per change rose 27% and preparation rose 30%.
Range-response bytes account for most of the download increase: 569 responses
returned 16.30 MB early, versus 619 returning 33.40 MB late. Root GET response
bytes stayed near 0.42–0.44 MB. This narrows the next investigation to page/range
read amplification and retained-run processing; it does not by itself separate
foreground probes from overlapping background compaction reads.

The qualifier now rejects missing phase data and has independent synthetic
regressions for latency, CPU, RSS, backlog, rate, cardinality, restarts, ACK bounds,
checkpoint coverage and request accounting. The original raw run had all required
phase fields; reanalysis with that stricter missing-data check gives the same
three failed bounds. No threshold was relaxed after observing the failure.

These results cover COUNT and numeric SUM over growing groups and historical
updates. They do not cover joins, ordered aggregates, growing membership inside
a hot group, future compaction tiers or the 128-root capacity boundary. A separate fixed-state churn control remains a useful diagnostic; this failed
run cannot establish steady state or a live freshness SLO.


The existing fast replay runner also tested cold-restored state sizes through
262,144 groups, with 16 warmup updates and 64 measured historical updates at each
size. All exact independent-oracle checks passed. The complete replay took 48.65
seconds after compilation:

| Groups | Preparation p50 / p95 ms | Measured object reads | Decoded bytes |
| ---: | ---: | ---: | ---: |
| 1,000 | 0.209 / 0.465 | 0 | 228,163 |
| 32,000 | 0.667 / 0.851 | 0 | 4,386,226 |
| 128,000 | 0.803 / 1.087 | 16 | 5,058,431 |
| 262,144 | 0.903 / 1.187 | 151 | 5,222,673 |

This is a different grouped application replay, with synthetic retained state and
no network latency proxy. It demonstrates why fast CPU timings alone are
insufficient: measured object reads increase sharply even while local preparation
remains below a millisecond at the median. Its oracle and page/request counters
complement the paced test. It does not reproduce the paced run's accumulated
compaction history. Reproduce the sweep with:

```sh
python3 scripts/fast_validation.py --mode growth --release \
  --state-rows 1000 32000 128000 262144 --repeats 1 \
  --warmup 16 --updates 64 \
  --output artifacts/benchmarks/steady-local/preloaded-growth-new
```


## Bounded cold reads

Historical immutable pages now retain bounded, negative-only Bloom filters after
their decoded rows leave the cache. Filters are admitted only after page identity,
checksum, canonical ordering and complete fences have been verified. A negative
answer avoids a page read; a positive answer still uses the ordinary verified
full-tuple weighted lookup. Filters are disposable process memory and do not
change the durable format. A CLOCK policy bounds eviction work without scanning
the filter cache on every admission. Staged filters remain private until promotion;
private scans do not populate the foreground filter cache.

Compressed foreground windows adapt between 16 and 256 KiB (or the smaller
configured cache cap) using the fraction
actually requested before eviction. Sparse probes shrink windows; dense use can
expand them. Private scans retain their larger windows. Cached windows remain
readable across width changes, including partial tail windows.

Additive statistics collect ordered nonzero group deltas until their planned
distinct missing windows fill the existing eight-read concurrency limit, a window
cannot fit, or existing resident entry/byte limits require draining. Reservations
charge the actual union of windows and pin cached covers. This avoids both
serial cold reads across overlapping runs and charging an entire maximum window
for every tiny page. Pending scalar vectors and structural storage are charged
in addition to serialized payload; these are resource estimates, not an RSS bound.
Cancelled deltas never trigger speculative reads. Foyer encoded-cache hits also
suppress optional object prefetch. All fetched pages still pass the ordinary
checksum, decoding, fence and multiplicity checks before use. Source coefficient
validation lazily selects pages across all immutable runs under one global
concurrency bound. It retains one exact totals vector, including memory-run
contributions and duplicate object occurrences, and checks coefficient overflow
and bag nonnegativity only after every contribution has been accumulated.

The durable sequence remains object PUT, one PostgreSQL transaction publishing
membership/results/progress, then source acknowledgement. This work adds no
checkpoint objects or publication step. Separate foreground/scan byte and GET
counters identify amplification, and completed merge histograms let qualification
require multiple distinct checkpoints merging at least 65,536 input rows.

Fast checks cover cancellation and retry, corrupted pages, duplicate reader
occurrences, exact weighted results, cumulative aggregate limits, cache promotion,
sparse and dense window pressure, and paired increasing-state workloads. Timing
is diagnostic; deterministic work and correctness bounds gate the fast suite.
A paced integration run must additionally pass freshness, cost-growth, native
PostgreSQL equality and acknowledgement checks. Freshness violations are retained
while the run continues collecting later compaction cycles, and fail the final
qualification; thresholds are unchanged.


### Avoiding coefficient-validation reads

Each immutable block index now records the maximum absolute canonical tuple
weight. An opened object retains the maximum of its blocks; a memory batch
computes its maximum once when finalized. Summing these maxima across every
reader occurrence and the delta gives a conservative triangle bound. When it
is at most `i64::MAX`, signed coefficient validation needs no old page reads.
Uncertain bounds still require exact full-tuple accumulation; intermediate
`BigInt` values preserve cancellation and narrowing occurs only at the end.
Nonnegative-bag validation always uses exact coefficients. Duplicate occurrences
and the unsigned magnitude of `i64::MIN` are included.

The summary is safety-critical writer metadata, authenticated by the existing
root-index digest, with the same trusted-writer provenance as its tuple fences.
The writer derives it from canonical weights. Opening validates its domain, and
ordinary page reads compare it with decoded weights, including decoded-cache
hits. It does not establish integrity of unread payloads or replace required
reads for computation.

This is a hard object-index cutover: nonempty block descriptors require the
weight summary. Existing views require rebuilding before using this version;
there is no old-format adapter. The live demo has not been upgraded.

Prefetch also narrows an older cached wide window to a bounded copied compressed
slice when the current smaller aligned window is completely covered. This
prevents hot wide covers from consuming the entire lookahead budget before
eight missing windows are planned. Non-nested widths retain the original cover,
and every copied slice is charged. No storage request is needed for those slices.


### Physical collection metadata

Each collector session memoizes fully verified immutable root-to-pack dependencies
under the existing GC metadata budget. Every collection still builds its live set
from the current SQL snapshot, including active reader protections. Dependencies
for absent or changed references are discarded, and store identity is retained
explicitly. Limits still precede destructive requests. The cache stores pack paths,
not tuple fences, and does not establish liveness or replace integrity scrubbing.
Cache misses retain the existing hash, schema, layout and address verification.
Dependency discovery discards tuple-fence values during JSON parsing because
physical collection only needs the pack descriptors.

A fast production mark regression repeats collection ten times over a 1,134,750-byte
index. Downloads fall from 11,347,500 to 1,134,750 bytes while the memoized dependency
entry charges 582 bytes. Root removal, reader-pin release, changed references,
store isolation, failed reads and cache-budget pressure remain correctness gates.


### Read-amplification qualification, October 24

Fast correctness and growth validation passed through 262,144 groups before the
20-minute uninterrupted native run. The common gate passed with 97.39% production
line coverage. Independent review covered the implementation and its safety
boundaries. The native run used local PostgreSQL and latency-injected object
storage, processing 1,199,567 changes at 1,000.09 changes/s. Source rows grew from
32,001 to 431,856 and output groups reached 399,857. Three distinct checkpoints
each witnessed two merges of at least 65,536 input rows. Native equality passed
at 10 and 20 minutes, acknowledgement ordering remained safe, and the final
journal drained to zero. Owned Docker fixtures were removed without errors.

Comparing the preregistered early and late six-minute windows:

| Metric | Early | Late | Growth gate |
| --- | ---: | ---: | --- |
| Processing p50 / p95 | 3 / 51 ms | 4 / 71 ms | pass |
| Sampled sink visibility p50 / p95 | 14.45 / 76.72 ms | 16.28 / 90.61 ms | pass |
| Preparation including object I/O | 65.84 us/change | 93.43 us/change | fail |
| CPU | 0.0963 ms/change | 0.1146 ms/change | pass |
| Peak RSS | 247.4 MiB | 283.6 MiB | pass |
| Estimated Class A per million changes | 927 | 929 | pass |
| Estimated Class B per million changes | 1,294 | 2,085 | fail |
| Downloaded bytes/change | 35.98 | 63.32 | fail |

There were no sampled freshness violations, but the overall qualification failed.
This is an improvement in read amplification and removal of the observed latency
cliff, not proof of steady state. The preceding run without GC dependency reuse
downloaded 160.02 to 497.13 bytes/change and used 7,527 to 10,577 estimated Class B
operations per million changes. With reuse, remaining foreground range GETs still
increased from 194 to 910 per million changes, while private scan GETs held at
355 to 356. Foreground fetched bytes increased from 11.15 to 33.75 per change.
The next investigation is this residual growing cold-probe demand and preparation
cost, with unchanged qualification bounds. Request classes are proxy estimates,
not billing measurements. The compact aggregate evidence is in
[steady-growth-samples.json](steady-growth-samples.json); private run evidence and
all failed runs remain under ignored artifacts. Nothing was pushed or deployed.

### Fast cold-probe qualification

Use `python3 scripts/fast_validation.py --mode steady --release --output PATH`
with a new ignored output directory before scheduling another native soak.
The fixed matrix carries production arrangements and their bounded caches through
32k, 64k, 128k, 262k and 524k rows, with two complete compactions per size and
ordinary tiered flushes between them. Each cycle executes 4,096 affected-key
changes, checks every statistics result against an independent map, and cold
reopens the final weighted bag. It reuses the existing statistics operator,
arrangement writer, test store, compaction counters and fast-validation runner.

Tokio time is paused. Injected 20 ms reads advance virtual time without waiting
20 ms on the wall clock. The report separates virtual elapsed I/O time from
wall runtime and records full-compaction costs separately from the measured
post-compaction probes and tiered flushes. The fixed forced-compaction cadence
is a cache-generation stress test, not the worker's checkpoint scheduling policy.
This matrix is not paced source traffic and does not simulate CDC, sink commits,
physical GC, CPU scheduling, memory/RSS or end-to-end freshness. It quickly
rejects growing storage/operator work; a pass only qualifies that component and
covered range. Full-worker steady state still needs separate evidence.

The runner saves `assessment.json` and `result.json` even when work continues to
grow. It requires no GET/byte regression against the control, at most 5% more decoding
or virtual elapsed I/O, and no more than 25% growth in
reads, bytes, decoding and virtual I/O per change between the largest two sizes.
Missing, duplicate or malformed case evidence fails. Wall timing is diagnostic,
not a noisy regression threshold. Do not weaken a failed bound or turn an
improvement against the control into a plateau claim.

### Coalescing adjacent cold windows

Prefetch now combines exactly contiguous missing windows from the same immutable
pack and store into one GET. It never bridges unread gaps. The readiness count
tracks missing contiguous spans instead of individual windows, allowing bounded
lookahead to fill useful I/O requests under the existing byte/entry and pending
operator budgets. At most eight groups load concurrently. Coalesced groups acquire
the ordinary singleflight stripes in sorted unique order, recheck resident
windows, fetch only still-missing spans and admit them before releasing locks.
A concurrently filled middle window splits the request rather than being fetched
again. Cancellation releases the guards and leaves probe heads unchanged.

The optional Foyer encoded-cache key now uses the compressed page content hash,
so an unchanged page remains reusable after moving to a different pack or offset.
Length, digest, canonical order, fences and coefficient checks still apply.
Neither change alters PUT, SQL publication or WAL acknowledgement ordering.

The final matrix ran in about 11 seconds after compilation. At 524,288 rows the two
cycles reduced GETs from 449 to 161 (64%) and fetched bytes from 10.83 MB to
9.18 MB (15%). Virtual elapsed read time fell from 3.58 to 2.52 seconds; diagnostic
wall time for measured probe/flush work was about 0.2 seconds in both cases. Full-compaction
work was unchanged and is reported separately. At 262,144 rows GETs fell from
170 to 24 without fetching extra bytes. Every size passed the control
non-regression check, and all per-step and cold-reopen bag comparisons passed.

The component plateau gate still failed: candidate GETs rose from 24 to 161
between the largest sizes; downloaded bytes/change rose from 552 to 1,121 and
virtual elapsed I/O rose from 56 to 308 us/change. Decoded bytes/change stayed
within its growth bound. Larger working sets still cause cold positive probes
and increasing I/O rounds. This fast rejection deliberately blocks another
wall-clock soak; neither component nor whole-worker steady state is qualified.
Two earlier synopsis-retention experiments and a weak window-content index were
rejected by this same fast matrix and removed from production code. Their private
evidence is retained. The surviving changes are contiguous-read coalescing and
the content-based optional encoded-cache key.

The final common gate passed with 97.36% production line coverage, including
sequential PostgreSQL replay, crash, publication and acknowledgement checks.
Independent review approved the ready implementation. It also caught a
singleflight admission race; the corrected implementation admits loaded windows
before releasing their stripes. A later overlap regression verifies readiness
against actual request groups when cached wide covers interrupt adjacent misses.
All failed experiments and qualification results remain in ignored artifacts.

The strongest remaining lead is compressed-cache pressure: the fixture retains
4 MiB of windows, while its second 524k-row compaction reads about 4.27 MB. The
corresponding 262k-row compaction reads about 2.14 MB. This suggests eviction and
repeated range fetches, not proof that all additional probes are unavoidable.
The next diagnostic should vary only compressed-cache capacity and count unique
and repeated fetched ranges; changing all cache budgets together would confound
the result. Stripe contention may affect elapsed I/O rounds but cannot alone
explain increased downloaded bytes. No wall-clock soak, push or deployment was
performed after the failed fast plateau check.
