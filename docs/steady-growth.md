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

### Continuous operation qualification

Periodic restarts are not a resource-management strategy. At a supported state
size and fixed input rate, live reader pins, metadata and cached generations must
stay bounded while compaction and GC keep up. Durable state may grow with source
cardinality. Finite capacity limits still apply on fixed resources.

Immutable indexes are now authenticated paged trees. Readers share coarse root
descriptors and small inline leaves; lower nodes load on demand. Foreground
metadata retains exact compressed frames by digest and length, rather than broad
windows that mix dense branch reads with sparse leaf reads. Typed decoded nodes
also include frame length and Rust type in their identity. Expected addresses,
summary fences, row counts and coefficients still validate on reads. Corruption,
failed reads and cancellation cannot advance a cursor or authorize a GC sweep.

Generated frames warm the same bounded encoded cache without reading bytes back
from storage. Leaf warming does not eagerly deserialize the generated leaves.
Cache hits share immutable frame bytes. Identical authenticated content may be
reused across upload namespaces; physical I/O and GC membership still use the
qualified object paths. The former foreground metadata window tier and its
namespace-seeding plumbing have been removed.

Metadata receives one quarter of the configured decoded-data cache budget,
split one quarter decoded and three quarters encoded. With the fixture's 16 MiB
cache, metadata caps are 1 MiB decoded/512 nodes and 3 MiB encoded/3072 frames.
Encoded admission charges payload and key/record metadata, rejects oversized
entries, and reserves at least 1 KiB per entry to bound cardinality. These are
resident charges, not allocator RSS or a global process limit. Ordinary readers
and output wrappers share metadata caches. Disabling decoded-data retention still
provides an 8 MiB navigation budget. Independent open calls can own independent
caches.

Scans peek at foreground decoded nodes without promotion and retain borrowed
nodes in private scan storage using their existing charges. Each active scan has
256 KiB/17 decoded navigation nodes and a 256 KiB metadata window with 4 KiB
metadata allowance. The combined index workspace remains 516 KiB, in addition
to the existing private data scan window. An 8,192-block regression measures the
cursor's actual workspace, checks foreground non-pollution and catches
branch/leaf ping-pong. All index GETs remain visible in shared counters.

Foreground data windows use Foyer's weighted LRU policy, adapt between 4 and 256 KiB,
and avoid retaining a wide cover and duplicate narrow slices. Scoped guards hold
planned bytes through consumption independently of eviction. Their payload and
entry caps are separate from resident cache charges; pin-map metadata adds bounded
working storage. Dropping a batch releases its guards; corruption invalidates
resident and pinned copies. Read-ahead completion is unordered, while prepared
keys and tuple processing retain their required order.

Read streams permit 32 concurrent jobs and use 256 flight stripes to reduce
unrelated lock collisions. This is a local stream limit, not a global GET cap
across readers/operators. Metadata lookahead may overshoot its soft target by one
32-key group; concurrent response/decoder work is additional bounded transient
memory. Resident byte budgets remain unchanged. Publication still orders all
object PUTs before atomic PostgreSQL membership/results/progress publication,
then source acknowledgement.

This is a hard storage-format cutover: old non-paged roots are rejected. Rebuild
existing views before deploying it. Large roots add one packed-index PUT, not one
PUT per node. Leaves whose complete raw encoding fits 16 KiB remain inline without
that PUT. The writer conservatively charges raw fences and ancestor summaries;
its 8 MiB index pack, 4,096 fences, 128 output roots and 256 trace runs remain
explicit capacity boundaries. Capacity errors are not retried indefinitely.

The fast `cold` screen executes four full-compaction cycles at each of 2,097,152
and 4,194,304 base rows. The `longevity` mode keeps the same live writer, trace and
caches while growing through 524,288, 1,048,576, 2,097,152 and 4,194,304 base rows.
Every step samples one checked rank in each of 256 strata across the full relation,
plus paired gap keys. This is deterministic stratified traffic, not independent
uniform random sampling. All changes actually execute against production
statistics, signed validation, flush and compaction, with a per-step independent
oracle and full streaming cold-reopen bag comparison after each size.

```sh
python3 scripts/fast_validation.py --mode cold --release \
  --output artifacts/benchmarks/cold-capacity-next
python3 scripts/fast_validation.py --mode longevity --release \
  --output artifacts/benchmarks/cold-regime-next
```

The revised longevity protocol declares eight warmup cycles per size, then 28
measured cycles: 144 compactions and 589,824 actual changes, including 458,752
measured changes. The warmup policy was selected after earlier failed observations
and fixed before this revised run. It cannot be extended after seeing a failure.
No state or cache resets occur. Correctness, cache/pin bounds, RSS and absolute
foreground service bounds apply to every warmup and measured cycle. Fixed
measured cohorts 2..8 versus the last seven retain the 25% trend threshold. The
broader comparison including warmup is reported separately; a cold-regime pass
does not turn a cache-fit transition into a broad plateau. Historical failed
112-cycle and warm-to-cold assessments remain failed in the retained evidence.

Paused Tokio time injects 20 ms GET latency and advances twelve idle hours after
each cycle, covering 72 days of clock range. **That is not 72 days of continuous
traffic.** Executed traffic is about 590 seconds at 1,000 changes/s. This component
has no worker source/sink timers; idle clock advancement adds no worker-deadline
coverage. The useful endurance evidence is repeated actual state transitions,
compactions and retirements without restarting the active fixture.

The assessment checks actual physical state bytes, post-retirement RSS and Linux
process peak RSS, as well as cache charges, released pins, object GET/PUT counts
and bytes. Production GC marking authenticates the live dependency set; only this
fixture's owned obsolete objects are deleted. A second mark must reuse bounded
coarse metadata without extra GETs. Process RSS includes the independent oracle,
allocator and in-memory object store, so it is not engine-only memory.

A 4.096 s foreground virtual-I/O bound per 4096 changes is a necessary condition for
1,000 changes/s, excluding CPU, bandwidth and sink work. Forced full compaction
every 4096 changes stresses reclamation, not production scheduling; its cost is
reported separately and compared per retained row. SQL GC protection/sweep races,
CDC, sink publication, WAL retention, maintenance backlog and real freshness
remain covered by other harnesses and operational monitoring. These component
results do not prove arbitrary-size growing-state stability or multi-week worker
uptime.


The independently frozen window-versus-frame comparison uses the same corrected
stratified workload, LRU policy, 32 local read jobs and 256 flight stripes on both
sides. At 4,194,304 base rows, exact frames reduce mean metadata downloads from
25.679 MB to 0.610 MB per 4096-change cycle, total downloads from 28.845 MB to
3.776 MB, and foreground virtual read time from 4.745 s to 2.915 s. The maximum
falls from 5.10 s to 3.00 s. At 2,097,152 rows both average 1.095 s because the
metadata already fits. This isolates the metadata-cache replacement; it is not a
comparison against the original commit or a measurement of worker freshness.

The first exact-frame 144-cycle assessment completed in 495 seconds after
compilation. All 589,824 changes matched the oracle, all cold reopens matched,
and all absolute cache, pin, memory and foreground service bounds passed. Coarse
resident roots stayed below 48 KiB, the GC dependency memo below 16 KiB and peak
process RSS below 1.15 GB. The four measured foreground-I/O cohort means changed
from 1.180 to 1.263 s, 1.297 to 1.291 s, 1.540 to 1.631 s and 3.300 to 3.386 s
as base rows grew through the four sizes.

**That assessment failed its complete trend gate.** At 524,288 base rows,
compaction GETs/row rose 57% and bytes/row rose 37%; at 2,097,152 rows, index GETs
rose from 26.86 to 36.43 and index bytes from 60.3 to 82.2 kB per cycle. Physical
state also grew during each size, so the next diagnostic records actual scan
coverage and compressed input size to distinguish repeated reads from changing
compression and cache coverage. The broad warm-to-cold comparison also failed.
The failed result is retained and does not qualify whole-worker steady state.


Fast correctness also caught a performance regression during this change. Prepared
metadata originally skipped the Bloom lookup when a decoded head was reusable.
That removed CLOCK reference touches, so useful synopses stopped surviving
eviction. Preparation now always consults the existing validated synopsis, and a
new data load rechecks it because earlier keys may have learned a filter after
preparation. A verified same-page head can still be reused. Both changes preserve
full weighted run multiplicity. The unchanged sparse-decoding gate passes again:
filtered/baseline decode ratios are 0.458, 0.525 and 0.527 at 32k, 128k and 262k
rows, versus 0.458, 0.744 and 0.845 in the rejected version. Reducing preparation
to eight keys did not fix the regression and that test-only experiment was removed.


Physical compaction diagnostics now classify ranges by authenticated qualified
index-pack identity, with metadata GETs/bytes independently checked against the
production index counters. Data and index objects share the `/pack/` prefix;
the earlier prefix-based split was invalid and is explicitly marked unusable in
the retained evidence. Whole-range totals and oracle results remain valid.
Correct classification shows no repeated data-page bytes in the cold screen;
the repeated bytes belong to metadata navigation.

The selected additional fix retains a borrowed foreground node in the scan's
existing private decoded cache, using the same immutable Arc and charge. It
survives foreground eviction without promoting or enlarging that cache. At four
million base rows, metadata compaction GETs across four cycles fall from
32/49/34/41 to 30/43/30/36, with unchanged foreground timing and memory budgets.
Metadata repeats fall but remain nonzero (about 1.05–2.36 MB per cycle in this
screen). The targeted eviction regression and unchanged full weighted oracles
cover the handoff.

Two alternatives were measured and removed entirely. Exact private branch frames
saved bytes but greatly increased GETs and virtual I/O. Separate coalesced branch
windows also saved bytes/CPU but increased total compaction GETs by 6% at four
million rows, with a corresponding 6% virtual-I/O increase. The selected reader
keeps the simpler 256 KiB decoded plus 260 KiB encoded private index workspace.
Remaining metadata navigation rereads are an explicit follow-up, not a reason to
restart the process periodically or a completed steady-state proof.


The final selected source repeated all 144 compactions in 517 seconds after
compilation. Its executable hash matches the final fast-correctness run. All
589,824 executed changes, cold reopens, retirement checks and absolute bounds
passed. Final cardinality was 4,484,520 rows with 45,093,226 physical state bytes;
coarse resident roots peaked at 47,518 bytes and process peak RSS at about 1.16 GB.
Maximum foreground virtual I/O was 1.30, 1.36, 1.72 and 3.54 seconds per
4096-change cycle at the four base sizes. The strict trend assessment still
failed the same four checks reported above, so neither the cold-regime nor broad
plateau gate is claimed to pass. Bounds remained fixed; no restart or active
cache reset was used to achieve those results.

At that checkpoint, the proposed next change was bounded forward index-navigation
state through a scan instead of relying on repeated root-to-leaf ordinal lookups to
hit a cache. It must preserve failure-atomic cursor positions, wide-fence limits,
weighted identity and GC/publication ordering. The retained physical range
counters provide a fast rejection test for that work. Whole-worker operation for
continuous days remains unqualified by this component run.


The final selected source passed `./scripts/check.sh`: 551 Rust tests, 123 Python
tests, sequential PostgreSQL core replay/crash/publication/lifecycle/rebuild
checks, strict Clippy/rustfmt/file-length rules and dependency checks. Production
line coverage is 97.31%. The common gate covers the final handoff fix; code
fingerprints did not change during that run. Owned PostgreSQL fixtures used tmpfs
and were removed; datasets, results and rejected experiments remain ignored.
Independent review found no outstanding actionable correctness/durability issue.
This common-gate pass does not override the four failed performance trend checks.

## Authenticated navigation and overlapping history

Immutable index nodes now retain their validated contents in an authenticated
handle. Construction validates the encoded node and its structural summary.
Reuse checks the caller's hash, encoded length, fences, row/block counts, weight
bound, depth and pack placement against that handle. It does not walk and
resummarize the node for every key. The handle is charged to the bounded index
cache, including its retained summary. Inline roots, fetched pages and warmed
uploads use the same constructor. This changes neither the persisted format nor
publication and acknowledgement ordering.

Checkpoint consolidation also considers overlapping logical key ranges across
size tiers. At least four overlapping cohorts can be merged when no selected
cohort holds more than half of the selected physical rows. Dominant bases are
excluded; multipack cohorts remain indivisible. This bounds relative row work,
not compressed bytes or PUT count. Coarse key envelopes can overestimate overlap
for sparse data. Existing size-tier consolidation still handles nonoverlapping
runs, and complete-state overflow handling preserves signed full-tuple identity.

The fast SQL replay now uses bounded ingestion and the production
freeze/upload/rebase path. A captured prefix is uploaded while a later
transaction commits, then installed with that newer suffix preserved. It retires
unreachable objects using the production mark kernel in an exclusively owned
fixture store. It does not simulate PostgreSQL GC protection/sweep barriers.
Independent integrated output checks run throughout; physical output verification
uses a separate cold query after the measured interval. Active endurance state
is never periodically restarted or fully compacted to reset its history.

```sh
python3 scripts/fast_validation.py --mode endurance --release \
  --state-rows 32000 128000 --updates 256 \
  --output artifacts/fast-validation/endurance-new
```

Each epoch contains up to 1,000 mixed source operations, including inserts,
updates, deletes, NULLs and signed aggregate inputs. Repeated operations on one
key retain their intermediate old/new tuples. Original affected operations pace
the logical clock at 1,000/s; missing deletes count zero, unchanged updates count
one. Source transactions remain indivisible. Checkpoint scheduling uses the
same 64 MiB memtable, five-second interval and 16-tick limit as the paused demo.
The test records foreground plus maintenance requests, bytes, cache charges,
run count and process RSS, and compares fixed early/late windows. It is an
unpaced component qualification, excluding CDC and PostgreSQL sink latency.
The separate `longevity` mode stresses larger state and virtual idle-time expiry;
its reported logical days are not days of continuously executed traffic.

### Paired component results, 2026-10-25

With identical corrected lifecycle fixtures on `417d0fc` and this implementation,
256 epochs produced these local preparation measurements:

| Initial source rows | Preparation p50, before / after | Preparation p95, before / after | Maximum runs, before / after | PUTs, before / after |
| ---: | ---: | ---: | ---: | ---: |
| 32,000 | 98.56 / 31.01 ms | 168.33 / 44.08 ms | 12 / 7 | 200 / 198 |
| 128,000 | 153.55 / 50.69 ms | 351.28 / 76.85 ms | 17 / 6 | 258 / 258 |

Each side consumed the same source transactions and passed the independent
oracles and final cold-state equality check. Request counters include bootstrap
through the last measured epoch. The 128k candidate passed all declared trend
and resource checks. The 32k candidate failed the maintenance elapsed-time trend check:
502 to 782 ns per affected operation, above its fixed 25% allowance. The baseline
also failed that check. Neither small absolute cost nor a baseline failure turns
a candidate failure into a pass.

An additional uninterrupted 512-epoch candidate replay executed 437,901 and
485,517 affected operations from 64k and 262,144 starting rows. It completed 80
and 86 checkpoints, respectively, with maximum run counts seven and six and
cold-state equality. Preparation p50 was 38.21 and 45.30 ms. These tests **did not
qualify the declared overall plateau**: maintenance elapsed time per affected operation
rose beyond 25% at both sizes, and GETs per operation rose 27% at 64k. At 64k,
the fixed comparison windows contained 110 versus 126 GETs and the same 108
PUTs, while maintenance elapsed time was 88 versus 161 ms. Preparation, transferred bytes,
PUTs and all absolute resource limits passed. These observations demonstrate a
substantial preparation improvement, not a complete steady-state proof.

Detailed inputs, paired results, failed assessments and larger-state evidence
are retained under ignored `artifacts/benchmarks/authenticated-overlap/`.
The demo remains paused; this implementation has not been deployed.

The larger-state lifecycle check completed 144 compactions and 589,824 executed
changes, ending at 4,484,520 state rows and about 45.1 MB of physical state.
All independent results, cold reopens, retirement checks and absolute resource
bounds passed without an active-state restart. Resident coarse roots peaked at
49,562 charged bytes, total index cache charge stayed below 4 MiB, and sampled
process RSS peaked at about 1.07 GB. Virtual idle intervals advanced the clock
across 72 days; actual executed traffic is only about 590 seconds at 1k changes/s.
The four previously observed strict growth checks still failed: normalized
compaction GETs/bytes at 524k starting rows, and foreground index GETs/bytes at
2M starting rows. This slice improves cached preparation but does not resolve
that separate cold navigation/scan amplification. No weekly continuous-traffic
or unrestricted steady-state claim follows from these results.

The final implementation passed independent review and `./scripts/check.sh`:
559 Rust tests, 126 Python tests, sequential PostgreSQL integration checks,
strict formatting/lints/dependency checks and 97.32% production line coverage.
All 18 additional SQL replay cases and storage-growth work bounds passed.
The owned PostgreSQL fixture used tmpfs and was removed after the gate.
The failed steady-state trend assessments remain failed and retained.


## Cycle-aligned SQL cache and history matrix, 2026-10-25

The SQL endurance fixture now separates fixed-cardinality churn from uniform
state growth. Fixed churn balances deletes with reinsertion of the same identity
inside its source transaction. A diagnostic hot-prefix variant appends cold rows
and restricts existing-row updates to a declared prefix; this variant uses
updates/inserts rather than the uniform variant's delete mix. All variants reuse
the application generator, independent oracle, bounded bootstrap and production
freeze/upload/rebase/retirement path. They do not restart active state or force
full compaction to clear history.

Qualification now compares installed natural-merge intervals instead of arbitrary
epoch quarters. Endpoints have no pending upload. Four natural intervals are
warmup; four early and four late intervals retain their merge-size histograms.
The largest observed merge tier must also have four post-warmup installed
boundaries, allowing two disjoint complete periods to be compared. Too few
largest-tier periods fails coverage. This prevents many small merges from
qualifying a case that has barely exercised its largest maintenance work.
The existing 25% trend bounds remain unchanged. Cache resource bounds now follow
the configured byte/entry budgets. Historical assessments remain historical;
they are not retroactively turned into passes by changing comparison windows.

The main matrix used 2k, 32k, 128k and 262,144 initial rows, each with both fixed
and growing state, 512 epochs, one row per aggregate group, a 16 MiB decoded
cache and 512 entries. Full independent output checks ran every 64 epochs and
at the end, followed by a separate cold reopen. A 384-epoch fixed-state control
used a 4 MiB cache. A 768-epoch fixed-state run repeated the largest tier, and a
512-epoch growth control kept existing-row activity within 4,096 identities.
These are deliberately different cache/workload profiles from historical
64 MiB / 4,096-entry demo-shaped tests.

All 12 executions passed the independent output and cold-recovery checks,
executing 5,893,274 affected source operations, 1,146 checkpoints and 620 natural
merges. These totals include an exact repeat described below. Actual source work
paces logical time at 1k/s; no idle-day jumps are used. Each size/mode owns a
separate fixture; these totals do not describe a single uninterrupted worker.

The decisive 768-epoch fixed-state comparison spans epochs 185–370 and 555–740.
Each period executes 185,000 affected operations and has exactly the same merge
histogram: ten merges in row-count bucket 15, four in 16, four in 17 and two in
19. Thus this comparison includes repeated large maintenance work, not just
small checkpoints.

| Equivalent complete periods, 262,144 fixed rows | Early | Late | Change |
| --- | ---: | ---: | ---: |
| Preparation, ms per 1k affected operations | 248.80 | 257.01 | +3.3% |
| Component CPU, ms per 1k affected operations | 290.65 | 300.05 | +3.2% |
| Decoded bytes per operation | 16,329 | 16,358 | +0.2% |
| All object-read bytes per operation | 153.31 | 226.60 | +47.8% |
| Foreground GETs per 1k operations | 0.205 | 0.573 | +178.9% |
| Foreground fetched bytes per operation | 52.01 | 119.99 | +130.7% |

CPU/preparation and logical decoding work were stable in these matched periods,
but read-byte and foreground-I/O bounds failed. Shorter 512-epoch fixed tests at
2k and 32k passed all checks. The 128k fixed case failed maintenance elapsed time.
The 262k 512-epoch screen passed cost checks but lacked repeated largest-tier
coverage; the longer test exposed I/O growth. Neither screen qualifies the
larger-state overall plateau.

The 4 MiB control kept 262,144 rows constant. Preparation was approximately
764–773 ms per 1k operations and CPU 802–824 ms. Total GETs rose from 21.2 to
60.4 per 1k operations; transferred bytes rose from 3.13 to 4.30 kB per operation.
Its largest-tier repetition was insufficient and its comparison windows contained
different merge-size mixes. This is a retained cold-read regression signal,
not proof of unlimited growth. It leaves little local processing headroom at
1k/s before real storage and sink delays are included.

The hot-prefix growth control increased rows from 262,144 to 313,344 while
restricting existing-row updates to 4,096 identities. Preparation rose from
194 to 247 ms per 1k operations, CPU from 215 to 271 ms, and decoded bytes from
12.15 to 14.53 kB per operation. It failed cost and largest-tier coverage checks.
A bounded set of logical identities is therefore not sufficient to guarantee a
bounded block working set in this fixture. Initial patterned values becoming
varied signed/NULL values can also change compression; that factor is not
isolated by this experiment and does not override failures.

Inspection found a concrete locality concern: group keys use canonical JSON
strings and their canonical lexicographic ordering. A static illustration with
4,096-row blocks places the first 4,096 integer identities in every block of a
262k-key or 313k-key relation. This is not an inventory of actual packed
statistics pages, but it explains why a numerically small hot prefix must not
be assumed physically contiguous. The next investigation should measure actual
hot-key page fanout and distinguish block decoding/cache admission from
range-read geometry, before choosing a bounded point-statistics cache or changing
physical key layout. Each group in this query has only one row; decoding roughly
100 rows per affected operation reflects page-level work rather than large
aggregate groups.

One initial 262k growing run overlapped release compilation and showed a 56%
CPU increase. The exact recorded trace and frozen binary were repeated without
compilation: CPU changed from 306 to 316 ms per 1k operations and preparation
from 269 to 279 ms. The contaminated result is retained and excluded from CPU
attribution. Foreground-I/O and insufficient-tier-coverage failures remained.

CPU ticks cover preparation, output integration and maintenance, excluding
explicit full-oracle comparisons. Preparation is measured separately; neither
number is source-to-sink freshness. Fixture RSS includes the serialized input,
expected results and in-memory object store; it is not worker-only RSS. All
configured cache/resource bounds passed. CDC, PostgreSQL publication, SQL GC
protection/sweep barriers, real object-store latency and continuous weekly
operation remain outside this component qualification. The demo remains paused.

Reproduce a full-tier fixed-state case with the standard entry point:

```sh
python3 scripts/fast_validation.py --mode endurance --release \
  --state-rows 262144 --updates 768 --endurance-shape fixed \
  --group-width 1 --oracle-interval 64 --cache-mib 16 \
  --output artifacts/fast-validation/fixed-full-tier-new
```

Use `--cache-mib 4 --updates 384` for the shorter cold-demand screen. Use
`--endurance-shape growing --hot-rows 4096 --updates 512` for the hot-prefix
growth diagnostic. These commands can exit nonzero on the retained performance
or coverage failures while all semantic checks pass. Raw traces, frozen binaries,
metrics, final assessments and rejected measurements are retained under ignored
`artifacts/benchmarks/cycle-state-matrix-20261025/`.

Independent ready-to-commit review found no actionable issue. The final
`./scripts/check.sh` gate passed 559 Rust tests, 133 Python tests, the sequential
PostgreSQL regression harnesses, strict formatting/lints/dependency checks and
97.32% production line coverage. Its owned PostgreSQL fixture used tmpfs and was
removed. Replay inputs were verified and compressed; results and frozen
binaries remain ignored. These checks do not override failed performance gates.


## Hot-key fanout and compressed read windows

An instrumented hot-prefix control found about 808 distinct keys per epoch
selecting 745 pages early and 890 late. Mean pages per key increased from 2.77
to 3.84; selected compressed bytes increased from about 480 kB to 619 kB. These
are Bloom-positive first-page candidates, including decoded-cache hits and
excluding continuation pages. Numeric identity locality does not establish
physical page locality.

Cross-epoch interval accounting found that 262.3 MB of 266.7 MB fetched in the
original cold control's late window had already been fetched in earlier epochs.
Totals reconcile with foreground read-byte counters. This does not prove a
replacement bug: its 1 MiB compressed cache faces several megabytes of reads per
epoch, and compressed physical state changes despite a fixed logical row count.
A constant-entropy control and demand accesses including hits would be needed to
quantify an ideal policy's avoidable reads. A finite warm-to-cold transition does
not itself imply indefinite growth or a need for periodic restarts.

Two changes address measured amplification. First, the reader caches complete
signed key slices from authenticated immutable objects. Identity includes root
content, serialized-key SHA-256 and Rust row type. A slice includes every
value/weight for one key in that object. Admission is safe only when the selected
first-reaching page ends beyond the key, or is the final page. Equal-last-key
nonfinal pages are conservatively skipped; spanning keys keep the existing
cursor path. Repeated object occurrences retain their original multiplicity.

Admission happens in the page-read branch, preserving cheap whole-page cursor
head reuse. Complete-key hits bypass index navigation and unrelated tuple
decoding without displacing those heads. Full scans cannot use or admit key
entries. Different compaction roots have different identities. This is optional
derived memory; object publication, PostgreSQL atomic result/progress publication
and source acknowledgement ordering are unchanged.

Second, compressed-window geometry now depends on the cache budget rather than
evicted-window byte density. Removing hot demands through key reuse made the
old density policy shrink windows from 256 KiB to mostly 4 KiB: an exact-budget
hot control used almost nine times as many GETs despite fetching fewer bytes.
A fixed 256 KiB control removed that regression but doubled cold read bytes.
A 64 KiB control retained more independent locations while adjacent cells still
coalesced into larger GETs, avoiding both failures.

Window sizing divides the byte budget by the smaller of 16 and the entry cap,
then rounds down onto the existing 4–256 KiB alignment ladder. Small budgets
still account for metadata before retaining a window. Sixteen is an
empirical allocation target, not a guaranteed resident count or theoretical
optimum. A 1 MiB compressed budget selects 64 KiB cells; a 4 MiB budget selects
256 KiB cells. Single-window scan caches retain wide coalesced reads. The old
density adaptation and utilization bookkeeping have been removed, rather than
kept as an alternate policy. Page format, packing and read-ahead readiness remain
unchanged. A fractional-budget regression verifies that prefetch and cache lookup
use reachable aligned identities and reuse covers without extra reads.

The key cache reserves an additional quarter of the decoded-page byte budget.
At 16 MiB decoded capacity, modeled page/index/range/filter/key caps total 32 MiB
rather than the previous 28 MiB. Key entries charge at least 512 bytes and at
most 64 KiB. Prepared probes charge owned cached slices to their workspace target,
with each key's allowance split across runs. Evicted slices retained by active
probes are separate from resident cache counters. Charges follow the existing
decoded size model and are not exact recursive heap or RSS bounds.

Fair-memory controls retain the old implementation with a 20 MiB decoded budget
and 1,024 entries, allowing its full page budget to fill, against the new 16 MiB
configuration. Smaller-budget controls also compare the new 4 MiB configuration
against old 4 MiB and 5 MiB configurations. A bounded key cache is a locality
tradeoff; its extra memory is not uniformly better than allocating more bytes to
other caches. Preserve these controls when evaluating cache defaults.

Frozen binaries execute recorded traces sequentially. Known overlap with another
checkout's compilation was preserved and excluded from CPU qualification.
Opt-in test diagnostics retain page fanout, range lengths and cross-epoch physical
intervals; expensive page probes are disabled for CPU comparisons. Rejected cache
policy, range coalescing and smaller-page experiments remain in ignored evidence,
without their production implementations.

These component tests exercise production computation, checkpoints, rebasing,
GC marking and independent oracle/cold-reopen equality. They exclude PostgreSQL
CDC/publication, SQL GC barriers, real object-store latency and sink freshness.
Fixture RSS includes input, expected results and an in-memory object store.
The demo remains paused. Results, input datasets, frozen binaries and failure
evidence are under `artifacts/benchmarks/cold-read-20261025/`.


The final paired matrix uses the existing high-cardinality `COUNT(*)`/`SUM(amount)`
query grouped by `account_id`, initially one source row per group. All cases
start at 262,144 rows. Hot cases grow to 313,344 rows while updates to existing
identities target 4,096 keys. This isolates reader/statistics work; it is not a
measurement of the deployed demo or all query shapes. CPU is component process
CPU per 1,000 affected change operations, excluding explicit oracle comparisons.

| Final binary, late comparison window | CPU ms/1k, before → after | Foreground GETs/1k | Fetched bytes/change |
| --- | ---: | ---: | ---: |
| Hot, 16 MiB decoded cache | 271.43 → 139.29 | 0.20 → 0.20 | 15.97 → 15.97 |
| Hot, 4 MiB decoded cache | 429.43 → 377.57 | 7.16 → 6.77 | 1,707.15 → 1,707.45 |
| Uniform cold, 4 MiB decoded cache | 827.08 → 815.69 | 57.83 → 21.12 | 4,103.23 → 3,854.37 |

The 16 MiB hot case reduces CPU by 48.7% and decoding by 78.9%. The cold case
reduces foreground GETs by 63.5% and transferred bytes by 6.1%, with CPU roughly
unchanged. The smaller hot case reduces CPU by 12.1% without the earlier GET
regression. PUT counts are unchanged for each recorded trace, oracle and cold
reopen checks pass, and configured resource caps pass. No compiler overlap was
observed by the final runs' one-second process monitor.

The larger-memory old control fills its 20 MiB page cache (853 entries peak) and
still takes 266.14 ms per 1k changes in the hot late window, versus 139.29 with
the new 16 MiB configuration. The smaller fair-memory control is less favorable:
the old 5 MiB configuration takes 305 ms per 1k changes and 4.66 foreground GETs
per 1k, beating the new 4 MiB configuration. The additional key-cache budget is
therefore a measured locality tradeoff, not a universal allocation improvement.


The growing hot cases still fail within-run cost trends: CPU rises from
78.31 to 139.29 ms/1k in the 16 MiB case, and from 189.69 to 377.57 ms/1k
in the 4 MiB case. Decoding in the larger-cache case rises from 527 to 3,059
bytes/change. These are paired late-window improvements, not stable growing-state
CPU behavior. Maintenance mixes differ and repeated largest-tier coverage is
insufficient in these shorter cases; this comparison does not isolate the cause
of their remaining growth.

The final 768-epoch fixed-state replay has repeated largest-tier coverage with
matched complete compaction windows. Early-to-late CPU is 291.78 → 294.32 ms
per 1k operations (+0.9%); preparation is 250.62 → 253.19 ms (+1.0%); decoding
is 16,064 → 16,110 bytes/change (+0.3%). Foreground reads still increase from
0.205 to 0.589 GETs/1k and 52.01 to 117.26 bytes/change. All four final cases
therefore retain `qualified: false`: the full-tier case fails I/O-growth checks,
and shorter screens also lack repeated largest-tier coverage. Paired performance
improvements do not establish an overall plateau or weekly restart-free operation.

All four cases pass oracle equality, cold reopen and configured resource bounds.
They execute 2,176,000 measured source changes and install 437 checkpoints in
about 704 seconds total, without active-state restarts. Peak retained cache charge
is approximately 26.7 MiB for the 16 MiB hot case and 29.1 MiB for the full-tier
fixed case; the key cache stays within 4 MiB (1 MiB for smaller configurations).
These retained charges exclude evicted entries still owned by bounded workspaces
and are not RSS. Fixture RSS stays below its 3 GiB cap, reaching about 2.69 GiB
in the full-tier case.

Current independent implementation and report review found no actionable issue.
The current `./scripts/check.sh` core gate passes 565 Rust tests, 133 Python tests,
the sequential live PostgreSQL harnesses, formatting/lints/dependency checks and
97.16% production line coverage. Its owned PostgreSQL fixture used tmpfs and was
removed. Frozen final-binary checks additionally pass 87 reader tests and 12
trace-probe tests. The ready-source hash manifest is unchanged across these
checks and performance replays. These checks do not override failed growth gates.

## Growing-state CPU and complete-cycle read controls

The next comparison keeps the previous binary and adds a seeded initial-value
control. The original endurance fixture begins with amounts `id % 7`, then
replaces them with values from -2500 through 2500 and NULLs. Its compressed state
can grow with unchanged logical cardinality. The optional
`--initial-random-amounts` control starts with that value range and a seeded NULL
pattern, preserving the subsequent chosen keys and replacement values. This
reduces an entropy confound; it does not prove a stationary distribution.

In the unchanged binary, the 262,144-row fixed-state control completes three
consecutive identical largest-tier intervals, each with 185,000 changes. Their
merge histograms match: ten tier-15, four tier-16, four tier-17 and two tier-19
merges. Component CPU is 297.41, 300.27 and 302.32 ms per 1,000 changes; total
read bytes are 259.93, 256.47 and 260.24 per change. Thus this finite fixed-state
control already has near-flat complete-cycle cost. Its shorter comparison
windows still fail strict growth checks and remain in the report. Matching
maintenance work matters when interpreting those failures.

The growing control starts at 524,288 rows, appends 102,400 rows, and changes
existing rows within a 4,096-key working set. Its unchanged-binary short-window
CPU grows from 81.85 to 175.85 ms per 1,000 changes; decoded bytes rise from
559 to 4,801 per change. Only three installed boundaries reach its largest merge
tier, so this run does not establish a complete-cycle plateau. The eager input
fixture also reaches 4.08 GiB RSS and fails the existing 3 GiB fixture gate.
This failure is preserved rather than addressed by raising the limit.

The replay harness now compresses each committed transaction and independent
expected bag using the existing Zstandard dependency, releases the raw input
JSON after parsing, and decodes only the current transaction before timing its
execution. It compares one oracle row at a time outside the measured section. Every periodic
oracle, initial/final validation, duplicate/positive-weight validation and cold
reopen check remains. The public state-row ceiling increases to 1,048,576;
production ingestion and resident budgets are unchanged. Fixture RSS continues
to include parsed transactions, integrated output and the in-memory object store,
so it is not a worker RSS measurement.

Decoded-page eviction now keeps an ordered recency set instead of searching all
resident pages for every eviction. It preserves exact LRU behavior, including
saturated-clock identity ties. Resident allocations no longer need a weak record;
weak tracking covers only externally owned nonresident allocations. Statistics
still account for resident and evicted ownership, duplicate admissions and
re-admissions. The extra recency node and identity copy are bounded by the decoded
entry cap, but are outside the existing modeled row charge, which is not an exact
heap or RSS bound. This removes repeated scans without changing tuple contents,
object format or publication order.

A private compressed-output warming experiment was rejected. It retained generated
pack windows only when the complete sibling output fit one bounded workspace,
and fault/cancellation checks passed. However, the matched fixed-state late
window increased fetched foreground bytes from 152.29 to 183.93 per change,
with unchanged decoding and PUT counts. The growing late window also fetched
more bytes. The extra staging state and borrowed-PUT interface change have been
removed; their frozen binaries, failed tests and comparison evidence remain
ignored. Uploading bytes does not itself justify retaining them over established
read demands.

Object PUT, atomic PostgreSQL membership/results/progress publication, and source
acknowledgement ordering remain unchanged. Cache promotion cannot publish a view
or acknowledge source WAL. These controls remain component tests and exclude
PostgreSQL CDC/sink latency and real object-store latency. Ignored evidence is in
`artifacts/benchmarks/growing-state-20261025/`; the demo remains paused.

The next cache boundary is the complete logical navigation-key answer, replacing
per-object slices. A bounded cache is shared by arrangements, with a distinct
owning scope for each trace lineage. Incoming memory deltas update already cached
keys using full weighted tuple identity. Whole-trace physical compaction and
flush/rebase preserve those answers; subset compaction and reopen start fresh.
This avoids rebuilding a hot answer from every immutable run after physical
objects change. It adds no per-key version history or unbounded resident index.

Immutable snapshots retain distinct tags. A read from an older snapshot changes
the active cache scope rather than accepting newer rows. Reads and write-through
updates hold the lineage lock through cache access; delayed fills must still
match both the tag and scope. Pinned answers remain immutable. Only a fully
consumed successful merged-key cursor can admit an answer. Partial, cloned,
failed or cancelled cursors cannot admit incomplete results. Optional cache
size or arithmetic failures discard cached state rather than change query
results. These rules also cover speculative branches and rollback.

Exploratory logical-cache timing screens accidentally used debug candidates
against retained optimized release controls. These comparisons are invalid and
excluded; two incomplete runs were stopped and their evidence retained. They
provide no timing evidence for or against the cache design. A concurrent test
run or compilation also excludes some screens from CPU qualification. Final
comparisons must use matching optimized release builds with no compiler overlap.

The final representation uses immutable shared typed answers, avoiding JSON
encoding and decoding on cache hits. It reserves one bounded active collector
rather than charging unallocated space to every queued miss. No encoded-answer
compatibility path remains.

The default bounded S3FIFO policy remains in use. An LFU experiment was rejected
because its working-set change test retained old protected entries. Oversized
answers continue through the ordinary merged cursor without optional admission.

Typed key entries retain at least 512 bytes of modeled charge and at most
64 KiB, including vector capacity and a serialized-size estimate of row payloads.
The payload estimate is counted with a bounded writer rather than allocating
a temporary serialized answer. Cache hits clone an immutable Arc; they perform
no deserialization. Concurrent caller-owned cursors can pin evicted answers, as
with decoded page cursors, so this resident budget is not a process RSS bound.
Statistics reserves one active collector workspace from its pending byte target,
while charging actual cached-answer pins to each prepared key. No collector
exists for a queued miss until its cursor is opened.

Each lineage also records whether it has ever admitted an answer in its current
scope. Before that first admission, incoming deltas advance the tag without
hashing every key for unused cache maintenance. Resetting the scope clears this
hint; eviction may leave it conservatively true, affecting only optional work.
Write-through reuses the existing sorted two-way `ZSet::apply` merge, now shared
with the cache, instead of rebuilding a per-key map and exact accumulator.
Canonical inputs contain each full identity once, so checked addition finalizes
the same two-way coefficient. Optional cache overflow still discards the scope.
Read-hit counters exclude write-through maintenance lookups.

The ignored comparison runner now verifies frozen binary hashes and recorded
Cargo release optimization level before starting a performance comparison.
Its compiler monitor excludes overlapping compilation. Retained debug diagnostic
screens remain explicitly excluded rather than mixed into final results.

Final comparisons use optimized release builds of `500fd73` and the logical-key
cache implementation, with identical recorded inputs and no observed compiler
overlap. The production-source hashes are unchanged across the measurements and
the common gate. A later test-only addition covers replacement and churn.
The query is `COUNT(*)` and `SUM(amount)` grouped by `account_id` over the existing
`entry(id int4, account_id int4, amount int4)` fixture. Amounts include NULLs.
All four paired workloads below restrict existing-row changes to the first
4,096 source rows; appended rows remain cold. With 12 rows per group, this is
about 342 hot maintained groups, not uniform million-row churn. The additional
million-distinct-group case also uses this 4,096-row hot set. The separate
262,144-group control changes existing rows uniformly.
CPU below is component process CPU per 1,000 affected source changes; it includes
fixture output integration and maintenance, and excludes explicit oracle checks.
Preparation and maintenance counters measure elapsed phases, not exclusive CPU.

| Recorded workload | Late CPU ms/1k, before → after | Late preparation ms/1k, before → after | PUTs, unchanged |
| --- | ---: | ---: | ---: |
| 524,288 → 626,688 groups; 1,024 epochs; hot 4,096 rows | 175.85 → 49.69 | 152.25 → 29.15 | 1,094 |
| 1,048,576 → 1,202,176 source rows; 12 rows/group; 1,536 epochs | 27.00 → 14.00 | 22.39 → 10.74 | 1,253 |
| Fixed 1,048,576 source rows; 12 rows/group; 1,536 epochs | 18.67 → 11.50 | 16.53 → 9.00 | 1,238 |
| 1,048,576 → 1,458,176 source rows; 12 rows/group; 4,096 epochs | 28.33 → 14.50 | 25.10 → 11.36 | 3,325 |

The first growing case reduces late CPU by 71.7%, preparation by 80.9%, and total
read bytes from 2,358.69 to 17.52 per change (99.3%). Foreground GETs fall from
3.785 per 1,000 changes to zero. Its CPU grows 42.31 → 49.69 ms/1k within the new
run, rather than 81.85 → 175.85 before. The short release screen takes 12.8 seconds
versus 19.0 before, with mean preparation 24.68 versus 54.43 ms per transaction;
bootstrap is essentially unchanged at 2.39 versus 2.33 seconds. These improvements
do not override the growing case's read-byte trend and largest-tier-coverage
failures.

The fixed million-row case passes every existing growth and resource check,
including repeated largest-tier coverage. Within-run CPU is 10.50 → 11.50 ms/1k
and preparation 9.01 → 9.00. Its baseline already passes the growth checks but
fails the fixture RSS cap; compressing retained fixture history fixes that bound.
This is not evidence that the old fixed-state engine lacked a plateau.

The uniform 262,144-group control completes three matched 185,000-change intervals
with identical full merge histograms. New CPU is 291.41, 291.14 and 294.59 ms/1k;
read bytes are 258.91, 259.26 and 261.87 per change. The unchanged baseline's
297.41/300.27/302.32 ms and 259.93/256.47/260.24 bytes were already nearly flat.
PUTs remain 929 and cold-verified final state remains 4,888,672 bytes. Both versions
still fail strict short-window I/O checks with unequal maintenance histograms.
The complete-cycle control does not support a claim of sustained fixed-state
I/O growth, or a claim that this change fixed such growth.

The longer grouped growth case executes 4.096 million changes, installs 820
checkpoints without an active-state restart, and finishes in about 94 seconds.
Preparation grows 9.19 → 11.36 ms/1k, while CPU grows 11.33 → 14.50, narrowly
failing the existing 25% CPU-growth threshold. Foreground GETs remain zero and
total read bytes are 1.89 → 1.76 per change. Only two largest-tier boundaries occur
(at epochs 1,520 and 2,645), below the required four. Its final object state and
PUT count match the baseline exactly. The baseline reaches 8.71 GB peak fixture
RSS and fails the unchanged 3 GiB cap; the compressed fixture reaches 1.67 GB.
These are whole-fixture memory figures, not isolated worker RSS.

A further high-cardinality run starts with 1,048,576 maintained groups and grows
to 1,458,176 over 4,096 epochs. It completes in 310 seconds, with 820 checkpoints,
4,361 PUTs, and 22,559,627 bytes of cold-verified final state. Preparation grows
25.92 → 29.74 ms/1k (+14.7%), but total CPU grows 42.31 → 73.23. Elapsed maintenance
grows 1.04 → 14.95 ms/1k and total read bytes 5.52 → 109.75 per change; the late
windows include much larger merges. One foreground GET occurs in the late
65,000-change comparison window. Largest-tier boundaries at 2,180 and 4,035
again provide only one complete interval. All correctness and resource checks
pass, with 2.59 GB peak fixture RSS, but overall steady-state qualification fails.
There is no retained before-build comparison for this additional workload.

All growing cases retain `qualified: false`. Larger-state preparation is improved
and its remaining short-window growth stays within the current bound, but total
CPU and maintenance/I/O qualification are not solved. Comparing different merge
mixes is not a complete-cycle growth proof. Four million changes at the configured
1,000 changes per logical second represent about 68 logical minutes, not many
logical days. None of these results proves arbitrary-size or weekly restart-free
production operation. PostgreSQL CDC/publication, real object-store latency and
sink freshness remain outside this component replay.

For fast regression, the new production-path replacement/churn test uses four
signed hot keys fitting half a 4 KiB point-cache budget, continuously admits cold
keys, and replaces hot values through 512 epochs, 64 flushes and 32 explicitly
invoked whole compactions. All 1,920 hot reads after warmup hit, weighted answers
are exact, and resident charge stays bounded. Optimized execution takes 0.02
seconds after compilation. This tests cache policy and inheritance at capacity;
its invoked compactions are not natural-cycle throughput qualification.

The final `./scripts/check.sh` core gate passes 588 Rust tests, 134 Python tests,
147 live worker regression results plus rebuild, formatting, Clippy and dependency
checks, with 97.14% production line coverage. Its owned PostgreSQL fixture uses
tmpfs and has been removed. Independent implementation and test review found no
blocker. Frozen release focused checks also pass reader, trace and canonical
merge tests. The remaining investigation is larger compaction/scan work and
run/cohort fan-in, using the existing preparation profiler to distinguish engine
phases from fixture integration. Extra immutable cold state is acceptable;
repeatedly reading and rewriting it needs measured justification.

## Root-local consolidation after reverting independent checkpoint uploads

The independent checkpoint/consolidation experiment in `a4fc822` added 27–30%
PUTs in the measured component workloads. That implementation is removed,
including its background worker, task coordination, separate publication path
and associated test/report interfaces. Checkpoint flushing again merges selected
durable state with the memory delta before uploading either result. There is no
runtime option or compatibility path for the removed experiment.

The next slice narrows an existing size-tier/overlap merge to complete immutable
roots in one navigation-key window when that excludes cold object rows. It reuses
the existing resident key bounds, streaming merge/upload kernel, coarse manifest
and checkpoint publication. Every repeated reference remains a distinct weighted
occurrence. Unselected roots from an original upload remain one retained cohort
fragment; they do not become independent historical uploads for scheduling.
Selected memory stays whole, so narrowing cannot require a separate delta PUT.

A candidate window must contain at least two original cohorts and at most
131,072 input rows after removing dominant object roots. An object exceeding half
the selected rows is excluded: rewriting a large hot root for tiny peers would
only move the amplification problem. A small, genuinely overlapping local window
without balanced peers waits for more peers, uploading its delta once through the
same flush path. Isolated disjoint deltas retain normal size-tier merging; a first
test caught and corrected over-deferral that prevented those tiers from clearing.

This is opportunistic whole-root locality, not a strict bound on all maintenance
work. A root may reference many packs; rows do not bound encoded bytes. Uniform
updates, windows without profitable locality, subset coefficient overflow and the
existing 128-root pressure boundary can still require larger merges. No manifest
migration or automatic historical-data retirement is introduced. The existing
PUT → atomic PostgreSQL publication → source acknowledgement order is unchanged.

Tests cover exact cold membership including duplicate references, inclusive key
boundaries with distinct full-tuple values, cold recovery after fragmented cohort
publication, and cancellation from an excluded cohort when a subset exceeds the
i64 coefficient domain. A 256-flush dominance regression requires the hot base
and cold objects to survive tiny updates, checks that no cold payload is read,
and compares exact weighted state through repeated cold restores.

The paired targeted benchmark uses four three-root cohorts, a fixed 256-row hot
set, and 100 / 1,000 / 10,000 cold rows per outer root (800 / 8,000 / 80,000 cold
rows overall). Each checkpoint replaces one hot full tuple with another. No active
state restart occurs; fixture construction and the final independent weighted
oracle/cold reopen are outside measured counters. Both frozen binaries use release
optimization level 3. The baseline differs only by expanding the original cohort
selection directly, bypassing the new window restriction; source snapshots and
hashes record that experiment without a production policy switch.

| Cold rows overall | Checkpoints | PUTs, old → new | Uploaded bytes, old → new | Read bytes, old → new | Total flush ms, old → new |
| ---: | ---: | ---: | ---: | ---: | ---: |
| 800 | 512 | 1,025 → 1,024 | 365,986 → 360,770 | 32,193 → 32,322 | 23.10 → 25.87 |
| 8,000 | 512 | 1,025 → 1,024 | 406,816 → 360,770 | 60,145 → 32,322 | 26.42 → 25.68 |
| 80,000 | 512 | 1,025 → 1,024 | 821,768 → 360,770 | 484,611 → 32,322 | 64.62 → 25.98 |

At the largest size, read bytes drop 93.3% and uploaded bytes 56.1%, with no PUT
increase. Maximum individual flush falls from 42.45 to 0.337 ms. This does not
mean every latency statistic improves: the smallest case's total flush time rises
12%, and p50 rises from 0.0195 to 0.0357 ms. The largest case's p50 also rises
from 0.0190 to 0.0299 ms; p99 changes from 0.2138 to 0.2047 ms. More frequent,
small balanced merges trade tiny ordinary-turn overhead for less retained cold
work in the largest turn. These are unpaced in-memory-store component results,
not PostgreSQL publication latency or networked freshness measurements.

Candidate final/maximum root count stays at 16 for both 256 and 512 checkpoints;
baseline final roots increase from 10 to 11, maximum 12. The additional roots are
retained cold fragments. The largest candidate hot merge bucket runs once by
256 and three times by 512, so the hot maintenance is not merely postponed beyond
the measurement. Nevertheless, the largest baseline merge occurs only once:
these finite comparisons do not satisfy repeated largest-tier qualification or
prove arbitrary-size steady state. All six paired cases pass exact final state,
manifest roundtrip and cold-oracle checks.

The common `./scripts/check.sh` gate passes 602 Rust tests (ten manual workloads
ignored), 134 Python tests and 147 live worker results plus rebuild. Formatting,
Clippy, dependency checks and 97.16% production line coverage pass. The owned
tmpfs PostgreSQL fixture is removed with its volumes; existing application and
PoC containers are untouched. Independent review covers the reversal, root
multiplicity, fragment recovery, signed overflow, balance/deferral and request
tradeoffs. Initial failure evidence for over-deferral is retained privately.

The new fixture's optimized six-case comparison completes in under one second
per binary after compilation. Request accounting, merge-size histograms, phase
times, source/binary hashes and reproducible experiment scripts are retained under
ignored `artifacts/benchmarks/root-window-20261026/`. These are fast component
checks for iteration; the common live gate remains a separate qualification cost.

Fresh paired replays also reproduce the old request costs exactly after removing
the split upload path:

| Reused workload | Source changes | Old / new PUTs | Old / new sampled GETs | Old / new sampled read bytes |
| --- | ---: | ---: | ---: | ---: |
| 524,288 starting groups, growing hot set | 1,024,000 | 1,094 / 1,094 | 1,415 / 1,415 | 25,875,088 / 25,875,088 |
| Million-row grouped source, growing hot set | 4,096,000 | 3,325 / 3,325 | 3,350 / 3,350 | 9,552,365 / 9,552,365 |

Final state inventories also match exactly: 10,176,629 bytes / 36 objects and
2,080,100 bytes / 35 objects. Maximum run count is ten in both policies. Both
cases pass compiled-query correctness, cold recovery and unchanged resource
limits. PUTs include bootstrap and final drain; sampled GETs/bytes stop before
final drain/cold verification. All six timed processes (six-case fixture per
binary plus two longer traces per binary) record no compiler overlap. Measured
CPU changes by -0.8% and -1.6%, preparation elapsed time by -1.1% and -1.7%; these
single paired runs do not establish a general throughput gain.

The longer cases expose no separable cold roots for this optional selection, so
unchanged I/O is expected. They retain 60 and 250 complete maintenance intervals,
but repeated-largest-tier coverage is still insufficient. Read-byte growth in
the first case remains 5.52 → 17.52 bytes/change. The second still fails maintenance
and CPU growth checks, plus strict zero-baseline miss/decoding checks. These are
not hidden or reclassified as a plateau. Whole-root windowing is a useful local
step; subdivision/sharing within large roots and bounded maintenance scheduling
remain necessary for broader growing-state qualification. No push, deployment or
demo restart was performed.

## Short maintenance acceptance profile

Run the next storage changes against this fixed profile before repeating the
larger qualification:

```sh
python3 scripts/fast_validation.py --mode maintenance --release \
  --output artifacts/fast-validation/maintenance-001
```

The directory must be new. The command deliberately exits nonzero today: it
reproduces remaining failures rather than approving the current engine. Rust
execution/correctness failures and performance failures are separate evidence;
`assessment.json` retains each performance check and `result.json` records the
command outcome. Failed artifacts and the frozen executable remain available.
The measured stages took 0.34 seconds for cold-root scaling and 29.31 seconds for
SQL replay after compilation. Workload generation, build time and assessment are
additional. The run requires actual release optimization level 3, records the
executable/build identity, and rejects observed compiler overlap.

The first layer preloads four comparable cohorts containing 256 hot identities
and increasing cold state. The partitioned layout places cold ranges in separate
roots; the broad layout puts hot and cold rows in the same root. One fixed
full-tuple replacement uses normal `Arrangement::flush` selection and must
produce a naturally selected installed merge. Every case records exact
before/after membership, verifies the weighted oracle and reopens with a fresh
writer/cache. These are independent preload cases, explicitly not continuous
compaction cycles.

| Cold rows overall | Layout | Read bytes | PUTs | Uploaded bytes |
| ---: | --- | ---: | ---: | ---: |
| 800 | partitioned | 987 | 2 | 4,085 |
| 80,000 | partitioned | 987 | 2 | 4,085 |
| 800 | broad | 4,173 | 3 | 6,762 |
| 80,000 | broad | 456,290 | 3 | 462,551 |

The broad-root read/upload scaling fails the declared fixed-change bound: late
bytes may exceed the smallest case by at most 25% plus 16 KiB for coarse index
work. PUT counts have no size-dependent allowance. Timing is reported but does
not decide this deterministic work check. This isolates the next storage problem:
root-local selection works, but it cannot preserve a cold region within a root.

The second layer reuses `fast_replay.endurance` and the actual compiled query:

```sql
SELECT account_id AS group_key, COUNT(*) AS rows, SUM(amount) AS total
FROM source.entry GROUP BY account_id;
```

All three cases execute 512 transactions of up to 64 affected source rows on one
live state, without restarts or forced full compaction. They preload 512 fixed
rows, 8,192 fixed rows, and 8,192 growing rows. Seeded initial entropy matches
updates. A 128 KiB decoded cache and 128 ms logical checkpoint cadence deliberately
bring the state/cache and maintenance boundaries into a short run. These settings
are scaled pressure, not the live demo's capacity or checkpoint cadence.

Logical time advances only for actual affected source DML at 1,000 rows/s. The
cases execute 32,768 / 32,768 / 30,161 source changes, representing 32.768 / 32.768 /
30.161 logical seconds respectively. Missing deletes do not invent source work.
The growing source ends with 9,930 groups. There are no artificial idle-day jumps;
this does not execute many logical days of a 1,000-row/s stream.

Test-only, opt-in probes record exact input rows of successful production merges
in a bounded, shared collector. Overflow fails closed. Every sample's events must
reconstruct the cumulative production merge-size histogram. Witnesses are taken
only at installed checkpoint boundaries without a pending upload. At least four
witnesses must be within 25% of the largest exact input size, including any new
largest merge observed in the final drain. All intervening work is charged; no
small or unmatched merges are omitted from comparison windows.

The 512-row control passes with 81 comparable witnesses (1,112–1,162 input rows).
The 8,192-row fixed case has five (16,400–16,590), and the growing case has five
(16,669–19,006). Both large cases retain real failures:

- Fixed-state index bytes rise from 1.12 to 1.90 bytes/change; comparable large
  cycles also show increasing index-read count per change.
- Growing-state read bytes rise from 2,027.77 to 2,592.57 bytes/change and uploaded
  bytes from 140.36 to 201.24 bytes/change. Comparable large-cycle index-read bytes
  rise from 2.39 to 5.11 bytes/change. Foreground read growth also remains failed.

The profile reuses the endurance cache, RSS, run, memtable and service-cost
checks. Checkpoint lag is bounded by the 128-row deadline plus one at-most-64-row
transaction; observed maxima are 128 / 128 / 191 rows. Final installed source
progress is exact and lag is zero. Retained roots/cohorts total 4 / 2 / 10 across
the query graph. Final drain GETs, bytes, PUTs, exact merge work and CPU/time are
reported separately, with I/O capped against two observed epoch turns
because a drain may install both an existing upload and a final capture. The last
GC observation must cover the entire cold-verified physical inventory.

Quiescence proves checkpoint uploads are complete, not that every retained
consolidation opportunity has disappeared. This fixture installs a pending
checkpoint at the next epoch; it is not a model of an overloaded production
coordinator or source WAL retention. The report retains both ordinary and
comparable-cycle growth failures instead of treating coverage as a plateau.

Total PUTs and uploaded bytes include bootstrap and final drain. Their ceilings
are pinned to the prior `b49148b` implementation: 1,028 / 2,114,565 bytes, 1,054 /
4,772,791 bytes, and 726 / 4,038,000 bytes. Current results match all three exactly.
A uniform PUT inflation cannot pass merely because early/late trends are flat.
The baseline also pins its executable SHA and exact replay hash; changing the
workload without revising the profile is rejected before execution. These are
component PUTs, not complete Cloudflare Class A billing counts.

Private evidence, including the initial report-filename collision and corrected
complete run, is retained in ignored
`artifacts/benchmarks/maintenance-acceptance-20261026/`. No engine storage policy,
production scheduling or publication order changed in this slice. No push,
deployment or demo restart was performed.

The final common gate passes 604 Rust tests (11 manual workloads ignored), 147
live worker results plus rebuild, formatting, Clippy, documentation and dependency
checks, with 97.16% production line coverage. The final Python suite passes 146
tests, including malformed evidence, hidden final work, same-tier/noncomparable
merges, uniform PUT inflation, workload changes and compiler overlap rejection.
The owned PostgreSQL container is removed with its volumes. Independent review
finds no remaining blocker.

A repeat of the complete short profile reproduces the same I/O failures, matching
PUT/upload totals and comparable-merge coverage; its measured SQL stage takes
28.64 seconds. CPU uses the existing process tick counter, so very short cycle
CPU differences have limited resolution. The reproducible I/O failures, rather
than tiny CPU timing differences, establish this acceptance failure. It remains
component evidence and does not prove production freshness or steady state.
