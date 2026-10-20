# M7 sliding window work

M7 completes the bounded reduction of repeated computation inside affected ROWS and
RANGE/GROUPS peer partitions. M5 measured preparation as the largest phase but
did not separate CPU from object I/O. Code inspection identifies repeated frame
construction and scanning for COUNT/integral SUM/AVG as avoidable work.

Use transient sliding count/sum accumulators as frame boundaries advance through
the already sorted occurrence vector. FILTER precedes argument evaluation; only
rows in at least one requested frame may be evaluated. Empty frames cannot touch
their argument. Count/sum state is exact and narrows at each output frame under
the existing PostgreSQL width rules. NULL, weighted occurrences, peers, computed
arguments and retractions must remain exact. Mixed extrema/distinct/navigation,
numeric SUM and other ineligible plans retain the scan evaluator.

This changes neither the persisted graph/layout nor the full affected-partition
read. It adds no durable index, provider dependency or SQL support. Temporary
state is bounded by aggregate count rather than frame length. Sorting, full
partition restore/retention and potentially large output deltas remain. Durable
publication is still PUT, atomic PostgreSQL membership/result/progress COMMIT,
then ACK. Existing compatible checkpoints must reopen unchanged.

Acceptance requires scan-versus-sliding exact comparisons, an independent source
oracle, weighted and seeded histories, empty/offset/peer frames, FILTER/overflow
and resource failures, cold restore, native-MV qualification, measured charged
work and complete-tick object read/write observations. Report evaluator timings
only from an isolated release build, with host/build/dataset provenance; no
claim of bounded delta reads or production capacity follows.

## Measured work and costs

The [recorded release samples](window-work-samples.json) retain toolchain, host,
source and binary hashes, dataset, command and three repetitions. The measured
implementation was uncommitted, so its parent Git revision is explicitly dirty.
Reproduce with a new output directory:

```sh
python3 scripts/window_work_baseline.py artifacts/window-work-NEW
```

This builds an isolated release test binary with inherited coverage/compiler/profile
overrides cleared, then invokes the same production evaluator and its retained
scan fallback. These are pure evaluator wall times, not worker CPU or transaction
latencies. Scan runs before sliding; the host is not pinned or load-isolated.
Three repetitions do not establish statistical significance or capacity.

| 1,000-row prefix case | Charged scan → sliding work | Scan wall time | Sliding wall time |
| --- | --- | --- | --- |
| ROWS | 2,003,000 → 7,000 | 355.5–370.2 ms | 4.17–4.42 ms |
| GROUPS, 19 non-NULL values plus NULL peers | 43,436 → 8,060 | 10.32–10.44 ms | 3.90–3.94 ms |

COUNT, SUM and AVG share each tested frame. NULL occurs every seventh input.
A separate 400-row controlled core test reports ROWS work 321,200 → 2,800 and
GROUPS work 17,280 → 3,260. Charged visits are an enforced evaluator work metric,
not machine instructions or total operator complexity. Sorting remains O(n log n)
and aggregate frame updates are O(n × aggregate count); frame output generation
and retained source/output reads remain additional work.

A complete cold-tick comparison uses a 400-row unbounded ROWS prefix and one
source-row replacement, with eight-row blocks and the data-block cache disabled.
Both evaluators produce the independently expected exact output delta and read
854 blocks, PUT 104 objects totaling 57,898 bytes. These are new-object and cache
miss counts during preparation, excluding restore; they are not total network
requests, peak storage or maintenance costs. The test restores a scan-created
checkpoint into the optimized evaluator and cold-restores its result afterward.
There is no new persisted accumulator/index or measured write amplification.

## Qualification boundary

The scan remains executable as the production fallback for ineligible sets and
as a test-only reference for eligible sets. Direct tests compare exact outputs
across empty, preceding/following, disjoint, clipped and peer ranges, NULLs and
weighted occurrences. Existing independent source histories cover updates,
deletes, key movement, duplicate projections, peer changes and cold restart.
The five-case `sliding_window_harness.py` runs in core/full qualification and
checks exact native-MV bags/output metadata plus an independent source model for
bigint, integer and smallint inputs, computed FILTER arguments and empty frames.
M6's guarded-overflow tests now also exercise the sliding path.

Additional tests exercise arbitrary-precision sums and a transient removal sum
outside i64 between valid final frames. Synthetic sufficient statistics check
int2/int4 SUM/AVG and COUNT narrowing without allocating billions of occurrences;
they are width-boundary tests, not source-codec acceptance tests. A full-query
work-budget test proves scan rejection preserves its checkpoint while sliding
completes under the same smaller budget. Input/output/scratch limits and exact
full-tuple validation still apply. Frozen PoC artifacts remain unchanged; this
slice introduces no new durable state representation or stateful operator.

M7 does not make arbitrary window deltas cheap. Large affected partitions still
read their full retained inputs and outputs. Numeric SUM, nonintegral MIN/MAX, DISTINCT,
navigation and other ineligible sets still scan frames; ranking, joins and
sessions have their prior behavior. Removing old contributions reevaluates their pure FILTER/argument; narrow or highly
consolidated frames can have extra evaluator work. These measurements do not
establish a universal speedup. More selective object reads require a separate
index/dependency design. No watermark truncation, source WAL solution,
new function support or live-query delivery is implied.


M7 qualification completed on 2026-10-16. Independent review passed. The common
core `./scripts/check.sh` gate passed with 32 Python tests, 300 Rust tests (two
ignored measurement tests), 110 worker result cases plus rebuild, and 97.46%
production line coverage. Strict Clippy, formatting, file limits and dependency
checks passed. Five targeted native live cases and the separate release evaluator
measurements also passed. The full release profile was not run for this slice.
M8 sink scalability, M9 deployment qualification and the function/type backlog
remain open; further selective window indexes remain additional optimization work.

E3 later extends sliding statistics to [mixed integral extrema](sliding-extrema.md);
the M7 measurements above retain their original COUNT/SUM/AVG scope.
