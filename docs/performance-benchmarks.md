# Performance qualification

The local suite compares immutable baseline and candidate Pgderive executables,
optionally alongside a digest-pinned RisingWave single node. Each engine has its
own PostgreSQL database, SeaweedFS instance and seeded object-storage latency
proxy. Paired engines run sequentially by default, with each fixture fully
removed before the next starts. Each producer runs independently of sink
publication. Every
measured DML statement affects exactly one row. Source transactions are either
single-statement autocommits or an explicit deterministic mixture through 1,000
statements; pacing always targets row changes per second.
This is a performance experiment, not a replacement for crash/replay tests.

## Run

Linux, Docker and Python 3.11+ are required. Install `psycopg[binary]>=3,<4` in your
Python environment. Build release binaries before starting the experiment and
copy the baseline executable to an ignored directory before rebuilding.
The harness records executable hashes rather than inferring their revision from
its own Git HEAD. Sequential mode needs memory for one PostgreSQL/SeaweedFS
pair and one engine at a time; optional RisingWave uses up to 8 GiB. Explicit
`--execution-mode concurrent` needs all fixture pairs and is diagnostic only.

```sh
python3 scripts/performance/run.py \
  --baseline artifacts/performance/binaries/baseline \
  --candidate target/package-release/x86_64-unknown-linux-gnu/release/pgderive \
  --risingwave --duration 600 --rate 1000 --transaction-mix varied \
  --output artifacts/performance/autocommit-001

python3 scripts/performance/matrix.py \
  --baseline artifacts/performance/binaries/baseline \
  --candidate target/package-release/x86_64-unknown-linux-gnu/release/pgderive \
  --scenarios steady-autocommit varied-transactions bursty-varied varied-growth wide-autocommit storage-stress --repeats 5 \
  --risingwave --output artifacts/performance/matrix-001

python3 scripts/performance/compare.py \
  artifacts/performance/matrix-001/steady-autocommit-*/report.json --gate
python3 -m unittest discover -s scripts/performance -p 'test_*.py'
```

Use a new output directory for every run; existing evidence is never overwritten.
The default proxy uses the existing `scripts/s3_latency_proxy.py` distribution:
approximately 26 ms GET and 70 ms PUT medians. `--proxy-scale 0` removes injected
delay, and larger scales stress storage. PostgreSQL defaults to `postgres:17`;
use `--postgres-image postgres@sha256:...` for cross-host image reproducibility.
All resolved image IDs are recorded. Both object-store and RisingWave images are
pinned by digest in the harness. RisingWave barriers default to 100 ms with every
barrier a checkpoint; change this explicitly with `--rw-barrier-ms`.

## Workload and measurement contract

`updates` changes a single marker row on every source transaction. Its grouped
sum increases monotonically. An observer associates each source commit with the
first sink observation that includes its marker, including when the engine
consolidates several commits into one publication. This measures a hot-key
workload; it is intentionally not representative of wide-key work.

`wide-updates` rotates updates across the initial source rows. Each update adds
one to quantity, so the sum of all sink groups is a monotonic commit-prefix
marker. The observer aggregates the sink on each poll; this adds work proportional
to output group count and must be kept constant in comparisons.

`mixed` alternates marker updates, inserts and a mixture of deletes and additional
inserts. New books and source rows grow without a cap. Every statement still
changes exactly one row. Marker updates occur every third statement; a transaction
is sampled if it contains a marker update. Its last marker represents that complete
commit even when inserts/deletes follow it inside the same transaction. Insert-only
and delete-only transaction visibility is **not** inferred from that sample. Final grouped
results are compared against a native PostgreSQL aggregation for both engines;
RisingWave's native materialized view is also checked.

The producer stops at the requested wall-clock duration. If it cannot sustain
the offered rate, its actual row-change and transaction counts and achieved rate reveal that
shortfall; it does not extend the experiment to disguise overload. `--transaction-mix autocommit` is the default. `varied` cycles through
`1,2,1,8,1,32,1,128,1,512,1,4,1,16,1,64,1,256,1,1000` row changes per commit.
Half of source commits are single-statement autocommits; larger transactions use
explicit transaction blocks. The final transaction is shortened to the offered row
budget. Row changes include repeated updates of a key inside one transaction,
which the engine may consolidate before evaluation. `--burst-seconds 1` emits each
second's offered load as a burst. An in-flight transaction finishes atomically even
if its commit acknowledgement falls beyond the input window. Reports distinguish
row changes/s, transactions/s, sampled transaction visibility and DBSP epochs. All connections have 10-second statement and 3-second lock timeouts, plus a
3-second connection timeout. Producer statements and complete source transactions permit up to 15 seconds. Final drain
time is bounded, and workload connections are canceled/closed during cleanup.
An engine may publish before the producing client records commit acknowledgement;
negative observed differences are clamped to zero and are not pure server timing.

`report.json` schema version 3 contains per-engine:

- Offered parameters in the adjacent manifest, achieved row changes/s, commit count and transaction-size distribution.
- Commit acknowledgement to PostgreSQL sink visibility p50/p95/p99/max.
- RisingWave native visibility separately from external PostgreSQL sink visibility.
- Per-window visibility, Pgderive processing, receipt-to-sink and background checkpoint wall-time distributions.
- Periodic process CPU, RSS, physical object counts/bytes and retained source WAL.
- Final source digest, exact native result equality and Pgderive durable progress
  ordering (`acknowledged LSN <= checkpoint LSN <= published end LSN`).

Source acceptance is distinct from engine consumption: a producer can commit
1,000 rows/s while the sink falls further behind. Use the read-only summarizer
to derive observed marker progress at the end of the input window and last-marker
drain time from saved timelines:

```sh
python3 scripts/performance/summarize.py \
  artifacts/performance/matrix-001/steady-autocommit-*/report.json
```

It reports input acceptance and consumer marker progress separately, plus latency,
CPU, resource peaks, canonical input-window engine/checkpoint distributions,
growth trends, and object request attempts.
Object accounting includes bootstrap, drain, GC, failures and retries; HTTP-method
Class A/B estimates are diagnostic, with ambiguous POST operations unclassified. Mixed-workload marker
progress is a lower bound and cannot prove visibility of trailing inserts/deletes;
final native equality checks verify those rows. Each source transaction also
returns `pg_current_wal_insert_lsn()` from its last DML statement, without an extra
network round trip. This WAL fence precedes that transaction's commit; it is not
labelled a commit LSN. Final Pgderive drain additionally waits until its published
source boundary covers the last fence, proving consumption even when trailing
unmarked changes have net-zero output. Last-marker drain is not a claim
of per-row delivery or whole-source-transaction external sink atomicity.

Engine events and completed proxy attempts carry monotonic timestamps. The
summarizer reports storage attempts by measurement window, separately from totals
that include initialization and shutdown. The manifest fingerprints the exact
harness source files and execution settings. Comparison and summary tools accept
only the current evidence schema; rerun earlier experiments rather than converting
them or guessing missing fields.

Raw engine events, RisingWave Prometheus snapshots, proxy logs, source commit and
visibility timestamps, fixture metadata, and container logs remain available for
analysis. Processing timers and barrier timers measure different work; do not
compare them as interchangeable end-to-end freshness distributions.

## Regression policy

Run the same scenario at least three times on the same quiet host. Keep duration,
initial state, group cardinality, proxy scale, image IDs and resource allocation
constant. `matrix.py` alternates baseline-first and candidate-first pairs. Gate
mode requires both orders, verifies their actual fixture lifetimes do not overlap,
and checks equal final source digests. Explicit concurrent mode remains diagnostic.
Before, during and after each run, sampled normalized host load must stay below
`--maximum-host-load-per-cpu` (default 0.25) and no competing performance fixtures
may be present. These observations do not exclude every unrelated host process;
use a quiet dedicated runner for qualifying baselines. Current Pgderive
RSS is uncapped while the RisingWave cap is explicit; memory measurements are
observations rather than equal-budget claims.

`compare.py` compares run-level medians using a practical relative/absolute margin
and three times run-to-run median absolute deviation. It reports possible
regressions; `--gate` makes performance findings an exit-code gate. Gate mode requires at least three controlled repeats, matched offered load, and no possible regressions; exploratory comparisons cannot pass. A single run is
inconclusive regardless of how many rows it contains. Latency verdicts are invalidated unless both source producers achieve the offered
row rate globally within the configured 2% tolerance and achieve comparable row
counts. Each measurement window permits the greater of that tolerance or one
maximum transaction quantum to account for atomic commit boundaries. For varied
transactions use windows long enough that 1,000 rows are a small fraction of the
window's offered load. Source acceptance and consumer capacity are separate checks. Gate mode additionally
requires the candidate's observed marker prefix to cover at least 98% of offered
rows by input end and its final marker to drain within 1,000 ms. Adjust these
explicitly with `--minimum-consumed-fraction` and `--maximum-drain-ms` for a declared
qualification policy. Mixed trailing non-marker transactions still require the
final native equality and published WAL-fence checks; marker drain does not
prove individual delivery. Actual resolved
image IDs, host CPU/memory/affinity, fixture resource limits, executed engine
configuration, PostgreSQL runtime settings and the proxy startup latency profile
must match
across repeats; `--exploratory` allows diagnostics with explicit exploratory
labels. Correctness and cleanup failures are hard failures. Check achieved throughput before accepting a latency
improvement. Inspect last-window versus first-window latency, RSS, physical state,
WAL retention and backlog rather than relying on one overall percentile.

`metrics.py` is the shared extractor used by both comparison and summarization.
It computes quantiles from raw observations, never by averaging window quantiles.
Input-window metrics remain separate from initialization and shutdown totals:

| Gated measurements | Scope |
| --- | --- |
| Visibility p50/p95/p99; processing and receipt-to-sink p50/p95 | Source commits or engine events within the input interval |
| Checkpoint p95 and total checkpoint milliseconds/change | Completed background maintenance events in the input interval; overlapping wall time is work, not a foreground stall fraction |
| Peak RSS, state bytes, retained WAL and pending marker-prefix changes | Input, drain, and combined measurement/drain peaks are compared separately; periodic samples can miss transient peaks |
| Final pending source changes | Zero only after native equality, marker drain and published source WAL-fence coverage |
| Class A, Class B, all attempts and upload bytes/change | Completed input-window proxy attempts normalized by accepted source changes; full lifecycle totals remain separately visible |
| Early-to-late growth | Signed changes between the first and last thirds after warmup, covering freshness, processing, receipt, checkpoints, queue, RSS, state, WAL and operations/change |

Growth qualification requires complete equal-duration windows, at least six steady
windows after `--warmup-windows` (default one), and sufficient raw observations.
Overall timing metrics need at least 20 marker/processing/receipt observations,
three checkpoints and three resource samples. Each growth third needs at least
20 marker/processing/receipt observations, one checkpoint and two resource samples.
Missing evidence fails qualification. The catalog declares the six required `qualification_profiles`; matrix preflight
rejects insufficient window coverage before starting any fixture. The 300-second
storage/capacity profiles use 30-second windows; 600-second profiles use the
60-second default. Short smoke runs remain useful diagnostics but cannot pass
the performance gate. Unknown request sizes and transport errors
also prevent qualification.

The default practical margins are 2 ms for timing distributions, 1 microsecond of
CPU/change, 0.005 checkpoint ms/change, 1 MiB RSS, 64 KiB physical state, 16 MiB WAL,
one transaction quantum for queue counts, 0.0001 operations/change and 0.1 uploaded
bytes/change. Each comparison also uses the larger of the 10% relative margin and
three times run-level median absolute deviation. Signed growth comparisons retain
decreases instead of clipping them to zero. These margins are explicit initial
policy, pending repeated controlled baseline evidence, rather than measured noise
claims. Sampling continues through native/source-progress drain; shutdown resource
qualification belongs to the separate recovery soak.

The catalog provides smoke, sparse, 1,000/s steady, mixed growth, large retained
state, elevated storage latency and 2,000/5,000/s capacity scenarios. These are
starting points for a broader qualification matrix:

| Axis | Required coverage | Status |
|---|---|---|
| Transaction overhead | Single-row autocommit, sparse to overload | Executable catalog |
| State growth | Initial rows/groups, growing inserts/deletes | Executable catalog |
| Storage | No delay, realistic delay, elevated delay | Executable catalog |
| Scheduling | Periodic bursts and varied transactions through 1,000 rows | Executable catalog; idle recovery and changing rates remain to add |
| Query shapes | Projection, joins with both sides changing, DISTINCT, extrema, windows | Extend fixtures using existing correctness portfolios |
| Distribution | Uniform, Zipf/hot keys, wide fanout, nulls, wide rows | Hot key and growing groups available; expand |
| Durability | Restart/replay, storage failures, slow sink, checkpoint recovery | Existing durability harnesses; add timed profiles |
| Resource budgets | CPU isolation, engine memory pressure, spill, bounded WAL | Observations available; controlled budgets need extension |
| Soak | Hours of growth, checkpoint/GC cycles, bounded retention | Increase duration; schedule on dedicated hardware |

Do not enable a nightly performance gate until these scenarios have repeated
baseline distributions and known observer overhead. Keep correctness failures
blocking independently of performance noise.

## Ownership and cleanup

The suite creates unique run-prefixed containers and databases. PostgreSQL and
SeaweedFS data live in tmpfs; RisingWave's local store is tmpfs. No persistent
Docker database volumes or shared fixtures are used. Cleanup verifies ownership
labels, saves logs, and removes only containers created by that run, including
anonymous volumes if an image unexpectedly creates one. `cleanup.json` records
mounts and unexpected volume counts. Proxy and engine child processes are stopped. Successful runs remove local engine
scratch files; failed runs retain them as failure evidence.
Downloaded images remain cached for repeated runs; no preexisting image is deleted.

Interruptions preserve evidence and run cleanup. SIGKILL or host failure cannot
execute cleanup: use the saved run ID/fixture container list and ownership labels
to inspect those resources before removing them. Never use Docker-wide prune.
No deployment or live demo changes are made by this suite. Deployment access and
operational handoff remain in ignored `artifacts/demo-oci/SESSION-HANDOFF.txt`.

## Completing the qualification matrix

Keep each new workload deterministic, with an independent PostgreSQL oracle and
an explicit observation contract. Add these profiles in this order:

1. **Rate changes and bursts.** Replay 30 seconds each at 1, 100, 1,000 and
   5,000 autocommits/s, then 30 seconds idle and another 1,000/s interval. Add a
   10,000-transaction burst. Record time to settle, oldest pending source age,
   batch-size distribution, missed freshness objectives and backlog drain time.
   This checks adaptation in both directions rather than only steady throughput.
2. **Query cost and key distribution.** Run projection/filter, inner join,
   left join, EXISTS/NOT EXISTS, DISTINCT, COUNT/SUM, MIN/MAX and ordered top-k
   separately. Use 12,000 and 96,000 initial rows; 10, 1,000 and 8,000 groups;
   uniform keys, a 90% hot key and join fanout 1/10/100. Include changes to both
   join inputs in one transaction. Keep SQL and compiler identity in the manifest.
3. **Transaction and row shape.** Compare 1/10/100/1,000 rows per transaction at
   the same offered rows/s. Add 32-byte, 1-KiB and 16-KiB payloads, NULL-heavy
   aggregates and key-changing updates. Report transactions/s and rows/s together;
   report output changes/s separately so fanout is visible.
4. **Growth and retained state.** Run the mixed insert/update/delete workload for
   at least one hour, and a constant-cardinality churn workload for the same
   duration. Compare first/last ten-minute latency, CPU per change, RSS, physical
   object bytes/counts, logical state bytes and retained WAL. Distinguish live
   state growth from garbage collection delay; never infer logical size from
   physical bucket size alone.
5. **Checkpoint and recovery.** Time checkpoints at arrangement-run thresholds 16/64/256 and
   intervals of 1/5/30 seconds, then kill before object publication, during uncertain SQL COMMIT and
   after publication before acknowledgement. Record recovery-to-ready and
   catch-up times, replayed transactions/epochs, object requests/bytes and unchanged
   sink row versions. Reuse the existing fault hooks and preserve failed evidence.
6. **Resource and storage pressure.** Pin engine CPU allocations, use 1/2/4 cores,
   exercise small/normal memory budgets and forced spill, and run proxy scales
   0/1/3 plus a bounded outage. Measure sustainable input rate, request counts,
   upload bytes, spill bytes, checkpoint stalls and WAL growth. Use the same
   explicit budgets for repeat comparisons; report any cross-engine differences.

The executable catalog currently covers aggregate autocommit, hot/wide updates,
mixed growth, initial state size and storage delay. The profiles above are the
remaining implementation backlog, not claims of completed coverage. SQL semantic
and durability harnesses remain the blocking correctness baseline meanwhile.

Use three tiers: a short correctness smoke in ordinary CI; three repeated steady,
wide, mixed and storage profiles on a dedicated nightly runner; and the complete
query/resource/fault matrix before releases. Preserve raw distributions and
run-level medians. Establish at least five quiet-host baseline runs per profile
before selecting regression margins, then require a confirming rerun for a
performance alert. A single noisy timing must not block an otherwise correct
change or silently replace the stored baseline.

The first measured scheduler comparison and its limitations are recorded in
[Adaptive epoch baseline](adaptive-epochs-benchmark.md), with a checked-in
[machine-readable snapshot](../scripts/performance/baselines/adaptive-epochs.json).
Use the adaptive implementation as the baseline for future regression checks;
its predecessor is retained to document the transaction-at-a-time bottleneck.

## Growing state and recovery qualification

The separate soak runner keeps one aggregate circuit alive as source rows and
output groups grow. The default is three hours at 1,000 row changes/s, with the
same deterministic mix of single-statement autocommits and transactions through
1,000 rows used by the comparison harness:

```sh
python3 scripts/performance/soak.py \
  --binary artifacts/package/bin/pgderive \
  --output artifacts/performance/growth-recovery-001
```

The producer continues committing through two worker SIGKILL/restarts. The second
crash follows a witnessed checkpoint PUT hold and newer foreground publication.
A commit after confirmed worker death proves that ingestion continued before
recovery began. Restarts retain the source slot, and unchanged seed result rows
must retain their CTIDs and xmin values. Every ten minutes and at input end, the
runner pauses source writes at a complete transaction boundary and compares both
bags with native SQL using bidirectional EXCEPT ALL. Final graceful shutdown must
leave the durable checkpoint equal to published logical time with an empty epoch
journal. ACK ordering is checked throughout.

Sampling continues through verification, downtime, recovery and final checkpoint
drain. Missing process measurements are recorded explicitly during witnessed
downtime; an unexpected process exit fails the run. Samples include source and
marker progress, freshness, engine phases, RSS, WAL, journal/protection/root counts,
S3-visible object counts/bytes and backend file bytes. S3 inventory and backend
file usage are different measurements: a deleted S3 object can leave filesystem
space pending backend reclamation.

Default budgets are 4 GiB engine RSS, 512 MiB retained WAL, 4 GiB S3-visible state,
60,000 changes after the observed marker, 4,096 epoch journal rows and 1,024
protection rows. Steady windows containing at least 20 marker samples must have
freshness p50 at most 100 ms and p95 at most 250 ms. Windows crossing an intentional
crash, native check or final drain are labelled and retain their measurements;
resource and ACK budgets still apply. At least three steady windows are required.
CLI options record any changed policy in the manifest. These are declared
qualification limits, not promises for every query or data size.

A short harness check uses `--duration 60 --state-rows 120 --groups 10
--verify-seconds 15 --sample-seconds 5`. It exercises control flow and cleanup;
it does not substitute for the three-hour qualification. Owned PostgreSQL and
object-store containers use 6 GiB and 8 GiB tmpfs limits, respectively. Failure
preserves a database dump and object-store archive before cleanup. SIGTERM enters
that cleanup path; SIGKILL of the runner or host failure cannot do so.

## Measured background-checkpoint growth qualification

The three-hour aggregate qualification at engine commit `3400777` processed
10,743,871 source changes in 105,750 transactions. Active ingestion averaged
1,000.03 changes/s; 18 native checks paused source writes for 56.47 seconds total.
Both witnessed crash/restarts retained the slot and replayed the committed suffix.
The final checkpoint equalled publication with an empty journal. Independent
review recomputed source continuity, LSN ordering, clock monotonicity, restart
replay, native equality, and binary/source hashes. Owned fixtures were removed
without cleanup errors. The compact
[evidence snapshot](../scripts/performance/baselines/checkpoint-growth.json)
records parameters, recovery events, request counts and raw-evidence checksums.

| Hour | Median steady-window p95 freshness | Worst steady-window p95 freshness |
| --- | --- | --- |
| 1 | 23.30 ms | 57.22 ms |
| 2 | 24.70 ms | 28.16 ms |
| 3 | 23.57 ms | 30.78 ms |

Steady latency did not grow in this run. Intentional crash/recovery windows had
maximum visibility delays around 20 and 41 seconds and are reported separately.
Source rows and output groups reached about 3.59 million; memory was not constant.
Peak sampled RSS was 640 MiB, retained WAL 116 MiB, journal size 810 epochs and
pending marker-prefix changes 17,601. S3-visible state peaked at 19.1 MB and ended
near 10.5 MB as checkpoint generations were collected. Local backend file usage
peaked at 267 MB and includes reclaimable history. These observations qualify
this workload and duration; they do not prove unlimited-duration memory bounds
or replace controlled repeated regression baselines.
