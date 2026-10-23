# Performance qualification

The local suite compares immutable baseline and candidate Pgderive executables,
optionally alongside a digest-pinned RisingWave single node. Each engine has its
own PostgreSQL database, SeaweedFS instance and seeded object-storage latency
proxy. Producers run concurrently and never wait for sink publication. Every
measured DML statement affects exactly one row and commits using autocommit.
This is a performance experiment, not a replacement for crash/replay tests.

## Run

Linux, Docker and Python 3.11+ are required. Install `psycopg[binary]>=3,<4` in your
Python environment. Build release binaries before starting the experiment and
copy the baseline executable to an ignored directory before rebuilding.
The harness records executable hashes rather than inferring their revision from
its own Git HEAD. Leave enough host memory for two PostgreSQL/SeaweedFS pairs,
two engines, and an additional pair plus up to 8 GiB for optional RisingWave.

```sh
python3 scripts/performance/run.py \
  --baseline artifacts/performance/binaries/baseline \
  --candidate target/package-release/x86_64-unknown-linux-gnu/release/pgderive \
  --risingwave --duration 600 --rate 1000 \
  --output artifacts/performance/autocommit-001

python3 scripts/performance/matrix.py \
  --baseline artifacts/performance/binaries/baseline \
  --candidate target/package-release/x86_64-unknown-linux-gnu/release/pgderive \
  --scenarios smoke steady-autocommit mixed-growth --repeats 3 \
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
changes exactly one row. Marker updates sample one third of commits. Insert and
delete visibility latency is **not** inferred from that sample. Final grouped
results are compared against a native PostgreSQL aggregation for both engines;
RisingWave's native materialized view is also checked.

The producer stops at the requested wall-clock duration. If it cannot sustain
the offered rate, its actual transaction count and achieved rate reveal that
shortfall; it does not extend the experiment to disguise overload. All generated
transactions are single-row and source throughput is consequently transactions/s
as well as rows/s. All connections have 10-second statement and 3-second lock timeouts, plus a
3-second connection timeout. Producer DML permits up to 15 seconds. Final drain
time is bounded, and workload connections are canceled/closed during cleanup.
An engine may publish before the producing client records commit acknowledgement;
negative observed differences are clamped to zero and are not pure server timing.

`report.json` schema version 1 contains per-engine:

- Offered parameters in the adjacent manifest, achieved rows/s and commit count.
- Commit acknowledgement to PostgreSQL sink visibility p50/p95/p99/max.
- RisingWave native visibility separately from external PostgreSQL sink visibility.
- Per-window visibility and Pgderive processing distributions.
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
CPU, resource peaks, raw epoch/maintenance distributions, and object request attempts.
Object accounting includes bootstrap, drain, GC, failures and retries; HTTP-method
Class A/B estimates are diagnostic, with ambiguous POST operations unclassified. Mixed-workload marker
progress is a lower bound and cannot prove visibility of trailing inserts/deletes;
final native equality checks cover those rows. Last-marker drain is not a claim
of per-row delivery or whole-source-transaction external sink atomicity.

Raw engine events, RisingWave Prometheus snapshots, proxy logs, source commit and
visibility timestamps, fixture metadata, and container logs remain available for
analysis. Processing timers and barrier timers measure different work; do not
compare them as interchangeable end-to-end freshness distributions.

## Regression policy

Run the same scenario at least three times on the same quiet host. Keep duration,
initial state, group cardinality, proxy scale, image IDs and resource allocation
constant. Concurrent engines share host resources, so repeat with CPU isolation
or separate hosts before attributing small differences to code. Current Pgderive
RSS is uncapped while the RisingWave cap is explicit; memory measurements are
observations rather than equal-budget claims.

`compare.py` compares run-level medians using a practical relative/absolute margin
and three times run-to-run median absolute deviation. It reports possible
regressions; `--gate` makes performance findings an exit-code gate. Gate mode requires at least three controlled repeats, matched offered load, and no possible regressions; exploratory comparisons cannot pass. A single run is
inconclusive regardless of how many rows it contains. Latency verdicts are invalidated unless both source producers achieve the offered rate
in every measurement window within the configured 2% tolerance and achieve
comparable commit counts. This checks source acceptance, not consumer capacity;
inspect visible-prefix throughput and drain time from `summarize.py` before
claiming an engine sustains a rate. Actual resolved
image IDs, host CPU/memory/affinity, fixture resource limits, executed engine
configuration, PostgreSQL runtime settings and the proxy startup latency profile
must match
across repeats; `--exploratory` allows diagnostics with explicit exploratory
labels. Correctness and cleanup failures are hard failures. Check achieved throughput before accepting a latency
improvement. Inspect last-window versus first-window latency, RSS, physical state,
WAL retention and backlog rather than relying on one overall percentile.

The catalog provides smoke, sparse, 1,000/s steady, mixed growth, large retained
state, elevated storage latency and 2,000/5,000/s capacity scenarios. These are
starting points for a broader qualification matrix:

| Axis | Required coverage | Status |
|---|---|---|
| Transaction overhead | Single-row autocommit, sparse to overload | Executable catalog |
| State growth | Initial rows/groups, growing inserts/deletes | Executable catalog |
| Storage | No delay, realistic delay, elevated delay | Executable catalog |
| Scheduling | Bursts, idle recovery, changing rates, large transactions | Add trace-based producers |
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
