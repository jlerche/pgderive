# Adaptive epoch scheduler: initial performance baseline

The implementation is commit `ad394c0`. The previous implementation is
`0f774fc`. Both release executables were copied before benchmarking and hashed;
the Rust source and Cargo metadata in the candidate package match the
implementation commit. The [machine-readable baseline](../scripts/performance/baselines/adaptive-epochs.json)
records binary hashes, host, image identities, effective-setting fingerprint
hashes, individual runs and median measurements.

Three parallel engines each received 10,000 single-row autocommit UPDATEs over ten
seconds, repeated three times sequentially. They used independent PostgreSQL and
SeaweedFS instances with identical latency proxies: seed 42, GET p50 26.13 ms,
PUT p50 69.75 ms. Initial state was 12,001 rows and 1,001 groups. The query groups
by book ID and maintains COUNT and SUM. The source producer never waits for the
engine. Sink visibility is observed every 10 ms using a monotonic aggregate
marker, including publications that combine multiple transactions.

The host was a Ryzen 9 7950X, 32 logical CPUs and approximately 123 GiB RAM.
RisingWave used the pinned single-node image in the baseline, one worker,
100 ms barriers, every barrier a checkpoint, and an 8 GiB memory cap. Pgderive
used its default 50 ms adaptive freshness objective, arrangement-run threshold
16 and five-second checkpoint interval. These policies and engine memory budgets
differ. This is a measurement of this PostgreSQL CDC/external-sink configuration,
not a general ranking of the products or their best achievable latency.

## Repeated single-row autocommit results

Values are medians of the three run-level measurements. All engines accepted
1,000 source writes/s. Source acceptance is not engine consumption throughput.

| Metric | Previous Pgderive | Adaptive Pgderive | RisingWave |
|---|---:|---:|---:|
| Visible marker prefix/s at input end | 55.2 | 995.1 | 947.2 |
| Sink visibility p50 | 81,049 ms | 37.6 ms | 481.7 ms |
| Sink visibility p95 | 153,230 ms | 147.7 ms | 714.0 ms |
| Last-marker drain after input stopped | 161,388 ms | 15 ms | 561 ms |
| Engine process CPU per source transaction | 1.804 ms | 0.160 ms | 1.149 ms |
| Sampled peak engine RSS | 56.9 MiB | 58.1 MiB | 798.6 MiB |

The old worker is overloaded, so its large latency reflects backlog accumulation.
Do not interpret the p50 ratio as a steady-state latency speedup. Adaptive Pgderive
kept pace with this short 1,000/s workload. Three ten-second runs do not establish
long-term growth behavior or maximum capacity.

Adaptive p50 visibility was 37.72, 37.32 and 37.59 ms across the repeats. Median
batch size was 46 source transactions, with about 208 publications per run versus
10,000 previously. There were 18 maintenance events versus 869 previously; counts
include post-input drain and shutdown. The checkpoint policy is unchanged, but
larger epochs amortize both publication and retained arrangement runs.

Adaptive processing p50 was 3 ms per epoch. Checkpoint maintenance p50 was about
154 ms, with p95 about 223 ms. These stalls remain a contributor to visibility
tails; the 50 ms objective is not a guarantee. Processing and RisingWave barrier
timers measure different work and are not interchangeable freshness metrics.

An earlier thirty-second probe accepted 30,000 writes per engine. Adaptive
Pgderive and RisingWave completed and matched their native oracles; the old
worker could not drain within five minutes. That run overlapped the correctness
gate and is retained as failure evidence, excluded from these successful repeats.

## Additional profiles

These are single runs, so they are provisional coverage rather than statistical
regression thresholds.

| Profile | Source input | Previous sink p50 | Adaptive sink p50 | RisingWave sink p50 |
|---|---|---:|---:|---:|
| Sparse updates | 1/s for 10 s | 6.4 ms | 6.1 ms | 652 ms |
| Distributed updates | 100/s for 20 s | 7,791 ms | 38.1 ms | 749 ms |
| Mixed inserts/updates/deletes | 100/s for 20 s | 7,196 ms | 37.7 ms | 760 ms |

Sparse traffic confirms there is no fixed batching delay. The distributed profile
uses the sum of all output groups as its observation marker. The mixed profile
samples one third of commits for latency; inserts/deletes are checked by the final
native oracle, not assigned inferred latency. Its source grows from 12,001 to
12,668 rows and from 101 to 668 groups. All three engines finish with the same
source digest and native result.

## Correctness, cleanup and reproduction

All six successful comparisons passed final PostgreSQL result equality; Pgderive
also passed `acknowledged LSN <= checkpoint LSN <= published LSN`. The separate
adaptive crash harness grouped 40 source transactions into three epochs, survived
two crash replays without changing sink tuple identities, and rejected a missing
journal boundary without sink writes. The common gate passed 400 Rust tests and
live qualification with 97.15% production line coverage. Independent review
approved the implementation and benchmark tools.

All run-owned containers, child processes and successful scratch directories were
removed. No Docker volumes were created. Raw evidence remains in ignored
`artifacts/performance/adaptive-repeat-{1,2,3}` and
`artifacts/performance/adaptive-profile-{sparse,wide,mixed}`; failed probe evidence
is preserved separately. `artifacts/performance/README.txt` identifies immutable
binaries and commands for another local session.

To reproduce the repeated input profile with new output directories:

```sh
for run in 1 2 3; do
  python3 scripts/performance/run.py \
    --baseline artifacts/performance/binaries/pgderive-0f774fc \
    --candidate artifacts/performance/binaries/pgderive-ad394c0 \
    --risingwave --duration 10 --rate 1000 --window-seconds 5 \
    --sample-seconds 2 --drain-seconds 300 \
    --output "artifacts/performance/epoch-recheck-${run}"
done
```

Use [Performance qualification](performance-benchmarks.md) for commands, metrics,
comparison rules and the concrete remaining query, growth, recovery and resource
profiles. This initial baseline is advisory. It does not enable a hard CI timing
gate or claim the comprehensive qualification backlog is complete.
