# Allocation reduction in engine probes and callback outputs

This pass starts from `625ece1` and runs in its own worktree. It reduces temporary
ownership and vector allocations without changing object formats, packing,
cache admission, publication order or the set of object-store operations.

## Changes

- Key probes stream borrowed run/head/page inputs over an owned index range,
  rather than constructing an owned request vector. Selected cursors move into
  the existing reusable-head slots only after every read and merged-head check
  succeeds. The original run order, bounded concurrency, repeated-reader
  multiplicity and failure/cancellation rollback remain intact. A regression
  checks failure during final coefficient consolidation after successful reads.
- Group and Partition consume callback outputs through a private row visitor.
  Uniquely owned canonical rows move into consolidation; shared rows are cloned
  directly from their existing container. Neither path builds a fallback vector.
  Partition releases temporary input owners after callbacks and validation, so
  identity outputs can become uniquely owned. Clone-count tests check both
  ownership cases and preservation of pinned batches.
- SQL statistics reserve the known vector width once. All emitted scalar values
  and native cache normalization/accounting remain unchanged.

## Measurement and qualification

The same nine existing production-path workloads and independent oracles as the
[previous pass](allocation-cache-factories.md) run against immutable baseline and
candidate snapshots. Plain release and DHAT-instrumented binaries are separate;
source, fixture, toolchain, lockfile and binary identities are archived. The final
comparison runs three serialized, alternating paired repeats for both versions
and modes: 108 processes in total.

```sh
workloads='reader replay spill spill_strings group processing join statistics topk'
python3 scripts/allocation_profile.py --revision 625ece1 \
  --output artifacts/allocation-round6/baseline \
  --target-dir /tmp/pgderive-allocation-round3-build --workloads $workloads
python3 scripts/allocation_profile.py --revision working-tree \
  --output artifacts/allocation-round6/candidate-v2 \
  --target-dir /tmp/pgderive-allocation-round3-build --workloads $workloads
python3 scripts/allocation_profile.py \
  --compare artifacts/allocation-round6/baseline/report.json \
            artifacts/allocation-round6/candidate-v2/report.json \
  --output artifacts/allocation-round6/paired --repeats 3
```

Use fresh output directories to reproduce. The paired runs follow the common
gate; host timing/RSS samples are diagnostic. Allocation requests and requested
bytes measure allocator churn, not copied bytes or sustained throughput. Tracked
heap peaks are distinct from RSS and retained-cache accounting. Workload totals
include fixture setup and independent oracles; they are not isolated hot-loop
counts.

Reader/replay operation counters are checked in plain and profiled modes for
every paired repeat. PUT counts and attempted bytes must match exactly, as must
reads, ranged-read bytes and delete batches. Existing trace tests cover probe
GET behavior, repeated runs, canceled/failed probes and externally pinned cursors.
The common gate runs sequential live PostgreSQL harnesses in an isolated tmpfs
fixture, including transaction, durability, capacity and SQL-oracle checks.

## Results

All **108 paired process runs** passed. The following medians count allocation
and reallocation requests across each complete workload.

| Workload | Baseline requests | Candidate requests | Request change | Requested-byte change |
| --- | ---: | ---: | ---: | ---: |
| Reader | 786,531 | 786,531 | unchanged | unchanged |
| Replay | 236,901 | 234,209 | -1.14% | -0.97% |
| Spill | 775 | 775 | unchanged | unchanged |
| String spill | 571 | 571 | unchanged | unchanged |
| Group | 6,416 | 6,398 | -0.28% | -1.21% |
| Processing | 58,103 | 57,477 | -1.08% | -3.40% |
| Signed join | 1,157,648 | 1,103,111 | -4.71% | -9.72% |
| Statistics | 83,532 | 80,676 | -3.42% | -6.65% |
| Top-k | 80,197 | 79,988 | -0.26% | -1.12% |

Signed join saves 54,537 requests and 24,143,072 requested bytes. Its tracked
heap peak falls from 210,080 to 196,386 bytes (**6.52%**). Replay's median tracked
peak rises by six bytes (1,155,999 to 1,156,005); the other seven workloads' peaks
are unchanged. These results do not establish zero allocation, measured copy
traffic, an RSS improvement or sustained throughput improvement.

Disjoint initial-profile attribution identifies the signed-join savings in actual
probe-processing frames: 6,294 probe-container requests and 48,243 copied object-
reference requests. Replay's net savings are 628 probe, 1,646 Partition-output,
301 Group-output and 117 statistics-vector requests. Visitor key clones that
remain necessary are included in the net totals; compiler-frame reattribution
is not credited as savings. Fixture/oracle allocation request counts are unchanged; their tiny requested-byte
variations are included in the whole-workload totals.
This attribution explains the changes; the final paired measurements establish
the totals in the table.

All **60 storage-counter equality comparisons** match, including 24 PUT-count /
PUT-byte checks. In each reader repeat and mode: 66 PUTs / 227,859 PUT bytes,
330 reads / 724,908 ranged-read bytes. Replay: 84 PUTs / 66,857 PUT bytes,
394 reads / 176,322 ranged-read bytes. Delete batches remain zero. No added
object operation is used to obtain the allocation savings.

[The compact evidence record](allocation-hot-path-results.json) contains raw
checksums, source/binary/lock identities, measurement ranges and medians,
object counters and coverage. Raw accepted runs remain under
`artifacts/allocation-round6/{baseline,candidate-v2,paired}` in this worktree.

`./scripts/check.sh` passed on the exact candidate source: 845 Rust tests,
214 Python tests, strict Clippy, formatting, source-size and dependency checks,
and sequential live PostgreSQL harnesses. The core worker gate verified 498
result checks plus rebuild/recovery; the full release profile was not run.
Production line coverage is 35,658 / 36,796 (**96.90727%**). Sixteen manually
invoked Rust tests are ignored by the common gate; the profiler explicitly runs
its selected allocation workloads. Independent reviews found no outstanding
implementation or integration findings.

The disposable PostgreSQL fixture used tmpfs and was removed after confirming
zero other client connections. Gate provenance and current source hashes match
the frozen candidate. An arithmetic experiment with no measured allocation
benefit was removed; its earlier candidate profiles and interrupted gate are
preserved as superseded evidence, excluded from the final comparison.
