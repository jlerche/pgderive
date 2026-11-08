# Engine processing allocation pass

This pass starts at `8fb0cd8`, the previously qualified allocation reduction.
Changes stay in the existing processing path and preserve object formats,
checkpoint cadence, publication ordering, and source acknowledgement rules.

## Changes

- Merged trace cursors retain the selected run ordinal and consolidated weight.
  Their current key/value borrows the identity already pinned by that run, rather
  than copying it on each selection. Cursor operations still stage run positions
  before fallible asynchronous work; errors and cancellation retain the committed
  position. A fully canceled identity still needs a temporary owned identity while
  all matching inputs advance.
- Group processing walks contiguous canonical input ranges, avoiding a temporary
  map, its allocations, and cloned navigation keys. Full weighted identities,
  callback order, complete delta validation, and cumulative work charges remain
  unchanged.
- Ordered selection also borrows canonical ranges and prior/delta heads. It calls
  the selection callback before advancing either iterator, eliminating temporary
  key/value copies without changing occurrence counts, offset/cutoff behavior,
  complete changed-tail validation, or selected-prefix reads.
- Scratch merges validate each new identity against the identity just removed
  from that same run, eliminating a duplicate owned previous tuple. The codec
  streams decimal weights into JSON strings and borrows unescaped decimal strings
  while decoding. Exact wire bytes, JSON escape acceptance, checksums, length
  limits, strict run order, and arbitrary-precision coefficients are preserved.
  Common coefficients that fit `i128` format without a decimal heap string;
  larger coefficients still use the big-integer formatter and its allocations.

A broader experiment sharing canonical `Value` storage was rejected and removed:
its new allocations were not represented by existing generic retained-storage
charges. Small `BigInt` arithmetic changes were also omitted because the pinned
implementation already stores the measured small coefficients inline.

## Qualification method

`scripts/allocation_profile.py` now includes existing engine processing workloads:

| Workload | Existing production-path test and oracle |
| --- | --- |
| `processing` | Three source projections, chained joins, additive statistics, twelve signed ticks, independent bag recomputation and reopen |
| `join` | 128 histories of sixteen simultaneous signed-input ticks, independently recomputed join state |
| `statistics` | Thirty-two ticks comparing ordered and forced-spill statistics with independent group state |
| `group` | Weighted cursor overlays, retractions, cold restore, and rejected underflow |
| `topk` | Occurrence cutoffs 0, 1, 3 and 8, signed updates, independent selected-state recomputation and reopen |
| `spill_strings` | String identities and exact coefficients, three resident limits, cross-run cancellation and exact encoded-size checks |

The existing reader, application replay, and forced-spill workloads remain in the
comparison. Every selected test body and its directly used fixture/oracle helpers
are fingerprinted. Fixture sources must match between baseline and candidate.
There is one selected test per process; async profiling excludes Tokio runtime
construction and teardown. Test setup, production work, and independent oracles
are included. These are workload totals, not per-transaction engine-only counts.

Builds use separate immutable plain and DHAT-instrumented source copies, pinned
Rust and dependency records, and an external reusable build cache. The production
manifest has no profiler dependency. After builds and validation, paired runs use
frozen executables, alternate side order, and run serially. Three repeats retain
raw heap profiles, time output, test logs, source/binary hashes, and sample order.

```sh
workloads='reader replay spill spill_strings group processing join statistics topk'
python3 scripts/allocation_profile.py --revision 8fb0cd8 \
  --output artifacts/allocation-round3/baseline-v3 \
  --target-dir /tmp/pgderive-allocation-round3-build --workloads $workloads
python3 scripts/allocation_profile.py --revision working-tree \
  --output artifacts/allocation-round3/candidate-v3 \
  --target-dir /tmp/pgderive-allocation-round3-build --workloads $workloads
python3 scripts/allocation_profile.py \
  --compare artifacts/allocation-round3/baseline-v3/report.json \
            artifacts/allocation-round3/candidate-v3/report.json \
  --output artifacts/allocation-round3/paired --repeats 3
```

Use fresh output directories when reproducing. Prerequisites and profiler-lock
validation are described in [the preceding pass](allocation-reduction.md).
Allocation/reallocation requests and requested-byte volume measure allocator
churn, not actual bytes copied. Tracked heap peak differs from RSS and retained
cache charges. GNU time output is diagnostic; these short tests do not establish
sustained throughput or a production memory bound.

## Results

The final comparison ran **108 serialized processes**: nine workloads, two
versions, plain and instrumented modes, and three paired repeats, alternating
version order. This worktree's builds and common gate finished before these runs;
the isolated PostgreSQL fixture was then removed. Other host processes were
uncontrolled and are recorded in the raw host-state evidence. CPU/RSS samples
remain diagnostic.

The table uses paired medians. Allocation request counts were identical across
all three repeats for each version and workload. Minima, medians, maxima,
source/binary/dependency hashes, and raw evidence checksums are in
[the evidence snapshot](allocation-hot-processing-results.json).

| Workload | Baseline allocation requests | Candidate allocation requests | Request change | Requested-byte change |
| --- | ---: | ---: | ---: | ---: |
| Reader | 1,501,705 | 1,182,217 | -21.28% | -10.76% |
| Application replay | 263,524 | 250,173 | -5.07% | -1.61% |
| Forced spills | 1,159 | 775 | -33.13% | -0.90% |
| String spills | 767 | 571 | -25.55% | -0.96% |
| Group overlays | 7,276 | 7,272 | -0.05% | -0.10% |
| Chained join/statistics | 61,520 | 61,290 | -0.37% | -0.03% |
| Signed join control | 1,176,252 | 1,176,252 | +0.00% | +0.00% |
| Statistics | 114,781 | 113,645 | -0.99% | -0.05% |
| Top-k | 94,467 | 94,407 | -0.06% | -0.06% |

The integer-only join control is unchanged. Group, top-k, and chained processing
show small whole-workload gains because these fixtures also allocate for object
staging, cache construction, and independent oracles. String-buffer identity and
clone-count regression tests directly cover the eliminated processing copies.
The trace selection allocation site disappeared from the reader profile,
removing exactly 319,488 requests and 22,044,672 requested bytes per workload.
Stack attribution is not a sum of inclusive caller totals.

Every plain and profiled reader/replay pair had identical object-store counts and
byte totals: reader **66 attempted PUTs / 227,859 bytes** and **330 reads / 724,908
ranged bytes**; replay **84 attempted PUTs / 66,857 bytes** and **394 reads / 176,322
ranged bytes**. All 24 PUT count/byte equality checks passed. Delete-stream batch
calls stayed zero. No object-operation, layout, or checkpoint-frequency tradeoff
was taken.

Tracked heap peaks were unchanged in six workloads. The spill peaks fell by
about 0.3% and 0.6%; replay varied by a few bytes between runs. These results
establish lower allocation churn for these workloads, not a general peak-memory
or sustained-throughput improvement.

The common **core** gate, `./scripts/check.sh`, passed with an isolated PostgreSQL
17.11 tmpfs fixture: **213 Python tests**, **810 Rust tests**, 14 intentionally
ignored manual Rust tests, strict Clippy, rustfmt, source-length checks, live
PostgreSQL/worker checks, dependency checks, and **97.01%** production line
coverage (34,354 / 35,413 lines). Full release qualification was not run.
Independent sub-agent reviews approved the implementation and measurement method
before committing. Earlier incomplete/failed/superseded artifact directories are
retained and excluded from these results. The guide as it existed during the gate
is preserved alongside the final gate log.

## Remaining measured targets

Run-position vectors still allocate while staging atomic cursor operations.
A reusable staging vector needs cancellation cleanup that drops page pins while
preserving its capacity, independent cursor cloning, and unchanged cache charges.
Streaming writer order validation also retains previous identities. Sharing
canonical row storage needs an explicit retained-storage accounting change before
it can be safely adopted. None of these followups justifies extra object PUTs.
