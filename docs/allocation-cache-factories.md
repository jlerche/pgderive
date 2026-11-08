# Allocation reduction in reader factories and canceled identities

This pass builds on cursor allocation changes (`905987d`), engine processing
acceleration (`a890089`), and tenant/capacity hardening (`7f5cae8`). Their integration
commit `a5c400f` has exactly the frozen benchmark baseline tree. The allocation
changes were qualified in a separate worktree, including the hardening changes,
before merging to local `main`. Nothing was pushed or deployed.

## Production changes

Reader scan/cursor factories construct only the fields they retain. Struct update
expressions previously evaluated complete constructors and then discarded their
index, points, range, metric, filter, or state allocations when overriding fields.
Explicit construction preserves the exact retained fields and ownership:

- Foreground caches are reused when enabled.
- Scan state remains private, while the intended parent index/points/filters and
  metrics retain their existing sharing rules and budgets.
- Disabled cursors keep the original shared state/index/points and fresh disabled
  filter/private range policy.
- Private scan/cursor containers preserve `capacity: None` from the previous
  constructors. Shared parent-owned caches keep their existing admission owners.
  Capacity-accounting policy is unchanged.

Bloom metadata sizing uses the existing exact JSON byte counter instead of an
allocated JSON vector used only for its length. The counter limit is `usize::MAX`,
preserving the previous unbounded sizing behavior, including metadata above
8 MiB. Metadata charges, Bloom bits, retained owners and admission remain the same.

Canceled trace identities borrow the last matching run head as their witness.
Earlier matching heads advance in their original order, then the witness advances
last. This removes temporary key/value copies without changing GET order, full
weighted identity, arbitrary-precision intermediate summation, or final narrowing.
The existing staging guard continues to preserve committed positions on errors
and cancellation and to release unpublished pins.

Left-join pairing copies eligible right fields directly into the result instead
of building and discarding a temporary map. Side tags, field ordering, nulls,
Unicode values and collision-error order remain unchanged.

No object formats, packing thresholds, checkpoint cadence or durable publication
ordering change. Additional object operations are not an optimization tradeoff.

## Measurement method

The same nine existing production-path workloads and independent oracles from
[the previous pass](allocation-cursor-reuse.md) run in immutable baseline and
candidate copies, using pinned Rust and dependency records. Instrumented DHAT
binaries and plain release binaries are built separately. Selected fixtures and
their helpers must match exactly between versions.

The baseline is an explicit frozen Git tree, protected from garbage collection
by the local ref `refs/allocation-profiles/round5/baseline-v2`. It combines the
reviewed cursor/processing integration with the copied hardening source, before
these allocation changes. Its tree identity is
`812e46206078427db6d21fc59f1baa364288147a`; it is not mislabeled as the older
worktree HEAD commit. The final evidence maps that tree to integration commit `a5c400f` and verifies
the entire tree is identical.

```sh
workloads='reader replay spill spill_strings group processing join statistics topk'
python3 scripts/allocation_profile.py \
  --revision refs/allocation-profiles/round5/baseline-v2 \
  --output artifacts/allocation-round5/baseline-v2 \
  --target-dir /tmp/pgderive-allocation-round3-build --workloads $workloads
python3 scripts/allocation_profile.py --revision working-tree \
  --output artifacts/allocation-round5/candidate-v2 \
  --target-dir /tmp/pgderive-allocation-round3-build --workloads $workloads
python3 scripts/allocation_profile.py \
  --compare artifacts/allocation-round5/baseline-v2/report.json \
            artifacts/allocation-round5/candidate-v2/report.json \
  --output artifacts/allocation-round5/paired --repeats 3
```

Use fresh output directories when reproducing. Raw profiles, logs, source and
binary hashes, dependency locks, fixture fingerprints and alternating sample
order are preserved. Three paired repeats run serially after builds and the
common gate finish. Reader/replay operation counters cover both plain and
instrumented modes and reject increased PUT attempts or attempted bytes.

Workload counts include fixture setup and independent oracles, not only hot
operator calls. Allocation/reallocation requests and requested bytes measure
allocator churn, not copied bytes. Tracked heap peaks differ from RSS and cache
charges. Short CPU/RSS samples are diagnostic, not a throughput qualification.

## Results

The final comparison contains **108 serialized process runs**: nine workloads,
plain and DHAT binaries, both versions, three alternating paired repeats. The
following are medians of allocation/reallocation requests across complete
workloads; requested-byte changes are allocator traffic, not copied-byte counts.

| Workload | Baseline requests | Candidate requests | Request change | Requested-byte change |
| --- | ---: | ---: | ---: | ---: |
| Reader | 790,967 | 786,531 | -0.56% | -1.63% |
| Replay | 243,820 | 236,901 | -2.84% | -5.53% |
| Spill | 775 | 775 | unchanged | unchanged |
| String spill | 571 | 571 | unchanged | unchanged |
| Group | 7,241 | 6,416 | -11.39% | -26.44% |
| Processing | 60,376 | 58,103 | -3.76% | -10.07% |
| Join | 1,157,648 | 1,157,648 | unchanged | unchanged |
| Statistics | 113,263 | 83,532 | -26.25% | -41.74% |
| Top-k | 94,057 | 80,197 | -14.74% | -32.97% |

Tracked heap byte peaks decreased in replay, group, processing, statistics and
top-k; they were unchanged in the other four workloads. These results do not
establish zero allocation, measured copy traffic, RSS improvement or sustained
throughput improvement.

Reader and replay counters match in every plain/profiled paired repeat,
including all 24 PUT-count/PUT-byte checks. Reader remains at 66 PUTs / 227,859
PUT bytes and 330 reads / 724,908 ranged-read bytes. Replay remains at 84 PUTs /
66,857 PUT bytes and 394 reads / 176,322 ranged-read bytes. Delete batches remain
zero. No additional expensive object operation is used to obtain these savings.

Disjoint initial-profile attribution identifies 29,040 removed factory requests
and 691 removed canceled-identity requests in statistics. Top-k removes 13,860
factory requests. Replay removes 4,675 factory, 1,980 canceled-identity, 194 Bloom
sizing and 70 temporary right-map requests. Field-string clones reattributed
from `stripped` to `pair`, and compiler-frame shifts, are not counted as savings.
The archived attribution is explanatory; the paired raw measurements establish
the final totals above.

`./scripts/check.sh` passed on the exact candidate source: 843 Rust tests,
214 Python tests, sequential live PostgreSQL harnesses, strict Clippy, formatting,
source-size checks and dependency checks. Production line coverage is
35,586 / 36,726 (**96.89593%**). Sixteen manually invoked Rust tests are ignored
by the common gate; the allocation workload tests are exercised explicitly by
the profiler. Independent implementation and integration reviews found no
outstanding findings. The disposable PostgreSQL fixture used tmpfs and was
removed after confirming zero other client connections.

[The compact evidence record](allocation-cache-factories-results.json) includes
raw evidence checksums, binary/source/lock identities, full ranges and medians,
coverage, object counters and the exact baseline commit/tree mapping. Raw runs
are retained under `artifacts/allocation-round5/{baseline-v2,candidate-v2,paired}`
in the allocation worktree. Earlier pre-fixture-fix profiles are preserved and
marked superseded; they are excluded from the final comparison.
