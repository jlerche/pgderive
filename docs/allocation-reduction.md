# Allocation reduction qualification

This pass starts at `e3d8d59`, after the earlier `6e344cf` allocation cleanup.
It keeps PostgreSQL publication, object formats, physical grouping, and durable
progress unchanged. Allocation counts describe complete existing test workloads,
including their fixture setup and independent oracles; they do not describe an
allocation-free worker or an arbitrary-query bound.

## Production changes

- Immutable PUTs transfer the owned vector into `Bytes`; the retained collision
  witness shares that payload instead of copying it. Exact existing-object size
  and content checks remain mandatory.
- Reader sizing serializes into a bounded counter instead of allocating JSON
  vectors just to obtain their lengths. The encoder and counter enforce the
  same 8 MiB limit and error.
- Consolidation creates a scratch directory only at the first nonempty spill.
  Resident-only evaluations do not create one. Successful sealing transfers the
  original directory without creating a temporary replacement directory.
- Spill writers, merge readers, and sealed cursors reuse record buffers. Framing,
  checksums, canonical-order checks, and all record/output budgets are unchanged.
  Failed disk operations retain their real scratch directory; failures before
  scratch creation report that no scratch exists.
- Integer statistics move owned coefficients; accumulator entries consume owned
  weights. Numeric bounds count the exact signed/padded display length without
  creating a second decimal string and a formatted display just for sizing.
- Merged cursor operations clone the run positions needed for atomic retry, then
  recompute their row. They omit the obsolete owned current-row clone. Public
  cursor cloning and pinned-reader behavior remain unchanged.

The scratch buffers retain their peak capacity until the operation/cursor drops.
This is an allocation-churn optimization, not a reduction in resident-map/cache
charges or a new RSS bound. Preserving retry and cancellation safety takes
precedence over mutating live cursors in place.

## Reproduce the measurements

The runner requires Linux, Python 3.12+, GNU time, and the pinned Rust toolchain.
The Cargo cache must contain `dhat=0.3.3`, its transitive crates, and index
metadata: instrumentation resolves offline. Populate a throwaway Cargo project
with that pinned dependency and run `cargo fetch` first if needed; the production
manifest is never changed. The runner uses existing production-path tests:

| Workload | Existing coverage |
| --- | --- |
| `reader` | 4,096 string rows; points, prefixes, full scans, rewrite, merge, compaction, cold recovery and oracle equality |
| `replay` | Recorded application fixtures through compiled queries, checkpoint/restart, independent expected bags and cold reopen |
| `spill` | Exact cancellation across resident limits 1, 2, 7, 4,096, including final overflow rejection |
| `numeric` | Existing numeric ordering, scale, and digit/display bounds |
| `batch` | Exact merge trees over 128 signed full-identity histories, retained as a control |

```sh
python3 scripts/allocation_profile.py --revision e3d8d59 \
  --output artifacts/allocation-reproduce/baseline \
  --target-dir /tmp/pgderive-allocation-profile-build
python3 scripts/allocation_profile.py --revision working-tree \
  --output artifacts/allocation-reproduce/candidate \
  --target-dir /tmp/pgderive-allocation-profile-build

# After both builds and other validation finish:
python3 scripts/allocation_profile.py \
  --compare artifacts/allocation-reproduce/baseline/report.json \
            artifacts/allocation-reproduce/candidate/report.json \
  --output artifacts/allocation-reproduce/paired --repeats 3
```

Each output directory must be new. Failed or incomplete evidence is retained.
Source snapshots, manifests, binary hashes, the dirty-tree patch, Cargo logs,
individual test logs, full heap profiles, GNU time output, and sample order are
kept under the output directories. The final comparison verifies frozen sources,
executables, workload/helpers, and both dependency locks before running.

Plain release test binaries have no profiler injection. A separate snapshot adds
only pinned `dhat=0.3.3`, its safe library allocator, and a guard at each selected
test entry. The original production dependency records are preserved exactly;
locked offline Cargo metadata and a locked build validate the augmented graph.
The profiler is not a production dependency. Its guard starts at test-body entry and drops after fixture destruction. For
async reader/replay workloads that excludes Tokio construction and teardown;
synchronous spill/numeric/batch workloads have no Tokio runtime.
Full heap profiles use sixteen-frame trimming. One exact test runs per process,
with one test thread and external replay/configuration/compiler overrides removed.

Reader and replay snapshots reuse the existing counted in-memory object-store
adapter. These identical measurement-only probes run in both plain and profiled
binaries; they do not change the production implementation. They report attempted
PUT counts/bytes, reads/range bytes, delete-stream batch calls, and replay inventory. The paired
comparison rejects any increase in PUT attempts or attempted PUT bytes for any
reader/replay pair, including both instrumented and uninstrumented modes.

Allocation/reallocation requests and requested bytes are allocator churn, not
memory-copy traffic. Tracked global byte peaks are distinct from RSS, retained
cache accounting, and per-call-site maxima. GNU time measures separate plain
processes; its short CPU/RSS samples are diagnostics, not a sustained-load
performance qualification or a production throughput claim.

## Measured results

The final comparison used three paired repeats for each workload and mode,
alternating baseline/candidate order. This worktree's builds and common validation gate had
finished before these 60 serialized process runs. Unrelated host builds were
active and are recorded in the raw host-state evidence; CPU/RSS remain diagnostic.
Counts below are medians;
per-run ranges, frozen-source/binary hashes, and raw evidence checksums are in
[the checked-in evidence snapshot](allocation-reduction-results.json).

| Workload | Baseline allocation requests | Candidate allocation requests | Request change | Requested-byte change | Tracked byte-peak change |
| --- | ---: | ---: | ---: | ---: | ---: |
| reader | 2,041,580 | 1,501,705 | -26.4% | -16.4% | +0.88% |
| replay | 281,354 | 263,524 | -6.3% | -2.5% | +0.61% |
| spill | 1,596 | 1,159 | -27.4% | -1.0% | +0.05% |
| numeric | 538 | 419 | -22.1% | -29.8% | +0.00% |
| batch | 14,586 | 14,586 | +0.0% | +0.0% | +0.00% |

The batch control has no measured allocation change. Other rows measure the
combined patch rather than attributing every improvement to every edit. An
intermediate candidate isolating the cursor change removed exactly 319,488
requests and 22,044,672 requested bytes per reader run; the corresponding obsolete
row-clone stack entries disappeared from the heap profiles.

Every plain and profiled pair had identical object-store counts and byte totals:
reader 66 attempted PUTs / 227,859 bytes and 330 reads / 724,908 ranged bytes;
replay 84 attempted PUTs / 66,857 bytes and 394 reads / 176,322 ranged bytes.
No PUT-frequency, object-layout, or checkpoint-cadence tradeoff was taken.
The replay fixture is small; its final checkpoint/reopen checks do not qualify
arbitrary growing-state or background-maintenance workloads.

Tracked peak bytes rose slightly in reader, replay, and spill runs. RSS samples
were mixed. These results establish lower allocation churn for these workloads;
they do not establish a peak-memory reduction, zero allocations, or sustained
throughput improvement.

The common core gate passed: 210 Python tests and 806 Rust tests passed; 14
manual Rust tests were intentionally ignored. Clippy, doc checks, sequential live
PostgreSQL/worker harnesses, and dependency checks passed, with 97.01% production
line coverage. A scheduler-sensitive
controller test now uses paused Tokio time without changing controller inputs or
production policy. Its earlier failure and incomplete/superseded measurements
remain under the ignored artifact directories and are excluded from these results.

## Remaining profile findings

The final reader still allocates while cloning its run-position vector to stage
atomic operations. Reusing staging capacity needs explicit cancellation, reader
pin, and retained-memory accounting. Streaming writer order validation also
retains a cloned previous identity per row. Those are measured follow-up targets;
changing object layout, checkpoint cadence, or extra object PUTs is not part of
this pass.
