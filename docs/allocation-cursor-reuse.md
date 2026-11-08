# Reusable engine cursor staging and streamed output ownership

The preceding allocation passes were fast-forwarded to `main` at `cba1c86`.
The merge used a clean separate main checkout and left the original experimental
checkout's staged changes untouched. This pass starts at that exact revision,
in the existing allocation worktree on `jacob/allocation-cursor-reuse`.

## Changes

Merged trace cursors now own committed positions and a reusable staging vector.
A safe RAII guard clones run positions into staging before asynchronous work.
Successful operations swap positions; dropping the guard clears retired handles.
Errors and cancellation clear partial staging without changing committed state.
Clearing releases row/object/cache pins while retaining vector capacity for the
next operation. Public clones copy committed state and start with empty staging.
Key-cursor collector admission remains between successful staging and the
synchronous commit, preserving its existing atomic behavior.

This retains one additional vector's cursor-slot capacity between operations,
proportional to the existing cursor run count. It retains no additional decoded
page pins at idle. Run-position/upper-bound cloning still exists; this change
removes repeated vector allocation rather than changing the cursor's retry model.
It is not a general resident-memory reduction or an allocation-free cursor claim.

The streaming writer validates ordering against its pending last row. On flush,
it moves that row into the boundary witness instead of cloning an identity after
every push. Accepting another pending row drops the obsolete boundary promptly.
Reused blocks still validate their complete first/last identity boundaries. Once
resident row-count/max-weight summaries are obtained, completed pack/block
metadata moves into index upload instead of being cloned and then dropped.
The no-cache path adds no new row-count validation.

Old-state group cursors represent their empty overlay with `None` rather than a
heap-allocated empty `Arc<Vec<_>>`. Changed-state cursors still share canonical
delta storage. Public interfaces, full weighted identity, and work charges stay
the same.

Object layout, encoded records, block/root/pack limits, checkpoint cadence, and
publication/acknowledgement ordering remain unchanged. No additional object-store
operations are introduced as a tradeoff.

## Measurement and validation

The same nine existing production-path workloads used by
[the previous pass](allocation-hot-processing.md) run against a fresh baseline at
`cba1c86` and a frozen candidate. Plain and DHAT binaries are separate, use pinned
Rust/dependency records, and retain source/binary/fixture hashes. Three paired
repeats alternate versions and run serially after this worktree's builds and
common validation gate complete. Object-store operation counters are compared in
both plain and instrumented reader/application replay workloads.

```sh
workloads='reader replay spill spill_strings group processing join statistics topk'
python3 scripts/allocation_profile.py --revision cba1c86 \
  --output artifacts/allocation-round4/baseline \
  --target-dir /tmp/pgderive-allocation-round3-build --workloads $workloads
python3 scripts/allocation_profile.py --revision working-tree \
  --output artifacts/allocation-round4/candidate \
  --target-dir /tmp/pgderive-allocation-round3-build --workloads $workloads
python3 scripts/allocation_profile.py \
  --compare artifacts/allocation-round4/baseline/report.json \
            artifacts/allocation-round4/candidate/report.json \
  --output artifacts/allocation-round4/paired --repeats 3
```

Use new output directories when reproducing. Prerequisites and lock validation
are described in [the original qualification](allocation-reduction.md).
Counts include each existing workload's fixture setup and independent oracles.
Allocator requests/requested bytes are not measured copy traffic. Tracked heap
peaks differ from RSS and cache charges. Short uninstrumented CPU/RSS samples are
diagnostic, not a sustained-load throughput qualification.

New regression coverage checks reuse of both staging buffers, empty/private
staging after cursor cloning, unchanged committed positions on errors and
cancellation, release of staged row pins, and reuse of canceled staging capacity.
Existing GET retry and collector tests remain active. Writer tests cover moved
String-buffer ownership and canonical-order checks within pending blocks, across
block/root flushes, and across reused-block boundaries. Group tests cover empty
old overlays and independent pinned-cursor retries.

## Results

The final comparison ran **108 serialized processes**: nine workloads, two
versions, plain/instrumented modes, and three paired repeats. Versions alternate
within adjacent pairs. This worktree's builds and common gate finished before the
runs, and its tmpfs PostgreSQL fixture was removed after zero other client
connections remained. Uncontrolled host processes are recorded in the host-state
evidence; CPU/RSS timings remain diagnostic.

The table shows paired medians. Each version/workload's allocation request count
was identical across all three repeats. Full ranges, source/binary/dependency
hashes, operation counters, and raw evidence checksums are in
[the evidence snapshot](allocation-cursor-reuse-results.json).

| Workload | Baseline allocation requests | Candidate allocation requests | Request change | Requested-byte change |
| --- | ---: | ---: | ---: | ---: |
| Reader | 1,182,217 | 790,759 | -33.11% | -20.45% |
| Application replay | 250,173 | 243,896 | -2.51% | -0.83% |
| Forced spills control | 775 | 775 | +0.00% | +0.00% |
| String spills control | 571 | 571 | +0.00% | +0.00% |
| Group overlays | 7,272 | 7,241 | -0.43% | -0.02% |
| Chained join/statistics | 61,290 | 60,185 | -1.80% | -0.73% |
| Signed join | 1,176,252 | 1,157,648 | -1.58% | -0.52% |
| Statistics | 113,645 | 113,263 | -0.34% | -0.25% |
| Top-k | 94,407 | 94,057 | -0.37% | -0.12% |

The spill-only controls are unchanged. Reader requests fell by **391,458** and
requested bytes by **37,392,112** per workload. Disjoint allocation-site analysis
accounts for the entire reduction: reusable staging removes 319,436 requests,
writer identity copying removes 69,632, and moved root metadata removes 2,390.
The cursor-staging vector site falls from 319,488 requests to **52**, rather than
allocating on each row advance. This is site-specific allocator attribution;
renamed Position functions and inclusive callers are not counted as savings.

The reader tracked heap peak increased by roughly **0.008%**. Seven workload
peaks were unchanged; replay varied by a few bytes. Larger cursor structures also
increase some boxed async-frame sizes. Reused capacity trades idle cursor-slot
storage for lower allocation churn while clearing staged pins; these results do
not establish a general peak-memory reduction or a sustained throughput gain.

Every plain/profiled reader and replay pair retained identical storage operations
and byte totals. Reader: **66 attempted PUTs / 227,859 bytes**, **330 reads /
724,908 ranged bytes**. Replay: **84 attempted PUTs / 66,857 bytes**, **394 reads /
176,322 ranged bytes**. All 24 PUT-count/byte equality checks passed, and
read/ranged-byte counters also matched. Delete-stream batch calls stayed zero.
There was no object-operation, format, or cadence tradeoff.

The common **core** gate, `./scripts/check.sh`, passed: **213 Python tests**, **816
Rust tests**, 14 intentionally ignored manual Rust tests, strict Clippy,
rustfmt/source-length checks, all required sequential live PostgreSQL/worker
checks, and cargo-machete. Production line coverage was **97.02%** (34,421 /
35,477 lines). Full release qualification was not run. Independent sub-agent
reviews approved the implementation and measurement method before committing.
The guide as it existed during the gate, final gate log, fixture/cleanup record,
host state, source snapshots, raw profiles, and normalized site attribution are
preserved under the ignored artifact directories and checksummed in the proof.
