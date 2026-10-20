# Local engine development roadmap

This work continues after M0–M8 without requiring a live deployment. All query
computation remains in-engine. Function/type expansion remains a separate backlog.
Completion requires the evidence below, not only documentation or an accepting parser.

## E1: Eight-family composition qualification

Qualify these exact families: computed projection/filter/DISTINCT; computed-key
join/grouped COUNT/SUM/AVG; LEFT JOIN/nullable-right filter/grouping;
EXISTS and NOT EXISTS/projection/grouping; grouped aggregates/derived filter/regroup;
grouped results/window aggregates; window results/derived filter/computed output;
three-source joins/computed projection/grouping. Record accepted forms and named
rejections in a machine-readable matrix. Use existing types/functions only.

Run ten deterministic seeds per family, thirty committed source transactions per
seed, plus rollback and cold restart. Histories must include simultaneous input
changes, key moves, duplicate output, NULLs, deletion and empty deltas. Compare
native materialized-view exact bags and names/OIDs/modifiers/collations after each
committed boundary, plus an independent source-model oracle. Preserve original
traces/fixture dumps; replay and same-kind transaction reduction must work for
these new families. Core runs one representative per family; the complete 80-case
matrix runs separately. Saved traces remain executable independent of generation.

## E2: Explicit expression-placement qualification

Computed LEFT JOIN equality operands and EXISTS/NOT EXISTS correlation operands
must accept side-local integral expressions with matching types and SQL NULL
behavior, including zero/one/multiple right matches. Mixed-input operands,
constant-only keys and nonintegral computed keys remain named rejections.
Computed window arguments must work in mixed aggregate sets, including FILTER,
empty frames and the scan path. Code inspection shows the unified join binder and
M6 window harness already implement these paths. E1 must independently verify
that evidence before marking these items complete; implement any exposed gap.

## E3: Measured state-access improvements

Measure larger local workloads with skewed groups and window partitions. Identify
an actual remaining expensive path, improve selected state reads or recomputation,
and retain before/after work, object request/byte and exact-result evidence.
Report unchanged costs explicitly. A favorable small wall-clock sample alone
cannot complete this item, nor can reusing M7's historical results.

## E4: Runnable local example

Provide a reproducible local setup with source SQL, configuration, sample source
transactions, native result comparison and stdout inspection. Keep application
and shared PoC data untouched; disposable PostgreSQL uses tmpfs. Include initial
run, updates, restart and cleanup commands, with a smoke check of those commands.

## E5: DBSP semantics and incrementalness audit

Audit the actual executable operator graph against weighted-bag DBSP identities:
linear map/filter/union; simultaneous-input join cross term; threshold/anti/outer
transitions; aggregate differentiation; window affected-output boundaries;
transaction time, checkpoint/compaction recovery and publication/ACK ordering.
Distinguish full-tuple identity, SQL equality and navigation prefixes.

For each operator family, record the delta equation or recomputation identity,
its source implementation, independent executable evidence, and the actual work
boundary (delta-local, affected-key/partition, or full scan). Add missing property
and locality tests. Probe fixed-size deltas while independently increasing
unaffected state and hot-key/partition size; count work/reads instead of calling
all correct output incremental. Preserve counterexamples and fix correctness
issues. Document inherently affected-output work and remaining amplification;
do not claim recursion/frontiers, constant update cost or arbitrary SQL support.

## Completion ledger at revision 5d7ff97

The requested E1–E5 scope is complete as of 2026-10-20 within these explicit bounds.
The [machine-readable evidence](engine-development-evidence.json) records the final
same-binary matrix and source/oracle fingerprints. Independent review found no
remaining actionable findings. The common gate passed 45 Python tests, 322 Rust
tests (three ignored), 127 core worker cases plus rebuild and 97.38% production
line coverage; formatting, strict Clippy, file limits and dependency checks passed.
The complete deployment/release qualification profile was not run.

| Item | Status | Evidence and remaining limits |
| --- | --- | --- |
| E1 | complete | [Eight-family matrix](composition-matrix.md): 80 final same-binary cases, 30 commits each, binary/native metadata and independent model checks, rollback/cold restart, real failure preservation/reduction/replay test; finite SQL forms, not exhaustive generation |
| E2 | complete | Computed LEFT/EXISTS paths credited and extended with zero/one/multiple weighted-match checks; mixed-window scan path retained; actual parser gaps fixed for computed DISTINCT and grouping after EXISTS/NOT EXISTS; old f58e076 checkpoints reopened without rebuild at that revision |
| E3 | complete | [Sliding integral extrema](sliding-extrema.md): exact scan/native comparisons, measured reduced recomputation, explicit unchanged cold I/O, six native type/frame cases; hot partitions and ineligible frame scans remain |
| E4 | complete | [Runnable local example](../examples/local/README.md): standalone source/configuration, native comparison, CDC, restart, stdout and owned tmpfs cleanup; default path smoke passed |
| E5 | complete | [DBSP semantics and work audit](dbsp-semantics-audit.md): equations/source review, existing signed/PoC/recovery evidence and new two-axis locality tests; reports affected-key/partition amplification rather than claiming uniform delta-local work |

Reproduction commands live beside each deliverable. Function/type expansion and
actual deployment/provider/capacity/WAL qualification remain separate workstreams.


The later [Feldera adaptation](feldera-adaptation-roadmap.md) supersedes the old
EXISTS checkpoint-compatibility result: persisted right-key counts change its
plan identity and require explicit rebuild. This ledger and its eighty-case
fingerprint manifest record the completed earlier revision, not current hashes.
