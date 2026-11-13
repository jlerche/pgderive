# Statistics validation handoff

SF10 compressed-window reuse reduced remote requests, but statistics and staging
still decoded the same cold prior pages in separate passes. The worst prior
1,001-change epoch spent 179 ms in statistics and 228 ms in arrangement staging.

Statistics now returns an opaque producer-validated output. For each affected key,
it has already checked that prior state is absent or exactly one unit-weight
value, then emits precisely the removal of that value and insertion of its
replacement. This establishes the final full-tuple coefficient bound without
another prior lookup. The proof certifies multiplicity, not callback SQL semantics.

Only the statistics producer can construct this output. It owns an immutable
Arc<Sealed> and borrows the exact prior snapshot. Staging requires both addresses
to match, rejecting other snapshots and even cloned Sealed wrappers sharing the
same underlying storage. The registry preserves the exact Arc through evaluation
and staging. The same representation covers resident and spilled deltas without
copying retained pages or introducing an unbounded per-epoch page cache.

Generic operator and input-bag validation remain in place. IndexedStatistics does
not receive this capability. Source-key proofs remain separate. Logical time,
run limits, checked memory charges, spill decoding/canonical checks, immutable
upload checksums and candidate isolation still apply. A producer failure cannot
issue a proof; a staging failure cannot publish the candidate.

Staging now has one append/upload path for proven and generic resident/spilled
edges. The redundant resident stage_bag helper and its unused accessors were
removed. No durable object format, plan revision, catalog schema, source LSN or
acknowledgement behavior changes. PUTs must still finish before PostgreSQL
membership/results/progress publication, followed by logical-slot acknowledgement.

## Fast validation

The production staging regression preloads 100,000 wide statistics and changes
513 distributed groups. It compares generic full-tuple validation with the
producer handoff, verifies exact deltas against an independent oracle, and asserts
zero additional GETs or decoded misses during proven staging. The generic stage
incurs 513 decoded misses; proven staging incurs zero for both resident and
spilled output. Release measurements were 19,825 us versus 2 us for resident
staging, and 24,533 us versus 1,648 us for spilled staging. These are local component
measurements, not real end-to-end freshness claims. The four focused tests finish
in 0.38 seconds including fixture construction in release mode.

Tests reject mismatched snapshots, deltas and time; multiple prior states for one
key; non-unit prior weights; failed object uploads; and corrupted spilled output.
Insertion, replacement and complete group deletion are checked through object
staging. Failed uploads leave the prior logical boundary unchanged, and corrupt
scratch evidence remains available until explicitly cleaned by the test.

Independent application replay passed all 18 exported cases. Fast growth replay
passed at 10k and 100k groups (32 measured updates each, zero measured PUTs);
median prepare+commit was 0.919 ms and 1.516 ms. These small random updates do not
represent dense SF10 epochs. Evidence is retained under
`artifacts/fast-validation/statistics-handoff-correctness-ready/`,
`statistics-handoff-growth/`, and the focused test logs in `artifacts/`.
The independent implementation reviewer approved the producer premise, identity
binding, generic validation boundary, spill checks and failed-upload behavior
before SF10 was started.

## SF10 verification

The common gate passed with 480 unit tests (three ignored), 147 core live worker
results plus rebuild, 97.40% production line coverage, strict Clippy and dependency
checks. Final independent review of the implementation found no actionable issue.

The real SF10 rerun retained the same query, 100k supplier groups, source dataset,
resource limits, 4,096-key source hot set and seeded storage-latency proxy as the
compressed-window baseline. PostgreSQL held 59,986,052 rows: 10,371,596,288 heap
bytes and 12,265,627,648 bytes including indexes. Both offered 30,000 changes in
300 transactions, sizes 1–1,000, over 30 seconds. Added storage delay is enabled
for that interval and disabled for bootstrap, drain and restart in both runs.

| Metric | Compressed-window baseline | Statistics handoff |
| --- | ---: | ---: |
| Completed changes in timed interval | 29,479 | 30,000 |
| Completed transactions in interval | 298 | 300 |
| Published epochs in interval | 127 | 169 |
| Processing p50 / p95 / p99 | 110 / 459 / 498 ms | 48 / 262 / 308 ms |
| Processing maximum | 504 ms | 363 ms |
| Observed-to-sink p50 / p95 / p99 | 162 / 566 / 610 ms | 67 / 322 / 426 ms |
| Observed-to-sink maximum | 631 ms | 500 ms |
| Timed CDC GET / PUT / HEAD | 66 / 20 / 40 | 66 / 20 / 40 |
| CDC plus drain GET / PUT / HEAD | 81 / 24 / 54 | 81 / 24 / 54 |
| Whole-run GET / PUT / HEAD | 1,404 / 28 / 70 | 1,413 / 28 / 70 |
| Final published decoded-cache miss counter | 47,592 | 31,555 |
| Peak sampled process RSS | 334,925,824 B | 376,975,360 B |
| Bootstrap sampled peak RSS | 264,908,800 B | 264,052,736 B |
| Bootstrap seconds | 2,913.42 | 2,867.74 |
| Sampled peak scratch bytes | 92,394,896,638 | 91,868,462,334 |
| Initial physical objects / bytes | 4 / 2,987,239 | 4 / 2,987,239 |
| Post-CDC physical objects / bytes | 28 / 4,090,085 | 28 / 4,129,542 |

Native PostgreSQL equality passed after bootstrap, all CDC and cold restart. No
worker retries. The last new publication completed 0.209 seconds before the end
of the timed interval; no source changes published during drain. Drain plus the
native full-table comparison took 6.628 seconds, mostly comparison work rather
than engine backlog. The baseline published its final 521 changes 0.133 seconds
after the interval ended. New cold restart reached ready in 1.658 seconds with
added storage delay disabled. Both completed six background checkpoints.

Processing p50 improved 56% and p99 38%; observed-to-sink p99 improved 30%. These
are per-published-epoch distributions, and adaptive epochs differ between runs.
The baseline leaves two transactions outside the timed percentile sample. The
30-second experiment demonstrates improvement on this workload, not long-term
sustained freshness. Observed-to-sink starts at engine receipt, not source commit.
Proxy counts describe completed attempts and include metadata/maintenance; they
are not cloud billing invoices. Recovery verification accounts for the small
whole-run GET difference. Physical post-CDC objects retain suffix generations,
so their byte differences do not establish a state-format or steady-state cost.

There is no added page cache in this change; the proof retains only a reference
to the prior snapshot and another Arc to the existing immutable delta. Resource
limits are unchanged. Nevertheless sampled whole-process RSS increased about
42 MB. These samples combine bootstrap/traffic/post-CDC observations and the
resource monitor, including allocator retention and runtime allocations; this
run does not establish a memory saving or explain that RSS difference. Bootstrap
still performs expensive spill/merge work. Both bootstraps overlapped validation,
so the elapsed-time difference is not an isolated bootstrap speedup claim.

The worst new 1,000-change epoch spent 220 ms in statistics, 98 ms in staging and
16 ms in PostgreSQL publication. The prior worst 1,001-change epoch spent 179 ms,
228 ms and 77 ms respectively; these are observations, not identical-epoch
microbenchmarks. The controlled fast regression establishes elimination of the
second statistics decode pass. Remaining cold decoding during the first
statistics lookup and validation of other generic arrangements still contribute
to the tail; p99 is not yet below 100 ms at SF10.

Evidence remains ignored under `artifacts/benchmarks/statistics-handoff/`, including
`comparison.json`, raw event/proxy/resource histories, source snapshot and binary
hash. The measured binary SHA-256 is
`ff59db5c59ac77286fdb5b0e7a6d8a5e31631089a94a43d42dacdad1a4b66504`.
Baseline evidence is in `artifacts/benchmarks/sf10-range-read/`; gate evidence is
`artifacts/fast-validation/statistics-handoff-gate/`. Owned benchmark and gate
containers were removed, no Docker volumes were created, and scratch was removed.
The fixture cleanup.json records scratch_removed=false because the runner removes
scratch afterward; sf10/scratch.json and cleanup-verification.json confirm that
final removal. The original fixture artifact is preserved.
Datasets and results are retained. Earlier test-setup failures remain as evidence
in ignored logs; the final checks pass.
