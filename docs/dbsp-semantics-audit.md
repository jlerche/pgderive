# DBSP semantics and incrementalness audit

Status: bounded audit completed on 2026-10-20 under E5 of
[the local engine roadmap](engine-development-roadmap.md). This audit concerns
the transaction-ordered acyclic runtime. It does not certify arbitrary SQL,
recursion/frontiers or constant-cost updates. Final test evidence is recorded
only after the relevant commands complete.

## Identities and boundaries

Let S be an integrated source bag at a complete committed boundary and d its
next consolidated signed delta. Every operator must emit Q(S+d)-Q(S), under the
same registered semantic environment. This identity is the reference for exact
maintenance; it does not bound work. Source PK validation is separate from generic
weighted operators. Tuple identity includes every retained field and signed
multiplicity; navigation keys never select a last writer. SQL key equality may
identify values whose observable binary representations remain different.

Map, filter and union are linear for fixed deterministic semantics:
`d map = map(d)`, `d filter = filter(d)`, `d union = dA + dB`. Projection collisions
must consolidate full output identities with exact intermediate coefficients.
Errors and final coefficient overflow abort preparation, preserving the old roots.
Implementation: `src/engine/dataflow/project.rs`, `src/engine/dataflow/expand.rs`,
`src/engine/plan/relational/circuit.rs` and the relational worker maps. Bounded expansion is
linear only for its explicitly admitted, deterministic finite expansion domain.

For a join, with both inputs pinned to the same old boundary:
`d join = dL join R + L join dR + dL join dR`. Omitting the cross term or letting one
input advance before reading the other violates simultaneous transaction semantics.
`src/engine/dataflow/join.rs` implements all three terms. Independent signed-bag
recomputation, multiplicity, final narrowing and failure-atomicity tests are in
`src/engine/dataflow/tests/join.rs`; repeated-key probe evidence is in
`src/engine/dataflow/tests/join_probes.rs`.

For each SQL left occurrence l, let m(l) be the total positive multiplicity of
matching right occurrences under SQL equality (NULL equality never matches).
EXISTS emits `L(l) * [m(l)>0]`; NOT EXISTS emits `L(l) * [m(l)=0]`. LEFT JOIN emits
all matching products when m(l)>0, otherwise one NULL-extended row per left
occurrence. A post-join filter removes rows; it must not manufacture a new
unmatched row when all matching rows fail the filter. DISTINCT emits one occurrence
per positive projected identity, including SQL NULL grouping. The implementation
differentiates the complete affected-key result, rather than applying a threshold
to each incoming signed record. See `src/worker/program/left_join.rs`,
`src/worker/program/distinct.rs`, the partition operator, and membership/outer-join
source-model tests. The matrix's left-group and membership-group families test
these identities through actual CDC and native PostgreSQL comparison.

COUNT and SUM maintain exact additive statistics where the compiled operator
selects that implementation; AVG retains sum/count and finalizes with qualified
PostgreSQL numeric scale/rounding. NULL inputs, empty global aggregates and absent
grouped keys are different cases. Nonlinear aggregate and window operators use
`Q(new affected partition) - retained old output`. `Partition::evaluate_retained`
requires matching old input/output times and a zero-preserving callback; unseeded
global aggregates cannot silently use that contract. See
`src/engine/dataflow/partition.rs`, `src/worker/program/aggregate.rs` and
`src/worker/program/partition.rs`. FILTER and empty-frame behavior must avoid
unnecessary argument evaluation, including overflow in excluded rows. M6's
expression-domain tests exercise that boundary.

Ordered windows differentiate results after source ordering/peer groups are
reconstructed. An insertion can change many following outputs; ranking and
navigation are not ordinary row-local maps. The runtime preserves occurrence
identity where positional ordering requires it and rejects ambiguous admitted
forms. M7's sliding COUNT/integral SUM/AVG only reduces frame evaluation work;
it does not eliminate full affected-partition reads.

## Work classification

| Path | Actual work boundary | Limitation |
| --- | --- | --- |
| Scalar map/filter/union/finite expansion | Incoming delta and bounded expansion | Full-tuple consolidation/spill may dominate |
| Inner equijoin | Changed keys, matching prior bags and delta cross product | Fan-out and number/layout of immutable runs matter |
| Additive grouped statistics | Changed groups/statistic deltas | Specialized lowering is not every aggregate combination |
| DISTINCT/EXISTS/NOT EXISTS/LEFT JOIN retained partition paths | Full affected key's retained input and output | Small output delta can still require a large hot-key scan |
| Generic grouped/ordered/window partitions | Full affected partition, then selected evaluator | Ineligible frames may scan each frame; ranking can revise many rows |
| Ordinary sink publication | Changed output identities/occurrences, bounded SQL chunks | Hash collisions/duplicates require heap work; million-occurrence cap remains |
| Bootstrap/rebuild | Complete source snapshot and complete result | Intentionally full computation |
| Compaction/GC | Selected physical runs and publication/root metadata | Maintenance cost is separate from logical delta cost |

The new `src/engine/dataflow/tests/locality.rs` probes a fixed one-identity delta
at hot-key sizes 1, 8 and 32, independently varying unrelated keys over 0, 64 and
1,024. It uses uncached one-row object blocks and asserts exact output. Join reads
are bounded by matching-key size plus one boundary block. Retained partition
callback visits equal the new hot bag's size, independent of unrelated keys;
input/output reads remain bounded by that hot bag plus boundary/output blocks.
These are one-run locality assertions, not evidence that increasing the number of
runs is free. They explicitly demonstrate affected-key incrementalness and its
remaining hot-key amplification, rather than calling correct output delta-local.

## Time, atomicity and physical state

Source LSNs order committed source boundaries; DBSP logical ticks are a separate
monotonic clock. All inputs to a tick use the same old arrangements. A prepared
graph cannot mutate the old visible roots, and failed/stale/foreign preparations
must not commit. Tests in `src/engine/dataflow/tests.rs` exercise pinned snapshots,
failed uploads, stale preparations, physical replacement and retry.

Consolidation/compaction must preserve the complete weighted collection across
run layouts, signed cancellation and old pinned readers. Exact intermediate
arithmetic precedes final i64 narrowing; run splitting cannot redefine the logical
finalization boundary. Existing reader/trace/compaction property tests cover
physical equivalence and cancellation. The accepted PoC source-map replay
`accepted_poc_trace_matches_independent_source_map_oracle` also runs in the common
gate; its frozen external evidence was read without modification. Fine-grained indexes remain object-local;
PostgreSQL stores coarse membership and authoritative progress.

Durable publication remains object PUT, then one PostgreSQL transaction publishing
membership/result/source progress, then verified acknowledgement. Uncertain COMMIT
requires authoritative reconciliation, not blind replay. The stdout sink prints
attempts before that commit and is deliberately not a transactional delivery
sink. Existing low-level and worker crash/commit-fault harnesses exercise these
boundaries. WAL retention and slot-loss operation are separate from this semantic
claim.

## Findings and completion

The audit found no new weighted-bag correctness defect in the reviewed operator
paths. Qualification exposed parser composition omissions, which E1/E2 corrected;
the broadening also preserves legacy column-only membership graph identities,
verified by actual prior-release checkpoint reopen. This is bounded evidence,
not a proof of arbitrary query or physical execution equivalence.

Incrementalness is mixed: map/filter/union are delta-local; joins probe matching
keys and retain necessary cross terms; retained nonlinear operators scan affected
keys/partitions. Growing unrelated state does not trigger a full bag scan in the
new one-run locality probes. Growing a hot key does increase partition work.
Selected mixed integral extrema now avoid repeated frame scans (E3), while numeric
and other ineligible frames can still exhibit quadratic recomputation. No constant
update-time, RSS or global run-count bound is claimed.

Independent review passed. The common gate passed the new two-axis locality
assertions, signed-bag and accepted-PoC replay, reader/compaction equivalence,
failure/commit recovery and the expanded worker tests. It reported 322 Rust tests
(three ignored), 45 Python tests, 127 worker cases plus rebuild and 97.38% production
line coverage. E1's final same-binary eighty-case matrix additionally passed 2,400
committed boundaries with exact native binary/metadata and independent model checks.
See [completion evidence](engine-development-evidence.json) and
[extrema limits and measurement](sliding-extrema.md). Future improvements should
use this classification to target actual affected-key/partition costs; they are
separate from the completed bounded audit.


## Subsequent direct group differentiation

The [incremental group adaptation](incremental-group-adaptation.md) adds direct
LEFT JOIN, DISTINCT and EXISTS/NOT EXISTS delta evaluators without changing
arrangement layouts. These paths still read full affected input groups, but no
longer read retained output or rebuild its complete matching Cartesian product.
The table above describes the audited baseline; the adaptation documents the
new computation bounds and the remaining input-read limitation separately.
