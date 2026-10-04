# Core DBSP and object-storage contract

The core targets weighted semantics and immutable object-backed traces. Demos
and SQL feature breadth are not acceptance criteria. The memory operators remain
a reference baseline alongside the fixed trace-backed graph; neither is a
complete DBSP runtime. Initial scope is transaction-ordered acyclic computation.

## Current audit

Full-tuple identity, signed multiplicity, simultaneous join-input cross terms,
and owned in-memory transaction staging have independent oracle checks. They do
not establish arbitrary physical-layout equivalence or storage durability.

The initial audit identified an arithmetic gap: `ZSet::from_updates` checked i64
overflow on each addition. Identical-tuple updates `[i64::MAX, 1, -1]` failed,
while `[i64::MAX, -1, 1]` succeeded. Projection collisions, grouped-count
projection, and termwise join accumulation had related exposure. The
accepted PoC RESULTS.md documents this class of defect for its i32 persisted
weights and fixes it with wider accumulation before final narrowing. Pgderive now
uses arbitrary-precision intermediate coefficients and narrows only finalized
logical collections to i64. Grouped counts finalize after incorporating prior
state, and joins finalize after combining all cross terms.

`Circuit<S>` stages clones of independent owned memory. It explicitly excludes
external effects and shared mutable state. It cannot be treated as a durable
commit protocol or used to clone mutable object-store-backed state. Backend
execution needs immutable snapshot descriptors and staged replacements.

## Implemented core slices

1. **Canonical weighted batches and arithmetic.** Define exactly when a logical
   batch is complete and when weights must fit i64. Accumulate contributions
   without rejecting cancellable intermediates, then check representability of
   finalized coefficients. Choose exact arithmetic or a proven bound; merely
   replacing i64 with unchecked wider arithmetic is insufficient for arbitrary
   join fan-out. Make permutation, partitioning, merge-tree and physical-run
   ordering irrelevant for representable finalized logical inputs and outputs.
   Temporary merges must retain exact coefficients until the declared logical
   finalization boundary, not narrow at arbitrary sub-run boundaries. A proposed
   partial compaction can exceed a bounded on-disk weight even when the complete
   trace fits; defer it or use a representable layout without changing query
   outcomes. Keep final overflow fail-closed and transaction-atomic. Cover normalization,
   map collisions, grouped counts, join products/cross terms, and retractions
   with an independent arithmetic oracle and hostile boundary cases.
2. **Immutable batches and stable arrangement reads.** Define sorted `(key,
   full value, weight)` batches, a cursor/seek contract, deterministic codecs and
   ordering, and a trace snapshot pinned to a published generation. Implement
   memory and object-backed readers behind the same logical contract. Probe
   all live runs plus the permitted staged deltas and consolidate exact full
   identities. Navigation prefixes never imply uniqueness. Use small blocks,
   range reads and object-local/resident indexes; PostgreSQL owns membership
   and coarse metadata. Corruption, missing objects and incompatible codecs
   fail closed. Verify identical reads across layouts, caches, run splits and
   consolidation, including old snapshot readers during replacement.
3. **Incremental operators over pinned traces.** Move the join off resident
   full-state maps. Evaluate both inputs against the same prior committed
   boundary using `ΔL ⋈ R + L ⋈ ΔR + ΔL ⋈ ΔR`; stage complete transaction output
   and new immutable input batches without making them visible early. Include
   grouped state in the same staging boundary. Commit visibility only after
   every node succeeds. Verify identical per-transaction deltas and integrated
   state against the independent memory oracle and recorded PoC trace under
   cold reads, injected GET/PUT failures and concurrent physical compaction.

All three slices are implemented as semantic baselines, including a fixed
trace-backed join/filter/grouped-count graph with atomic local root publication.
Object-backed replay, signed bag oracles, SQL comparisons, failure/retry checks
and physical-compaction races cover that graph. Readers use a
versioned JSON codec with explicit schema/ordering requirements; local trace
visibility is not durable catalog publication. Object writes alone do
not commit logical state. The composed grouped-join engine now publishes object PUTs followed by one
PostgreSQL transaction containing membership, result changes and source progress.
The registered source stream acknowledges only that verified durable boundary.
Catalog-generation races are validated at publication; uncertain commits require
authoritative resolution. The diagnostic listener still leaves its slot
unacknowledged. The continuous worker applies the same protocol through bootstrap, cold reopen,
bounded retries, maintenance and graceful shutdown; the common gate includes
COMMIT faults and witnessed process-kill recovery.

## Boundaries to preserve

- Source LSN and logical time are different domains. No recursive/frontier
  semantics are implied by transaction ordering or immutable run generations.
- Full-row generic weighted identity remains separate from source PK checks.
- Physical compaction replaces equivalent run membership; it does not introduce
  a logical transaction or advance source progress. Garbage collection is
  separate and must preserve objects pinned by readers/recovery.
- SQL codecs, NULL semantics and sink constraints still require explicit tests;
  a correct core will not establish them automatically.
- General snapshot/CDC bootstrap, worker recovery and operational WAL-retention
  safety remain explicit later requirements, not consequences of this audit.

Evidence read without modification: the prior PoC's README.md, RESULTS.md,
`src/bin/zset_contract_tests.rs.inc`, and recorded `zset-contract-source.jsonl`.
The existing local event fixture and oracle replay remain regression checks.
Independent review and the common 80% coverage/quality gate remain required.
