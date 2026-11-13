# Specialized incremental group operators

This adaptation uses Feldera DBSP's separation of specialized incremental
operators from generic affected-group recomputation as an architectural reference.
Pgderive's implementation uses its own weighted batches, immutable object
arrangements and PostgreSQL transaction/publication contracts. Upstream inspected:
`fe76f6a012bfcdb1b9fe4e0455c5774bf6d6ae38`.

## Cursor group execution

`Group` callbacks receive `GroupInput`: the affected key, complete consolidated
signed delta, limits, and access to pinned old state. `before()` and `after()`
produce cursors; the latter overlays the bounded delta on retained full identities.
They do not build complete old or new group bags. Cursors preserve position on
failure. Every changed identity is validated before a callback may stop reading,
so an invalid retraction in an unvisited tail still fails preparation.

Callbacks emit exactly `Q(new) - Q(old)`. Generic `Partition` recomputation remains
for other operators and as the independent full-evaluator test reference. The
materialized incremental callback introduced in `61d37b6` has been removed rather
than retained as a second production API.

## Selected operators

- LEFT JOIN streams `dL join R_new + L_old join dR`; the cross term occurs once.
  Its unmatched component is `L * [right is empty]`: stable absence emits only
  the left delta; presence transitions retract old unmatched rows or insert new
  unmatched rows. Products and retractions use exact intermediate coefficients.
  A one-right-row insertion into two n-identity bags uses `5*n + 2` charged
  visits/products and emits n matching pairs. It still streams the hot key's old
  and new input; it does not have a persistent ordered side index.
- EXISTS/NOT EXISTS maintain exact `BigInt` counts per right key, then project
  positive counts to a constant membership marker. Positive-to-positive count
  changes consolidate to an empty membership delta, so they never open the left
  group. Zero crossings produce the required qualifying/unqualifying left-row
  deltas. The marker sorts before left rows, allowing constant-prefix presence
  checks instead of scanning all left rows.
- SELECT DISTINCT reads old/new first representatives and emits their difference.
  This is ordered representative maintenance rather than a redundant total
  occurrence counter. It preserves SQL equivalence classes and the existing
  observable binary representative choice. Point validation covers all changed
  source identities, including unselected ones. Unchanged tails are not scanned.

The runtime does not read retained output for these direct paths. Their output
arrangements remain durable and staging still validates accumulated coefficient
overflow before publication. No work is delegated to PostgreSQL computation.

## Layout and recovery

Cursor execution itself preserves graph/arrangement identities for DISTINCT and
LEFT JOIN. Persisted existence counts add explicit `RightCounts` and
`RightPresence` IR nodes and arrangements, changing the plan's content identity.
An old existence checkpoint is rejected, not silently reused with missing counts.
Use the existing explicit `query rebuild CONFIG.toml` route for such registrations.
New count state reopens through ordinary object checkpoints. Counts may exceed
i64 even though each full-tuple multiplicity remains in its bounded domain.

## Cold-read evidence

With one-row object blocks and caches disabled:

| Probe | Baseline | Specialized/compacted | Boundary |
| --- | --- | --- | --- |
| SELECT DISTINCT, delete one unselected identity from a 256-identity group | 258 misses with the full evaluator | 6 misses with cursors | Complete query preparation; old full checkpoint restored before each measurement |
| EXISTS, positive right count 2 to 1; left sizes 8/128/1,024 | Hot-left scan avoided | 4 misses at every size | Complete query preparation; source and count state reads remain |
| First representative, 64 runs with eight matching identities | 320 misses | 5 after compaction | Operator reads; signed cancellation and unrelated keys included |
| Inner join, same 64-run state | 512 misses | 8 after compaction | Eight matching identities; required fan-out remains |

`run_locality.rs` separately varies 1/4/16/64 runs, 1/8 hot identities and 0/64
unrelated keys per run. All cases compare exact weighted results and equivalent
compacted state, with unchanged logical time. Compaction can produce multiple
nonoverlapping object roots; only one contains this selected key. Its upload and
full-state scan costs are separate from the measured subsequent query reads.

These are cold-block/work measurements, not wall-clock, RSS or throughput claims.
Join fan-out, affected-left output on threshold crossings, and overlapping-run
reads remain real costs. The later [persistent prefix-window summaries](prefix-windows.md) address that
separate item for a qualified RANGE family. See [the full adaptation scope](feldera-adaptation-roadmap.md).

## Verification

The common gate is `./scripts/check.sh`. Dedicated tests cover cursor overlays,
early-stop tail validation and retry, weighted source-model comparisons against
full evaluators, cold restart, old membership layout rejection, exact large
counters, hot-key work, cold reads and multi-run compaction equivalence.
The common gate passed 329 Rust tests (three ignored), 45 Python tests, 127 worker
cases plus rebuild and 97.39% production line coverage. Strict Clippy, formatting,
file-size and dependency checks passed. Independent review found no actionable
implementation findings. See the [qualification manifest](feldera-adaptation-evidence.json).
The complete release profile was not run; those results predate the subsequent durable prefix-window qualification.

Upstream references:
[distinct](https://github.com/feldera/feldera/blob/fe76f6a012bfcdb1b9fe4e0455c5774bf6d6ae38/crates/dbsp/src/operator/dynamic/distinct.rs),
[group transformers](https://github.com/feldera/feldera/blob/fe76f6a012bfcdb1b9fe4e0455c5774bf6d6ae38/crates/dbsp/src/operator/dynamic/group.rs),
[left joins](https://github.com/feldera/feldera/blob/fe76f6a012bfcdb1b9fe4e0455c5774bf6d6ae38/crates/dbsp/src/operator/dynamic/outer_join.rs).
