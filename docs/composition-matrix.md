# Eight-family local qualification matrix

This implements E1/E2 of the [engine development roadmap](engine-development-roadmap.md).
The executable inventory is [queries/composition/matrix.json](../queries/composition/matrix.json).
It uses existing scalar/type implementations; unknown functions still fail binding.

| Family | Accepted finite form | Independent model |
| --- | --- | --- |
| distinct | Derived reverse/COALESCE/ABS projection, filter on computed bucket, then DISTINCT | Python set of projected tuples, including NULL labels |
| join-group | Side-local ABS equality, grouped COUNT/SUM/AVG | Nested-loop weighted matches, raw group lists |
| left-group | Side-local integral addition equality, nullable-right filter, grouped COUNT/SUM | Explicit unmatched rows only when no match existed |
| membership-group | Computed equality EXISTS/NOT EXISTS, then grouping | Right membership predicate, never right-duplicate multiplication |
| regroup | Grouped AVG, derived numeric filter, regroup with COUNT/MAX | Independent mean/filter/regroup |
| group-window | Grouped AVG followed by ordered SUM window | Independently sorted group means and frame slices |
| window-project | SUM window, derived numeric filter and COALESCE/addition output | Raw-source partitions and frame slices |
| three-join | Three source relations, computed equality/projected aggregate argument and grouping | Raw account/entry nested loop with region membership |

Each full family runs seeds 10000–10009 with exactly thirty committed transactions,
plus an explicit rollback and a cold restart. The third source is a separately
published table with its own primary key. Histories include simultaneous source
changes, key moves, duplicate visible tuples, NULLs, retractions and an empty-delta
pulse. The full current matrix therefore checks 80 cases and 2,400 committed
boundaries. Core uses one seed and seven commits per family plus rollback/restart.

After snapshot, every committed boundary and cold reopen, compare a native
materialized view's full bag and output names/OIDs/modifiers/collations, then
compare native send representations with EXCEPT ALL in both directions. This
preserves numeric display scale and NULL positions. The independent Python source
model additionally checks raw source rows and computes each family's output without
using engine IR or PostgreSQL to implement the oracle.

The qualification exposed two parser gaps: membership parsing previously admitted
only column outputs, rejecting computed DISTINCT targets; it also rejected grouping
after EXISTS/NOT EXISTS. Both now use the existing partition parser/binder, before
wrapping DISTINCT. DISTINCT can therefore compose with admitted derived scopes,
and EXISTS results can feed admitted grouping. This adds no alternative executor,
network computation or persisted format. Other explicit membership restrictions
remain: SELECT 1 correlations, equality-only conjuncts, native correlated sources,
and no extra inner predicates, LIMIT, OR or uncorrelated operands.

Computed LEFT JOIN and EXISTS operands and mixed computed window aggregates were
already implemented by M6. Current qualification credits those implementations:
the matrix exercises integral addition keys, NULLs and grouping; existing M6 live
cases include mixed COUNT/SUM/AVG/MIN/MAX plus numeric SUM (the scan path), FILTER
and empty frames. The weighted unit history now tests right multiplicities
3 -> 1 -> 0 -> 1 -> 0 with cold restore at each boundary and fail-atomic overflow.
Named native-valid rejections cover DISTINCT ON, mixed-input/constant-only/
nonintegral computed keys and inequality correlation.

## Reproduce and preserve evidence

```bash
mkdir -p artifacts/composition-full
PGDERIVE_HARNESS_TIMEOUT_SECONDS=1800 PGDERIVE_S3_LATENCY_SCALE=0 \
  python3 scripts/run_with_s3_proxy.py artifacts/composition-full \
  python3 scripts/composition_matrix_harness.py artifacts/composition-full \
  --full -- target/debug/pgderive_worker
```

Use an isolated PostgreSQL endpoint through `PGDERIVE_TEST_POSTGRES_PORT`; source
fixtures have unique owned names. The complete matrix takes longer than the
wrapper's default ten-minute timeout, so the full gate supplies a thirty-minute
limit unless explicitly overridden. Infrastructure interruption is not a semantic
counterexample. A `--completed-root DIR` can reuse complete thirty-commit cases
only when the executable and source/oracle fingerprints, SQL, initial rows and
the entire history agree. Partial cases rerun in
fresh fixtures, leaving their original evidence intact.

Each case retains its original versioned trace, exact SQL, native oracle records,
process/object evidence and result. `--replay FILE` runs a saved trace. Replay requires the current version-2 query specification, including the DISTINCT
derived projection/filter chain, and the current trace envelope. Actual differential failures preserve fixture dumps and
invoke same-kind bounded transaction deletion reduction, as in the original M5
portfolio. `composition_matrix_failure_harness.py` deliberately corrupts only its
independent oracle at a sentinel source row, verifies 1-minimal reduction and dump
preservation, then replays the minimized trace successfully without injection.
It does not claim an engine defect.

`verify_composition_matrix.py ROOT... --output SUMMARY` checks the current complete
80-case set against saved source histories and actual native SQL witnesses. Obsolete query forms and mismatched saved query descriptions are rejected without
rewriting retained evidence.

## Completion at revision 5d7ff97

Qualification completed on 2026-10-20. The final same-binary run passed all eighty
cases and 2,400 committed boundaries; each case recorded 33 native oracle checks
(snapshot, committed states, rollback and cold reopen). The final verifier checked
the executable and current source/oracle fingerprints for every case. The live
reducer test and standalone uninjected saved-trace replay passed. A worker built
from f58e076 created column-only DISTINCT/EXISTS checkpoints; revision 5d7ff97 reopened
both with the same binding and slot, without rebuild, and continued exact results.

Independent review passed. The common core gate passed 45 Python tests, 322 Rust
tests (three ignored), 127 worker cases plus rebuild and 97.38% production line
coverage, including the core matrix and six extrema cases. The complete release
qualification profile was not run. The [completion evidence](engine-development-evidence.json)
records per-case trace/result hashes and the bounded qualification scope.


Subsequent persisted existence counts intentionally change the EXISTS/NOT EXISTS
plan layout. The old EXISTS compatibility result above is historical, not a
current promise: those registrations now require explicit `query rebuild`.
The obsolete cross-binary compatibility harness has been removed. Current
checkpoint identity and wide-count tests are in `worker/tests/membership_counts.rs`.
See [the current adaptation](incremental-group-adaptation.md).
