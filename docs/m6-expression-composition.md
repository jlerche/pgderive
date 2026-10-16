# M6 expression composition

M6 completes the bounded support for computed window aggregate arguments and window
partition/order keys, intermediate AVG rendering before downstream SQL scopes,
and side-local integral equality expressions for left joins and EXISTS/NOT EXISTS.
Existing typed kernels and explicit supported coercions are reused. Derived scalar
projections may apply those kernels, integral widening and integral-to-numeric
casts to finalized outputs. No function catalog expansion or remote computation
is included.

Window aggregate expressions stay attached to their aggregate. Evaluate FILTER
first and evaluate the argument only for rows belonging to the current frame.
Empty frames must not evaluate overflowing arguments. Partition/order expressions
are mapped after source WHERE and before partitioning/sorting. ROWS, ROW_NUMBER and
navigation still require the raw logical occurrence key in partition/order; an
arbitrary transformation of a unique key is not a proof of uniqueness. Computed
ordering accepts integral/timestamp results; partition keys use the existing
deterministic equality types. Ranking, RANGE without offsets and GROUPS can use computed peer keys. Named/excluded frames,
RANGE distances, window DISTINCT and computed LAG/LEAD arguments remain outside
this slice.

A derived boundary renders AVG's internal sum/count encoding into PostgreSQL
numeric text before filters, maps, grouping, windows or joins consume it. Rendering
is an internal engine map, distinct from the final public-output rendering; each
scope owns its field names. Public results retain ordinary query output columns.

Outer/existence operands remain side-local integral expressions with exactly
matching result OIDs. They run eagerly for each input row, including when the
opposite side is empty; this retains the conservative ON-map error contract.
Unmatched right fields are NULL-extended by the existing outer operator. EXISTS
and NOT EXISTS preserve each full left tuple's multiplicity while right-side
membership is nonlinear. Mixed-input operands, residual ON predicates, implicit
cross-width equality casts and arbitrary correlated subqueries remain rejected.

Acceptance requires native-MV bags and output metadata, independent source
recomputation, NULLs/duplicates, simultaneous input updates, retractions, empty
frames, guarded argument errors, cold restart and failure atomicity. Existing
M5 portfolio traces remain replayable; promoted blockers move to an explicit
qualified list instead of disappearing from the compatibility inventory.


The twelve live modes cover ROWS with computed COUNT/SUM/AVG/MIN/MAX and numeric
SUM; GROUPS with computed peer keys; empty frames with overflowing SUM/COUNT inputs;
computed RANK keys; derived AVG filtering, arithmetic, regrouping, windows and
joins; and left/EXISTS/NOT EXISTS computed keys. Each mode checks the identical
native MV and source model after committed and rolled-back history steps and a
cold restart. The Rust scope test additionally joins two independent rendered AVG
scopes and two scalar scopes, then retracts and restores them.


Qualification completed on 2026-10-16. Independent review passed. The common core
`./scripts/check.sh` gate passed: 32 Python tests, 292 Rust tests (one ignored),
105 worker result cases plus rebuild and 97.44% production line coverage. Strict
Clippy, formatting, file limits and unused-dependency checks passed. The full
release profile was not run. M7–M9 and the function/type backlog remain open.
