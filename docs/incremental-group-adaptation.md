# Specialized incremental group operators

This first adaptation uses the operator separation in Feldera DBSP as an
architectural reference, with an independent implementation for Pgderive's
weighted batches, immutable arrangements and PostgreSQL transaction boundaries.
The inspected upstream revision is `fe76f6a012bfcdb1b9fe4e0455c5774bf6d6ae38`.

## Direct group differentiation

`Partition::with_incremental` supplies a callback with old input, new input and
the complete signed input delta. The callback emits `Q(new) - Q(old)` directly.
The generic complete-group evaluator remains available for other operators and
as a test reference. All source inputs still advance at one committed boundary;
invalid negative input bags fail before the callback, and prepared state remains
unpublished until the existing durable publication protocol completes.

This preserves existing graph identifiers, input/output arrangements and codecs.
A checkpoint written by the full evaluator can be reopened by the direct
implementation. There is no new PostgreSQL state or computation fallback.

## Selected operators

- LEFT JOIN computes `dL join R_new + L_old join dR`. This includes the cross term
  exactly once. Its unmatched component is `L * [right is empty]`: when both old
  and new right bags are empty, emit only the left delta; when presence changes,
  retract old unmatched rows or insert new unmatched rows. Products and signed
  retractions use exact intermediate coefficients before final narrowing.
- EXISTS/NOT EXISTS emit only the left delta while qualification remains true;
  when right presence changes they insert or retract the qualifying left bag.
  If neither boundary qualifies, they emit nothing.
- SELECT DISTINCT compares the old/new projected representatives and emits their
  difference. It preserves the existing representative choice, including SQL
  equality groups whose binary representations differ. It does not substitute
  SQL equality for observable tuple identity.

The runtime no longer reads retained output for these direct paths. LEFT JOIN
avoids reconstructing all existing matching pairs. The work test fixes a one-row
right insertion and increases both old sides over 8, 32 and 128 identities:
callback visits are exactly `5*n + 2`, with `n` newly matching output pairs.
This counts visits and pair construction, not allocation, sorting or object I/O.

## Bounds and follow-on work

This is the computation slice. The runtime still reads, validates and materializes
full affected input groups, then constructs their new bags. Right presence is
found from those bags rather than a separately persisted count. This is not yet
Feldera's trace-cursor/threshold access pattern, and there is no claim of update
cost independent of hot-key size or run count.

Next adaptations are persisted membership summaries, cursor access for direct
operators, and narrowly qualified persistent ordered aggregate summaries. Window
and trace-merge behavior are unchanged by this slice. These require explicit
state-layout/version decisions and independent read-cost evidence.

## Verification

`./scripts/check.sh` is the required gate. Dedicated tests cover weighted empty,
matched and unmatched transitions, simultaneous left/right changes, hot-key work,
legacy-state reopen and invalid input failure atomicity. Existing source-model
histories also run both direct and full evaluators, with cold restart of the
direct implementation. Native core qualification covers outer joins, membership,
computed keys and composed queries.

Upstream references:
[distinct](https://github.com/feldera/feldera/blob/fe76f6a012bfcdb1b9fe4e0455c5774bf6d6ae38/crates/dbsp/src/operator/dynamic/distinct.rs),
[group transformers](https://github.com/feldera/feldera/blob/fe76f6a012bfcdb1b9fe4e0455c5774bf6d6ae38/crates/dbsp/src/operator/dynamic/group.rs),
[left joins](https://github.com/feldera/feldera/blob/fe76f6a012bfcdb1b9fe4e0455c5774bf6d6ae38/crates/dbsp/src/operator/dynamic/outer_join.rs).

The common core gate passed on 2026-10-20: 325 Rust tests (three ignored),
45 Python tests, 127 worker cases plus rebuild, and 97.39% production line
coverage. Formatting, strict Clippy, source-size and dependency checks passed.
Independent review found no actionable correctness or durability findings.
Local evidence: `artifacts/coverage/run-ufOzQ9rp` and
`artifacts/worker/run-jZx0h8Y0`. The complete release profile was not run.
