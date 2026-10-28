# Epoch service cost feedback

The existing epoch controller uses a bounded rolling history of 32 completed
publications. It keeps the same source transaction assembly, ready-backlog
coalescing, freshness objective and defensive execution limits. This changes
scheduling estimates, not relational results or durable publication ordering.

Input features are source change count, charged source bytes and distinct source
primary-key images. Old and new images both count when a key changes. Hashing
runs once as each complete transaction is appended; decisions do not rescan the
epoch. The hash set is capped by the existing resident entry/byte limits. Once
saturated, accounting uses all old/new images as a conservative estimate, freeing
the set. These are cost-only raw representations, not PostgreSQL equality proofs,
join-output cardinality, or affected aggregate-group counts.

Preparation, INSERT/UPDATE/DELETE effects and sink DML are learned separately.
Mutation counts come from existing sink classification, after occurrence checks;
same-key replacements count as one UPDATE. Future output is forecast from observed
input/output relationships. The controller cannot know future join fanout before
execution. All fits use normalized nonnegative coefficients with bounded work;
they are refreshed after publication and cached for gather decisions. Correlated
features need not have individually identifiable coefficients.

Non-DML publication time is measured directly, including locking, layout checks,
progress and COMMIT. It is an empirical amortizable phase estimate, not a claim
that PostgreSQL overhead is constant. Sequential preparation admission waits are
measured even with optional phase diagnostics disabled. Failed first preparation,
forced maintenance and remaining local publication bookkeeping stay in the total
service forecast as overhead, rather than teaching the per-input preparation
coefficients. Parallel internal waits remain part of preparation; summing those
would double-count overlapping critical paths.

Residual uncertainty includes output-forecast error and changing contention.
Exploration is independently bounded in rows, bytes and key images. Marginal cost
is the difference between current and next-transaction feature predictions.
Predictions are empirical estimates, not latency upper bounds: unseen SQL work,
source backlog, new key distributions and external stalls can miss the objective.
Feature extraction during collection and model fitting after feedback are bounded
but outside the reported publication service interval. Whole-worker CPU and
source-to-sink freshness remain the acceptance measurements for their overhead.

No new SQL round trips, dependencies, arrangement formats, checkpoint policy or
acknowledgement paths are introduced. Object upload still precedes authoritative
publication, and source acknowledgement still follows durable checkpoint progress.

## Validation

The common core gate passed 669 Rust tests, 147 Python tests, 158 core worker
results plus query rebuild, sequential live PostgreSQL checks and 97.17%
production line coverage. Independent ready-code review approved the change.
The logical-time production collector tests cover paced coalescing, shutdown,
backlog bounds and source ordering; other tests cover key moves/saturation,
collinear/noisy fitting and admission feedback with optional phase metrics off.

Four paired production-worker comparisons used frozen release binaries against
`c44e019`: 20 seconds per engine, 1,000 source changes/s, 4,096 initial source rows,
256 groups, mixed INSERT/UPDATE/DELETE, autocommit and varied transactions through
1,000 changes, both execution orders, and the existing object-storage latency
proxy. Every run passed native equality, final progress and ACK checks.

| Median across execution orders | Baseline | Candidate |
|---|---:|---:|
| Autocommit processing p50 | 3 ms | 3 ms |
| Autocommit marker visibility p50 | 30.85 ms | 31.04 ms |
| Autocommit CPU/change | 0.146 ms | 0.141 ms |
| Varied processing p50 | 2 ms | 2 ms |
| Varied marker visibility p50 | 11.08 ms | 11.37 ms |
| Varied CPU/change | 0.041 ms | 0.041 ms |

Estimated object API classes were identical in every pair: 29 Class A and 31
Class B operations per complete run, including startup and drain; both made 25
PUTs. These are local
request classifications, not Cloudflare billing measurements. There is no material
latency improvement established here; the change supplies richer bounded feedback
without a measured short-run request or CPU regression. Short marker observations
do not qualify growing-state sustainability, per-DML latency or atomic visibility.

An initial gate failed the existing phase-accounting assertion because creation
of the preparation profile moved before input conversion. Resetting its lap clock
at the input/preparation boundary retained admission feedback without charging
input conversion twice. The original logs and fixture dump are retained along
with the passing rerun. All owned Docker fixtures were removed without creating
persistent volumes. Evidence and frozen binaries remain under
`artifacts/benchmarks/epoch-cost-20261027`.
