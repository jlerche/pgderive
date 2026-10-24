# Additive numeric SUM

Grouped `SUM(numeric)` and `COUNT(numeric)` now use the existing Statistics
operator together with integral COUNT/SUM/AVG. The shared traversal accepts a
reversible scalar algebra; its integer constructors and numeric SQL binding use
the same complete-epoch fold, probes, resource accounting and publication path.
Mixed MIN/MAX queries retain the general aggregate implementation.

A numeric summary persists an exact finite coefficient, counts of active finite
input display scales, and separate counts of NaN and each infinity. Signed deltas
can contain negative counters; validation occurs after combining the complete
epoch with the prior summary. Zero detection includes hidden scale and special
counters. Removing the highest-scale input therefore changes the result's scale,
and removing a special value restores the remaining finite sum. NULL and FILTER
reuse the shared aggregate contribution logic.

This follows PostgreSQL's numeric aggregate rules, including NULL for no non-NULL
inputs, NaN precedence, and NaN when both infinity signs remain. The implementation
also retains exact finite contributions while a special value masks the output.
See [PostgreSQL numeric.c](https://github.com/postgres/postgres/blob/REL_17_STABLE/src/backend/utils/adt/numeric.c).

The existing 4,096-character numeric input/result limits still apply. Intermediate
summaries use exact coefficients; final display limits are not applied to partial
cancellation or a hidden finite total masked by NaN/infinity. Record, work, output
and native cache limits account for the entire scale histogram. Cold persisted
state validates scales and counters before performing scale arithmetic.

Committed PostgreSQL input can use the existing full-primary-key proof to omit
raw group state for numeric statistics. Generic weighted callers still retain
full-tuple validation. Durability remains immutable PUT, atomic PostgreSQL
membership/result/progress publication, then slot acknowledgement.

The physical plan revision and native statistics cache domain change to v2.
Existing views require rebuild; there is no compatibility reader. The previous
numeric implementation supplied correct full-partition evaluation but explicitly
excluded numeric OID 1700 from its integer-only Statistics codec. Numeric SUM was
supported functionality, but its incremental summary was missing.

Validation includes mixed integer/numeric/DISTINCT histories, scale-only changes,
NULL/FILTER, special removals, exact signed cancellation, spilling, cold restart,
and constant cold reads as source group cardinality grows. The native materialized
view oracle additionally compares send representations for SUM, so numeric value
equality cannot hide a display-scale mismatch. Numeric SUM modes run in the core
local gate using the existing harness.

The SF1 comparison is recorded in [numeric-sum-benchmark.md](numeric-sum-benchmark.md).
