# Exact numeric arithmetic

Division, numeric AVG, rounding and casts execute in the engine using BigInt
coefficients, display scales and integer quotient/remainder arithmetic. There is
no floating-point intermediate or connected PostgreSQL computation fallback.
The native input/result budget remains 4,096 characters/digits. Internal AVG sums
can use a wider bounded range when the final mean fits the ordinary result budget.

## Operations and scale

Numeric `/` follows PostgreSQL's normalized base-10000 leading-digit scale
selection: at least 16 significant digits and both operand display scales, capped
at 1,000 fractional digits. Quotient/remainder arithmetic rounds half away from
zero. Nonterminating fractions receive PostgreSQL's finite rounded numeric result.
Mixed integral/numeric inputs promote to numeric. Integral `/` truncates toward
zero and checks zero divisors and native-width overflow.

Numeric overloads include ROUND and TRUNC with one or two arguments, CEIL,
CEILING, FLOOR and ABS. ROUND breaks halfway ties away from zero; TRUNC discards
toward zero. Negative places operate left of the decimal point. ABS preserves
scale; integer-valued rounding functions return scale zero. Two-argument
ROUND/TRUNC admit valid integral-to-numeric coercions. Unary integer/unknown-NULL
rounding calls resolve to float8 in PostgreSQL and reject here; cast explicitly
to numeric.

Casts cover integral to numeric, numeric to int2/int4/int8, unconstrained numeric,
and numeric(p,s). Integral targets round before range checking. Numeric modifiers
accept precision 1..1000 and scale -1000..1000, including scale greater than
precision. Values round before the integral-digit overflow test. Constrained
numeric permits NaN and rejects infinities. Casting back to plain numeric clears
the result typmod; identity projections retain their source modifiers.

NULLs, NaN, signed infinities and division-by-zero follow native numeric rules.
Existing CASE and FILTER guards retain lazy evaluation. Errors preserve the last
published results/progress and cannot acknowledge beyond that durable checkpoint.

## Numeric AVG and retractions

AVG(numeric) uses exact sum and non-NULL count. The existing reversible numeric
SUM summary retains removable display-scale and NaN/positive/negative-infinity
counts. Deleting the highest-scale input can shrink mean display scale; deleting
a masking special exposes the retained finite sum. Empty/all-NULL groups and
frames return NULL without division.

The existing grouped/statistics/window paths supply aggregates. Supported FILTER,
computed arguments, derived scopes, HAVING and UNION ALL compositions use the
same kernels. Publication remains object PUT, atomic PostgreSQL result/membership/
progress commit, then slot acknowledgement. The expanded scalar capability
catalog changes the captured semantic fingerprint, so existing registrations must
be explicitly rebuilt before restarting them with the upgraded worker. There is
no compatibility path.

## Reference and evidence

Feldera a2e791e12dd2fe02906a6fa2065520d5fcc73760 supplies the weighted sum/count
AVG and DBSP retraction model. Its fixed-precision decimals, truncating decimal
casts and ties-even unary rounding do not define PostgreSQL numeric semantics.
PostgreSQL 17 numeric.c controls scale, rounding, special values and typmods.
See [numeric types](https://www.postgresql.org/docs/17/datatype-numeric.html),
[math operations](https://www.postgresql.org/docs/17/functions-math.html), and
[numeric.c](https://github.com/postgres/postgres/blob/REL_17_STABLE/src/backend/utils/adt/numeric.c).

The 18 modes in scripts/numeric_arithmetic_harness.py compare original-SQL native
materialized views, native send bytes, names/types/modifiers/collations and an
independent Decimal oracle through CDC and cold restart. Histories retract special
values and 30-place inputs, cross empty states, and qualify guarded computed
arguments. Division-by-zero, integral overflow and post-rounding typmod overflow
cases check native errors and the publication boundary. Rust tests add weighted
retractions, compaction, width boundaries and wide intermediate AVG sums.
