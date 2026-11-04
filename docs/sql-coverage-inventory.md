# SQL coverage inventory and first slice

Inventory at `d06b55e`, checked against the closed registry, support matrix,
planner contract and existing qualification harnesses. The working tree was clean.
The handoff's proposed text-function work partly predates the current implementation:
length, slicing, trimming, replacement, translation, splitting, repetition and
code-point functions already execute locally. This inventory is a prioritization,
not a certification of every valid combination.

| Priority | Successful-result gap | Admission / environment boundary | Failure behavior and disposition |
| --- | --- | --- | --- |
| First slice | UTF8 `lpad` and `rpad`, useful for report labels and identifiers | Only `(text,int4)` and `(text,int4,text)` with existing varchar/smallint coercions; UTF8, no locale lookup | Reuse strict calls, deferred folding and lazy expressions; exact output-size budget is an engine resource limit |
| High | lower/upper, pattern matching, formatting | Locale/provider and version dependencies must be qualified before admission | Do not approximate using Rust Unicode conversion; reject today |
| High | float, JSON/JSONB, arrays and broader casts | Source codecs, scalar operators and relational contexts need separate qualification | Keep unsupported sources/signatures rejected before activation |
| High | Arbitrary scalar subqueries and broader correlation | Current aggregate scalar and equality correlation contracts remain bounded | Cardinality, guarded reads and error recovery need a coherent extension |
| Medium | Recursion, named zones, broader windows | No frontier/recursive execution or named-zone runtime | Architectural work; no fallback evaluation |

## Planner failure inventory

The native differential corpus is the executable baseline. Its merge-join settings
are explicit; agreement does not imply agreement with every PostgreSQL execution
plan. Current planner implementation still documents the following differences:

| Class | Cases | Disposition |
| --- | --- | --- |
| Compiler/planner candidates | Contradictory restrictions; folding after substitution; unread ordered aggregates/windows; nested set-operation flattening; subqueries under false/NULL filters; removed joins with failing constants | Retain documented limitations. Each needs a minimized native phase test before changing the shared planner. This slice does not assert these claims were independently requalified. |
| Execution-model decisions | Skipped join inputs/probes, join-order-dependent two-sided predicates, computed merge-sort equalities, LIMIT 0 beside joins, membership evaluated before joins eliminate rows | Incremental input maintenance can evaluate rows a selected native plan never reads; do not swallow errors to imitate that plan |
| Environment-dependent planning | Statistics-dependent OR restrictions and non-default function costs | Existing cost/selectivity model remains bounded; one forced plan cannot establish universal error equivalence |
| Resource domain | 16 KiB expression inlining boundary, bounded numeric/temporal/state domains and scalar expansion budgets | Declared engine limits, distinct from PostgreSQL semantic errors |

## Implemented slice and qualification

Padding uses the existing typed call IR and one shared kernel. Length counts Unicode
scalar values, including combining marks separately. Both functions truncate from
the right, clamp negative lengths to zero, cycle the fill from its beginning and
leave short strings short when fill is empty. Default fill is one ASCII space.
NULL propagation, argument evaluation, folding and lazy branches use the existing
mechanisms. Padding does not consult a collation or session setting; existing source
collation admission and environment binding still apply. The registry contributes
function dependencies to the existing semantic catalog fingerprint. Because this
is a conservative shared baseline, adding these functions changes the binding
fingerprint; existing registrations from the prior baseline require explicit
rebuild. There is no compatibility adapter.

Expanded output is limited to 16 MiB in UTF8 bytes, checked before repetition.
This is a Pgderive resource failure, not a native PostgreSQL allocation limit.
No SQLSTATE equivalence is claimed for engine diagnostics.

The support matrix adds all four exact signatures to the existing live native-MV
and independent Python oracle harness. It compares duplicate bags, binary send
representations and column names/OIDs/modifiers/collations through snapshot,
committed updates/deletes, multirow transactions and cold restart. Existing fixture
rows cover NULL, empty and multibyte input. Rust tests add fill cycling, empty fill,
INT_MIN/MAX lengths, truncation, rejected overloads, lazy guards and byte-budget
boundaries. The same live harness now tests a shared smallint ABS overflow, unchanged
published bag/progress, source repair and safe acknowledgement; this is not a claim
that PostgreSQL shares the padding resource limit.

Six minimized padding cases extend `error_order_corpus.json`: row-dependent versus
constant division errors, CASE/COALESCE guards, strict constant NULL and zero length.
The Rust consumer checks specified planning/execution phases. The native runner now
uses EXPLAIN to distinguish planning errors and records SQLSTATE separately from
wording. The common gate saves full SQL, source rows, server version/settings,
forced planner settings and each outcome in `artifacts/error-order/run-*.json`.
Set `PGDERIVE_ERROR_ORDER_EVIDENCE` for a standalone run. Existing corpus entries
without a phase remain outcome-only assertions; recorded phases are observations.
All fixtures use rolled-back transactions or isolated schemas at controlled source
boundaries. The paused demo is outside this workflow.

During qualification, the first recovery poison used `group_id=-2147483648` in
this multi-expression query. Pgderive hit the documented `right(text, INT_MIN)`
restriction before ABS, while native PostgreSQL reported integer overflow. The
fixture was corrected to isolate `abs(small_value)` with `small_value=-32768`
and a safe `group_id=1`. The original event, source fixture and native diagnostic
remain under `artifacts/worker/run-7EbNrZlt/postgres-support`; this illustrates
why competing errors and declared domain restrictions need separate cases.

Validation: `./scripts/check.sh` (default core profile) passed, including 1,166
native error comparisons, the live support matrix (52 signatures, five coercion
cases, 13 expression rejections and five source-type rejections), sequential
worker/recovery qualification, Clippy and dependency checks. Production line
coverage was 97.02%. Evidence is in `artifacts/coverage/run-vOJec86D`,
`artifacts/worker/run-arqZ2rZX` and `artifacts/error-order/run-*.json`.
Independent sub-agent review found no remaining blockers after the fixture and
SQLSTATE assertions were corrected. Padding resource failures are unit-qualified;
the live recovery check covers shared scalar overflow, not native allocation-limit
equivalence. No planner error-equivalence claim is added by this slice.


## Settled output liveness slice

At `0fda8dd`, native PostgreSQL 17.11 returns `true` for the following query with
one auction row (`id=1`) and one bid row (`price=0`), while Pgderive reports an
execution-time division by zero:

```sql
SELECT COALESCE(true,s.v) AS v
FROM (SELECT id IN (SELECT 1/price FROM source.bid) AS v
      FROM source.auction) s;
```

The outer output was folded only after liveness had kept the membership mark and
its failing input alive. Settled pruning now applies the existing scalar folding
to retained output maps before collecting reads. The existing mark/global-aggregate
pruning then removes work those outputs no longer use. Failed speculative folding
leaves the original expression intact for the final authoritative folding phase;
shared aggregate-input folding preserves the separately planned FILTER argument.
No evaluator, constant-propagation framework or second planner representation is
added. Frozen flattening provenance and filter-movement phases remain unchanged.

Qualification adds native-pinned controls for decided and needed membership reads,
CASE with a NULL condition, Boolean operands, unread failing constants, materialized
CTE boundaries and constant aggregate FILTER arguments. Empty input and duplicate
outputs retain their native multiplicities. A live `dead-membership` composition
fixture begins with a zero divisor in the unused input and compares native MV bags,
column metadata and an independent memory oracle through simultaneous source writes,
retractions, empty right input and cold restart. Existing live subquery failure and
repair qualification continues to cover work that remains observable.

The previous broad dead-read documentation was incomplete: a simple flattened
`COALESCE(1,s.v)` over `SELECT 1/other AS v` already worked. Boundary cases exposed
the real issue. This slice also fixes decided reads of ordinary global SUM outputs.
Unread ordered aggregates/extrema and computed outputs kept by LIMIT ordering still
have separate documented differences. Direct subqueries in lazy branches remain
unsupported; row-dependent guards do not imply conditional subplan execution.
Boolean casts outside the existing cast domain are not added. Physical plans that
change require explicit rebuild under the existing registration identity contract.

Minimized native and pre-fix engine evidence is retained in
`artifacts/planner-dead-reads/reproduction-native.json` and
`artifacts/planner-dead-reads/reproduction-engine.log`. The probe also preserved the
separate LIMIT/ordered-MAX differences and direct-lazy-subquery admission rejection;
those are not presented as fixed by this slice.

Validation for this planner slice: `./scripts/check.sh` passed in the default core
profile with 1,187 native comparison cases (21 additions), 498 live worker result
records plus rebuild, and 97.02% production line coverage. Clippy and dependency
checks passed. Independent review found no blockers; its requested empty-global-SUM
cardinality control was added together with needed-SUM error/NULL controls.
Evidence is indexed in `artifacts/planner-dead-reads/completion.json`; the passing
gate is `artifacts/planner-dead-reads/check.log`, with worker evidence under
`artifacts/worker/run-FuGHKViy` and coverage under `artifacts/coverage/run-7kS8lS60`.
This is core regression qualification, not the full release profile.
