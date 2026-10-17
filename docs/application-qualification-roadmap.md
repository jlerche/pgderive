# Application qualification and production roadmap

The M0–M4 structural baseline is complete for its documented domain. This roadmap
keeps function/type implementation separate from query composition and operating
evidence. The public result remains an ordinary PostgreSQL table; all computation
stays in-engine. Neither delivery subscriptions nor network computation fallback
is included.

| Milestone | Deliverable | Acceptance |
| --- | --- | --- |
| M5 | Application portfolio, seeded compositional differential histories, replay/minimization and opt-in phase measurements | Checked-in coverage matrix; exact native-MV and independent bag checks, output metadata, rollback, duplicates/NULLs, retractions and cold restart; preserved reproducible failures; measured local release baseline with honest phase boundaries |
| M6 | Additional expression placement/composition | Frame-aware computed window arguments/keys and selected derived finalization/coercion paths, independently qualified; no implied new function catalog |
| M7 | Less stateful recomputation | Measured reduction in repeated frame work for selected nonlinear windows, with exact output and explicit unchanged read/write costs |
| M8 | Scalable sink changes | Qualified batched/indexed occurrence DML without public bookkeeping or loss of representation distinctions; unchanged publication/ACK semantics |
| M9 | Controlled production qualification | Actual deployment/storage-provider, workload capacity, WAL/slot-loss, restore, maintenance and recovery evidence; explicit supported envelope |

## M5 scope fixed before implementation

Six application recipes cover catalog projections, order summaries, account joins,
optional account matches, cohort summaries and running balances. Seeds select
query parameters and bounded variants, not arbitrary SQL. A machine-readable
portfolio records accepted templates and named unsupported blockers for M6 and
the separate function backlog. Native PostgreSQL must accept rejected portfolio
queries before their engine diagnostic is treated as a compatibility gap.

Generated transaction histories preserve source primary keys and include signed
values, UTF8 text, NULL keys/values, duplicate visible results, simultaneous input
changes, key moves, insertion/deletion, rollback and empty result deltas. Results
are checked after each committed boundary against the identical native MV and an
independent Python source model, with exact output names/OIDs/modifiers/collations.
A killed process resumes object-backed state halfway through each history.

Every run saves its generated SQL template, initial rows, operations, seed and
version before executing it. Differential failures retain fixture dumps, workers,
original trace and same-kind replay evidence. A bounded transaction-deletion
reducer runs in fresh owned fixtures; it reports whether it reached 1-minimality
or its attempt budget. It does not claim to minimize SQL syntax or input values,
and infrastructure failures are not treated as semantic counterexamples.

Opt-in phase metrics preserve initialization-only semantic inspection and the
PUT → atomic PG membership/result/progress transaction → ACK order. Measure actual
boundaries: source decode/transaction assembly (excluding receive wait), input
construction, engine preparation including object I/O, output finalization,
PG membership/layout work, sink DML, COMMIT, and local completion. Startup/recovery
and compaction/GC have their own observations. Scalar versus operator CPU and
object I/O overlap inside preparation; do not invent a disjoint breakdown.

Release baseline evidence includes source/sink sizes and skew, process CPU/RSS,
proxy object request counts/bytes, observed client-transaction-to-publication latency, phase
samples, cold restart and maintenance. Client timings include psql process startup and end at the published event before ACK.
Verification time is excluded from each publication latency sample but included in total harness duration. LLVM/debug
qualification timings are never represented as release capacity measurements.
No production SLO or remote-provider durability follows from this local baseline.

## M5 qualification interface

| Recipe | Compositions exercised |
| --- | --- |
| Catalog cards | UTF8 `reverse`, integer `abs`, `COALESCE`, arithmetic and filtering |
| Order totals | Computed aggregate inputs, nullable keys, COUNT/DISTINCT/SUM and retractions |
| Account matches | Computed equality keys, duplicate output and simultaneous changes to both inputs |
| Optional matches | Left join with post-join nullable predicate |
| Cohort summary | Derived grouped COUNT with outer filter |
| Running balance | Nullable partitions, unique occurrence order, bounded preceding/following and empty frames |

The machine-readable [portfolio](../queries/application/portfolio.json) is the
blocker matrix. At M5 completion, ungrouped COUNT and computed windows had misleading
diagnostics. M6 promotes computed windows, derived AVG and computed outer keys into
`qualified_compositions`; ungrouped COUNT retains its documented diagnostic gap. Numeric ABS and locale-sensitive LOWER
remain on the separate function/type backlog. This inventory is representative,
not an enumeration of all unsupported PostgreSQL features.

`./scripts/check.sh` includes six generated cases (one seed per recipe).
`./scripts/check.sh --full` includes twelve (two per recipe). Both preserve each
accepted SQL template, source bootstrap and transaction history before execution.
Replay a recorded case using a new output directory:

```sh
python3 scripts/run_with_s3_proxy.py OUTPUT python3 scripts/application_portfolio_harness.py OUTPUT --replay REPLAY.json -- target/debug/pgderive_worker
```

Replay validates a versioned closed SQL grammar and bounded source operations
before touching PostgreSQL. Stored parameters and frames define the query;
regenerating random choices is unnecessary. Differential kinds distinguish native
bag/metadata failures from source-model or independent-oracle disagreements, which
may themselves indicate harness defects. The reducer requires reproduction of the
same kind and never treats an infrastructure exception as a semantic failure.
To exercise retention/reduction explicitly, run `application_failure_harness.py`
through the same proxy wrapper. It deliberately corrupts the independent oracle
when a sentinel source row exists, checks 1-minimal transaction reduction and the
fixture dump, then replays without injection. It does not assert an engine defect.

For release measurements, run:

```sh
python3 scripts/application_baseline.py artifacts/application-baseline-NEW
```

This builds locked release binaries in an isolated target directory, clears
inherited coverage/compiler/profile overrides, and runs twelve serial cases by
default: 128 entries, 16 accounts and 20 generated transactions per seed.
`PGDERIVE_TEST_POSTGRES_PORT` selects the owned PostgreSQL fixture server.
The output retains build hashes/toolchain, per-case source hashes/server version,
raw phase samples, Linux process measurements and proxy request evidence.
`worker.phase_metrics=true` enables clocks; the default is false. `open_recovery_us`
starts after the initial control connection; client launch-to-ready covers the
whole startup. Publication subphases nest inside publication and must not be
added twice. Decode excludes receive wait and sums per-event rounded microseconds.
The standalone release build has no coverage instrumentation; optional phase
clocks remain enabled and their overhead is part of these samples.

## Recorded local baseline (2026-10-16)

The checked-in [sample evidence](application-baseline-samples.json) records twelve
successful release cases on PostgreSQL 17.11, local tmpfs Docker PostgreSQL and
local SeaweedFS through a proxy with zero injected latency. Each case began with
128 entries/16 accounts and ran 20 generated transactions (including rollbacks)
and a cold restart. Toolchain, binary/source hashes, seeds, key distributions and
per-case measurements are included; the implementation was measured before its
commit, so the recorded Git parent is explicitly marked dirty.

Across the twelve cases, per-case publication-latency medians were 30.8–53.4 ms
and p95s 33.7–62.9 ms. Worker peak RSS was 23.9–27.2 MiB; sampled cumulative worker
CPU across process generations was 0.20–0.39 seconds per case. Client cold startup
was 160.8–193.1 ms. Each case observed 5–6 maintenance events, 56–300 object GETs
and 41–242 PUTs, including startup/recovery/maintenance traffic. Actual output
changes ranged from 6–484 inserted and 8–538 deleted occurrences per case.
These are finite serial samples on this host, not concurrency or capacity bounds.

The largest measured tick phase was preparation including object I/O: per-case
medians 4.6–23.2 ms, versus publication 3.6–7.9 ms. This supports measuring state
reads/recomputation for M7 and sink scaling for M8; it does not separate object
latency from operator CPU or prove which implementation change will dominate at
larger scale. The client latency also includes psql startup and scheduling outside
the measured worker phases. M6 should first close the named composition gaps,
keeping new function kernels on the separate support backlog.

M5 is complete for this finite scope. The common core gate passed with 32 Python
tests, 287 Rust tests (one ignored), 93 worker result artifacts plus rebuild,
and 97.39% production line coverage. Strict Clippy, formatting, file limits and
dependency checks passed. Separately, all twelve release-baseline cases, the
live independent-oracle fault/reduction test and standalone saved-trace replay
passed. The full release qualification profile was not run for this slice;
M6 completion is recorded below; M7–M9 and the function/type backlog remain open.


## M6 expression composition

The [M6 contract and qualification matrix](m6-expression-composition.md) extends
expression placement using existing typed kernels. The M5 application recipes and
versioned traces remain supported. M6's twelve additional composition cases run in
both core and full gates; the core count becomes 105 plus rebuild. The historical
release baseline above remains an M5 measurement, not a new M6 performance claim.

M6 is complete for its documented scope: independent review and the common gate
passed with 105 worker cases plus rebuild and 97.44% production line coverage.
M7 targets [sliding frame work](m7-window-work.md), reducing repeated computation
while preserving exact results, durable publication and the existing full
affected-partition read boundary. M8 and M9 remain subsequent milestones.


## M7 completion

M7's [sliding frame optimization and measured limits](m7-window-work.md) are
complete for eligible COUNT/integral SUM/AVG ROWS and peer windows. It reduces
repeated frame computation while retaining the existing persisted graph and full
affected-partition reads. Exact scan/source/native-MV comparisons, failure
atomicity, cold checkpoint compatibility and measured unchanged object costs are
qualified. Independent review and the common core gate passed: 110 worker cases
plus rebuild and 97.46% production line coverage. M8 is next; M9 and function/type
expansion remain separate.
