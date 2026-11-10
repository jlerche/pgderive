# Development and release gates

`./scripts/check.sh` runs the broad semantic matrix with virtual time and a
focused live durability suite. `./scripts/check.sh --full` additionally runs the
exhaustive live query and operational release qualification. Both keep strict
linting, dependency checks and the 80% production line-coverage floor; production
modules are not excluded to make the faster gate pass.

## Deterministic semantics and timers

The default Rust tests replay 417 histories in 26 SQL families from
`tests/fixtures/semantics.json.zst`. These reuse the original harness queries,
source layouts and complete transaction endpoints. Expected weighted bags are
computed by PostgreSQL during an explicit fixture refresh, independently of the
engine. Source deltas preserve full-row identity and multiplicity, including
simultaneous changes to both join inputs. The corpus includes NULLs, duplicate
outputs, key moves, retractions, numeric display scales, temporal boundaries,
empty states, compiler rejections and cold reopen.

Replay uses the production compiler, bootstrap, incremental prepare/commit,
freeze/upload/rebase and checkpoint-restore paths. The object store is in memory
and Tokio time starts paused. Every history verifies each committed output and
cold reopen. Native output types are compared with the frozen PG description;
the PG result layout is hydrated as in production query description. Codec and
SQL send-representation checks remain live tests/full qualification. Four numeric
group-key cases retain the existing live oracle's permitted-spelling rule: PG
freezes the actual source spellings allowed for each equal key in each state.
Replay permits only those exact spellings, while preserving all other cells and
multiplicities. It does not normalize numeric sums or decode both sides through
the production numeric parser.

Paused-clock tests also exercise the production epoch collector, checkpoint
timeout, tenant upload gather loop and capped worker retry backoff. They advance virtual time, including a
one-hour checkpoint timeout, rather than sleeping for those durations. Runtime
wall-time measurements are diagnostic; they do not drive expected semantics.
DBSP epoch time, virtual elapsed time and source WAL addresses remain distinct.

The existing expression-error corpus also runs against the engine in paused
clock tests. Its PostgreSQL differential refresh belongs to `--full`.

The semantic manifest fixes family counts, case identities, recipe fingerprints
and expected oracle/restart counts. Missing or stale fixtures fail the Python
gate. The coverage phase checks a fresh execution report and verifies that the
named virtual timer tests actually ran. It cannot pass by filtering away replay
or supplying a report with missing histories or oracle checks.

## Focused live boundaries

The default worker suite retains 21 result cases plus rebuild and tenant reports:

- A memtable/checkpoint case and publication while a checkpoint upload is held.
- Real result-table pgoutput decoding, PostgreSQL codec/support checks and
  projection publication with a lost COMMIT reply.
- Transaction faults before/after authoritative COMMIT and five witnessed process
  kills during bootstrap PUT, restore GET, publication COMMIT, checkpoint COMMIT
  and physical deletion.
- Multi-query execution, tenant-separated cross-database packs, unavailable-source
  reclamation and central-manifest COMMIT faults.
- Query lifecycle and fresh-generation rebuild with pinned-reader reclamation.

The complete low-level replication harness remains live. The default gate
provisions four independently owned PostgreSQL containers with tmpfs storage.
Live groups run concurrently across these instances, with strictly sequential
harnesses within each instance. A database never serves overlapping mutating
harnesses. These checks use real PostgreSQL/object storage, event witnesses and
explicit fault barriers. Their timeouts are failure bounds, not a way to simulate scheduling.
The exhaustive capacity, query-family, binding and operational matrices remain
in `--full`. Live-only callback witnesses in the source harnesses remain there;
the frozen replay covers their committed semantic histories.

Live fault scenarios retain their production retry count but use a 1ms base
retry delay. Paused-time tests separately verify the production exponential
schedule, its 5s cap and stop interruption. Live waits still observe actual CDC,
publication, process death and recovery.

Covered executables and the ignored manifest protocol test are built once with
one unified Cargo target/feature set. Live suites invoke those exact artifacts;
no Cargo compilation or coverage-counter cleanup runs concurrently with them.
The unit tests and live suites start together after this build. All processes
inherit the same per-process coverage output pattern. Reporting waits for both
phases and the semantic-report verifier to pass. A unit or semantic-report
failure still waits for live cleanup/evidence preservation before returning an
error. Within the full live phase, integration and worker qualification remain
sequential. The protocol
log must show the exact named test and one passing test, preventing an obsolete
filter from silently running zero tests.

All lane servers are removed only after the final result/report verifier passes.
On failure, owned harness process groups stop, the servers remain available, and
cluster archives are saved alongside the logs for ephemeral CI runners. The gate
never restarts or resets an existing demo or PoC instance.

## Refreshing native fixtures

Use an independently owned PostgreSQL endpoint with logical replication and the
`pgderive_dev` database. Any Docker database must use tmpfs. Do not reset demo or
shared PoC data. For example, with an owned endpoint on port 55466:

```sh
PGDERIVE_TEST_POSTGRES_PORT=55466 PGPASSWORD=postgres \
  python3 scripts/export_semantics.py artifacts/semantics-refresh
cp artifacts/semantics-refresh/semantics.json.zst tests/fixtures/
cp artifacts/semantics-refresh/semantics-manifest.json tests/fixtures/
```

The exporter runs existing fixture builders and history definitions against a
native PG oracle, without starting workers, creating logical slots or accessing
object storage. It uses one SQL connection per fixture and unique owned schemas.
It preserves failed source fixtures and logs. The gate never invokes this command
implicitly. Review changed histories and oracle results before accepting a
refresh; retain the independent memory-oracle tests and full live qualification.

## CI and timing

Push/PR checks run the development gate. The Checks workflow can be dispatched
with `full=true` for complete release qualification. Each proxy-wrapped live
suite records `timing.json`, including failures and timeouts. The common gate
prints its total elapsed wall time. Coverage and virtual replay reports remain
under `artifacts/coverage/run-*`; live evidence remains under `artifacts/worker`
and `artifacts/replication`.


Measured locally on 2026-11-10, the default gate passed in **241 seconds
(4m01s)** with cached builds, down from **895 seconds (14m55s)**. The preceding
run spent about 8m43s in sequential worker suites and 2m19s in replication,
including an avoidable 66s rebuild. The new longest live lane took about 152s;
the 865-test Rust unit phase took 55.36s. All 222 Python tests, 417 native-oracle
histories, 21 focused live results, replication and rebuild/tenant reports passed.
Production line coverage was 96.02%, with no production module exclusions added.

The covered all-target build for the changed Rust code took 61s in a separate
probe; the measured complete gate then reused that build. A fresh dependency
build or a slower host adds compilation time, so this is a measured warm run,
not a universal CI time limit. The exhaustive `--full` release gate was not run
for this gate-only change.

A subsequent run overlapping the unit and live phases passed in **190 seconds
(3m10s)**, versus 241s when those phases were sequential. The same 865 Rust tests,
417 semantic histories and live checks passed, together with 223 Python tests
(including shell overlap/failure regression checks). Production line coverage
remained 96.02%. Unit execution took 61.99s under concurrent load, compared with
55.36s in isolation; the net saving was 51s. This also used cached builds.
