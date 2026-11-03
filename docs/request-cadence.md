# Checkpoint and collection cadence

Result freshness remains independent of object durability: each complete committed
source epoch publishes result deltas and its replay journal boundary in one PG
transaction, then installs the prepared local state. Checkpoint completion uploads
objects, atomically advances catalog membership/checkpoint progress, installs the
local replacement, closes upload protection, and only then acknowledges the
checkpoint LSN. A five-second source/UI update does not require a five-second
object upload. The epoch freshness objective is unchanged.

The portfolio template now schedules checkpoints every 60 seconds and sets
`maintenance_ticks=0` to disable the optional unsealed-run trigger. Other users
can retain a 1..64 run threshold. The default checkpoint interval is 60 seconds;
the supported maximum remains 60 seconds. Every other trigger remains active:

* A charged memtable admission failure forces maintenance before retrying once;
  another failure becomes a permanent capacity error.
* 248 runs or 120 in-flight unsealed suffix runs in any arrangement, or 4,096
  replay epochs, force a drain/checkpoint before another epoch is received.
* `checkpoint_wal_bytes` (default 64 MiB, positive) forces maintenance when the
  published end is that far beyond the durable checkpoint. One complete epoch
  may overshoot this threshold. WAL addresses count unrelated database writes as
  well as query changes; they are separate from logical time.
* Predictive maintenance debt backpressure still drains an in-flight upload
  before admitting work likely to exceed memory/run/replay limits.
* Graceful stop checkpoints every published suffix. Upload and publication
  deadlines still fail without acknowledging unsafe progress.

These limits bound the live charged arrangement state, replay epoch count, and
published WAL-distance lag; they do **not** impose a physical PostgreSQL WAL disk
cap. Idle unrelated writes, long transactions, replication slot restart position,
failures, or operation-budget exhaustion can retain additional WAL. Operators
must set PostgreSQL `max_slot_wal_keep_size` and monitor retained/backlog WAL;
reaching that server cap can invalidate a slot and require rebuilding. No worker
can guarantee both unlimited outages and bounded retained WAL without such an
explicit failure policy.

Physical GC runs on its own `gc_interval_ms` timer (default 300,000, accepted
1,000..86,400,000). At most one bounded collection is owned by a worker. New
checkpoints do not accelerate this clock; idle workers continue periodic passes
so pagination and eventual reclamation do not depend on future source changes.
The existing catalog roots, protections, grace periods, generations and bounded
listing/deletion work remain authoritative. Restart allows an immediate pass;
the operation budget still applies to its listing. A long pass never overlaps
its successor.

Portfolio monitoring inventories each query prefix at most hourly, with at most
eight listing pages per attempt. Failed attempts retain the previous sample's
original age and are also throttled for an hour. This external Python collector
is outside the Rust worker operation-budget boundary; a two-query small-prefix
deployment consumes at most two monitoring listings/hour after this change
(previously about 120/hour); eight-page attempts have an absolute maximum of
16 listing requests/hour combined. A truncated inventory fails explicitly instead of
publishing partial totals. The sampling timestamp remains visible; other process
and freshness metrics retain their existing collection cadence.

Cadence-only arithmetic for two continuously changing, tiny-state workers,
ignoring startup/shutdown and pressure: a 5s checkpoint target means up to 1,440
combined checkpoints/hour; 60s means 120 (91.7% fewer). The recorded hour had
1,229 checkpoints, so the timer target would reduce that count by about 90.2%.
A five-minute independent GC target means 24 combined passes/hour, compared to
1,230 recorded listing pages/hour for one-page passes (98.0% fewer). These are
cadence calculations, not measured guarantees under pressure; packing and
transport retry savings must be measured separately. No live demo experiment,
restart, or deployment is required for these changes.

## Recorded local production-path comparison

`scripts/request_cadence_harness.py` ran two independent worker processes on the
same isolated 16-bid/4-auction sources, with separate PostgreSQL catalogs, result
tables, replication slots and object prefixes. One query maintains the grouped
join/count/sum; the peer maintains its top ten. Three groups fit both results,
allowing an independent SQL equality check of both bags after every update.
Thirteen source changes arrived five seconds apart during each 65-second case.
The proxy records actual method attempts; packing is identical in both cases.
The five-second control uses checkpoint=5s, maintenance=16 and GC=5s; the selected
case uses checkpoint=60s, maintenance=0 and GC=300s. The control's independent GC
clock approximates the former listing cadence; it does not restore the removed
checkpoint-coupled collector implementation.

| During the 65-second sample, both workers combined | 5s control | 60s checkpoints |
| --- | ---: | ---: |
| PUT attempts | 79 | 12 |
| Listing pages | 26 | 0 |
| Class A attempts | 105 | 12 |
| Conditional PUT 412 responses | 0 | 0 |
| HTTP 5xx responses / worker retries | 0 / 0 | 0 / 0 |
| Batched delete POST requests (free) | 8 | 0 |
| Maximum statement-start-to-both-publications observation | 69.6 ms | 45.0 ms |

This is an 84.8% PUT reduction and an 88.6% Class A reduction attributable to the
cadence settings on this tiny fixture with current packing. Zero listings in the
selected sample excludes its initial sweep: including bootstrap, graceful final
checkpoints, and one cold reopen, the cases used respectively 137 (108 PUT + 29
LIST) and 45 (42 PUT + 3 LIST) Class A requests, a 67.2% reduction. Startup and
forced final flush overhead are therefore visible rather than amortized away.
Both sinks matched SQL; source acknowledgements stayed at or behind the object
checkpoint, and cold reopen preserved exact logical time. The five-second input
cadence did not wait for the one-minute checkpoints.

Each case's two workers and the subsequent cold reopen shared one durable budget
ledger, configured for 10,000 Class A attempts per UTC day. Ledger PUT, listing,
and total counters exactly matched the proxy's whole-case counts: 137 used /
9,863 remaining in the control, and 45 used / 9,955 remaining in the selected
case. Reopening did not reset usage. No unrelated caller or monitoring request
is included in those worker-ledger totals.

The control recorded 64 selected compaction input runs across eight checkpoint
service reports (including shutdown), accounting for 44 total service requests
in those selected checkpoints; those reports include their flush/read/write work,
so 44 is not an isolated compaction PUT count. The selected case recorded no
compaction selections in this short sample. Sparse-pack compaction and shared
object retention have separate production-path coverage; these measurements do
not establish a long-duration compaction rate.

The checked-in [sample results](request-cadence-samples.json) preserve the summary.
Raw proxy requests, worker events, configs and final ledgers remain under
`artifacts/request-cadence/totals-top10-budget-20261103/`. Earlier same-query
control experiments remain under the sibling artifact directories. All local
fixtures completed cleanup; the live demo stayed paused throughout. These
measurements are a bounded local fixture, not a live R2 billing projection.

## Exact storefront production-path comparison

The `--demo` fixture passed in 142 seconds using the exact storefront
aggregate-before-catalog queries from `examples/portfolio/render.py`, the source
DDL from `source.sql`, and `workload.initial_sql()` / `changes_sql()`. Only source
schema names change. It seeded all 20 production products, published only those
two source tables, used the normal bounded execution limits and `product_id`
result key, and applied 13 complete production generator phases in transactions
five seconds apart. Catalog and fact mutations exercised the same-transaction
join case. Both results matched native materialized views and the existing
shared raw-source memory oracle after every phase. The earlier 16-bid/4-auction
fixture remains a separate analogue.

| Exact storefront, both workers combined | 5s control | 60s checkpoints |
| --- | ---: | ---: |
| PUT attempts during 65s sample | 84 | 14 |
| Listing pages during sample | 27 | 1 |
| Class A attempts during sample | 111 | 15 |
| Recurring listing pages, excluding overlapping initial sweep | 26 | 0 |
| Conditional PUT 412 responses during sample | 0 | 0 |
| HTTP 5xx responses / worker retries | 0 / 0 | 0 / 0 |
| Batched delete POST requests during sample (free) | 6 | 0 |
| Maximum statement-start-to-both-publications observation | 40.3 ms | 36.5 ms |
| Checkpoint completions including shutdown | 16 | 2 |
| Whole case Class A, including bootstrap/shutdown/cold reopen | 143 | 37 |
| Whole case PUT / LIST attempts | 114 / 29 | 34 / 3 |
| Whole case conditional PUT 412 responses | 1 | 1 |

PUT attempts fell 83.3% and Class A attempts fell 86.5% during the sample;
including startup, forced final work and cold reopen, Class A fell 74.1%.
One initial sweep listing completed after the sample began in each case. The
selected 300-second GC timer therefore records one overlapping initial page,
not a recurring collection during 65 seconds. All initial overhead remains in
the table and ledger totals. The control's independent five-second GC timer
approximates the old listing cadence; both cases use current packing, so this
comparison isolates cadence settings rather than claiming additional packing
savings. Five-second result updates continued independently of object uploads.

Each case shared a durable 10,000-attempt/day ledger across both workers and the
cold reopen. The counters exactly matched the recorded whole-case proxy PUT and
LIST attempts: control 143 used / 9,857 remaining; selected 37 used / 9,963
remaining. The conditional 412 attempts were charged. Reopening preserved
usage; source acknowledgements stayed at or behind the object checkpoint, and
cold recovery preserved exact logical time. Both workers' final checkpoints
matched their published time after graceful stop.

The control recorded 70 selected compaction input runs across six checkpoint
service reports (including shutdown), with 39 total service requests in those
selected checkpoints. Those reports include flush, compaction, reads and writes;
39 is not an isolated compaction PUT count. The selected case recorded no
compaction selection in this short run. This sample does not establish a
long-duration compaction rate.

The [sample results](request-cadence-samples.json) append this exact-demo run while
preserving the analogue evidence. Raw proxy requests, both worker streams,
configs and final ledgers are under
`artifacts/request-cadence/exact-demo-20261103/`. Fixtures completed cleanup; no
live-demo service or credential was used, and the demo remained paused.

To repeat against a fresh owned disposable local PostgreSQL fixture:

```sh
mkdir -p artifacts/request-cadence/exact-demo
PGDERIVE_TEST_POSTGRES_PORT=55436 PGPASSWORD=postgres PGDERIVE_S3_LATENCY_SCALE=0 \
  python3 scripts/run_with_s3_proxy.py artifacts/request-cadence/exact-demo \
  python3 scripts/request_cadence_harness.py --demo artifacts/request-cadence/exact-demo \
  -- target/debug/pgderive_worker
```
