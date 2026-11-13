# Historical in-process query memory experiment

These measurements describe commit `e3d8d59`, before production tenant ownership
and pooled capacity. See [current worker behavior](multi-tenant-worker.md).

The throwaway `jacob/experiment-multi-query-memory` branch starts from local main
`6e344cf`. It runs each existing worker in its own task on one Tokio runtime:

```sh
target/release/pgderive_worker query-a.toml query-b.toml query-c.toml
```

All configurations are loaded and validated before any worker starts. Each query
still owns its catalog registration, sink, replication slot, object prefix,
execution cache and memtable. This experiment shares process/runtime overhead;
it does not share source decoding, arrangement state or caches across queries.
Use distinct owned resources just as for separate worker processes. Ctrl-C or a
terminal query error requests all peers to stop, waits for their normal durable
drain, and returns the first error. Finite `max_transactions` workers can finish
independently while other queries continue. Startup/retry/checkpoint deadlines
remain per-query. Query events share stdout and can interleave; catalog health
and progress remain query-specific.

The measurement harness reuses the exact storefront queries, isolated fixture
adapter, native SQL comparison and independent raw-source memory oracle. It
alternates totals and top-ten queries, all reading the same source publication.
600 temporary order lines over 20 products change once per second. Every query
has a 32 MiB execution cache, 4,096 cache entries, a 32 MiB memtable, a 60-second
checkpoint interval and a 300-second GC interval. Each case runs sequentially
and owns fresh fixture schemas and prefixes. Each query is checked before and
after the measured changes, after graceful stop, and after a cold reopen. Samples
record per-query WAL lag and immediately check acknowledgement ordering. Final
verification waits for every query's published source LSN to include the last
mutation's pre-COMMIT WAL fence, so an old matching result cannot pass as caught up.

Run against a disposable logical-replication PostgreSQL fixture with enough
slots/senders (at least eight for the default maximum). Use tmpfs database
storage, never recreate a shared PoC container. The existing local object store
and recorded proxy wrapper supply durable objects and actual request traces:

```sh
cargo build --release --locked --bin pgderive_worker
mkdir -p artifacts/multi-query-memory/comparison
PGDERIVE_TEST_POSTGRES_PORT=55436 PGPASSWORD=postgres PGDERIVE_S3_LATENCY_SCALE=0 \
  python3 scripts/run_with_s3_proxy.py artifacts/multi-query-memory/comparison \
  python3 scripts/multi_query_memory_harness.py artifacts/multi-query-memory/comparison \
  --counts 1,2,4,8 --seconds 300 --mode both -- target/release/pgderive_worker
```

`comparison.json` contains measured source throughput and mean/sample-maximum
RSS and PSS. Each case preserves per-second process samples and worker event
logs. RSS sums count shared pages more than once in the separate-process case;
PSS apportions shared pages and is the better physical-memory comparison. These
are worker-only Linux measurements, excluding PostgreSQL and object storage.
Container accounting and provisioned memory can differ from RSS/PSS.

Short comparisons show startup and initial churn costs, not memory plateaus.
Even long fixed-state runs do not qualify growing source/group cardinalities.
This branch changes no live demo configuration or service.

## Memory budgets are private per query

`cache_bytes=32 MiB` caps the main decoded cache's charged structural/payload
retention, not all caches or process RSS. With these settings, each query also
has nominal 8 MiB budgets for index navigation, compressed ranges, negative
filters and complete-key reuse, plus the default 4 MiB native-value retention
budget. Adding the separate 32 MiB memtable gives about 100 MiB of nominal
retention budgets per query, or 600 MiB across six queries. Entry limits can
restrict retention earlier. These are demand-filled budgets, not reservations,
and summing accounting charges does not give an exact physical-memory limit.

Active pins, transient computation, metadata, client/runtime allocations and
allocator overhead are outside the primary decoded-cache cap. The memtable
holds new arrangement changes between object checkpoints; cached reads can
survive checkpoint completion. Releasing either need not immediately release
allocator pages to the OS. Sharing a process amortizes common overhead but
does not pool these budgets across queries or tenants. The observed RSS/PSS
must therefore not be treated as a maximum at full cache/memtable occupancy.

## Initial 30-second comparison

A release-build run on this branch sustained 600 source changes/second for every
case below. Each case applied 18,000 source updates, verified every query reached
the final source fence, and passed native SQL/memory-oracle equality and cold
reopen. These are single short samples with cold caches, not live-demo plateaus.
The timed interval is shorter than the 60-second periodic checkpoint interval;
graceful-stop checkpoints are checked afterward. Use the longer default run to
measure periodic maintenance and cache warming before drawing steady-state conclusions.

| Queries | Shared process mean RSS, MiB | Separate processes summed mean RSS, MiB | Shared mean PSS, MiB | Separate summed mean PSS, MiB |
| --- | ---: | ---: | ---: | ---: |
| 1 | 51.78 | 48.44 | 47.44 | 44.23 |
| 2 | 71.80 | 110.84 | 67.52 | 82.29 |
| 4 | 100.59 | 221.71 | 96.39 | 143.23 |
| 8 | 149.55 | 432.71 | 145.27 | 253.81 |

At eight queries the shared process uses about 43% less PSS than separate
processes. The one-query cases also differ by a few MiB, illustrating single-run
variance. Do not interpret the RSS difference alone as physical memory savings,
or extrapolate these short observations to warmed larger-state queries. The raw
samples, proxy requests and per-case reports remain under ignored
`artifacts/multi-query-memory/release-comparison/`.

## Multi-tenant comparison

`scripts/multi_tenant_memory_harness.py` creates separate owned PostgreSQL
databases, with two queries per tenant and three different schema/data profiles:

* Storefront: product catalog and order lines, totals and top-ten queries sharing
  one source stream, using the existing storefront fixture and raw-source oracle.
* Ledger: account/entry tables with tenant/query-specific seeds and thousands of
  entries; distinct aggregate and framed-window recipes from the application
  grammar, verified by its independent memory oracle and native SQL.
* Auction analytics: auction/bid tables, Boolean/UUID/integer/text columns,
  filtered typed aggregates and remainder predicates, using the existing SQL
  fixture's native and nested-loop memory checks.

The profiles cycle when requesting more than three tenants. Schema names and
object prefixes remain distinct for every query. Ledger and auction queries have
independent datasets/source streams, while the storefront pair shares its source.
The target is 600 changes/second per independent stream, so every query is offered
600 changes/second; total unique source changes differ from total query-changes.
The report records both and the measured rate rather than assuming delivery.
Each reader tracks its own database/source-stream WAL fence. Samples record query
lag and enforce ACK ordering, and all final fences must be included before native
and memory-oracle comparisons, graceful drain and cold reopen.

Use a dedicated disposable logical-replication PostgreSQL instance; successful
cases drop only their newly created databases, and failures preserve them and
their logs. Never point the harness at production or reset a shared PoC instance.

```sh
mkdir -p artifacts/multi-query-memory/tenants
PGDERIVE_TEST_POSTGRES_PORT=55437 PGPASSWORD=postgres PGDERIVE_S3_LATENCY_SCALE=0 \
  python3 scripts/run_with_s3_proxy.py artifacts/multi-query-memory/tenants \
  python3 scripts/multi_tenant_memory_harness.py artifacts/multi-query-memory/tenants \
  --tenants 1,2,3 --seconds 300 --mode both -- target/release/pgderive_worker
```

The existing independent worker paths still own all caches and arrangements;
this experiment introduces no cross-query result, plan, source or state cache.

A release-build 90-second comparison passed all publication fences, native and
memory oracles, graceful stops and cold reopens, at 600 measured changes/second
per stream. Three tenants use five independent streams and six queries, so each
mode processed 270,000 unique source changes and 324,000 query-changes.

| Tenants / queries | Shared mean RSS, MiB | Separate summed mean RSS, MiB | Shared mean PSS, MiB | Separate summed mean PSS, MiB |
| --- | ---: | ---: | ---: | ---: |
| 1 / 2 | 75.21 | 118.17 | 70.95 | 89.64 |
| 3 / 6 | 193.87 | 449.07 | 189.59 | 320.98 |

The heterogeneous six-query shared process uses about 41% less PSS than separate
processes in this finite sample. These intervals include periodic checkpoint
work but are still too short to establish plateaus or increasing-cardinality
bounds. Raw results remain under ignored
`artifacts/multi-query-memory/tenant-comparison/`. PostgreSQL instances were
separate from live/PoC data and used disposable tmpfs storage. The common gate
ran concurrently on a different owned PostgreSQL instance; these samples are
memory observations, not CPU isolation or maximum-throughput qualification.
