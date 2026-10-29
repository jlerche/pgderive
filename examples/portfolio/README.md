# Now: a live storefront

This example maintains a product leaderboard directly in PostgreSQL. A catalog
supplies product names and categories; signed order-line quantities and historical
unit prices supply the facts. A sale adds units and revenue, a refund has negative
quantity, and deleting a line retracts it. `order_lines` counts line items, not
orders. Money is stored and rendered in exact integer cents.

The all-products query groups order lines by product ID and computes COUNT,
SUM(quantity) and SUM(quantity * unit_price_cents) in a derived relation, then
joins those aggregates to the product catalog for names and categories. The second query adds revenue descending,
product ID ascending and LIMIT 10. Both are independently maintained production
queries over one two-table source publication. Each has its own worker process,
slot, catalog, result relation and object prefix. The production planner proves product_id unique through the one-to-one join,
so both PostgreSQL result tables use that integer primary key. Supabase Realtime
can publish their INSERT/UPDATE/DELETE changes. No engine key proof is weakened
for the demo; text metadata remains outside the sink key.

`source.sql` defines the two source tables with FULL replica identity and C
collation on catalog text. `workload.py` keeps 20 fictional products and bounded
line IDs. Bulk sales change the cutoff on an elapsed five-second clock, including
an exact revenue tie. Small real sales, refunds, adjustments and cancellations
continue between bulk batches. Catalog and fact changes also share transactions,
exercising the incremental join cross term. Historical line prices never change
when the catalog is edited. The generator targets at most 2 source transactions
per second; actual throughput and phase cadence are measured, with no catch-up
burst after a slow transaction.

The main page focuses on real maintained top-ten snapshots and a sanitized source
activity feed. Under the hood reveals the schema, both SQL queries and the
independent all-products view. Metrics is closed by default; opening it exposes
both process panels and histories with shared filters/time axes. Monetary strings
use BigInt rather than floating-point rounding. Result notifications trigger
explicitly ordered authoritative refetches; polling recovers missed events.

These assets extend an existing owned Supabase/PostgREST/Realtime/Caddy stack with
`db`, `rest`, `realtime`, `edge`, `engine` and `engine-top10`. Database roles, TLS,
Realtime setup and the locally served Supabase SDK must already exist. See
[deployment](../../docs/deployment.md). PostgreSQL Docker storage must be tmpfs.
Only SELECT on the two result tables is public; source tables and engine catalog
metadata remain private. Do not enable RLS on engine-owned result tables.

Render an exclusively owned fresh stack configuration:

```sh
export DEMO_OBJECT_ENDPOINT=https://your-object-store.example
export DEMO_OBJECT_BUCKET=your-private-bucket
export DEMO_OBJECT_PREFIX=your-exclusive-fresh-demo-prefix
export DEMO_ANON_KEY=your-public-browser-key
python3 examples/portfolio/render.py /path/to/owned/stack-r2
```

The historical `examples/portfolio` directory name is retained for repository
callers. `archive/totals.html` preserves the prior page; it is not published against
the new schema. Before an explicitly authorized fresh demo, archive the existing
page, private configuration, source data and both catalogs. Retire only recorded
inactive demo slots and the named demo schemas/source fixtures. Retain old object
prefixes, inspect each new prefix as empty, and preserve unrelated databases,
publications, Realtime, edge services and object namespaces. Never clear a bucket.

Use both Compose files. Run `query create /etc/pgderive/query.toml` once per
service, wait for ready and stop gracefully; subsequent workers use `query
restart`. Object credentials are an external mode 0600 file mounted at
`/run/credentials/r2.json`; PostgreSQL credentials stay in the private environment.

Install `generate.py`/`workload.py` under `generator`, and `collect.py`, `queries.py`,
`history.py`, `observe_latency.py` under `metrics`, retaining the deployment's
read-only SQL helper. The generator atomically publishes `activity.json` with
sanitized committed events. The source-commit observer follows each query's
independent durable progress, including transactions with no result-row change.
Its pre-COMMIT WAL target is an inclusion proxy only while the generator is the
sole source writer, with up to 50 ms polling quantization; it sends no replication
feedback. Missing/stale values remain distinct from measured zero.

Both process panels expose RSS, cache/memtable/run diagnostics, object-service
requests/read/write bytes, preparation/publication/checkpoint latency, publication
counts, progress, WAL and prefix-scoped physical inventories. CPU and PUT-only
request counts are not collected. Process counters reset on restart; latency
windows cover 60 seconds. Inventories are sampled once per minute. Each query has
its own bounded 24-hour history; snapshots need not represent the same transaction.

`query_concurrent_harness.py` compares both production workers with native
materialized views and an independent catalog/fact memory calculation. It covers
real sale/refund/cancel mutations, same-transaction join inputs, ties, refill,
independent pause/restart/drop, cold replay and ACK ordering. It runs in the common
gate. Focused Python and production-JavaScript probes qualify pacing, exact
rendering, query isolation, histories and unavailable states.
