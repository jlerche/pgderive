# Portfolio totals and a live top ten

This example maintains two queries over `public.demo_positions`: all book
COUNT/SUM totals and the first ten groups ordered by units descending, NULL last,
then book ID ascending. Each query has one supervised process, an independent
logical slot, catalog schema, sink and object prefix. Both use the same source
publication. Computation and state are independent; the browser can observe
different committed source positions in the two results.

The assets extend an existing Supabase/PostgREST/Realtime/Caddy demo stack with
services named `db`, `rest`, `realtime`, `edge` and `engine`. They contain no
infrastructure credentials or public-host configuration. Database roles, TLS,
Realtime tenant setup, the locally served Supabase SDK and restricted public
access must already be provisioned. See [deployment](../../docs/deployment.md).
Disposable PostgreSQL must use tmpfs. This example does not make a tmpfs database
durable: losing its source/catalog requires explicit fresh bootstrap.

Render into an owned stack directory with these environment variables:

```sh
export DEMO_OBJECT_ENDPOINT=https://your-object-store.example
export DEMO_OBJECT_BUCKET=your-private-bucket
export DEMO_OBJECT_PREFIX=your-exclusive-fresh-demo-prefix
export DEMO_ANON_KEY=your-public-browser-key
export DEMO_PAGE_MODE=leaderboard # or totals
python3 examples/portfolio/render.py /path/to/owned/stack-r2
```

The renderer writes `query.toml`, `top10.toml`, a Compose override and public
assets. The object-store credential is an external mode0600 file owned by the
engine UID, mounted at `/run/credentials/r2.json`; the PostgreSQL password comes
from the existing private Compose environment. `DEMO_POSTGRES_HOST` defaults to
the internal `db` service. Each schema exclusively owns its configured prefix.
Use a new prefix for a fresh demo; never delete an unknown bucket or historical
prefix to clear state.

Use both Compose files for worker operations. First creation runs
`query create /etc/pgderive/query.toml` for each service; wait for each query's
ready event, stop it gracefully, and configure subsequent starts with
`query restart /etc/pgderive/query.toml`. The supplied override already uses
restart. Start `engine` and `engine-top10` explicitly after successful creation.
Changing the page mode does not require a query rebuild. The original totals
page is preserved in `web/totals.html` and remains available at `/totals.html`;
`DEMO_PAGE_MODE` chooses which page is copied to `index.html`.

The leaderboard is generated from that same original dark dashboard template.
It retains every original metric, the complete metric-history controls and
charts, the complete book overview and result activity. Added panels show the
top-ten result and measurements for each independent query. Numeric snapshots
arrive as text; the leaderboard uses BigInt, and the original chart receives
Numbers only after an exact integral range check.

For a newly owned database, `source.sql` creates the demo source and schemas;
`workload.initial_sql()` seeds twenty baseline books. Never run those commands against an
unrelated database. Before an authorized reset of an existing demo, stop its
generator and workers, archive private configuration, the original page and a
consistent source/catalog dump, and record the old image, slots and prefixes.
Retain old object namespaces for rollback/evidence. Reset only the registered
demo slots, dedicated catalog schemas and `demo_positions`; preserve the rest
of PostgreSQL and Supabase. Grant anonymous SELECT only on the two result
tables, deny source/metadata/writes, retain the engine's required disabled result
RLS contract, add both result tables
to `supabase_realtime`, and expose both result schemas through PostgREST.

`generate.py` limits source traffic to at most two transactions per second;
its actual rate depends on PostgreSQL/Docker execution and is measured on the page. Set
`DEMO_ROOT` to the demo deployment root containing `stack-r2` and `generator`.
It records exact affected-row counts and a bounded recent transaction history.
It guards the WAL backlog of both queries before issuing more source work.
Small quantity pulses and NULL-position insertions/deletions keep all-book
activity visible. An elapsed-time clock advances phases at thirty-five-second
intervals, independent of commit throughput, and resumes without a catch-up burst.
The phases raise an outside book into the
top ten, remove its boost so book ten refills, and create a cutoff tie that book
ten wins by ID. One low-quantity book also opens and closes outside the cutoff,
keeping that original totals-page behavior visible. The workload does not grow
source state without bound.

Install `collect.py` and `history.py` together under the deployment's `metrics`
directory, retaining the existing totals latency observer and read-only SQL
helper. The collector preserves the original totals/history fields and adds
`queries.totals` and `queries.top10`, with separate progress, WAL, RSS, processing
latency and prefix-scoped object inventories. Only totals has the existing
source-commit observer; top ten does not borrow its latency. The page renders
each query's measurement age and fetch time independently. Whole-portfolio
metrics always come from the complete totals view. Browser requests cast integer
and numeric outputs to text and use BigInt, preserving exact synthetic counts.

The leaderboard subscribes to both result tables, refetches their authoritative
snapshots, and specifies destination ordering explicitly. Three-second polling
recovers missed notifications. Realtime notification time, snapshot fetch time
and engine progress are distinct observations.

`scripts/query_concurrent_harness.py` runs the real workload and both production
workers against one owned publication. Native materialized views and an
independent source-row calculation check results while either query is stopped,
restarted or dropped and its peer continues. It verifies cold replay, distinct
stable slots, independent progress and ACK ordering. The common gate runs this
qualifier; `test_portfolio_demo.py` checks phase boundaries and both page modes.
