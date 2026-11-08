# Concurrent tenant queries

Run independently owned queries on one runtime:

```sh
pgderive_worker --resources process.example.toml tenant-a-query.toml tenant-b-query.toml
```

Every `[worker]` configuration requires `tenant_id`, a nonempty identifier of at
most 128 ASCII letters, digits, dots, underscores or hyphens. Queries keep private
source connections, decoded values, arrangements, results and replication slots.
Tenant IDs bind ownership and resource accounting; the trusted SaaS control plane
must authenticate tenants, authorize queries and select credentials. These guards
do not make arbitrary tenant-supplied configuration or SQL safe to execute.

A catalog schema has one immutable tenant owner and catalog UUID. Multiple queries
of that tenant can share its catalog and exact object root, with distinct query IDs,
result tables and slots. Different catalogs must have disjoint object roots.
PostgreSQL ownership and conditional-create S3 ownership markers reject foreign
catalogs, tenants and ancestor/descendant root collisions, including separate
processes. Markers live under `.pgderive-ownership/` at bucket root; credentials
need read and conditional-create access there in addition to the data prefix.
Claims and ancestor branch markers persist after query deletion. Reusing a claimed
root for another catalog is intentionally unsupported. Existing unbound catalogs
or nonempty unbound roots are rejected: provision fresh owned namespaces.

`--resources` reads strict process-level TOML. Defaults are 64 configured queries,
64 per tenant, 128 MiB shared retained cache and 128 MiB shared memtable capacity;
each tenant may borrow up to the whole pool by default. Set smaller tenant ceilings
for a shared SaaS process. Query cache and memtable settings remain additional
local ceilings. Unused shared capacity can be borrowed by another query or tenant.
No decoded cache entries or logical state are shared between tenants. Encoded
Foyer cache memory is conservatively reserved at its configured size until the
cache releases ownership; that reservation is not dynamically loaned. Persisted
encoded-cache keys include the tenant ID and durable catalog UUID, so reassigning
a stopped worker's cache directory cannot produce cross-tenant or cross-catalog
hits. Worker reads never fall back to unscoped cache entries.

Charges follow retained allocation owners, including evicted rows pinned by readers,
old memtables, candidates and checkpoints. Retained cache families and native-value
retention use the shared cache pool. Native credits release with their last
owner; other allocation credits are swept on pressure and sampled diagnostics.
Diagnostic usage and high-water credits may conservatively include expired
weak-backed reservations awaiting a sweep; these are not physical memory peaks. Pressure evicts disposable cache data and
requests checkpoints; tenant-only pressure targets that tenant, while exhausted
process capacity can request peers to release capacity. A blocked complete epoch
is retried without consuming more source transactions. Recovery is serialized and
can checkpoint reconstructed published prefixes before continuing WAL replay.
An intrinsically oversized input fails with a capacity error.

These are retained-allocation limits, not RSS or full heap limits. Runtime stacks,
connections, CDC buffers, bounded operator workspaces, metadata and allocator
fragmentation require additional memory. Query execution limits still bound their
own temporary work. A container memory limit needs headroom above these budgets.

SIGINT and SIGTERM request durable drain from every query. A terminal query failure
is attributed and leaves healthy peers running; the process reports degraded
status and exits unsuccessfully once its remaining queries stop. Shared accounting
invariant failures drain all peers. Failed final checkpoints cannot produce a
successful shutdown. JSON diagnostics include tenant/database/catalog/query
identity and process capacity totals with only the emitting tenant's usage.

Historical process-sharing measurements are in [the experiment notes](multi-query-memory.md).
They predate pooled capacity and are not measurements of this implementation's
steady-state memory or capacity isolation.

The manual retained-state microbenchmarks exercise the production native-value
reservation and diagnostic paths at up to 100,000 retained values across 64 query
budgets, plus 100,000 weak-backed cache entries:

```sh
cargo test --locked --lib pooled_ -- --ignored --nocapture
```

They report admission and diagnostic bookkeeping cost. Use end-to-end workload
measurements for billing estimates. Native admission uses running tenant/process
credits; weak-backed cache admission can still sweep the registry under pressure.
