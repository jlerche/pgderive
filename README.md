# Pgderive

Pgderive is a PostgreSQL-only incremental view maintenance engine based on DBSP.
Committed `pgoutput` transactions are normalized into consolidated full-tuple
weighted batches and verified by a real-PostgreSQL row mutation harness. There
is an initial in-memory incremental equijoin; materialized-result sinks,
object-backed state and transactional publication are implemented for the typed
project/inner-join/group/count/nullable-integer-sum composition. A bounded SQL compiler now targets the grouped-join worker; see
[the SQL subset and compiler boundaries](docs/sql-compiler.md).

## Quality gate

Use Rust 1.95.0, pinned in `rust-toolchain.toml`, a C toolchain and libclang
(`libclang-dev` on Debian/Ubuntu) for the bundled pg_query PostgreSQL parser.

```bash
cargo install cargo-machete --version 0.9.2 --locked
cargo install cargo-llvm-cov --version 0.8.4 --locked
./scripts/check.sh
```

The gate runs custom-lint tests, Rust file-length checks, rustfmt, strict Clippy,
Rust tests, coverage, and unused-dependency detection. GitHub Actions runs the
same gate. Coverage combines unit tests and the sequential live PostgreSQL
harness, so the common gate needs local PostgreSQL or Docker plus `psql`. Compiler warnings and dead/unused code
are errors; unsafe code is forbidden. Clippy denies `all`, `pedantic`, and
`nursery`, plus explicit complexity and selected restriction lints.

| Limit | Threshold |
| --- | --- |
| Cognitive complexity | 15 |
| Function length | 80 lines, as counted by Clippy |
| Function arguments | 5 |
| Type complexity | 150, Clippy's score |
| Block nesting | 5 |
| Rust file length | 1,000 physical lines, including comments and blanks |

Rust files may override their limit once in the first ten lines, with a reason:

```rust
// pgderive: max-lines=1500 -- generated protocol definitions kept together
```

The custom file-length lint is a repository/CI check, not a compiler plugin.
Malformed, duplicate, or late directives fail the gate. See [AGENTS.md](AGENTS.md)
for the working rules and exception policy.

The gate requires at least **80% aggregate production Rust line coverage** using
[cargo-llvm-cov](https://github.com/taiki-e/cargo-llvm-cov). All production modules
and executable entry points are included; only test-source files are excluded.
`./scripts/check_coverage.sh` runs the coverage portion independently. JSON and
summary reports remain under `artifacts/coverage/run-*`; CI uploads verification
evidence even on failure. This is a line-coverage threshold, not branch coverage
or a substitute for the independent correctness oracles.

## Configuration

```bash
cp config.example.toml config.local.toml
export PGDERIVE__POSTGRES__PASSWORD=postgres
cargo run --locked --bin pgderive -- config.local.toml
```

The TOML file selects the PostgreSQL endpoint, existing logical replication slot,
publication, TLS mode, and listener limits. `PGDERIVE__SECTION__KEY` environment
variables override file values. Keep passwords in the environment or the ignored
local file. Supported TLS modes are `disable` for local development and
`verify-full` with optional `ca_file` for remote connections. Hosted connections
have not been tested yet. Replication must use a direct connection, not a
transaction-pooling endpoint.

The server requires `wal_level=logical`, a replication-capable role, a `pgoutput`
slot, and a publication selecting the source tables. For a manual fixture:

```sql
CREATE TABLE public.listener_demo(id bigint PRIMARY KEY, value text);
ALTER TABLE public.listener_demo REPLICA IDENTITY FULL;
CREATE PUBLICATION pgderive_dev_pub FOR TABLE public.listener_demo;
SELECT pg_create_logical_replication_slot('pgderive_dev_slot', 'pgoutput');
```

Run the listener using the configured database, then insert/update/delete source
rows in a second session. Stdout contains one JSON object per complete committed
transaction: xid, commit/end LSNs, table/column names, operation, old/new rows,
and a consolidated `batch.updates` array. Each weighted update contains its full
`tuple` (schema, table, all columns) and signed `weight`. Inserts contribute +1,
deletes -1, and updates retract the old row and add the new row. Identical tuples
consolidate within the commit; zero weights disappear. Keys do not define tuple
identity. This batch is stateless; source LSNs are not DBSP logical timestamps.
Column values are PostgreSQL text representations or JSON null, not generic
typed DBSP tuples. Relation metadata is decoded with the pinned
`postgres-replication` parser from Supabase's rust-postgres fork.

`max_transactions=0` listens until Ctrl-C; a positive value stops after that many
commits. `max_transaction_changes` bounds buffered row changes per transaction.
Unknown protocol events, binary tuples, and unchanged TOAST values fail closed.
Use `REPLICA IDENTITY FULL` when complete old rows are required; a diagnostic
connection does not reconstruct missing old images from primary-key-only CDC.

**This listener does not acknowledge received or printed rows.** It has no durable
publication yet, so slots retain WAL while listening and may replay transactions
on reconnect. Drop a manually created demo slot after stopping the listener:

```sql
SELECT pg_drop_replication_slot('pgderive_dev_slot');
DROP PUBLICATION pgderive_dev_pub;
DROP TABLE public.listener_demo;
```

## Real PostgreSQL harness

```bash
./scripts/check_integration.sh
```

The script uses a local PostgreSQL instance at port 55434, reusing the existing
PoC service when available. Otherwise it starts this repository's PostgreSQL 17
Compose service with ephemeral tmpfs database storage and no persistent Docker
volumes. Stopping this Compose service discards its database contents. The script
creates `pgderive_dev` only if absent and never resets an existing database. It also reuses the PoC SeaweedFS service at port 8333, or starts this repository's pinned SeaweedFS image with tmpfs storage when unavailable.

The harness starts the same listener used by the executable, waits for the
replication connection, and drives fifteen committed transactions:

1. Insert a person, an auction, and ten bids: 12 row changes.
2. Update the person to a NULL name, change the auction category and ten bid
   prices together: 12 row changes. Both join inputs change in this commit.
   An additional rolled-back insert must not appear in CDC.
3. Delete five bids: 5 row changes.
4. Insert then delete a bid, update a bid without changing its value, and change
   the person twice back to its original value: 5 row changes, an empty batch.
5. Move the category and a price through NULL, restore qualifying rows, move the
   category again, remove all qualifying prices, recreate qualifying bids
   (including a NULL bidder), then delete them: six more commits.
6. Create zero-sum and all-NULL groups, turn NULL into zero, remove the final rows,
   then recreate and remove a zero-sum group: five more commits.

After every commit, it checks full old/new row images against an independently
accumulated source-row map and compares that map with source SQL. It also checks
each weighted batch equals the difference between full source SQL snapshots,
and that durable slot progress was not advanced. The first four batch sizes are
12, 24, 5, and 0.
The harness also integrates auction–bid join result deltas and compares them with
a full SQL join after every commit, plus filtered projections and grouped counts.
This fixture uses Nexmark table shapes and a small explicit query graph; it is
not a Nexmark benchmark.

Each run creates a fresh `pgderive_harness_<pid>` schema. Publication/slot names
must begin with `pgderive_` and must be unused; existing names are rejected.
Normal completion removes the fixture schema, publication, and slot. Failed runs
retain them for inspection and print their names; the next run refuses
to overwrite a leftover publication or slot.

JSONL, logs, configuration/source/binary hashes, and version information are saved
in a fresh ignored `artifacts/replication/run-*` directory, including stderr on
failure. Run mutating harnesses sequentially. The PostgreSQL service remains
running after the check; this repository does not provision persistent database
volumes.

The diagnostic listener excludes snapshot bootstrap and durable result
publication; the continuous worker implements them. Schema evolution, TRUNCATE,
recursion and operationally safe WAL-retention management remain unsupported.

## First DBSP operator

`pgderive::engine` provides generic `ZSet<T>` and `IncrementalJoin<K, L, R>`
primitives. Collections retain full tuple identity and signed i64 multiplicity.
One `step` accepts both transaction deltas and evaluates
`ΔL ⋈ R + L ⋈ ΔR + ΔL ⋈ ΔR` against the previous input state. It commits both
inputs and advances a separate logical tick only after checked arithmetic
succeeds. Errors leave operator state and time unchanged. Empty commits still
advance time. Keys only select matching tuples; they do not impose uniqueness.
The fixture adapter skips NULL keys to match SQL equality.

This is an in-memory semantic baseline with nested scans and cloned state. It
is exercised by the live harness, while the standalone listener continues to
emit diagnostic batches. This reference operator does not itself persist state; the composed MVP below
uses object-backed arrangements, snapshot bootstrap, recovery and durable
acknowledgement. SQL planning, recursion and frontier tracking remain ahead.

Tests follow the independent raw-history nested-loop oracle from the PoC's
`src/bin/zset_contract_tests.rs.inc`: 128 deterministic signed-input histories
with 16 steps each, simultaneous updates, multiplicity, cancellation, and atomic
failure on arithmetic/time overflow. Prior accepted evidence is in the PoC's
`zset-contract-default-checks.jsonl`; its storage/recovery results are not claims
about this new engine. Storage/recovery checks were inspected and left unchanged;
the source event trace is replayed by the grouped-count tests described below.
Filter/map, an owned acyclic circuit boundary, and grouped counts now compose
with this join. Object-backed arrangements and durable publication are described
below.

## Weighted filter and map

`ZSet::try_filter` and `ZSet::try_map` transform signed deltas without input
mutation. Projection consolidates identical output tuples, including negative
weights and cancellation. Callbacks must be deterministic functions of the full
tuple; changing predicates between retraction and insertion is unsupported.
Callback errors and arithmetic overflow return no partial output.

The live harness composes the auction–bid join with category=20 and price>=205
filtering, then projects category/bidder, preserving bag multiplicity. It checks
the integrated projection against grouped SQL after every source commit.

## Transaction-atomic acyclic circuit

`Circuit<S>` stages an independent clone of owned graph state, evaluates its
explicit Rust node order, and publishes state/output with one logical tick only
when the entire transaction succeeds. The harness graph is source deltas → join
→ filter/map → integrated bags and grouped counts. Downstream errors roll back
the join too.
Tests inject failure after all nodes and projection overflow, then retry the
same input and verify the original tick and result.

All mutable operators and outputs must be owned by `S`, whose clone must be an
independent value snapshot. Callbacks must be deterministic and have no external
side effects or shared mutable handles. The ownership contract is documented,
not enforced by a dynamic planner. This stages memory only; it does not publish
PostgreSQL DML, persist state, acknowledge source progress, or provide recursion.

## Incremental grouped counts

`GroupedCount<K>` sums signed full-tuple input weights per group. Changes emit
a retraction of `(key, old_count)` and an insertion of `(key, new_count)`, each
with unit weight. A zero count removes the group; a net-zero transaction emits
no count rows. Signed counts are supported as algebra, while SQL COUNT(*)
equivalence assumes valid nonnegative source bags. NULL grouping is supported
with `Option` keys. Arithmetic failure leaves count state unchanged, and the
enclosing circuit protects upstream/downstream state as well.

The harness builds qualifying bid counts per auction category (price>=205),
checking full SQL joins, filtered/projected bags, and SQL GROUP BY counts after
every commit. Tests include group creation/disappearance, signed weights,
NULL transitions, simultaneous input changes, and failure/retry.

A checked-in event-only copy of the accepted PoC trace replays all 120 recorded
transactions (111 commits, 9 rollbacks) through join/filter/map/count against an
independent source-map recomputation, scoped to projects 1–32. Initial source
rows are supplied explicitly in memory. Fixture provenance and scope are in
[tests/fixtures/README.md](tests/fixtures/README.md). This strengthens algebra
validation; it does not establish storage durability or CDC snapshot bootstrap.

## Core implementation focus

Further work prioritizes correct DBSP weighted semantics and object-backed
arrangements. The demo requirement and demo-oriented roadmap are withdrawn.
See [the core contract and audit](docs/core-contract.md) for the three core slices:
canonical weighted arithmetic, immutable batch/trace readers, and incremental
operators over pinned traces. The initial intermediate-overflow gap is fixed by exact intermediate arithmetic
and declared i64 finalization boundaries.

## Canonical batch arithmetic

`engine::Batch<K,V>` is an immutable sorted logical batch over complete `(key,
value)` identities, and `IndexedZSet<K,V>` names the keyed reference collection.
`BatchBuilder` accumulates exact arbitrary-precision coefficients; unfinished
builders can merge without narrowing at arbitrary physical boundaries. `finish`
removes zero coefficients and rejects only final weights outside i64.

The same exact accumulator backs collection normalization, projection collisions
and all join cross terms. Grouped counts incorporate prior state before narrowing
the final count. Every materialized operator output and committed state must fit
the i64 domain. Failed finalization retains the prior circuit/operator state.
This defines bounded logical collections with exact intermediate arithmetic, not
unbounded persisted weights. Immutable codecs and trace readers use the same finalization contract.

## Immutable batch readers and traces

`BatchReader`/`BatchCursor` expose fallible forward traversal and key seeks for
memory batches and `ObjectBatch` JSON-v1 runs. Object runs are content addressed,
written with create-only PUTs, and reopened from trusted coarse `ObjectRef`
metadata. Per-block hashes and a trusted index hash are verified before rows are
exposed. Schema IDs must identify the exact key/value types and Rust ordering;
Serde must preserve full identity. This is an explicit initial codec contract,
not a generic SQL encoding or a compressed/binary format.

`TraceSnapshot` pins immutable run membership. Merged cursors consolidate all
full identities exactly, including signed cancellation across arbitrary physical
run boundaries. `Trace::prepare_runs` checks the complete logical delta and next
state, and `commit` rejects stale or foreign preparations. Compaction validates
weighted equivalence, changes physical generation, and retains logical time.
Old snapshots remain readable because objects are retained; GC is not implemented.

This standalone API is a storage semantics baseline. The catalog publication
path below composes it with durable PostgreSQL state.
Index fences are resident/object-local. Object cursors retain one decoded block;
merged reads open one cursor per run. Preparation currently scans/materializes
complete candidate state for validation, and writers buffer full objects. No
bounded-memory claim is made. Filesystem-store tests reopen real immutable bytes;
cloud endpoint provisioning remains unsupported; the composed worker below adds
retries, catalog recovery and fenced concurrency control.


## Operators over pinned object traces

`Stream<B>` is one typed, complete transaction batch with logical time. The
initial `TraceQuery<K,L,R,G>` binds its pure, deterministic classification
function at construction; changing query semantics requires a new graph. It is an explicit acyclic equijoin → fallible
filter/group projection → grouped count graph. It probes prior arrangements
through key seeks and evaluates `ΔL⋈R + L⋈ΔR + ΔL⋈ΔR` with exact intermediate
products. Full `(key,value)` identity is preserved; neither source key is assumed
unique. Counts read affected groups from their prior trace, incorporate prior
counts before narrowing, and emit unit-weight old/new count rows.

Preparation uploads immutable input/count deltas and validates candidate states.
One local root assignment publishes all three arrangements together. Failed
reads, callbacks, finalization and uploads leave the root unchanged; stale or
foreign candidates are rejected. Empty transactions advance the shared logical
clock. Compaction publishes equivalent physical memberships at the same tick,
invalidates preparations based on the replaced root, and retains pinned readers.
It neither emits a logical batch nor deletes objects.

Tests compare signed updates with independent bag recomputation and memory
operators. The accepted PoC event replay checks all 111 committed transactions
(9 rollbacks skipped) against the independent source-map oracle, checking input
arrangements as well as counts. Object task arrangements are scoped to the
fixture's project domain 1..=32; source-map validation still processes every
recorded event. The sequential live PostgreSQL harness compares the object graph
and memory graph with SQL after every committed transaction. Missing objects,
filesystem PUT failures, corrupt immutable-upload collisions, arithmetic failures, retries and compaction
races are exercised. These are correctness checks, not performance benchmarks.

This fixed Rust graph is not a SQL planner or general circuit scheduler. Object
encoding and full candidate validation remain buffered; many runs cause repeated
reads. The registered grouped-join composition below adds catalog publication,
authoritative restart, sink DML, acknowledgement and object GC. This fixed
reference graph itself remains a semantic baseline.

## Composable typed MVP operators

`Stream<T>` now represents a typed circuit edge; `TimedBatch<T>` represents one
logical delta-tick value. `CircuitBuilder<S>` binds source, unary and binary
callbacks to a validated plan, derives an executable topological schedule from
typed dependencies, and supports fan-out, chained joins and multiple outputs.
All nodes read the same immutable prior state. Deferred arrangement replacements
form a candidate only after the whole tick succeeds; the existing Engine owns
local prepare/commit publication. The production grouped SQL worker uses this
scheduler.

`Project` consolidates full-tuple filter/projection collisions, `Join` includes
all simultaneous-input terms, and `Arrangement` stages immutable object state.
The lower-level `Graph<State, Input, Output>` evaluator remains available for
reference/custom compositions. Source transactions choose delta-tick boundaries
in the PostgreSQL adapter; SQL syntax supports the bounded grouped and
single-source projection subsets.

The older `TraceQuery` count graph remains a reference/check fixture while the
MVP composes these reusable operators. Graph tests cover project collisions,
join fan-out/cross terms, pinned readers, downstream errors, empty transactions,
and stale/foreign work. Aggregate and live S3 compositions build on these APIs.

`GroupSum` adds grouped weighted numeric aggregation. Its immutable `SumState`
contains total row multiplicity, non-NULL multiplicity, and sum. Products and
prior-state accumulation use arbitrary precision before final i64 checks. A
present zero sum is retained, all-NULL groups emit NULL, and removing a group's
last row retracts it. Internal statistic changes and visible result changes are
separate typed edges; both can be staged in the same graph root. Identical visible
rows cancel even when internal counts change.

This MVP aggregate requires valid nonnegative SQL source bags, although deltas
include retractions. Group totals are checked but do not prove per-tuple source
validity. Numeric measures and finalized sums/counts are i64; overflow fails the
transaction. It does not yet implement arbitrary PostgreSQL numeric types.
Independent source-bag histories and all 111 committed recorded PoC transactions
check object-backed grouped sums, including prior-state/product cancellation.


## Project/join/group/sum MVP with SeaweedFS

The live harness composes reusable typed operators into a query that projects
auction categories and bid prices, joins by auction ID, groups by category, and
computes SUM(price). Both projected input arrangements, aggregate statistics,
and integrated visible SUM rows live in immutable S3 objects. Preparation stages
all four traces and one graph root publishes them together. Every transaction is
checked against direct PostgreSQL SUM and independent source-map recomputation.
Compaction at tick five checks pinned readers and unchanged logical time.

`./scripts/check_integration.sh` provisions an isolated `pgderive-mvp-tests`
bucket if absent and uses a fresh unique object prefix per run. It starts a fresh
local HTTP latency proxy on a dynamic port and routes the configured S3 client
through it. Request logs, storage profile, CDC output, and source/binary hashes
are retained under `artifacts/replication/run-*`. S3 objects are retained under
the isolated prefix; no accepted PoC objects or frozen binaries are changed.
The common coverage gate includes this sequential live S3/PostgreSQL harness.

The proxy's default GET p50/p95/p99 is 26.13/38.86/86.13 ms, and PUT is
69.75/101.10/137.23 ms, from the labeled raw tables in the
[2025-03-04 public S3 benchmark](https://topicpartition.io/misc/AWS-S3-PUT-latency-benchmark)
(500 KiB, EC2/S3 eu-north-1, 100 samples). The source's TL;DR swaps the medians;
the raw tables are authoritative for this profile. These are one workload's
measurements, not universal S3 latency. The proxy applies a seeded synthetic
inverse CDF: linear interpolation through p0=0, p50, p95, p99, with the top 1%
clamped at p99. HEAD uses the GET model; DELETE/POST use PUT. Each sampled delay
is added before forwarding; local service/network overhead remains additional.
The model has no payload-size/bandwidth scaling or correlated tails. Seeds are
reproducible per operation, though concurrency can change request assignment.

```bash
./scripts/check_integration.sh                         # published latency profile
PGDERIVE_S3_LATENCY_SCALE=0 ./scripts/check_integration.sh # fast local correctness
PGDERIVE_S3_FAIL=GET:1 PGDERIVE_S3_LATENCY_SCALE=0 ./scripts/check_integration.sh
PGDERIVE_S3_FAIL=PUT:1 PGDERIVE_S3_LATENCY_SCALE=0 ./scripts/check_integration.sh
python3 scripts/s3_latency_proxy.py --port 8334         # standalone local proxy
```

The failure cases disable S3 client retries, verify that the first failed
transaction did not publish any graph state, then explicitly retry the same
transaction and run all SQL checks. Arbitrary injection positions are available
in the proxy; the harness's expected-failure mode targets the first transaction.
The MVP uses valid SQL bags and nullable i64 measures/sums. It has no SQL parser.
The registered contract, bounded writers, restart manifests, atomic publication
and collection implemented in the following sections complete the durable path.
These fixtures establish correctness within their workloads, not cloud throughput.

## Registered production engine contract

`engine::plan::Plan` validates explicit source/type contracts, topologically
ordered operator arities, reachable outputs, and named persisted arrangements.
Its SHA-256 identity covers the complete registration and semantic revision.
Callback implementation changes require a new revision; caller-supplied schema
identities remain an explicit Rust/codec contract until SQL lowering exists.

`Engine` binds an executable evaluator to a plan and checks the actual immutable
state membership, schema identities, and every arrangement tick before local
publication. Missing, extra, duplicate, or differently timed state fails without
moving visibility. `query::GroupedJoin` is the production reusable two-source
project/join/group/COUNT/SUM executor; the live harness supplies only domain
adapters and independent oracles. Its integrated output includes COUNT(*) and
nullable SUM. A separate registered three-source chained-join test checks
simultaneous input changes against full bag recomputation. Durable publication,
resource-bounded execution and startup schema inspection remain subsequent slices.

## Bounded immutable batch format

New batches use v2: create-only, content-addressed data blocks and a manifest
containing object-local fences and checksums. Encoding borrows at most 65,536
input rows and caps each encoded block and the resident manifest at 8 MiB;
reading fetches and checks one block at a time. The source `Batch` and downstream
transaction operators still reside in memory until the execution-budget slice.
The row-count setting determines chunk boundaries: a chunk over the byte cap
fails rather than splitting automatically. Reduce `block_rows` for wide rows.
Failed uploads leave unpublished objects for later garbage collection and never
advance engine state. Retry collisions check stored size and exact bytes.

Packed v1 objects remain readable within the same 8 MiB block/index caps.
Oversized legacy objects require an offline rewrite before this reader accepts
them. Durable membership will publish the manifest root; reclamation must follow
its block references rather than treating that root as the complete batch bytes.

## Transaction execution limits

`[execution]` configures resident record bytes/entries, individual record size,
contribution count (including join fanout), simultaneous scratch runs, scratch
bytes, and finalized output bytes/entries. Source transactions have the same
record/output byte limits plus the listener's change-count limit. A failed source
transaction cannot commit or continue with partial rows; reconnect/replay is
required. The diagnostic listener continues to leave its slot unacknowledged.

Source normalization and production project, join, and grouped COUNT/SUM use
checksum-protected, sorted local scratch runs. Coefficients remain BigInt across
spills and k-way merging. Aggregate contributions merge before adding prior
statistics and narrowing; join merges all three terms before narrowing. Scratch
is temporary execution work; durable arrangement data remains in object storage.
Successful evaluations remove scratch. Failures retain `pgderive-spill-*`
directories with their path in consolidation errors for investigation.

Final edge batches remain bounded in-memory collections. Byte budgets measure
serialized records, with entry caps bounding container overhead; they are not an
exact Rust allocator/RSS measurement. The engine rejects oversized work before
local publication. Candidate trace validation now streams without collecting
full state, but still scans all identities. The affected-key reads and bounded
cache described next avoid scanning unrelated input records during evaluation.
Maintenance uses the streaming-compaction path described below.

## Affected-key arrangement access

Production joins batch probes by navigation key and reuse pinned run heads for
all input values under that key. Object-local full-tuple fences exclude unrelated
runs and select the first relevant block directly. Key cursors keep every distinct
value and stop before a later key's blocks; zero-weight identities still cancel
across all matching runs. The delta cross term uses sorted key ranges instead of
comparing every left row to every unrelated right row.

`Arrangement::stage` checks only changed full identities against the pinned prior
coefficients before uploading. It relies on the prior boundary having already
been validated; it does not rescan unchanged state to detect unrelated external
object loss. Full scans remain available for the independent oracle and storage
validation. Cache residency is bounded by `execution.cache_bytes` and
`cache_entries`, shared across all production query arrangements. The cache holds
verified encoded bytes; active decoded blocks and resident indexes are separate
reader pins. Cursor clones share decoded blocks rather than copying them.

Cold, cached, evicted, and differently split runs are checked for equal weighted
results. Selected-block failures retain cursor position for retry. Unrelated
missing blocks do not force a key probe to read outside its scope; fresh recovery
validates its complete durable membership through the manifest path below.

## Durable arrangement manifests

`GroupedJoin::checkpoint` exports the committed plan identity, logical time,
arrangement schemas, physical generations, and ordered object-root membership.
Repeated roots remain separate runs so their weighted multiplicity survives a
restart. PostgreSQL stores this coarse membership, its count and digest; block
fences and fine-grained indexes remain in object storage.

`catalog::Catalog` saves a checkpoint in one PostgreSQL transaction using an
expected epoch to reject stale writers. Loading uses a consistent snapshot and
rejects incompatible registrations, incomplete membership, altered references,
and inconsistent arrangement clocks. Catalog times, generations, and epochs
must fit PostgreSQL's nonnegative signed bigint range.

Reopening validates all referenced roots and blocks with a cold reader before
attaching the shared cache. A warm cache cannot conceal missing or corrupt
storage during recovery. The live harness checkpoints at ticks 5 and 15 and
restores all four arrangements in a separate process, comparing their state
with independent source recomputation. It also checks stale writes, rollback,
metadata corruption, and missing roots and blocks.

This checkpoint API persists arrangement metadata only. It does not apply sink
DML, record source progress, or acknowledge the replication slot. Atomic
publication of those effects uses the separate API described next.

## Atomic destination publication

`Catalog::claim` binds an owned destination and a stable source registration to
an empty tick-zero checkpoint. PostgreSQL fences each claimed writer; reclaiming
ownership invalidates older writers even when their publication epoch matches.
Once bound, the metadata-only checkpoint API cannot overwrite that query.

`GroupedJoin::publish_prepared` holds exclusive runtime ownership while checking
its preparation, encoding result deltas, and committing PostgreSQL membership,
destination changes, and source commit/end LSN plus transaction ID together.
Immutable objects were uploaded during preparation. Publication sets
`synchronous_commit=on`; local visibility moves only after confirmed COMMIT.
Logical ticks and WAL addresses remain separate. A failed or uncertain COMMIT
returns an error; uncertain outcomes require authoritative recovery before retry.
Replication acknowledgement remains disabled in the diagnostic harness.

The initial explicit destination codecs use JSONB identity. Bag destinations
store the complete `(key,value)` tuple and its nonzero signed coefficient;
navigation keys do not overwrite other values. Grouped destinations store an
encoded group, positive COUNT(*) and nullable SUM, checking the exact prior row
before INSERT/UPDATE/DELETE. Registered codecs must preserve tuple identity under
JSONB equality. Native SQL column mapping comes with the later SQL compiler.
Destinations are engine-owned tables; external writers are unsupported.

The live harness compares both destination encodings with SQL after every source
transaction, checks persisted membership and source positions, rejects replaced
workers and metadata-only overwrites, and injects destination DML failure to
verify complete rollback. An isolated arithmetic fixture exercises same-key bag
multiplicity, cancellation and final-coefficient overflow without acknowledging
its synthetic source positions.

## Recovery, replay and acknowledgement

`Catalog::load_durable` reads membership, source positions and worker ownership
from one repeatable-read snapshot. `Writer::reconcile` first locks the query row
to wait out the original publication transaction. It accepts only the exact
unchanged prior boundary or the exact candidate, epoch and transaction identity.
A writer marks COMMIT uncertain before awaiting its response, so cancellation or
connection loss cannot permit blind retry or authorize acknowledgement.

`GroupedJoin::restore_checkpoint` cold-validates every arrangement before replacing
local visibility and invalidating old preparations. Replay uses WAL order; the
transaction at the exact durable end must also match its commit position and ID.
A conflicting or overlapping transaction fails closed.

The registered stream plumbing resumes at the durable end and leaves received
transactions unacknowledged. Before updating pgwire feedback it rechecks the
writer against PostgreSQL membership, progress and fencing, and requires matching
source registration. The diagnostic listener remains unacknowledged. The
continuous worker composes this path with the verified bootstrap described below.

The local SQL fault proxy disconnects before forwarding COMMIT, or discards its
response only after PostgreSQL confirms successful COMMIT. Evidence records which
outcome occurred without recording SQL or credentials. The harness verifies both
resolutions, cold recovery, replay rejection, and blocked blind retries. After
its fifteen diagnostic transactions, it resumes from durable tick 15, proves that
periodic feedback cannot acknowledge merely received tick 16, then publishes and
acknowledges tick 16. A separate process reopens its state and exact source position.

A third proxy mode holds an issued COMMIT while its PostgreSQL transaction stays
open. The harness cancels the publication future, verifies acknowledgement is
blocked, and proves reconciliation waits on that transaction's query-row lock.
Releasing COMMIT then recovers the exact committed candidate. This directly
checks cancellation safety and the serialization barrier used for resolution.

## Consistent snapshot bootstrap

`source::Export` creates a new persistent pgoutput slot through the PostgreSQL
replication control protocol and owns its idle snapshot-exporting connection.
Initial reads import that exact snapshot in a read-only repeatable-read SQL
transaction. Both control and SQL sessions verify cluster system ID, timeline
and database. The source role needs replication permission, full SELECT access,
and EXECUTE on `pg_control_system()` and `pg_control_checkpoint()`. SQL reads set
`row_security=off`: RLS may be enabled on a Supabase table, but a reader that would
see only a policy-filtered subset is rejected. Use an authorized full-row reader.

The frozen source contract accepts explicit full-table publications of ordinary
persistent, nonpartitioned tables with primary keys and REPLICA IDENTITY FULL.
The initial codecs support boolean, int2/int4/int8, text, varchar and UUID, with
NULL preserved. Domains, enums, numeric/floating-point, timestamps, arrays, JSON,
generated columns and nondeterministic collations are unsupported. Snapshot SQL
uses exact quoted column names and pgoutput-compatible text representations,
including boolean `t`/`f`. Bounded cursors, server-side encoded-row limits and the
same spill/output limits as CDC prevent an unbounded initial copy. Unchanged TOAST
and TRUNCATE remain fail-closed; publications must emit TRUNCATE rather than omit it.

After initial object PUTs, `Catalog::activate_snapshot` atomically publishes tick
one memberships, initial destination rows, exclusive slot ownership and the
slot's consistent point with synchronous COMMIT. Snapshot progress has no source
transaction ID. No query or feedback capability exists before activation. An
uncertain activation requires reloading the exact authoritative candidate;
activation replay cannot apply destination DML twice. Cold reopen precedes CDC,
which resumes at that point and records real source transactions from tick two.
The legacy tick-zero claim path is retained for diagnostic/arithmetic fixtures;
registered native sources cannot use it to bypass initial-copy activation.

Observed wire-layout drift and current native metadata drift reject execution or
publication. Publication holds source table locks that exclude table DDL through
COMMIT. The MVP requires administratively immutable publication membership,
slot ownership and source schema while a query is active. In particular, a
privileged removal and re-addition of a publication table can omit intervening
CDC while leaving identical final metadata; the current checks cannot detect
that history. Table locks do not lock publication membership. Plan changes need
an explicit new registration/bootstrap, not an in-place SQL schema migration.
One authoritative catalog namespace owns each registered slot; external sink,
metadata, publication and slot writers are unsupported.

Resume checks reject a different cluster/timeline/database, an active or temporary
slot, a non-pgoutput plugin, missing/lost WAL, and a slot whose confirmed or restart
position is past engine progress. This catches an externally advanced or recreated
slot before pgwire can silently skip source transactions. These checks do not
solve WAL-retention sizing, privileged mutation races or failover recovery.

The live harness covers a nonempty snapshot with concurrent changes on both join
inputs, exact snapshot-plus-CDC reconstruction, initial atomic COMMIT response
loss on both sides of the boundary, replay rejection, fresh-process reopen,
primary-key/publication drift, advanced/recreated slots, RLS rejection, bounded
copy rejection, quoted names and boolean/null codecs. A failed copy retains its
persistent slot and evidence. Restarting an unfinished exported copy requires a
fresh snapshot boundary. The continuous worker journals its physical slot name
and can reset only that exact unactivated slot. Activated state always resumes
its durable source boundary. Fenced collection handles abandoned upload objects.

### Streaming maintenance and physical collection

`GroupedJoin::prepare_compaction` merges complete full-tuple identities through
bounded cursors, then writes consolidated v2 blocks and roots. Compaction never
advances DBSP time or emits result deltas. Default writer budgets are 8 MiB per
block/index, 4,096 rows per block, 4,096 block fences per root and 128 output roots;
input traces are capped at 128 runs for compaction. Exceeding a budget fails before
publication. `publish_compaction` verifies the exact authoritative prior
membership, publishes a new physical epoch with unchanged sink/source progress,
and then replaces local visibility. Lost COMMIT responses require authoritative
maintenance reconciliation and cold reopen before any further publication or ACK.

For a catalog using GC, reserve with `Writer::protect_upload` **before PUT** (bootstrap uses
`Catalog::protect_upload` before progress exists) and
use `prepare_protected` or `prepare_compaction_protected`. Each reservation has a
random, durable physical namespace; equal content in different reservations has
different physical addresses. Publication accepts newly managed roots only from
an active reservation at the current writer fence, or exact existing committed
roots. Release an upload with `Protection::published` after authoritative
confirmation and after all its writes have finished. Never reuse a reservation's
namespace after release. These rules protect new uploads from a delayed DELETE
whose PostgreSQL session has already disconnected.

Readers and recovery must call `protect_checkpoint` before opening objects and
restore from its `stored()` checkpoint, which is the exact boundary pinned by the
reservation. Keep that protection until **every** dependent reader is finished;
then call `reader_finished`. An in-process `Arc` by itself does not protect objects
from an external collector. Reader protections never expire automatically. After
an authoritative writer claim, `seal_fenced_uploads` seals abandoned uploads from
older fences; their original writers can no longer publish them. An unfinished
bootstrap retains its reservation until activation/recovery establishes ownership.

`Catalog::collect` uses an exclusive schema lifecycle barrier, validates every
committed/protected root, discovers block addresses from object-local indexes,
finishes bounded enumeration, and only then deletes unreachable canonical v2
objects from sealed upload namespaces. Active uploads stop collection. Metadata
budgets default to 4,096 roots, 262,144 enumerated/reachable paths and 16 MiB of
encoded catalog metadata. Missing/corrupt roots or exceeded budgets abort before
DELETE. Partial sweep failures are safe to retry. Committed memberships, source
progress and destination data are never changed by collection.

Use a store prefix owned exclusively by the catalog schema. Unknown names and
legacy objects written without managed upload reservations are retained; migrating
that diagnostic state needs a separate offline procedure. Closed namespace ledger
rows remain durable and grow over time. Reader protection cleanup after an actual
reader crash requires evidence that its readers have ended; there is no time-based
lease expiry. WAL retention, continuous worker policy and sustained workload
qualification remain separate work.


## Continuous worker

`cargo run --locked --bin pgderive_worker -- worker.local.toml` runs the explicit
native-bound grouped inner join described in `worker.example.toml`. Create the
source tables, their two-table publication and an empty owned catalog schema
first. Source tables require primary keys and `REPLICA IDENTITY FULL`; publication
membership and native types must remain fixed. PostgreSQL must permit logical
replication, consistent snapshot COPY and destination DML. The current storage
configuration supports local loopback S3-compatible endpoints; cloud endpoint
configuration remains outside this local qualification.

The worker imports an exported snapshot, atomically activates object membership,
the grouped destination and source boundary, then consumes complete source
transactions. Both join inputs may change together. Group keys preserve NULL;
COUNT(*) counts matches and integral SUM ignores NULL measures. Every source
transaction publishes immutable objects before committing membership, sink DML
and progress together. Only authoritative publication permits slot feedback.
The destination columns are `group_key` (JSONB), `row_count`, and nullable `total`.
The worker accepts legacy selectors or the bounded SQL frontend described in
[the compiler contract](docs/sql-compiler.md). Grouped SQL executes this composition;
single-source projection/filter SQL binds a separate two-node circuit and weighted
bag sink with explicit native scalar JSON encoding.

The configured slot is a logical alias. A synchronous PostgreSQL registration
journal records an unpredictable physical slot name before creating it. Restart
may reset that exact owned slot only before activation; once activated it resumes
the durable slot and checkpoint. It rejects changed source/query/destination or
object-prefix configuration, missing/recreated/advanced slots and unjournaled
existing slots. Preserve the journal, schema and object prefix together. Do not
manually drop the physical slot to repair a worker failure.

One worker owns a query through a PostgreSQL session advisory lock and fenced
publication. Consecutive failures retry with bounded backoff and authoritative
cold reopen; a lost COMMIT response never permits blind replay. Private recovery
pins can be retired by a succeeding fence, but recovery must reconfirm ownership
and exact membership before exposing readiness. External reader pins remain
until their owners explicitly release them. Compaction/GC happens between source
transactions and preserves source position and logical time. Ctrl-C drains the
current publication and shuts down replication. SIGKILL requires restart recovery.

JSON events expose readiness, publication time/source end, transaction processing
latency, cache bytes/entries/hits/misses, maintenance and retries. The worker
processes one source transaction at a time, with one pgwire event of read-ahead.
Execution limits bound decoded transaction output, operator contributions,
resident consolidation, scratch, finalized batches and cache. They are not an
RSS bound. The vendored pgwire framing patch rejects payloads over 1 MiB before
allocation; a one-event channel limits read-ahead. The wire cap is independent
of operator budgets, and an oversized frame fails without acknowledging it. WAL retention/backlog requires independent operational
monitoring; crash consistency does not bound retained source WAL.

The checked-in sequential qualification uses isolated schemas and compares every
settled result against PostgreSQL, preserving failure dumps and process logs:

```sh
cargo build --locked --bin pgderive_worker
mkdir -p artifacts/worker/my-run
PGDERIVE_S3_LATENCY_SCALE=0 python3 scripts/run_with_s3_proxy.py artifacts/worker/my-run \
  python3 scripts/worker_harness.py --faults --kill --ticks 12 --burst 16 \
  artifacts/worker/my-run -- ./target/debug/pgderive_worker
```

Omit the latency-scale override to use the recorded public-S3 interpolation
profile. `--faults` sequentially tests COMMIT loss before/after registration,
activation and source publication. `--kill` checks a between-transaction
process kill. `scripts/check_worker.sh`, also run by the common coverage gate,
adds witnessed kills during bootstrap PUT, pinned cold restore, publication
COMMIT, maintenance COMMIT and GC DeleteObjects. Its recovery test retains an
external reader pin while retiring the abandoned private recovery pin. This command is qualification evidence, not a throughput or
cloud-latency guarantee.


For a larger local spilling and backpressure qualification, use `--rows 4096
--auctions 128 --burst 16`. The fixture sets 128 resident identities and a 64 KiB
resident byte budget to exercise external consolidation. Request logs, source
fixtures on failure, worker metrics and SQL comparison results remain under the
selected evidence directory. This establishes correctness within that measured
workload; it does not claim arbitrary scale, an RSS ceiling or production S3
throughput. Source schema changes, recursive graphs, floating-point sums, outer
joins, arbitrary SQL and managed Supabase privilege provisioning are outside this
engine MVP.
