# Pgderive

Pgderive is a PostgreSQL-only incremental view maintenance engine based on DBSP.
Committed `pgoutput` transactions are normalized into consolidated full-tuple
weighted batches and verified by a real-PostgreSQL row mutation harness. There
is an initial in-memory incremental equijoin; materialized-result sinks,
object-backed state, and transactional publication come in later slices.

## Quality gate

Use Rust 1.95.0, pinned in `rust-toolchain.toml`.

```bash
cargo install cargo-machete --version 0.9.2 --locked
./scripts/check.sh
```

The gate runs custom-lint tests, Rust file-length checks, rustfmt, strict Clippy,
Rust tests, and unused-dependency detection. GitHub Actions runs the same gate,
followed by the live integration harness. Compiler warnings and dead/unused code
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
creates `pgderive_dev` only if absent and never resets an existing database. No object-store service is needed for this slice.

The harness starts the same listener used by the executable, waits for the
replication connection, and drives four committed transactions:

1. Insert a person, an auction, and ten bids: 12 row changes.
2. Update the person to a NULL name, change the auction category and ten bid
   prices together: 12 row changes. Both join inputs change in this commit.
   An additional rolled-back insert must not appear in CDC.
3. Delete five bids: 5 row changes.
4. Insert then delete a bid, update a bid without changing its value, and change
   the person twice back to its original value: 5 row changes, an empty batch.

After every commit, it checks full old/new row images against an independently
accumulated source-row map and compares that map with source SQL. It also checks
each weighted batch equals the difference between full source SQL snapshots,
and that durable slot progress was not advanced. Batch sizes are 12, 24, 5, and 0.
The harness also integrates auction–bid join result deltas and compares them with
a full SQL join after every commit.
This fixture uses Nexmark table shapes; query operators and a Nexmark benchmark
are not implemented yet.

Each run creates a fresh `pgderive_harness_<pid>` schema. Publication/slot names
must begin with `pgderive_` and must be unused; existing names are rejected.
Normal completion and handled errors remove the fixture schema, publication, and
slot. A hard process kill can leave resources for inspection; the next run refuses
to overwrite a leftover publication or slot.

JSONL, logs, configuration/source/binary hashes, and version information are saved
in a fresh ignored `artifacts/replication/run-*` directory, including stderr on
failure. Run mutating harnesses sequentially. The PostgreSQL service remains
running after the check; this repository does not provision persistent database
volumes.

The current scope excludes snapshot bootstrap, schema evolution, TRUNCATE,
generic tuple codecs, durable result publication,
recursion, and operationally safe WAL-retention management.

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
emit diagnostic batches. There is no circuit planner, persistence, snapshot
bootstrap, recursion, frontier tracking, or source acknowledgement.

Tests follow the independent raw-history nested-loop oracle from the PoC's
`src/bin/zset_contract_tests.rs.inc`: 128 deterministic signed-input histories
with 16 steps each, simultaneous updates, multiplicity, cancellation, and atomic
failure on arithmetic/time overflow. Prior accepted evidence is in the PoC's
`zset-contract-default-checks.jsonl`; its storage/recovery results are not claims
about this new engine. The recorded PoC checks were inspected, not replayed or
modified. Next, linear filter/map operators can compose with this join before
adding arrangements and the durable publication boundary.
