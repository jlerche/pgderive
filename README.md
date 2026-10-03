# Pgderive

Pgderive is a PostgreSQL-only incremental view maintenance engine based on DBSP.
The first slice is a configurable `pgoutput` listener and a real-PostgreSQL row
mutation harness. There are no computation operators or materialized-result sink
yet. Object-backed state and transactional publication come in later slices.

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
transaction: xid, commit/end LSNs, table/column names, operation, and old/new rows.
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
Compose service. It creates `pgderive_dev` only if absent and never resets an
existing database. No object-store service is needed for this slice.

The harness starts the same listener used by the executable, waits for the
replication connection, and drives three committed transactions:

1. Insert a person, an auction, and ten bids: 12 row changes.
2. Update the person to a NULL name and change ten bid prices: 11 row changes.
   An additional rolled-back insert must not appear in CDC.
3. Delete five bids: 5 row changes.

After every commit, it checks full old/new row images against an independently
accumulated source-row map and compares that map with source SQL. It also checks
that durable slot progress was not advanced. This is a source/transport fixture
using Nexmark table shapes, not a Nexmark benchmark or DBSP implementation.

Each run creates a fresh `pgderive_harness_<pid>` schema. Publication/slot names
must begin with `pgderive_` and must be unused; existing names are rejected.
Normal completion and handled errors remove the fixture schema, publication, and
slot. A hard process kill can leave resources for inspection; the next run refuses
to overwrite a leftover publication or slot.

JSONL, logs, configuration/source/binary hashes, and version information are saved
in a fresh ignored `artifacts/replication/run-*` directory, including stderr on
failure. Run mutating harnesses sequentially. Local development volumes and the
PostgreSQL service remain running after the check.

The current scope excludes snapshot bootstrap, schema evolution, TRUNCATE,
generic tuple codecs, weighted operators, durable result publication, recursion,
and operationally safe WAL-retention management.
