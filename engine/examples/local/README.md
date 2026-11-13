# Run a local maintained query

From the repository root, run:

```bash
python3 examples/local/run.py
```

Prerequisites are Docker, `psql`, Python 3 and the repository's pinned Rust build
prerequisites (including libclang). This builds the CLI, starts separately named
PostgreSQL and SeaweedFS containers on dynamically assigned loopback ports, creates
source tables/publication and runs the SQL in `query.sql`. Both database and object
storage use disposable tmpfs. Existing development/PoC containers are untouched.

The example imports a snapshot, compares the ordinary result table with a native
PostgreSQL materialized view, applies `changes.sql`, compares again, restarts the
worker and applies another transaction. Comparisons include exact binary values
and multiplicities. It then runs the same query with the stdout sink and prints
one generated DML/parameter record. All processes/services are cleaned up on exit;
logs, configurations and a machine-readable result remain under
`artifacts/local-example/`. This is a functional example, not a capacity test.

To keep the owned services, source data and stopped query registrations for manual
experimentation:

```bash
python3 examples/local/run.py --keep --output artifacts/my-local-query
```

Read `result.json` for the chosen PostgreSQL port. With that port, inspect results:

```bash
PGPASSWORD=postgres psql -h 127.0.0.1 -p PORT -U postgres -d pgderive_demo \
  -c 'TABLE demo_engine.totals'
```

Restart the retained query in one terminal and run source transactions in another:

```bash
export PGDERIVE__POSTGRES__PASSWORD=postgres
cargo run --locked --bin pgderive -- query restart artifacts/my-local-query/worker.toml
```

```bash
PGPASSWORD=postgres psql -h 127.0.0.1 -p PORT -U postgres -d pgderive_demo \
  -c 'INSERT INTO demo.bid VALUES(6,1,42)'
```

Ctrl-C drains and stops the worker. Inspect stdout diagnostic records with:

```bash
jq -c 'select(.event == "sink_batch")' artifacts/my-local-query/stdout.jsonl
```

The stdout query has its own source slot and registration. Its output is attempted
SQL, not durable delivery; [stdout sink semantics](../../docs/stdout-sink.md) apply.
You can resume it using `query restart .../stdout.toml`. Normal query validation
and lifecycle commands are described in the root README.

The runner prints an exact `docker rm -f ...` command for its two owned containers.
Use it when finished. Removing them discards all example data and progress; retained
configuration files cannot restart a deleted instance. Choose a fresh output
folder for a fresh run. Local passwords/HTTP storage are for these loopback-only,
disposable services.
