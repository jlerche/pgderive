# Diagnostic stdout sink

Set the transport in the existing worker configuration:

```toml
[worker]
sink_output = "stdout"
sink_table = "groups"
# Keep the other worker/source/storage settings unchanged.
```

`postgres` is the default. In stdout mode, `sink_table` labels the hypothetical
destination; the engine neither creates nor mutates that result table or its
index. It still connects to PostgreSQL for source CDC, binding validation,
coordination metadata and durable progress, and uses object storage for engine
state. This is a running diagnostic query, not a dry run: committed progress
advances and the replication slot is acknowledged after authoritative publication.
The separate diagnostic WAL listener retains its existing no-ACK behavior.

The worker's JSON-lines stdout stream includes `event: "sink_batch"` records
alongside its ordinary lifecycle/health events. Each version-1 record contains:

- `status: "attempted"`;
- `context`: query ID, hypothetical schema/table, native column metadata, logical
  `time` and source `end_lsn`;
- `sql`: the same parameterized INSERT or DELETE statement used by the PostgreSQL sink;
- `parameters`: the ordered bind arguments (`$1` is a JSON batch);
- `occurrences`: total affected occurrences the PostgreSQL sink would require.

For example, inspect only diagnostic batches with:

```bash
cargo run --locked --bin pgderive -- query create worker.local.toml |
  jq -c 'select(.event == "sink_batch")'
```

The SQL and parameters are separate so quotes, Unicode, NULLs and numeric scale
remain unambiguous. Payloads use the engine's native transport encoding; consume
the supplied SQL rather than treating the payload as a flat array of SQL columns.
Deletes precede inserts, with the existing 512-identity/4-MiB chunk targets and
million-occurrence publication limit. Initial snapshot insertions are printed;
a cold restart resumes durable progress without reprinting the snapshot. Empty
delta batches produce no sink records. This stream describes DML, not schema DDL.

A flushed record is an attempt, not proof of commit. Records are emitted before
the PostgreSQL metadata transaction commits. Failures can leave a partial batch
sequence; retries can duplicate attempts, and a crash or discarded output can
lose diagnostics even when progress committed. Stdout is not a durable delivery
sink and has no exactly-once or replay guarantee. The `ready` and `published`
worker events report successful local activation/publication, but do not turn
stdout into a transactional stream. Write/flush errors fail the publication
attempt; successfully printing alone never authorizes slot acknowledgement.
Destination casts, missing occurrences, triggers and constraints are not checked
because destination DML is not executed.

Sink mode is part of the immutable registration binding. Restart cannot change
between stdout and PostgreSQL. Rebuild is unavailable for stdout registrations,
including attempts to change their configuration to PostgreSQL; drop/create a
fresh diagnostic query instead. Drop retires the owned slot/state/metadata but
never drops a same-named external result table. Recreate after drop allocates a
fresh generation; diagnostic contexts may use its internal staging name during
bootstrap. Existing PostgreSQL bindings retain their original serialized form.

The sequential live harness replays emitted SQL into a separately created table
and compares exact binary bags against a native PostgreSQL materialized view.
It covers snapshot, transactional update/insert, duplicate NULLs, numeric-scale
changes, cold restart, drain, mode-change/rebuild rejection, drop and recreate.
