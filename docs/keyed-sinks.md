# Keyed PostgreSQL sinks

The default sink preserves arbitrary SQL bags and NULLs. An opt-in key gives a
proven unique, non-NULL output a physical PostgreSQL primary key, for consumers
such as Supabase Realtime. It does not change weighted engine identity.

```toml
[worker]
sink_key = ["book_id"]
# Other worker settings omitted.
[worker.query]
sql = "SELECT book_id, COUNT(*) AS positions FROM public.positions GROUP BY book_id"
```

The current proof accepts a final grouped aggregate (including ordered grouped
aggregates), optionally followed by filters or maps that leave the key unchanged. Every grouping column must appear
unchanged in the declared output key and must be statically non-NULL. Aliased
output labels and composite keys are supported. Keys are currently limited to
boolean, integer, UUID, timestamp and timestamptz types. Nullable keys, incomplete
grouping keys, projections without a grouping proof, transformed output keys,
text/numeric keys and other unproven shapes fail initialization. These are proof
limitations, not claims that the SQL could never have a key. PostgreSQL enforces
the proven constraint as a final safety check; there is no upsert or row collapse.

The key is persisted in the sink binding. Restart checks column nullability and
the exact immediate, validated primary key. A changed or missing key is rejected.
Within each complete result delta, a retraction and insertion with the same key
become one native PostgreSQL UPDATE. The UPDATE verifies the entire old tuple
using its binary representation; a missing or changed destination row aborts
publication. Each key must have at most one unit retraction and one unit insertion.
Unmatched retractions and insertions remain DELETE and INSERT operations,
respectively. All operations share the existing atomic publication transaction. Object publication,
source progress and slot acknowledgement retain the existing durability ordering.
Both UPDATE and DELETE look up candidates using the complete declared key and
then verify the exact old tuple's binary representation. The keyed path creates
only its primary-key index; it does not create or depend on the generic full-row
hash index. PostgreSQL remains free to choose a sequential scan for large
batches. Non-key value updates can therefore use HOT when page space and any
other user-created indexes permit it; HOT is not guaranteed.

Existing development sinks may still have the obsolete full-row hash index.
This change does not silently drop an existing index or provide a migration
adapter: explicitly remove the owned obsolete index while stopped, or recreate
the sink as part of the deployment. Restart alone retains its index-maintenance
and HOT penalty. This slice does not change immutable arrangement formats.

Consumers receive UPDATE events for replacements, INSERT events for new keys,
and DELETE events for removed keys. Unkeyed bag sinks retain occurrence-based
DELETE/INSERT publication.

Existing unkeyed registrations retain their serialized binding. A key cannot be
added through restart or a layout-preserving rebuild: use a new registration and
sink, then switch consumers and resnapshot. Rebuilds preserve the public table OID
only when both the result columns and key contract match. Stdout sinks do not
support keys. This feature creates no RLS policy or public grants.

`scripts/keyed_sink_harness.py` runs in the common live gate and compares binary
results against a refreshed native materialized view. It exercises initialization
rejections, overlapping key replacement, rollback, missing-key detection,
unchanged durable progress on rejection, restart and an empty result. PostgreSQL
table statistics also verify that replacements execute actual UPDATE statements.

The keyed harness also tests reordered composite keys across 512-identity chunk
boundaries, key-directed deletions, absence of a second lookup index, and stale
UPDATE/DELETE rejection. A numerically equal destination value with different
binary numeric representation still rejects the publication without changing
progress or other sink rows.

`scripts/sink_publication_measure.py` reuses the keyed fixture and native binary
oracle for paired production-worker measurements: UPDATE/DELETE batches of
1, 32 and 600 keys, three repetitions, worker CPU, publication clocks,
`pg_stat_statements` execution/WAL, HOT counts and rolled-back EXPLAIN ANALYZE of
the actual emitted DML. Use a disposable PostgreSQL with pg_stat_statements
preloaded and JSON statement logging enabled; the harness resets its statement
statistics. Run through the existing zero-latency S3 proxy and repeat with
`--candidate-first` before the two worker executable paths. Matched fillfactor60
provides controlled page-space headroom; it does not predict a deployed table's
HOT rate or production throughput. See the [physical strategy roadmap](physical-strategy-roadmap.md)
for publication reuse and subsequent property/maintenance work.

Local paired measurements (2026-10-27, baseline `a4cca7d`) used 1,899
UPDATEs and 1,899 DELETEs per worker. At 2,000 groups, repeating in both
execution orders reduced total PostgreSQL sink execution from 22.6–22.9 ms to
16.1–16.7 ms and sink DML WAL from 758,162 to 394,798 bytes. Eligible HOT
updates were 1,729/1,899 versus zero with the full-row index. Publication medians
were 2.87–2.93 ms versus 3.17–3.20 ms; worker CPU changed only modestly.
At 20,000 groups, execution was 9.29 versus 18.77 ms and WAL 376,910 versus
774,959 bytes, but worker CPU was 510 versus 490 ms. These short local trials
establish a sink-side improvement, not a general CPU or freshness guarantee.
Captured single-row plans used the primary-key index at 20,000 groups;
PostgreSQL selected sequential scans at 2,000 groups. Evidence and actual
rolled-back plans are retained under the ignored
`artifacts/benchmarks/keyed-access-20261027/` directory.
