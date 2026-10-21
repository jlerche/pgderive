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
