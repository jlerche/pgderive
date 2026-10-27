# M8 batched occurrence publication

This occurrence-based DELETE/INSERT path remains the default for unkeyed bags.
[Proven keyed sinks](keyed-sinks.md) additionally pair same-key replacements into
native UPDATEs, with the same exact old-tuple and affected-row checks.

The bounded target is batched INSERT/DELETE and an ordinary result-table expression
index for candidate occurrence lookup. Keep only query columns in the public table;
no persisted weights, occurrence identifiers or engine arrangements are added.
PostgreSQL performs storage casting, lookup and DML, never query-expression fallback.

Use bounded JSON input batches and decode each tuple once per batch. Retractions
run before insertions. Group identical decoded binary representations inside a
delete batch so requested identities cannot select the same CTID twice. Select
at most the requested multiplicity and verify the total affected count; an
underflow or suppressed DML must abort the complete publication transaction.

For unkeyed bags, a nonunique fixed-width hash expression index narrows candidate lookups. Hashes
are not identity: the final predicate compares native send representations,
including NULL, numeric scale and array metadata. Distinct representations may
share a hash. New destinations create the index at initialization. Restart validates the existing
index and rejects missing or altered definitions; it never backfills old tables.
No index catalog polling is added per computation.
The index is ordinary PostgreSQL storage infrastructure, not durable engine state.

Keep the existing million-occurrence transaction cap. Batches target at most 512
identities and 4 MiB JSON input; a single oversized prevalidated tuple travels
alone. All chunks share the existing synchronous membership/result/progress
transaction. Object PUT still precedes it and slot ACK still follows verified
COMMIT. Query checkpoints, source bindings, public OIDs and typed layouts remain
compatible. Rebuild/drop must retain normal PostgreSQL table/index lifecycle.

Acceptance requires exact typed/binary identity and pgoutput, duplicates/NULLs,
hash collisions (including numeric scale), multi-batch retractions and insertion,
large values, missing/suppressed DML rollback, cold restart and missing/altered index rejection. Measure query counts, index plans/buffer work and complete publication
cost with explicit index storage/write tradeoffs. No production capacity claim.

New destination initialization creates its index with PostgreSQL's ordinary
CREATE INDEX locking and privilege requirements.
A conflicting index with the reserved name fails initialization with a repair
error. The index adds storage and write amplification; its fixed-width key avoids
index tuple limits for wide output values. Hash collisions affect lookup work,
never occurrence identity. Dropping the index during a run preserves correctness
and restart rejects the missing index until it is explicitly repaired.

The qualification harness includes 1,800-row source transactions, multi-chunk
key moves and drains, later-chunk trigger suppression and missing occurrences.
Failures preserve the complete sink, membership and progress boundary and do not
advance slot acknowledgement. Low-level publication checks additionally exercise
numeric-scale hash collisions, decoded aliases, quoted columns, collated text,
arrays with NULL elements and more than 4 MiB of wide values.

Reproduce the bounded before/after release measurement with
`python3 scripts/sink_publication_baseline.py artifacts/sink-publication-sample`
against an isolated PostgreSQL fixture. Before and after use separate Cargo target
directories, retained binary hashes, build logs and source fingerprints. Actual
extended-protocol Execute messages count DML statements; the observer records no
SQL values. The sample uses local PostgreSQL and zero injected object-store
latency, includes phase-clock instrumentation and excludes oracle verification
from publication latency. It is not a throughput or deployment qualification.

## Recorded local release comparison

The [retained samples and build fingerprints](sink-publication-samples.json)
compare M7 commit `6d52093` with this M8 implementation on PostgreSQL 17.11.
Each of three committed transactions moves all 1,800 source keys. Consolidation
produces 1,698 distinct retraction and insertion identities, including NULLs and
numeric-scale distinctions. Both versions pass native materialized-view binary
bag comparison and the independent source model after each transaction.

| Measurement | M7 | M8 |
| --- | --- | --- |
| DML Execute messages per transaction | 3,396 | 8 |
| Sink DML phase | 903–925 ms | 14.4–14.7 ms |
| Complete publication phase | 909–932 ms | 20.3–21.2 ms |
| Source psql call to published event | 1,081–1,100 ms | 187–196 ms |
| Initial total destination storage | 112 KiB | 168 KiB |

The representative occurrence lookup changes from a sequential scan (1,799 rows
filtered, 11 shared buffer hits including row locking) to an index scan (one
binary-mismatching candidate filtered, four shared buffer hits including locking).
The initial index occupies 56 KiB. These hot local samples have no injected object
latency and do not establish production capacity or maintenance cost. PostgreSQL
may choose other plans for different data distributions; hash collisions and
large duplicate bags still require heap work. Object serialization/read volume
and affected-partition recomputation are unchanged by M8.

## Qualification completion

M8 completed on 2026-10-16 after independent review and `./scripts/check.sh`:
301 Rust tests passed (two ignored), 33 Python tests, sequential live PostgreSQL
qualification, 112 core worker results plus rebuild, and 97.50% production line
coverage. Strict Clippy, formatting, source-file limits and dependency checks
passed. The complete release qualification profile was not run; the separate
release comparison above is a bounded M8 measurement. M9 deployment/provider,
capacity, WAL retention, slot-loss and operational recovery qualification remains
open, as does the separate function/type support backlog.
