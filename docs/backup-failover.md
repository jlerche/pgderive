# Backup, restore and failover boundary

PostgreSQL is authoritative for object membership, sink rows, registered source
identity and durable source progress. An object prefix alone is not a recoverable
query. A database backup without every referenced immutable object is also
insufficient. Logical source LSNs cannot be transplanted to another cluster or
timeline, and engine logical time is not a replacement for them.

For a preservation backup, stop all query workers gracefully and verify no live
owners remain. Suspend external compaction/GC and prevent new workers starting.
Take a consistent PostgreSQL backup of sources, engine metadata and results plus
required WAL using your PostgreSQL backup tooling. Record the engine executable
revision, configuration without secrets, native source layouts, source cluster
identity, timeline, progress, current object membership and protection roots.
Copy and verify every reachable immutable object referenced by the snapshot and
pins before resuming GC; include manifests/index pages and data objects. Do not
infer that copying only newest objects or listing a prefix establishes closure.
Retain an inventory and checksums with the database backup. PostgreSQL backup
completion and object-provider retention are separate obligations.

Current release policy after restore or promoted failover is fresh rebootstrap,
not automatic reuse of the copied query journal. A logical restore changes native
relation identities; a promotion changes timeline. Registration and rebuild
boundary checks deliberately reject those changes. Logical slots are not assumed
preserved, synchronized or valid merely because a database restore succeeded.

On the restored/promoted database, provision the source publication and a new
owned catalog schema, query identity, slot alias and object prefix. Validate the
query against the actual catalog, create/bootstrap a fresh query, compare its
result to the identical native PostgreSQL materialized view, then perform an
application-controlled cutover to the new result. Preserve the old catalog and
object backup for investigation; never edit identity/progress to bypass rejection.
The existing `query rebuild` command handles same-cluster/timeline slot loss and
query changes, but is not a cross-timeline migration operation. Cross-database
application cutover and automatic HA are not implemented.

A backup does not solve source WAL retention during ordinary operation. Monitor
slot validity and retained bytes, set a deliberate PostgreSQL retention cap, and
accept that invalidation requires rebootstrap. No ACK skipping is supported.
These are supported recovery constraints and an operator procedure, not evidence
that arbitrary PostgreSQL backup products or cloud-provider failover were tested.

## Logical restore and rebootstrap rehearsal

`scripts/query_recovery_harness.py` rehearses the conservative logical-restore
policy on two independently owned tmpfs PostgreSQL 17 servers. It stops the
worker, records exact progress/source state and reader roots, saves a custom-format
`pg_dump` of the complete fixture database, and copies every object under the
exclusively owned prefix. It verifies membership/reader-root index hashes and
referenced data blocks against the saved files, rather than treating a listing
count as closure. The fixture uses directly indexed objects; paged index and
physical backup/PITR/promotion restore are outside this drill.

The source server is stopped before restoring with PostgreSQL 17 `pg_restore`
on the second server. Saved objects are uploaded to a separate owned mirror
prefix and read back against their hashes. That mirror is a preservation copy,
not a compatible query namespace: copied metadata is never rewritten to force
checkpoint adoption. The drill verifies restored source rows and progress,
rejects ordinary restart of the copied query, and creates a fresh catalog/query,
slot and object prefix against the restored sources. It checks the fresh result
against the identical native materialized view and independent memory oracle,
then switches an application consumer view in one PostgreSQL transaction.
Simultaneous changes, NULLs, deletes and cold restart are checked after cutover.

The evidence directory retains database.dump, dump/restore logs, object files and
hash manifest, source identities/configuration, oracle records and measured
restore/bootstrap durations. Successful fixture servers are disposable; failed
servers and logs are retained. The fixture's object mirror and original object
backup namespace remain preserved. This exercises logical dump/restore and local
SeaweedFS copy, not a selected production backup product or provider durability.
For a real rollout, repeat with the deployment's backup tooling, object retention,
query sizes and cutover process; select an RPO/RTO from those measurements.

Logical pg_dump is not a physical backup with archived WAL or point-in-time
recovery. It supplies the consistent source/metadata/results snapshot for this
fresh-bootstrap policy. Do not interpret this rehearsal as qualification of
cross-timeline checkpoint reuse or automatic failover.
