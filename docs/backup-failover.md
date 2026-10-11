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
