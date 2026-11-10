# Tenant-wide object storage

Workers require a `tenant_storage` configuration, separate from the source
`postgres` connection. Every query assigned to a tenant must use the same durable
manifest authority and physical storage root. See `worker.example.toml`.

The manifest PostgreSQL database is part of the durability boundary. It can be
hosted separately from source databases. It needs durable storage, synchronous
commits, backups, and session-affine connections: transaction-pooling proxies are
unsupported because uploader fencing and collection use session advisory locks.
The worker forces `synchronous_commit=on`. A manifest outage stops publication;
it must never be treated as permission to discard objects or acknowledge WAL.

## Packing and publication

An engine object key is a stable logical ID within `(tenant ID, source catalog
UUID)`. A coarse manifest entry maps that object to an immutable physical pack,
byte range and SHA-256 checksum. All object families use this layer, including
root/index objects. Source PostgreSQL continues to own query metadata, result
transactions, progress and WAL acknowledgement.

One process runtime packs compatible uploads from all queries of a tenant,
including queries over different source databases and schemas. Defaults are a
4 MiB target and at most 5 ms of gathering delay. Uploads do not wait for an
unavailable query to contribute. Different tenants never share a physical pack.
Compatibility also requires the same manifest, bucket/endpoint, credential
identity, request limits and operation-budget policy. Distinct processes share
the durable manifest and reclamation protocol but have separate upload queues;
run queries together to combine their uploads.

Publication order is:

1. Commit a uniquely owned physical upload intent in the manifest database.
2. PUT the immutable pack.
3. Commit its logical mappings in the manifest database.
4. Publish membership, result changes and source progress in the existing source
   PostgreSQL transaction, then acknowledge its logical slot.

A lost commit reply retains the physical intent and any committed mappings.
An accepted request remains owned by the uploader if its caller is canceled.
Immutable create conflicts are returned as `AlreadyExists`; the engine verifies
the existing bytes before treating a retry as successful.

## Reclamation while a database is unavailable

Source query collection proves logical unreachability and deletes only logical
mappings. Deletion seals the upload namespace before removing a mapping, so a
late upload cannot recreate the deleted logical object. The tenant collector is
the sole authority that deletes physical packs.

The tenant collector can copy live chunks into a replacement pack, atomically
change their locations and delete the old pack. It does not connect to source
databases. It conservatively retains an unavailable database's mappings while
reclaiming holes left by healthy queries. Reads resolve and pin the physical
location in one manifest SQL transaction; relocation waits for these pins.
Readers return buffered bytes only after confirming their pin transaction.

`pgderive storage collect query.toml` runs a bounded physical collection turn
using the manifest endpoint even when that configuration's source is offline.
Workers also collect periodically. Upload owners are fenced using durable state
and session advisory locks, rather than a retention TTL. Physical inventory
eventually removes unreferenced late PUTs from fenced owners.

Empty-pack retirement skips reader locks. A failed repack still attempts the
independent retired-pack and inventory sweeps. Collection runs bounded turns:
after an idle 30 seconds it drains remaining work with 100 ms spacing, returning
to the idle interval once no progress is needed. Failed turns retry after one
second. Maintenance workspaces are admitted in FIFO order across tenant groups;
each turn can retire 32 empty packs and batch-delete 16 retired packs.

An offline source can still retain its own logically dead objects until it
returns and its query collector proves them unreachable. No protocol can infer
that proof merely from source unavailability. Source WAL retention also remains
an operational concern.

## Capacity and SaaS isolation

Upload admission charges payload storage plus concatenation and request overhead
before copying payloads: 32 MiB per tenant upload group and 64 MiB per runtime.
Each request acquires tenant admission before process admission. The queue is
bounded to 128 requests. One physical maintenance workspace runs at a time per
runtime; it can require approximately another 16 MiB while copying a pack.
Metadata sessions are pooled, at most eight per configured manifest endpoint,
plus uploader/collector owner sessions. These charges are separate from the
existing cache and memtable pools and are not an RSS limit.

Tenant IDs are trusted routing/ownership identifiers, not authentication tokens.
A SaaS control plane must authorize configuration, credentials and source/catalog
bindings. SQL predicates, foreign keys, catalog identities and physical ownership
markers enforce separation inside the service. They do not protect a manifest
database from a client with unrestricted SQL access. Do not expose manifest or
object-store service credentials to tenants.

Backups must preserve the manifest and the referenced immutable objects along
with source query metadata. Never restore a manifest independently to an older
point while letting collection operate against newer source checkpoints. Do not
reuse a bound physical root with a fresh manifest. This development interface
requires a fresh manifest namespace and does not adopt preexisting raw objects.
Existing raw-store queries must be freshly bootstrapped into a new owned source
catalog and physical root. Never point their existing checkpoints at an empty
manifest. Fence an old primary before promoting a manifest database replica;
independent writable clones must never share the same physical storage root.

Health metrics expose the shared upload buffer and collection failure count.
Physical object-service request counters are labeled with their shared tenant;
the same counter shown by multiple queries must be counted once, not summed.
