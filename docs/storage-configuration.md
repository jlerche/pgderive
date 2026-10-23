# Object storage configuration

For an HTTPS S3-compatible service, replace the entire example object-store
section, including its inline credentials:

```toml
[object_store]
endpoint = "https://objects.example.com:443"
bucket = "application-results"
region = "us-east-1"
credentials_file = "/run/credentials/pgderive/s3.json"
# ca_file = "/etc/pgderive/object-store-ca.pem"
```

The endpoint must be an origin: no userinfo, path, query or fragment. HTTPS
requires an explicit region. Normal system certificate trust and hostname
verification remain enabled; `ca_file` adds a PEM trust root. Loopback HTTP with
an explicit positive port is supported only for local fixtures using a bucket
whose name starts with `pgderive-`.

A credential document contains `access_key_id`, `secret_access_key` and an
optional `session_token`. File credentials require Unix, a regular file with no
group or other permissions (for example mode 0600), and at most 65536 bytes.
Unknown fields, missing/empty credentials and malformed JSON are rejected.
Errors do not include document contents. Nonblocking descriptor opening prevents
FIFO paths from hanging startup; the opened descriptor is checked before reading.
Protect parent directories and provision the file through your secret manager.
Credential files are read when constructing the client; rotation requires a
worker restart. Storage configuration requires `credentials_file`; inline
credential fields are rejected. For local fixtures, copy `s3.example.json` to a
private file with `install -m 600 s3.example.json s3.local.json`.

Before initial query registration, HTTPS startup performs a read-only HEAD probe
under the owned prefix. Missing probe objects are expected. The probe verifies
transport, certificate validation and readable-object access; it does not prove
PUT permission or durability. Objects still use conditional create-only PUTs.
Publication remains object PUT, then atomic PostgreSQL membership/result/progress
COMMIT, then slot acknowledgement. Client retries are disabled so worker recovery
continues to reconcile failures against authoritative PostgreSQL state.

The common gate runs the actual Rust S3 client against an owned non-loopback TLS
proxy to SeaweedFS. It checks rejection of a wrong CA and hostname before initial
registration, conditional signed PUTs with a session token, native PostgreSQL and
independent memory equality, simultaneous source changes and cold restart. This
qualifies this private-network fixture, not AWS or another cloud provider, remote
host failure durability, or production capacity. The proxy records header presence
and create conditions without recording credentials or signature values.

## Bounded garbage-collection deletes

Collection completes bounded root marking and managed-object enumeration before
issuing deletes under the lifecycle barrier. It submits at most 256 candidate
paths per adapter stream and finishes that stream before submitting another.
A request error or an object error inside HTTP 200 stops the sweep before
protection retirement or metadata COMMIT. Retry re-enumerates; deleting an
already missing object is idempotent. Quiescence, unknown-object retention and
late-PUT namespace protection keep their existing contracts.

The pinned S3 adapter implements even a singleton `delete()` as a bulk-delete
POST. An actual-adapter local HTTP fixture measures 513 singleton POSTs versus
three batches of 256, 256 and one path. Its partial-error retry deletes all keys,
including 255 keys already removed by the first attempt. This measures request
amplification, not cloud latency or capacity. The durable object format is unchanged.

New immutable data blocks use canonical full-tuple weighted JSON compressed with
Zstd level 3 (`pgderive/block/<sha256>`). Hashes cover compressed bytes. Resident
fences and root indexes remain JSON (`pgderive/batch/<sha256>`). This is the sole
object format; old JSON layouts and versioned paths are unsupported. Existing
registrations using those layouts must be rebuilt. Decoded JSON is capped at
8 MiB per block, stored bytes at 8 MiB + 64 KiB, and Zstd frames/windows are checked before
rows are exposed. Streaming block-byte budgets refer to uncompressed JSON.

An optional Foyer memory/disk cache retains encoded immutable blocks:

```toml
[encoded_cache]
directory = "/var/cache/pgderive/my-worker"
memory_bytes = 16777216
disk_bytes = 268435456
```

Use a dedicated directory per worker. Disk capacity must be a multiple of 16 MiB
and at least 64 MiB. The existing execution cache limits still bound decoded
blocks; the Foyer memory budget is additional. A directory lease prevents
concurrent recovery/writes, including asynchronous cache shutdown. Cache hits are
checked against the expected SHA-256; corrupt or failed cache reads fall back to
object storage. Recovery always bypasses both cache tiers and verifies durable
objects before attaching shared caches. Cache contents are disposable and confer
no authority to publish or acknowledge WAL. Omit this section to disable Foyer.

Compaction already streams consolidated weighted rows and advances only physical
membership generation. `Arrangement::compaction_job` now exports a serializable
`CompactionJob`; a separate process can deserialize it and call
`execute::<K, V>(store)` with the same codec and object-store prefix. The result is
an unpublished `CompactionResult`. `Arrangement::adopt_compaction` rejects stale
input membership, foreign job/output namespaces, clock changes and any change to
weighted tuple contents, cold-reading replacement objects before returning a new
snapshot. The ordinary inline compactor and detached executor share the same
streaming implementation.

The coordinator must hold a durable checkpoint reader pin and upload reservation
until the executor has stopped, then retain the upload reservation through fenced
PG publication. A process failure or uncertain publication must preserve these
protections for authoritative recovery; timeouts alone do not prove quiescence.
Only the writer publishes with the existing PG transaction/fence; the executor
has no PG publication or slot-ACK capability. GC remains a separate operation.
This is a portable library boundary, not a job queue or automatic worker subprocess
scheduler; the existing continuous worker continues to compact inline.
