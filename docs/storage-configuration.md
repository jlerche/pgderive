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
worker restart. Inline credentials remain available for fixture compatibility;
file and inline credentials cannot be combined.

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
