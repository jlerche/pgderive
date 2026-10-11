# PostgreSQL TLS and worker privileges

Use a DNS host name matching the server certificate and configure:

```toml
[postgres]
host = "postgres.example.com"
port = 5432
database = "application"
user = "pgderive_worker"
tls = "verify-full"
ca_file = "/etc/pgderive/postgres-ca.pem"
```

Provide the password through a protected `PGDERIVE__POSTGRES__PASSWORD`
environment value. Do not put secrets in checked-in configuration. `verify-full`
requires TLS and peer/name verification for SQL sessions, replication control and
streaming CDC. An optional CA adds trust; verification is never disabled. The
streaming client requires a DNS name for verify-full rather than an IP literal.
`disable` is for local fixtures. Configure PostgreSQL hostssl access and reject
hostnossl for the worker role to require encrypted connections server-side too.

An administrator provisions source tables, REPLICA IDENTITY FULL and their explicit
full-table publication. Give the worker a LOGIN REPLICATION role with no SUPERUSER,
CREATEDB, CREATEROLE or BYPASSRLS privileges. Grant CONNECT on the database, USAGE
on source schemas and SELECT on source tables. The worker needs USAGE and CREATE
on its exclusively owned metadata/result schema, plus EXECUTE on
`pg_catalog.pg_control_system()` and `pg_catalog.pg_control_checkpoint()` for
cluster/timeline identity checks and storage fencing. It owns tables/views it
creates in its schema. Prefer separate source and engine schemas in deployment;
do not transfer ownership of source tables to the worker. Source INSERT, UPDATE,
DELETE and ALTER permissions are unnecessary. No broad pg_monitor or superuser
grant is needed for the qualified worker path.

REPLICATION is a powerful PostgreSQL privilege: publication membership is not an
access-control boundary preventing a trusted replication role from reading other
WAL. Use database/instance isolation and a trusted worker as appropriate to your
security model. This release does not implement tenant isolation or automatic
privilege provisioning. RLS source tables are outside the supported contract.

The common gate creates a fresh owned tmpfs PostgreSQL server and a restricted
role. It independently rejects a wrong CA and DNS certificate name through all
three connector paths, rejects cleartext access and source DML/ALTER, observes
encrypted worker SQL sessions and a walsender, and checks native PostgreSQL and
independent memory equality through bootstrap, simultaneous changes, cold
restart, rebuild and drop. Failed fixtures are preserved. The test requires the
Linux loopback alias localhost.localdomain to resolve to 127.0.0.1, Docker,
OpenSSL and psql; it neither modifies host DNS nor resets shared PostgreSQL.
