# Object operation allowance

Configure the production S3 store with a durable local ledger. The allowance
below is illustrative; choose capacity from measured request demand and the
required operating window:

```toml
[object_store.operation_budget]
ledger = "/var/lib/pgderive/class-a.json"
class_a_limit = 10000
window_secs = 86400
```

The parent directory must already exist and live on durable local storage. All
processes sharing an allowance must use the same absolute path and window length,
including two query workers. Mount the same durable ledger directory into both
containers; a container-local or ephemeral directory does not survive replacement.
Keep the ledger and its appended `.lock` sidecar protected from deletion and replacement by
unrelated callers. This is a local-filesystem guarantee requiring working advisory
locks, atomic rename and directory fsync; it is not a distributed filesystem or
account-wide Cloudflare guarantee.

Each UTC-aligned reservation window permits at most `class_a_limit` attempts across
ordinary PUT, copy, and explicit listing pages. The clock is sampled after the
ledger lock is acquired. A reserved request can finish across a window boundary;
provider billing windows and request completion times are not ledger windows. Reservations are serialized with an OS file
lock, persisted with fsync and atomic replacement, and charged **before** remote
I/O. The S3 client has zero internal retries. Every worker retry, conditional PUT
(including HTTP 412), compaction PUT, and GC page crosses this same boundary and
requires another reservation. A failed, interrupted, or cancelled request keeps
its reservation: the process cannot know whether the remote request was billed.
There is no refund. Bounded ordinary PUTs are supported; multipart is rejected.
When budgeting is enabled, generic streaming and delimiter listings are rejected
because the library hides their physical pagination. Use `PageStore::page`.

GET, range GET, HEAD and DELETE remain allowed after exhaustion; they are outside
the Class A allowance. Existing service counters report read/write consumption.
External monitoring, CLI commands, other applications and raw harness adapters
are outside enforcement. In particular, a metrics collector inventorying R2
through a separate client must either stop inventorying, use a separately bounded
cadence/allowance, or explicitly share this reservation mechanism. An account-wide
spend ceiling cannot be inferred from these process-local service counters or
this voluntarily shared ledger.

The JSON ledger records usage, remaining capacity, PUT attempts, listing pages,
copies and the window start. It can be read without object requests. Worker
`object_service.operation_budget` heartbeat diagnostics show the latest locally
observed durable snapshot; another process may have spent more since that
snapshot. The next reservation refreshes it under the shared lock. Counters
survive worker restarts. Invalid ledgers fail closed. Backward clock movement
cannot reset a window. Forward clock movement can advance it; correct host time
is an operational requirement. Window length changes fail closed; raising or
lowering the configured allowance changes capacity in the current window without
resetting usage. All sharing processes must be updated together before raising it.

Exhaustion fails the attempted operation before network I/O with an explicit
`operation budget exceeded` error. The worker recognizes this typed exhaustion error as permanent and exits its
retry loop; restarting cannot reset the ledger or spend additional Class A
capacity in the exhausted window. Upload protection and previously published progress remain durable.
There is no unsafe source acknowledgement or dropped update. Progress can stop
and source WAL can accumulate; operators must monitor retained WAL and configure
PostgreSQL retention deliberately. A budget does not bound PostgreSQL WAL bytes.

To recover, retain the ledger, wait for the next UTC accounting window (then
restart a worker whose retry policy has exited), or authorize a higher limit and
restart with matching configuration. Inspect progress, upload protections and WAL
health before resuming. Do not delete the ledger to reset usage. No live demo
configuration or process is changed by this implementation.

The sequential common gate runs `scripts/operation_budget_harness.py` against an
isolated PostgreSQL fixture and instrumented local S3 service. With a one-attempt
allowance, actual worker bootstrap stops with a permanent capacity error after
exactly one PUT; restarting spends no additional Class A capacity and publishes
no progress. Raising the same ledger's allowance to 1,000 permits bootstrap and
SQL-verified operation without resetting the prior charge. The bootstrap recovery phase ended with 10 used / 990 remaining (9 PUTs and one
listing). A running-worker checkpoint test then exhausted a 12-attempt allowance
after publishing epoch 2: checkpoint 1, one durable journal epoch, unchanged safe
ACK, and nonquiescent upload protection remained. Restart at the same cap spent
zero additional Class A attempts and preserved sink tuple identities. Raising the
allowance to 1,000 recovered epoch 2 without sink rewrites and checkpointed it on
graceful stop, clearing the journal. Final usage was 17 attempts (14 PUTs and three
listing pages), with 983 remaining. Focused tests also
exercise packed metadata and shared-data exhaustion, conditional failures,
concurrent reservations, rollover, corrupt persistence, and allowed reads/deletes.

A later authorized demo deployment must mount the durable shared ledger into
both worker containers, add matching budget configuration, apply the new cadence
settings and hourly monitoring collector, configure a source-server WAL retention
cap and slot-rebuild response, and validate the binary/state-format upgrade or
rebuild procedure before starting either worker. This implementation
neither deploys those changes nor resumes any paused service or automation.
