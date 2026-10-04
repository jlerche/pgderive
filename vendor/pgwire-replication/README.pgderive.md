# Pgderive framing patch

Source: crates.io pgwire-replication 0.4.1 (original MIT/Apache-2.0 licenses retained).
The source is copied from the Cargo registry. Original crate archive SHA-256:
`20f9e5e3d56a31f59f606cedb6526b6e16dc06e4b34790804796c009940d70c2`. The unused upstream tokio-util dependency is removed for cargo-machete.
The sole behavioral patch changes
protocol::framing::MAX_MESSAGE_SIZE from 1 GiB to 1 MiB. Both buffered streaming
and startup framing reject larger payloads before resize/allocation. The oversized-header test exercises both startup and streaming readers and
asserts the streaming reader does not grow its allocation. The wire limit is independent of the
subsequent decoded transaction/output budgets. Oversized rows fail closed and
remain unacknowledged; increasing operator budgets does not increase this cap.

Upstream modules over 1,000 lines have an exact file-local directive to preserve
upstream structure. Do not refactor vendored code as part of engine work. Replace
this patch with upstream configurable pre-allocation limits when available.
