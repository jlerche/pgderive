# Checkpoint page handoff

Compaction previously used the non-admitting scan cache for both input reads and
generated output. Replacement blocks were therefore absent from foreground cache
when the new checkpoint was installed. The numeric SUM SF1 trace showed 51 cold
GETs in a 1.456-second epoch after checkpoint maintenance.

Input scans still peek at foreground blocks without admitting cold input or
promoting its LRU. Generated pages now enter a private output workspace. After all
sibling uploads succeed and membership is validated, a complete fitting workspace
is promoted to foreground cache, and replacement readers use that foreground
cache. Cache identities retain codec, schema, content hash and Rust type.

One workspace is shared across all arrangements of an asynchronous flush or
explicit whole-graph compaction, including its eight concurrent upload tasks. It
has the same decoded-byte and entry caps as foreground cache. If either cap would
be exceeded, it releases all staged pages and stops admitting further output;
foreground contents and LRU are untouched. This prevents a large cold rewrite
from flooding foreground cache. Standalone arrangement compaction uses the same
mechanism with one workspace for that operation.

This is an explicit temporary resource allowance: an in-flight maintenance
operation can own one additional decoded-cache budget, not one per sibling.
Foreground admission limits are unchanged. Existing cursor pins, write buffers,
native views and allocator overhead remain separate from that estimate, as before.
The optimization applies when the complete generated output fits; large outputs
keep the existing cold-read behavior rather than evicting the foreground hot set.

Promotion replaces duplicate identities with the staged rows and charges, removing
all duplicates before admission. This avoids retaining larger vector capacities
from older cache entries and makes complete-output-fit admission deterministic.
Locally generated rows have already passed canonical ordering, weight and block
encoding checks. Cache contents alone expose no object reference or logical state.

Object PUTs still complete before membership can be returned. PostgreSQL still
publishes membership/results/source progress atomically; slot acknowledgement
still follows publication. Failed sibling uploads drain before returning errors
and do not promote staged pages. Cold recovery still reads and authenticates
storage; a warm cache does not make a missing/corrupt checkpoint recoverable.
No durable object format, SQL plan revision, or catalog schema changes are needed.

Regression checks cover changed content hashes with zero post-compaction GETs,
cold reopen, failed sibling uploads, shared budgets across schemas, byte and entry
overflow, disabled retention, duplicate allocation charges, and preservation of
foreground hot pages during oversized compaction. Existing crash/publication and
transaction-boundary harnesses remain in the common gate.
