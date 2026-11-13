# Immutable upload path

For object-backed preparation, a logical tick is evaluated against the prior
committed arrangements. The circuit then stages independent arrangement deltas with at most eight concurrent stages.
Every stage validates the delta, completes its conditional data/index PUTs and
returns an unpublished candidate. All in-flight stages are drained even when one
fails, before upload protection can be released. Candidate state, result changes,
source progress and logical-slot feedback do not advance on a prepare failure.

After successful PUTs, the writer retains its locally constructed block fences
and index reference rather than issuing HEAD/GET to reopen its own new index.
Fresh canonical rows enter the existing bounded decoded-block cache, so the next
tick need not fetch blocks that were just encoded locally. Streaming compaction
uses the same approach. The public streaming reference-only API still retains
only the current root's fences; resident metadata is kept only for callers that
need the returned immutable readers.

These shortcuts apply only to successfully uploaded local bytes. Cold recovery
still checks trusted root size/hash, schema, fences and block integrity. An
AlreadyExists response still requires equality checks; a cache is disposable and
cannot establish the durability of an arbitrary restored reference.

Object checkpoints still require completed PUTs, then one PostgreSQL transaction
publishing memberships and checkpoint progress, then logical-slot acknowledgement.
Sink transactions between checkpoints use retained source WAL as described in
[memtable replay](memtable-replay.md). Parallel uploads do not introduce
background durability. Tests count requests, verify overlap, inject PUT failure,
require every stage to drain, and retain cold missing-root rejection.


Immutable block descriptors include a required maximum absolute canonical tuple
weight. The hashed index authenticates this writer-generated summary alongside
its fences. Readers reject malformed summaries and verify them before decoded
cache admission. Trace validation can use the conservative sum of run maxima to
prove signed coefficients fit without fetching old payloads. This does not prove
bag nonnegativity. Existing nonempty indexes without this field are rejected;
rebuild existing views before upgrading. No compatibility reader is retained.
