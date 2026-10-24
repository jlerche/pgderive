# Bounded snapshot and transaction ingestion

Snapshot rows and committed CDC epochs enter the same `OpenInput` adapter. Appending
rows does not advance logical time or run operators. Sealing consolidates complete
full-tuple identities before the existing relational circuit evaluates one logical
epoch. Transport chunks, external-sort runs and object blocks do not create ticks.

The initial PostgreSQL snapshot is imported from the logical slot's exported
snapshot. Every source table is scanned in that same transaction. The row stream
uses protocol backpressure and bounded transport batches; it does not collect a
whole table. It streams a direct SELECT, avoiding the server-side FETCH utility
materialization. The local SQL statement timeout follows the existing startup
deadline, which also bounds initialization as a whole. Changes committed after
that snapshot remain in the slot for CDC.
The initial result publishes at logical time 1 with the snapshot's consistent LSN.
Source LSNs remain distinct from logical time.

Intermediate edges have one immutable cursor interface, backed by a resident batch
or canonical local scratch. Size-tiered merging bounds open spill runs, retaining
exact coefficients until the complete identity is consolidated. Operators consume
sealed edges. Fallible projections therefore never see rows canceled by later
transport chunks. Statistics finalize once per complete group; joins include both
inputs' changes and their cross term. Physical spilling does not reject a CDC
transaction that fits the configured logical-output and memtable budgets.

Publication still uploads protected immutable objects before the single PostgreSQL
transaction that activates membership, sink rows and source progress. Slot
acknowledgement follows durable publication. Local scratch is disposable, never a
durable checkpoint. Checksummed frames and trusted written byte lengths reject
content damage and whole-record truncation in intermediate and sealed scratch runs.
Failures retain scratch evidence; successful cursors release it when their last
owner drops.

This is bounded execution, not unlimited execution. Existing CDC transaction
assembly limits still apply. Work and pre-cancellation contribution budgets span
the complete operator epoch, including all navigation keys. Final result batches,
individual complete-key groups,
and operators requiring a complete input (indexed statistics and windows) retain
configured materialization limits. Resident budgets control spilling; scratch
bytes and contributions independently bound total work. Large bootstraps require
sufficient local scratch capacity and explicit work allowances. Typed capacity
errors and disk-full/out-of-memory I/O failures terminate instead of retrying the
same impossible input. Transient transport failures keep the existing retry path.

Scale validation and its remaining limitations are recorded in
[streaming-bootstrap-validation.md](streaming-bootstrap-validation.md).
