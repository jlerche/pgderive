# Canonical batch handoff

This execution path adapts the builder principle used by Feldera's ordered DBSP
batches and RisingWave's chunk builders: complete an immutable batch without
reconstructing it through another general accumulator. Reference checkouts
inspected for this change were Feldera f687bbcaa (`operator/dynamic/aggregate.rs`,
`dyn_weigh_generic` and ordered builders) and RisingWave 8184836f24
(`common/src/array/stream_chunk.rs` and chunk builders). Pgderive continues to use
its existing weighted row representation; this is not a columnar execution rewrite.

The spillable consolidator keeps exact BigInt coefficients until final logical
consolidation. Its resident entries remember the encoded size of the retained
key representative and its current coefficient. Repeated size checks count JSON
bytes without constructing a temporary record buffer. Actual spills still encode
and checksum records, and merged spill outputs are measured again after exact
coefficient combination. When a spill adopts a new ordering-equivalent key
representative, that representative is measured independently.

Resident finalization reuses the exact sizes already checked while building the
batch. The checked canonical constructor rejects zero coefficients, duplicate
identities and out-of-order rows. A complete merged coefficient must fit i64
before it can enter this batch. The generic exact accumulator also emits its
ordered rows directly into a vector, rather than rebuilding an intermediate map.

Scratch coefficients are quoted decimal strings. After checked i64 narrowing,
the final batch's JSON coefficient has identical decimal digits without the two
quote bytes. This gives exact immutable batch sizing that can be reused by later
edge checks. Smaller record and cumulative byte limits are still enforced when
that cache is read. No cache is installed for a partially failed batch.

Full-tuple identity, spill/output limits, failed scratch retention and final
coefficient overflow behavior remain in force. No object encoding, physical plan
identity, checkpoint schema, PostgreSQL publication transaction, source LSN or
acknowledgement ordering is changed by this optimization.
