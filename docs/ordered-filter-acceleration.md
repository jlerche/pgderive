# Ordered identity-preserving filters

`Project::filter` accepts a predicate and emits unchanged full identities with
unchanged signed coefficients. SQL Filter nodes use this constructor. Generic
Project and Expand callbacks keep their exact Consolidator loops; they can still
collide, reorder, expand and cancel full identities.

Only the filter constructor certifies ordering. Its builder verifies strictly
increasing identities before crossing each resident-chunk boundary. It retains a
bounded vector and appends to one checksummed scratch run on spill. No sorting,
intermediate coefficient map or run merge is needed. The existing quoted-decimal
coefficient wire format and scratch reader remain shared with consolidation.

Record, resident byte/entry, contribution, scratch and materialized-output limits
still apply. Sealed scratch edges can exceed materialization limits, matching the
existing streaming contract. Materialization checks bounds before and during
collection. Predicate/read/write failures retain diagnostic scratch; dropping
unpublished work and successful final ownership release clean it up. A cursor
pins the sealed run until its last owner drops. The change does not alter IR,
transaction, object publication, durable progress or acknowledgement protocols.

## Measurements

Six processes, balanced old/general/filter mode ordering, medians of process
medians, release build on a shared host with other builds active. The baseline is
the used a890089 Project/Expand path copied into measurement-only source. Each
iteration's complete weighted output is checked outside construction timing.
Inputs include negative weights, unchanged full identities, half selected rows,
empty or 128-byte string payloads, and resident or forced-spill output.

| Input rows / payload / result | Previous | Ordered filter | Speedup |
| --- | ---: | ---: | ---: |
| 4,096 / empty / batch | 316.654 us | 101.708 us | 3.11x |
| 4,096 / 128 bytes / batch | 543.061 us | 272.321 us | 1.99x |
| 16,384 / empty / sealed | 6.268 ms | 1.997 ms | 3.14x |
| 16,384 / 128 bytes / sealed | 10.758 ms | 4.032 ms | 2.67x |
| 16,384 / 128 bytes / batch | 8.365 ms | 7.014 ms | 1.19x |

The unchanged generic-path control varied from -6.4% to +4.4% across cases, with
no consistent direction. These are component measurements, not a dedicated-host
regression qualification or a general whole-query speedup claim. An initial
per-row output enum was discarded after its control showed overhead. The final
generic loops retain their original implementation with one outer filter branch.

Focused tests cover resident/spilled sources and outputs, i64 coefficient
boundaries, complete identities sharing keys, one-run wire byte accounting,
limits, predicate-error ordering, checksum corruption, failure evidence and
ownership cleanup. Raw measurements, source, review and qualification evidence
are under `artifacts/benchmarks/ordered-filter-20261108`.

Compiled-query replays passed the independent oracle, cold reopen and unchanged
object-store counter checks. Initial fixed-order timing runs suggested slower
small epochs, but an identical-binary control also shifted by roughly 18–22% on
several recipes. A balanced three-way run with two baseline copies reversed the
candidate comparison. These shared-host results validate weighted outputs, cold reopen, and I/O
counters for the exercised traces, but cannot establish a general whole-query
speedup or regression.
All observations and diagnostic variants are retained in the evidence directory.
