# Shared relational properties

One initialization-time analysis of the bound contract and resolved graph supplies
sink-key admission, positional-window order proofs and committed statistics
state selection. It replaces the isolated sink, occurrence and source-key walkers.
Execution does not perform extra per-row property validation.

Each edge records candidate keys, unchanged-value identities, NULL facts,
navigation columns, equality metadata and a conservative global row bound.
Aliases retain value identity. A projection losing a key component loses that
proof. Filters preserve keys and direct IS NOT NULL conjunctions refine NULL
facts. Computed expressions do not inherit uniqueness merely because they read
a key. Grouping proves the complete grouping tuple, with NULLS NOT DISTINCT row
identity; sink primary keys additionally require non-NULL values and supported
native equality. Text/collated and numeric sink keys remain unsupported.

Inner joins combine keys from independent occurrences. A side's key survives
only when compatible navigation keys prove at most one match on the other side.
Self-join aliases have separate occurrences. Forks sharing lineage conservatively
drop keys and reset value identities, preventing false equivalence of two row
variables. Outer joins mark the right side nullable; left keys survive a proven
at-most-one match. Existence filtering preserves left keys. UNION ALL drops keys.
Expansion needs the input key plus its generated ordinal. Unsupported lookup and
internal state nodes explicitly stop inference. Candidate proofs are capped at
64 per edge; dropping extra proofs only rejects an optimization.

All consumers use the same inference rules. Positional ordering can use any
proven key, including a retained alias. Committed grouped COUNT/SUM/AVG may omit
historical input validation state for a proven unique full-row input. Arbitrary
weighted execution retains strict historical validation. These facts do not
change navigation-prefix identity, transaction boundaries or join cross terms.
The publication locks, old-tuple checks and durability/ACK order are unchanged.

Plan revisions change to v3 and include the contract and certified nodes. Existing
registrations require explicit rebuilding before deploying this change. No old
physical state is silently reopened with new assumptions.

Qualification uses compiler proof tests, strict-versus-committed weighted output
comparisons with both-input transactions and cold restore, and the existing
PostgreSQL native binary oracle for keyed projections, inner/outer/pair joins,
key moves, deletes, restart and safe acknowledgement. Rejected and successful
runs remain under ignored `artifacts/benchmarks/relational-properties-20261027`.
Component results do not establish production steady state.

## Initial measured effect

Against release `845ed5f`, the same optimized production-worker harness ran a
20,000-group joined COUNT/SUM query over 60,000 initial bid rows, then 18 matched
small/chunk-crossing update and delete epochs. Both binaries passed the native
binary oracle and safe-ACK check after each epoch. The certified plan retains
one fewer aggregate input arrangement (separately asserted against strict plans).

| Component | Baseline | Shared analysis |
|---|---:|---:|
| Preparation median, including object I/O | 7.75 ms | 5.03 ms |
| Worker CPU across measured epochs | 1,190 ms | 930 ms |
| Maintenance elapsed median | 57.05 ms | 37.48 ms |
| Whole-lifecycle object PUTs | 39 | 30 |
| Whole-lifecycle object GETs | 231 | 161 |
| Whole-lifecycle uploaded bytes | 1,738,255 | 1,124,423 |

These are one paired local run with zero injected latency, not a throughput SLO
or a growth/steady-state qualification. Whole-lifecycle counters include
bootstrap, checkpoint/GC and shutdown, rather than only foreground publication.
The final evidence and frozen binaries are in the `final` subdirectory of the
ignored benchmark directory above. Earlier interrupted-gate evidence is retained.
