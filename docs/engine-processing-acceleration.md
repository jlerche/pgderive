# Engine processing acceleration

This change removes rebuilding trees where the engine already has canonical full
identities. Partition reads adopt the checked canonical cursor sequence; the old
bag and complete delta merge linearly. The merge deliberately preserves the old
bag's representative on ordering-equivalent identities. Existing membership,
coefficient, record, output and work limits still apply. Full output consolidation
continues through the exact accumulator.

Join reuses verified key probes across each nonempty delta side. After every
successful probe it releases heads not selected for the current key and drops the
redundant cached cursor. Disjoint runs therefore do not accumulate decoded blocks
across a tick. Failed reads preserve the previous probe state; cancellation drops
unpublished local probe owners. All three incremental join terms remain intact.

The prior investigation established a dense partition hotspot and a smaller,
batch-dependent join setup cost. Whole-query timings were mixed; this change does
not establish a general throughput or freshness improvement. Current-main
qualification and benchmark evidence is recorded with the final review artifacts.

No scalar constant materialization is included: the earlier experiment failed the
PostgreSQL-derived error-order corpus. Native scalar returns and ordered filters
are separate investigations, not changes to expression evaluation dependencies.

## Measurements against cba1c86

Release component benchmarks, three process repetitions, medians of per-process
medians. Partition alternates frozen baseline/candidate processes; Join alternates
the exact original and changed operators inside each process. Every iteration
checks the complete output bag. No dependency or compiler flags changed.

| Production operation | Baseline | Candidate | Time reduction |
| --- | ---: | ---: | ---: |
| Partition, 16 prior rows | 4.606 us | 3.921 us | 14.9% |
| Partition, 1,024 prior rows | 257.876 us | 131.356 us | 49.1% |
| Partition, 8,192 prior rows | 2.457 ms | 1.030 ms | 58.1% |
| Join, 256 affected keys / 8 memory runs | 345.543 us | 291.458 us | 15.7% |
| Join, 2,048 affected keys / 8 memory runs | 2.732 ms | 2.310 ms | 15.5% |

Partition uses one dense old group, one added identity and a cheap counting
callback. Join uses two prior values per key and positive one-sided changes;
existing independent-oracle tests cover simultaneous signed changes and cross
terms. These fixtures do not measure network latency. Builds ran concurrently on
the shared host; small differences are tentative. Earlier whole-query replay was
mixed and is not superseded by these component results.
