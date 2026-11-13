# Bounded ingestion scale validation

This records the pre-optimization baseline. Updated numeric SUM results are in
[numeric-sum-benchmark.md](numeric-sum-benchmark.md).

The bounded-ingestion implementation passed independent code review and the common
local gate: 464 Rust tests, live PostgreSQL harnesses, 97.39% production line
coverage, clippy and dependency checks. A final-build PostgreSQL control forced
spilling and matched native results after snapshot, 30,000 CDC changes and restart.
A permanent-capacity control failed once without retry despite eight allowed
attempts. These checks do not establish large-state recovery performance.

## Scale workload

TPC-H-derived lineitem, grouped by supplier, with `COUNT(*)` and
`SUM(l_quantity)` using PostgreSQL `numeric`. All 16 source columns are retained;
date fields are midnight timestamps because native source date binding is not
supported. This is a scale diagnostic, not an official TPC-H score.

| Measurement | SF1 | SF10 |
| --- | ---: | ---: |
| Source rows | 6,001,215 | 59,986,052 |
| PostgreSQL heap bytes | 955,973,632 | 10,371,596,288 |
| PostgreSQL total bytes, including PK | 1,145,487,360 | 12,265,627,648 |
| Initial readiness | 462.234 s | Failed: 3,600 s startup timeout |
| Startup sampled peak engine RSS | 641,019,904 B | 842,219,520 B |
| Sampled peak local scratch | 8,416,265,129 B | 91,960,473,444 B |
| Initial published physical objects | 108 / 401,408,061 B | Unavailable: no activation |
| Native equality after snapshot | Passed | Unavailable |
| Native equality after 30,000 changes | Passed | Unavailable |

SF10 scanned all source rows and reached upload, then timed out. Its 290 partial
objects totaling 1,116,171,658 bytes do not measure completed engine state. The
harness stopped it before another full bootstrap attempt. The previous spill-run
capacity failure is resolved, but SF10 initialization remains too slow under this
profile. Neither large-state restart nor SF10 CDC performance was validated.

## CDC overload and remaining work

SF1 offered 30,000 changes over 30 seconds in 300 transactions, with sizes from
single-row commits through 1,000 rows, using a 4,096-key source hotset. Only 45
changes across six source transactions completed during that full-latency
interval. Four completed epochs had processing p50 1,057.5 ms; two ten-second
windows had no completed epochs. These latency samples are censored and cannot
be treated as sustainable throughput. Catch-up took another 493.399 seconds
after injected latency was removed. Whole-run sampled engine RSS peaked at
1,369,317,376 bytes, including shutdown observations.

A 6,616-change catch-up epoch took 105.186 seconds: 90.002 seconds in Partition,
14.792 seconds staging arrangements, and 68.575 ms publishing to PostgreSQL.
Independent review verified that `SUM(numeric)` uses general Partition
reevaluation instead of additive statistics. Exact numeric additive statistics is
a broadly reusable next optimization. Full-row staging, repeated bootstrap
passes, and checkpoint/recovery access remain additional costs.

SF1 physical inventory after CDC was 169 objects / 609,678,782 bytes. Retained
checkpoint generations and GC transients contribute to this inventory; its growth
does not establish equivalent logical-state growth. The planned large-state
restart did not run because stopping the old worker exceeded the harness's
120-second timeout. Cleanup forced termination and retained the evidence.

SF1 completed request accounting, including bootstrap, drain and shutdown work:
423 PUT, 273,421 GET, 41 HEAD and 2 POST. Estimated R2 classes are 425 A,
273,460 B and 2 unclassified POSTs. SF10 made 290 PUTs before failure. These
are method-based estimates, not billing invoices or steady-state request rates.

## Method and reproducibility limits

The existing performance fixture, generator, request counter and latency proxy
were reused. Bootstrap and catch-up used zero added proxy latency. Only the
30-second CDC interval used the original GET/PUT latency distribution. Bulk runs
overlapped; SF10 was paused during SF1's timed phase except for approximately
39 ms at its beginning. Bootstrap timings are not isolated-machine comparisons.
Source writes began after readiness, so concurrent-write WAL retention during
bootstrap was not measured.

The explicit scale profile raised work contributions to 1 billion, cumulative
scratch writes to 512 GiB, output entries to 1 million and startup to 3,600 seconds.
Resident input stayed at 4 MiB / 4,096 entries, native value cache at 4 MiB and
decoded cache at 16 MiB / 256 entries. This does not establish that the default
resource profile supports either dataset.

The scale runs measured the streaming implementation before final scratch
integrity and complete-operator budget hardening. Measured binary SHA256:
`640772b2a6c1cbc15301e8d6ceedbce459c202f029f859506958a94895c63774`.
The final reviewed binary was separately gated and PostgreSQL-qualified; SHA256:
`49142f90983a378e234ccdbe9f0c61c350c6a077b1af4c8b76fc4a43d081e018`.
Do not silently attribute those scale timings to the final binary.

Raw results, source attestations, logs and failure evidence are retained under
ignored `artifacts/benchmarks/tpch-streaming/`; datasets and checksums remain under
ignored `artifacts/datasets/tpch/`. All owned PostgreSQL and S3 fixtures used tmpfs,
created zero Docker volumes, and were removed along with temporary scratch.
