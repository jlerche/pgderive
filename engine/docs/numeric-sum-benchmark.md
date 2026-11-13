# Numeric SUM SF1 comparison

The final common gate passed: 468 Rust tests, 147 core live worker results plus
rebuild, integration checks, 97.41% production line coverage, clippy and dependency
checks. Independent code review found no blocking issues. The numeric native-MV
oracle compares binary send representations through scale-only updates, NULLs,
FILTER, removable NaN/infinities and cold restart. Final release SHA256:
`7bd8b826049cb0dddf6b63bb4171c7ddbdb67e114ca2bf127c7e9748ed010823`.
The measured and final release hashes are identical. The runner records base HEAD
`5b85dfe` in its commit field; the recorded working-tree source attestations
identify the implementation measured before this slice was committed.

## Same recorded workload

TPC-H-derived SF1 lineitem: 6,001,215 rows, PostgreSQL heap 955,973,632 bytes,
total 1,145,487,360 bytes including primary-key index. Group by supplier with
COUNT(*) and SUM(numeric quantity). Full 16-column source schema; date fields are
midnight timestamps because native source date binding remains unsupported.
This is not an official TPC-H score.

Reused the previous ignored runner and the production performance fixture,
generator, latency proxy and request counter. Offered 30,000 changes in 300
transactions over 30 seconds, approximately 1,000 changes/s, with transaction sizes
from one to 1,000 rows and the same 4,096-source-key hotset. Source size is fixed;
this is not a long-term growth or uniform cold-key test.

The timed CDC phase uses the original GET/PUT latency distribution. Bootstrap,
catch-up and cold restart use zero added proxy latency, matching the baseline's
phase protocol. Initial preparation overlapped validation; timed CDC explicitly
waited for the successful gate and its fixture cleanup. The baseline bootstrap
overlapped SF10. Bootstrap times therefore are not isolated-machine comparisons.
The existing explicit scale work/resource allowances were unchanged.

## Results

| Measurement | Previous scan path | Additive numeric statistics |
| --- | ---: | ---: |
| Changes completed inside 30 seconds | 45 / 30,000 | 30,000 / 30,000 |
| Completed source transactions inside interval | 6 / 300 | 300 / 300 |
| Completed epochs inside interval | 4 | 168 |
| Processing p50 | 1,057.5 ms | 9 ms |
| Processing p95 | 5,292 ms | 91 ms |
| Processing p99 | 5,292 ms | 1,276 ms |
| Observed-to-sink p50 | 1,478.5 ms | 14.5 ms |
| Observed-to-sink p95 | 7,590 ms | 1,144 ms |
| Observed-to-sink p99 | 7,590 ms | 1,480 ms |
| Prepare including object I/O p50 | 1,050.178 ms | 2.600 ms |
| Post-interval drain and native comparison | 493.399 s | 0.325 s |
| Initial physical state objects / bytes | 108 / 401,408,061 | 4 / 298,001 |
| Physical objects / bytes after CDC | 169 / 609,678,782 | 4 / 298,055 |
| Sampled startup peak engine RSS | 641,019,904 B | 60,428,288 B |
| Sampled whole-run peak engine RSS | 1,369,317,376 B | 201,355,264 B |
| Sampled peak local scratch | 8,416,265,129 B | 8,654,167,695 B |
| Bootstrap/readiness, overlapping work | 462.234 s | 241.148 s |
| Native equality after snapshot and CDC | Passed | Passed |
| Large-state cold restart | Not reached: shutdown timed out | Passed |

The baseline latency sample is right-censored by severe overload: two ten-second
windows had no completed epochs. These medians are not equivalent steady-state
samples and do not justify a numeric speedup multiplier. Completion of all offered
work is the stronger evidence of improvement. The 0.325-second drain includes the
final native query comparison, not just waiting for publication. Physical objects
include retained generations/GC transients and exclude current memtable contents;
physical inventory is not the total logical state or the WAL replay cost.

New processing p50 by ten-second window was 8, 9.5 and 9.5 ms. p95 was 61, 623 and
289 ms. This short fixed-state run establishes recovery from the old overload; it
does not establish consistently sub-100-ms freshness or a long-term capacity bound.

## Remaining tail latency

Tail spikes remain. The slowest epoch took 1,456 ms: 1,103.523 ms in Statistics,
340.349 ms staging, and 7.613 ms in PostgreSQL publication. Its request interval
contained 51 GETs with 1,332.767 ms total injected delay and a cache-miss increase
of 51. Similar spikes followed checkpoint maintenance and populated another set
of cached pages. This supports cold checkpoint-page reads and sequential probes as
the next tail-latency investigation, rather than whole-group numeric rescanning.
This is evidence from traces, not a newly implemented cache fix.

## Object requests and cost interpretation

Across the complete runs (different durations and shutdown outcomes): previous
423 PUT, 273,421 GET, 41 HEAD, 2 POST; new 28 PUT, 393 GET, 16 HEAD, 6 POST.
Estimated classes: previous 425 A / 273,460 B / 2 unclassified; new 34 A / 403 B /
6 unclassified. During the 30-second full-latency interval: previous 5 PUT and
1,862 GET; new 20 PUT, 280 GET, 10 HEAD, 5 POST. New estimated classes are 25 A,
285 B and 5 unclassified. The baseline completed only 45 changes in that interval,
so its lower PUT count is not a cheaper way to sustain the same throughput.
These are proxy method/listing estimates, not billing invoices. POST remains
unclassified; fixture inventory requests that bypass the proxy are outside scope.

## Provenance and cleanup

The previous baseline is retained under ignored
`artifacts/benchmarks/tpch-streaming/`; it measured the pre-final-hardening streaming
binary documented there. This run is under ignored
`artifacts/benchmarks/numeric-sum/`: raw events, proxy requests, per-phase journal,
comparison.json, tail-analysis.json, memory/scratch samples, source attestations,
final binary hash and cleanup evidence are retained. Received-to-sink samples are
joined from epoch events to published events by logical time, correcting the old
runner's empty field without changing the workload.

All owned benchmark and gate PostgreSQL/S3 fixtures and temporary scratch were
removed; zero Docker volumes were created. Existing shared demo/dev containers
were untouched. Dataset files and checksums remain under ignored
`artifacts/datasets/tpch/`. No SF10 rerun or deployment was performed in this slice.
