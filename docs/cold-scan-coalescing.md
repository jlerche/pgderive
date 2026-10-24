# Cold scan read coalescing

Full scans now reuse the existing authenticated compressed-window reader rather
than fetching each small page separately when decoded retention is disabled.
Each such object cursor owns one 256 KiB compressed window. Sequential pages in
the same window share a GET; crossing windows or packs replaces that window.
Pages larger than the window use the existing direct-range path. This changes no
object format, index, page geometry, object membership or publication protocol.

Foreground cursors with an enabled decoded cache keep their existing shared
caches. Ordinary zero-retention cursors preserve encoded disk-cache admission and
share foreground counter/pin accounting, but their compressed windows are private.
Compaction cursors peek at foreground decoded entries without admission or LRU
promotion, reuse encoded hits without inserting newly fetched pages, and own
private windows and accounting. Generated-output cache
handoff retains its separate state. Dropping cursors releases private windows.

The range reader now always admits to its own bounded window tier; the previous
read-only scan branch is removed. Scan isolation comes from ownership of a private
window instead of disabling coalescing. Existing cancellation/singleflight logic
is reused. Page length/hash, decompression bounds, canonical identities, weights,
row counts and fences are still validated before rows are exposed. Checksum
failure invalidates affected window data, allowing a retry after repaired storage.
Already fetched valid immutable bytes remain usable if remote storage subsequently
disappears; a new cold reader still fails. Failed page loads retain cursor position.

Memory is bounded per cursor, not per engine: at 256 input runs a compaction may
retain 64 MiB of compressed windows in addition to decoded heads. A boundary-crossing
read may transiently hold a previous and a new window plus its selected payload;
large individual pages retain the existing page allocation bounds. Foreground
cache budgets are unchanged. Independent concurrent scans can fetch the same
window separately; no unbounded scan cache or shared scan LRU is introduced.

## Fast validation

All 483 unit tests passed (three ignored), including encoded-cache reuse, exact
weighted results, shared decoded accounting, compaction, failure/retry and page
integrity. Release range tests passed. A 3,000-page cold scan reads one 118,413-byte
window instead of 3,000 separate ranges, both with disabled foreground retention
and with explicit compaction-style scan isolation. It compares every key, full
value and coefficient with the independently constructed input and checks that
foreground scan admission remains disabled. The small compaction fixture likewise
falls from eight GETs to one and still rejects corrupt state without changing its
manifest. Existing point-read coalescing remains 1,026 GETs to four in its synthetic
statistics fixture; this is a separate point-read comparison, not the cold-scan
before/after result.

The fast production application replay and virtual-time checks passed. Raw logs,
failed intermediate test evidence and final results are retained ignored under
`artifacts/benchmarks/cold-scan-coalescing/`. The common gate and SF10 measurements
are recorded below after completion.

Growth replay at 10,000 and 100,000 groups passed across three seeds, with 32
measured updates per case and no measured PUTs. Median prepare+commit was
0.461–0.481 ms and 0.735–0.878 ms respectively. These local component measurements
exclude network pacing and are not an end-to-end freshness claim. The earlier
single-seed 16 KiB baseline measured 0.469 / 0.804 ms; these short observations
show no large preparation regression rather than proving a throughput change.

## Snapshot throughput investigation

During this run, a read-only one-million-row PostgreSQL EXPLAIN ANALYZE probe
measured approximately 0.116 s for scanning all columns, 6.538 s for the current
full-row text/JSON snapshot expression, and 1.618 s for the same JSON expression
limited to query columns plus primary-key columns. This is a single warm local
observation, excludes row transfer/client work, and overlaps bootstrap CPU. It is
not an additive decomposition of the pipeline or proof of a 5–7 s/million target.
Raw plans are retained in `postgres-snapshot-probe.json`.

The current source path converts every column to text/JSON, decodes a row map,
clones every field into relational input, and performs repeated sizing/serialization
before generic spill consolidation. Early required-column projection, leaner
snapshot transport/decoding, and serialization reuse are concrete next candidates.
Snapshot and CDC must retain one bounded ingestion path and exact transaction,
PK-validation and snapshot/LSN handoff semantics. A replacement should remove the
old path rather than retain a compatibility fallback. No such change is included
in the scan-coalescing benchmark.

Arrangement indexes currently use resident first/last full-tuple fences, without
Bloom filters. Key-appropriate filters could reduce negative point lookups inside
many overlapping run ranges. They do not accelerate full scans or snapshot row
encoding; evaluate them with negative-probe workloads and equality-consistent
hashing before adding a storage-format feature.

RisingWave checkout was fast-forwarded to
`1c69fba082ea181ac97ef487f6e341c95bb68493` for the follow-up investigation. Its
[PostgreSQL reader](https://github.com/risingwavelabs/risingwave/blob/1c69fba082ea181ac97ef487f6e341c95bb68493/src/connector/src/source/cdc/external/postgres.rs#L347)
uses bounded primary-key scans and per-column decoding, with specific casts for
some types, rather than wrapping every source row in JSON. Its
[snapshot chunk builder](https://github.com/risingwavelabs/risingwave/blob/1c69fba082ea181ac97ef487f6e341c95bb68493/src/stream/src/executor/backfill/cdc/upstream_table/snapshot.rs#L177)
yields chunks during the scan. The
[CDC backfill executor](https://github.com/risingwavelabs/risingwave/blob/1c69fba082ea181ac97ef487f6e341c95bb68493/src/stream/src/executor/backfill/cdc/cdc_backfill.rs#L1014)
forwards them downstream, and the
[hash aggregate](https://github.com/risingwavelabs/risingwave/blob/1c69fba082ea181ac97ef487f6e341c95bb68493/src/stream/src/executor/aggregate/hash_agg.rs#L638)
applies each chunk, flushes dirty groups under memory pressure and commits state
at barriers. It can persist substantial source/operator state; the distinction is
that computation does not wait for a generic whole-input scratch consolidation.
External CDC backfill and backfill from an existing RisingWave relation are
separate mechanisms; the latter's snapshot/log-store design must not be confused
with PostgreSQL CDC backfill.

The intended adaptation is a bounded unpublished execution workspace that accepts
chunks, evaluates operators and flushes operator state before the full snapshot
ends. Final validation/publication remains at the snapshot boundary, followed by
CDC from its recorded LSN. Our exported-snapshot handoff can stay; adopting
RisingWave's interleaved PK/offset backfill algorithm is unnecessary for that first
change. Full-tuple weights, both-input join cross terms, validation at transaction
completion and bounded resource/error behavior remain requirements. Replace the
blocking input lifecycle and update its callers; do not retain it as a bootstrap
compatibility fallback.

An additional EXPLAIN probe using four projected text columns without the JSON
wrapper took 0.231 s/million on the server. It is another single observation, not
a client-throughput result. A per-column text transport is a candidate that can
reuse existing PostgreSQL text semantics without requiring all new binary type
codecs. Any replacement must retain server-side oversized-row protection and
codec settings; faster unbounded row transfer would not be an acceptable fix.

## SF10 result

Final binary SHA256:
`1b6d064414730ab9bfa91bd6d9bb6e33403776fed93a7ce5c77e3230d86c90e6`.
The comparison baseline is `6a62e6f`, with the same 16 KiB pages, packed objects,
cache budgets, 59,986,052 source rows, 100,000 supplier groups, query, deterministic
transaction sizes and latency-proxy seed. PostgreSQL heap was 10,371,596,288 bytes
and total relation size including indexes was 12,265,627,648 bytes. Both runs
completed all 30,000 changes in 300 source transactions during the 30-second
offered interval; no CDC publications remained for drain.

| Measurement | Before | Coalesced scans |
| --- | ---: | ---: |
| Cold restart GET / HEAD | 3,140 / 14 | 38 / 14 |
| Cold restart response bytes | 5,800,476 | 5,800,476 |
| Ready-event recovery time | 4.775 s | 0.640 s |
| Whole-run GET / PUT / HEAD | 5,361 / 28 / 70 | 134 / 28 / 70 |
| CDC + drain GET / PUT / HEAD | 82 / 24 / 54 | 82 / 24 / 54 |
| Initial stored objects / bytes | 4 / 4,142,528 | 4 / 4,142,528 |
| After-CDC stored objects / bytes | 28 / 5,800,476 | 28 / 5,800,476 |
| Whole-run sampled peak RSS | 341,413,888 B | 326,856,704 B |
| Sampled peak scratch | 91,871,166,235 B | 92,087,577,458 B |
| Bootstrap | 2,792.58 s | 2,958.53 s |
| Published CDC epochs | 152 | 139 |
| Processing p50 / p95 / p99 | 73.5 / 245 / 273 ms | 82 / 269 / 318 ms |
| Observed-to-sink p50 / p95 / p99 | 106 / 280 / 604 ms | 121 / 321 / 417 ms |
| Preparation p50 / p95 / p99 | 40.28 / 180.75 / 218.86 ms | 46.30 / 185.56 / 227.05 ms |

Cold restart GETs fall 98.8%, and whole-run GETs fall 97.5%, without skipping
validation, reading fewer state bytes, increasing PUTs or changing physical state.
Recovery timing uses the ready event's `open_recovery_us`, excluding the later
native SQL comparison. Bootstrap and cold restart have zero injected delay; only
CDC uses the full seeded object-storage latency distribution. Initial bootstrap
in this run overlaps common-gate CPU and the short read-only PostgreSQL probes,
so its elapsed time is not an isolated before/after speed comparison. This slice
does not optimize the blocking snapshot ingestion pipeline discussed above.

This is a cold-scan request/recovery improvement, not a claimed hot-CDC latency
improvement. Adaptive epoch counts differ between short runs, processing tails
are worse in this observation and freshness tails are better; repeated workload
qualification is needed before inferring a general latency shift. Timed CDC
requests, transferred bytes and retained physical bytes are unchanged.

Native PostgreSQL results matched after initialization, CDC and cold restart.
Six maintenance checkpoints completed with zero retries. The common
`./scripts/check.sh` gate passed with strict lint/dependency checks, 483 unit tests
(three ignored), 147 core worker checks plus rebuild, and 97.39% production line
coverage. Independent implementation review approved the range/counter ownership
and failure semantics. Owned Docker fixtures and scratch were removed, zero
Docker volumes were created, and both datasets and raw results were retained.
`cleanup-verification.json` confirms actual absence separately from the runner's
earlier cleanup record.
