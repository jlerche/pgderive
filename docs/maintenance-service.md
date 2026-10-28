# Maintenance service budgets

Physical maintenance reuses the existing workspace admission, cooperative reader
charges and checkpoint-pressure controller. Publication cadence, merge selection,
immutable formats, source replay boundaries and PUT → PostgreSQL publication → ACK
ordering are unchanged. There is no second epoch or maintenance scheduler.

## Service policy

The existing `Workspaces` policy now also implements object-request admission.
Execution and request pools are separate: an upload already owns an execution
permit, so sharing that exact pool would deadlock with capacity one. Both pools
retain one foreground slot when their capacity exceeds one. Maintenance joins
the same FIFO global queue, so newly arriving foreground work cannot continually
overtake a queued maintenance request. Capacity one remains supported.

An admitted epoch tracks foreground demand from source-input preparation through
PostgreSQL publication and local commit. It releases the demand guard on errors or
cancellation. Idle CDC and voluntary epoch gathering do not count as demand.
This guard does not reserve an execution permit during database waits. Execution
permits also track active and queued foreground phases, including cancelled
waiters. The physical maintenance scope uses the existing reader byte/operation
charges: one work unit per charge plus one per started 4 KiB. After 128 charged
units it yields explicitly; with foreground demand, the quantum is 32. Native
Tokio cooperative fuel remains in place. Idle maintenance can use the longer
turn; busy maintenance receives positive turns and never waits for the foreground
to become idle. Existing block/index codec limits bound indivisible operations.
These are cooperative work and admission guarantees, not reserved CPU percentages,
hard elapsed-time limits or a foreground freshness SLO.

Scopes cover frozen checkpoint upload, detached compaction and the existing
collector. They are installed inside spawned maintenance work; task-local state
is not assumed to propagate through `tokio::spawn`. They exclude result publication
and ACK transactions. GC retains its existing transaction coordination inside its
scope. Cancellation retains the existing upload-protection/quiescence rules and
releases admission/demand guards; budgeting never cancels a PUT for preemption.
The existing debt forecast and hard replay/memory/run limits still govern intake.

## Accounting

Checkpoint and collection events include per-job `service` reports. Heartbeats
include cumulative `object_service` counters split by foreground and maintenance,
plus request-pool occupancy and waits. The existing fast replay records one service
report per installed checkpoint and rejects missing reports.

- `work_units`, `work_bytes`, `yields`: charged physical operations/encoded or
  decoded bytes and explicit service yields. Repeated processing is charged again;
  this is not distinct retained state or measured CPU time.
- `requests`: admitted ObjectStore API calls, including failures. These are not
  physical HTTP or Cloudflare Class A/B counts: range calls, paginated listings,
  bulk deletes and client retry behavior can change those counts.
- `read_requested_bytes`: requested ranges, or the advertised response range for
  whole/offset reads once headers arrive. Known failed range requests are included.
  `read_bytes` counts delivered body chunks or returned logical range slices.
  Cancelled unread bodies contribute no delivered bytes. Coalesced overfetch and
  network buffering are not measured; these are not billed-transfer counters.
- `write_requested_bytes`: payloads of attempted PUTs. `written_bytes` counts
  acknowledged successful payloads. An uncertain failed PUT can still exist
  remotely; these counters never authorize deletion or retry shortcuts.
- `retained_bytes`: authenticated block payload reused without rewriting it.
- `referenced_pack_bytes` and `sparse_pack_slack_bytes`: output-root pack coverage
  observed during existing index construction. Sparse means less than half live.
  Shared packs can appear in several roots/jobs, so these are occurrence-weighted
  rewrite opportunities, not an additive global physical-GC debt inventory.

Accounting adds no object reads or writes. It does not change which roots are
merged, rewrite sparse packs automatically, or retire logically required state.
Those policies belong to bounded local maintenance and membership work (#6).

## Fast validation

The current-thread production streaming writer reduces maximum uninterrupted
advancement from 43 to 11 rows under simulated foreground demand. It produces
identical root references, GET/PUT work and uploaded bytes. The measured poll
maximum was 0.326 versus 0.099 ms in one optimized local run; the work bound is
the deterministic assertion, not that hardware-specific timing.

Paused-clock request tests exercise distinct capacity-one execution/request pools,
stream cancellation and failed PUTs. Under 128 maintenance PUTs and 512 concurrent
foreground reads, the initial foreground probe takes only its injected 10 ms
read service (no queued-maintenance delay), both streams finish within the
10-second logical limit, and all request permits are released. A separate test
repeats FIFO admission/cancellation for 1,024 busy cycles.

Matched immutable binaries against `910237f` execute the unchanged maintenance-v1
workload, in alternating candidate/baseline order, with compilation excluded from
timings. Each run contains three 512-epoch fixed/growing cases and installs 690
checkpoints. PUTs are identical: 1,028 / 1,063 / 742. GETs through final drain
are identical in both larger cases (11,318 and 11,955). The tiny control rises
from 1,777 to 1,779 GETs as scheduling changes cache-warming timing. Every final checkpoint lag is
zero and every upload is quiescent. Across the two orders, foreground preparation
medians are effectively flat (about 2.10, 16.5 and 15.9 ms); this does not establish
improved production foreground freshness or an independently offered-rate SLO.

The maintenance-v1 acceptance gate remains red in baseline and candidate for
existing cold overlap, read/index growth and absolute cost limits. Bounds and
quiescent final debt do not establish production steady state. Original failures,
matched outputs and frozen binaries are retained under the ignored
`artifacts/benchmarks/maintenance-service-20261027` directory. An initial candidate
run reused a baseline-overwritten Cargo artifact; it is explicitly invalidated.
The rebuilt candidate has a distinct hash and the required service reports.

Publication demand is covered by the existing capacity-one streaming fixture: it
marks foreground work without owning a workspace, completes maintenance with
identical authenticated output and storage work, and requires shorter polling
turns. The held-checkpoint worker harness also checks that successful publication
reports the epoch demand guard. This closes a demand-lifetime gap, not the remaining
production qualification or physical-debt inventory work.

The epoch-demand follow-up was compared against `194660c` in both execution
orders, with independently offered 1,000 changes/s, varied transactions,
43,200 source rows and injected object-store latency. All four 20-second runs
passed native equality and ACK checks, with zero final pending changes. Processing
p50/p95 remained 2/12 ms; lifecycle PUTs stayed 27, GETs 30 and HEADs 12 in
every run. These short comparisons show no measured throughput or request-cost
regression, not a demonstrated foreground latency improvement or steady state.
The capacity-one streaming fixture reduced maximum uninterrupted advancement
from 43 to 11 rows with identical output and storage work. Common validation
passed with 97.19% production line coverage; independent review found no
actionable issues. Evidence is retained under the ignored
`artifacts/benchmarks/epoch-service-20261028` directory.
