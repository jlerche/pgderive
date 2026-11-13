# Production qualification

Status: **not qualified for a general production deployment**. Correctness gates
and bounded GC admission can pass without demonstrating sustainable service at a
specified state size and offered load. Do not infer weekly uninterrupted operation
from logical-time controller tests or scaled component replay.

The current capacity contract includes 4,096 live/protected roots, 262,144 marked
objects or sealed upload ledger entries, and 16 MiB of coarse GC metadata.
Collection consumes one global page of at most 1,000 objects per turn and bulk
DELETEs at most 256 paths per application batch. The default worker admits eight
workspace phases and sixteen object-store API operations. These bound specific
resources rather than whole-process retained bytes.

## Acceptance that must pass

- The common gate covers Rust/Python checks, sequential PostgreSQL
  transaction/CDC/publication/recovery/ACK tests, worker regressions, rebuilding,
  line coverage and dependencies. The full profile adds the complete supported
  query matrix and release fault/health checks.
- Fast paged-GC validation must cover more than one page, partial failure, cursor
  publication rollback after physical DELETE, fresh-process/cache continuation,
  active readers, unknown objects, late PUTs and mid-pass namespace closure.
- The existing optimized `maintenance-v1` workload and original cost/growth ceilings
  must pass without relaxing their thresholds. Complete natural large merges,
  final drain, cold reopen and the independent result oracle remain included.
- Whole-worker retained allocations and pinned cache/read/upload memory need a
  declared budget at the target query/state sizes. Concurrency limits alone do
  not qualify RSS.
- End-to-end CDC, sink publication and source acknowledgement must maintain bounded
  lag under the declared constant change rate through growing retained state,
  repeated largest-tier consolidation, GC passes and injected failures. Upstream
  WAL retention and object-store request allowance must stay within their limits.

The latter three items remain open where the recorded evidence is red or absent.
No deployment or production-ready claim is authorized by the first two alone.
The [steady-growth evidence](steady-growth.md) records exact runs and failures;
ignored benchmark directories retain binaries, source hashes and raw results.

## Latest assessment (2026-10-26)

Independent review accepted incremental GC subject to the mandatory gate. The
live harness passes cursor rollback, fresh-cache multi-page resume, unknown
retention, late-quiescence and mid-pass upload closure tests. The unchanged
optimized maintenance replay takes 29.24 seconds; all three cases remain
unqualified under their original growth/request/byte ceilings. Total PUT counts
and uploaded bytes are identical to the prior commit. See the recorded numbers
in `steady-growth.md`; the new physical GC should not be credited with repairing
index-read or consolidation growth that it does not address.

The first full release run passed 241 worker results, including the full 80-case
composition matrix, then stopped on a lookup bootstrap projection defect. That
was corrected with typed inner-SELECT dependencies and independently reviewed;
all five native lookup modes subsequently pass. It is not a passing full-profile
run on the final source. The remaining release phases and a final common gate
are recorded separately. A new 1,101-object admin drop regression fails the
one-page implementation and passes after explicit draining was added.

The final common gate passes: 146 Python tests, 639 Rust tests (11 ignored),
the sequential live snapshot/CDC/GC harness, 153 core worker results plus rebuild,
97.16% production line coverage, formatting, Clippy and dependency checks.
Segmented release evidence verifies all 379 query results and the remaining
fault/health/TLS/recovery/package/container/capacity reports; it remains distinct
from one uninterrupted final full-profile run. The corrected registration-race
fixture retires through production drop before destroying its catalog, preserving
the full-prefix cleanup assertion. The final optimized replay takes 28.93s and
still fails its original performance ceilings; PUT counts and bytes are unchanged.
General production qualification remains rejected for the open criteria above.
