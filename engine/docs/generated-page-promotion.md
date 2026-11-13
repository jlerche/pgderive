# Bounded generated-page promotion

Generated immutable pages previously used an all-or-nothing decoded workspace:
if any sibling output exceeded its shared byte or entry budget, every staged
page and synopsis was discarded. Outputs just above the threshold therefore
lost all of their optional warming.

The workspace now keeps complete pages that fitted before overflow. Overflow
still stops subsequent row cloning, across all sibling arrangements. It never
retains partial page contents or partial logical-key answers. Admission uses the
existing decoded vector-capacity plus encoded-payload charge, not an RSS bound.

After successful uploads, handoff refreshes fitting duplicates and admits a
bounded subset of these pages. It can fill unused foreground capacity or recycle
warming that foreground readers have never consumed. The existing ordered
recency set represents unconsumed warming with timestamp zero; a foreground
`get` assigns an ordinary positive timestamp. Maintenance `peek` does not count
as foreground demand. Pages consumed by the foreground are not evicted by this
handoff. Current handoff admissions become recyclable only after the full
bounded subset is admitted, preventing it from evicting its own earlier pages.
Normal foreground cache admission retains its existing LRU eviction behavior.

This is conservative admission, not a prediction of the next hot keys. Staging
retains a generation-order prefix, but the identity map determines which subset
is promoted when foreground capacity is limited. A full foreground cache can
reject every new page. A larger duplicate can be skipped even when other unused
pages could theoretically make room. Consumed but no-longer-useful pages can
also remain until ordinary foreground cache pressure evicts them.

Page synopses are independently bounded and always describe a whole authenticated
page. Synopses retain their existing bounded CLOCK admission policy, including
eviction and a second chance for referenced entries. Decoded-page protection
does not imply synopsis retention. Missing synopses mean a possible match, never
an absent key.
This does not change the complete-or-absent logical-key answer cache.

No object format, pack geometry, compaction selection, checkpoint cadence,
publication, or acknowledgement ordering changes. Optional caches do not expose
object membership or durable progress. Failed uploads and abandoned workspaces
do not promote these decoded pages. Existing upload draining, protection,
transactional publication and cold validation remain authoritative.

## Validation

The production compactor threshold fixture checks cold equality and point probes
both inside and outside the retained subset. At four-page capacity, outputs of
three and four pages already fitted. Five-page output previously retained zero
pages and decoded five during probes; partial promotion retains four and decodes
one. Sixteen-page output decodes twelve instead of sixteen. Every case writes
the same two objects and identical uploaded bytes. GET count stays unchanged
because the existing range cache coalesces these tiny payloads. This fixture
isolates admission, not realistic page width or a wall-clock freshness SLO.

Tests additionally cover byte overflow, disabled caches, sibling budgets,
duplicate replacement capacity, pinned allocations, typed identities, recency,
recycling unused generations, protecting foreground hits and unrelated hot
arrangements, failed payload/index PUTs, and cold reopen.

The unchanged `maintenance-v1` SQL replay ran in both execution orders using
frozen optimized binaries, with no compiler overlap. Each case executes 512
epochs of 64 source changes, validates against the native/memory oracles and
cold reopen, and drains captured checkpoints. Counts below include final drain;
cumulative samples are not summed.

| Case | PUTs, both | GETs, before → after | Median preparation, before → after |
|---|---:|---:|---:|
| Fixed 512 groups | 1,028 | 1,779 → 1,779 | 2.062 → 2.088 ms |
| Fixed 8,192 groups | 1,063 | 11,318 → 11,352 | 16.194 → 16.037 ms |
| Growing from 8,192 groups | 742 | 11,955 → 11,928 | 15.624 → 15.586 ms |

Uploaded bytes, physical stored bytes, maximum run counts and final roots match
exactly. Downloaded bytes change by at most 0.071%; the largest GET increase is
0.301%. These small CPU changes do not establish a general speedup. All final
checkpoint lags are zero and uploads are quiescent; quiescence does not mean
all possible consolidation debt is absent. The component improvement is the
removal of all-or-nothing page retention at the overflow boundary.

Two intermediate policies were rejected: preserving every resident decoded page
and preventing synopsis replacement recorded 4–5% more GETs. Recycling unused
decoded warming alone did not resolve the synopsis regression. The final policy
retains the established CLOCK synopsis policy. The rejected results remain
ignored under `artifacts/benchmarks/partial-promotion-20261028/`, alongside an
invalid timing run that overlapped compilation. A failed-upload test initially
used one-based PUT numbers; the shared store uses zero-based numbers, corrected
to payload/index calls zero and one. The original failed evidence is retained.

The frozen performance binary includes the initial test-only failure selector;
the production implementation is identical to the final source. The corrected
failure test runs in the common gate. Existing steady-state qualification
failures remain visible. This slice does not establish sustainable maintenance
debt, bounded production lag or weekly unattended operation.

Independent review approved the final code and evidence. The common
`./scripts/check.sh` gate passed 684 Rust tests, 147 Python tests, the sequential
live PostgreSQL harnesses including crash/replay, concurrent DDL and query
rebuild, 97.19% production line coverage and dependency checks. The disposable
PostgreSQL fixture used tmpfs with no persistent Docker volumes and was removed.
Coverage evidence is `artifacts/coverage/run-tCd0dC7I`; worker evidence is
`artifacts/worker/run-RYAIdAgp`.
