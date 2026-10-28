# Byte and fan-in selection for physical maintenance

Maintenance now charges a proposed localized merge by referenced encoded payload
bytes and actual immutable root/extent fan-in. Cohort count is not physical fan-in:
one cohort may contain many roots. Existing tier/overlap triggers, range
localization, row balancing, packed uploads and weighted consolidation remain.

The initial profitable-subset targets are **32 MiB** and **16 inputs**. A job
already within both keeps its selection. Otherwise selection admits complete
memory input first, then representatives of the existing cohorts and remaining
roots in retained order. It removes a dominant object if it exceeds half the
selected rows or bytes. At least two independent cohorts must participate.
Excluded localized pieces revert to their complete original runs or extents, preventing
prefix/suffix duplication and unnecessary retained fragments. Only accepted
extents prepare their small authenticated fence cache.

This retained-order preference is an admission heuristic, not a durable age or
starvation guarantee. If no balanced multi-cohort subset fits, or selected memory
input is indivisible and oversized, the complete profitable selection proceeds.
Projected root/extent pressure at 96 also permits progress beyond the targets.
The existing full-state coefficient-overflow and 128-root capacity escalations
remain: excluded runs may contain essential cancelling coefficients. These are
explicit, counted exceptions. This is not a hard per-job CPU, memory, transfer or
latency bound. Hierarchical membership and smaller independently schedulable
histories remain separate work.

## Authenticated byte accounting

Every index address contains a required `payload_bytes` sum of encoded block
lengths beneath it. Construction, structural validation and cached authenticated
handle reuse verify the sum with checked arithmetic. Nested extents sum covered
subtree certificates and boundary leaves during the existing extent traversal.
The cost is available without payload decoding or additional metadata GETs.

It counts referenced payload occurrences, including repeated references across
runs. It excludes index bytes and pack slack, and is neither unique storage size
nor actual transferred bytes: read-ahead can fetch more. Memory inputs use their
existing cached encoded-size charge before compression. Serialization accounting
tests verify that selection reuses this charge rather than rescanning rows.
Actual request/read/write and retained-pack accounting remain separate in the
existing maintenance service ledger.

Extent metadata is immutable and shared through `Arc`; mutation for preparing
fences uses copy-on-write. Resident accounting includes the allocated span.

The required authenticated field is a **hard physical format cutover**. Both
worker plan revision families are now v4. Existing registrations require an
explicit view rebuild before deployment; there is no old-format reader or
migration adapter. Catalog table format is unchanged. New code and old code must
not be alternated against the same state.

## Durability and evidence

Selection only chooses physical work. Source transaction assembly, complete
full-tuple weighted identity, output semantics and publication remain unchanged:
immutable PUTs precede authoritative PostgreSQL membership/results/progress,
which precede logical-slot acknowledgement. GC and reader protections still
cover retained dependencies. Failed work leaves the original snapshot usable.

The service ledger now records selected bytes/runs and oversized/root-pressure
selections, plus coefficient-overflow/root-limit full compactions. These are
summed observations across a scoped job, not global debt or unique object bytes.
The fast report validator requires these fields.

Validation evidence is retained under
`artifacts/benchmarks/maintenance-selection-20261027`. Paired results use frozen
baseline `033ed67` and candidate test executables, the unchanged maintenance-v1
workload, and its existing cost/growth gates. A new physical fixture reuses the
production writer/merger, memory oracle, repeated flushes and cold reopen. Its
complete-merge control represents the original all-overlapping selection; it is
not an alternative production implementation. The paired and targeted results are summarized below.


## Fast validation results

The frozen baseline/candidate pairs use both execution orders and the unchanged
maintenance-v1 cases. PUT counts were exactly **1,028 / 1,063 / 742** and GET
counts **1,779 / 11,318 / 11,955** for both binaries. Upload bytes increased
**0.50% / 0.25% / 0.21%** from the additional authenticated metadata. Median
preparation time was within roughly 2%. Checkpoint uploads reached quiescence,
final checkpoint lag was zero, and maximum/final root counts matched. This does
not mean retained consolidation debt was zero.

The existing fixed/moving-key component fixture ran 1,024 flushes at each cold
size. PUTs, GETs and maximum/final root counts matched in all four cases. The
larger case retains 80,000 cold rows; it is not an arbitrary-size capacity proof.

The new wide fixture has 24 overlapping roots, 8,192 rows per root, and 1,024
additional text bytes on half the inputs. Against complete merging of these same
inputs using the same kernel:

| Physical work | Complete merge control | Bounded selection |
|---|---:|---:|
| First merge elapsed | 817 ms | 399 ms |
| First merge downloaded bytes | 87.1 MB | 43.5 MB |
| First merge PUTs | 29 | 15 |
| Total PUTs through 64 more flushes | 157 | 156 |
| Total downloaded bytes through those flushes | 87.1 MB | 87.1 MB |
| Final retained runs | 6 | 13 |

A 40-root narrow case reduced first-merge time from 10.2 to 2.9 ms with the
same 130 total PUTs. Its complete-cycle reads rose from 10.1 to 15.5 kB and it
retained seven runs versus two. Splitting work can add small repeated reads and
retained roots; it is not automatically a total-I/O improvement. The wide case
shows a smaller first job with the same payload reads and fewer PUTs over this
window. Extra retained roots can affect future larger merges; this test does not
rule out deferred consolidation work. These are unpaced local component
measurements; they exclude PostgreSQL publication and real object-store latency.

The unchanged maintenance-v1 acceptance assessment remains **red for both
builds**. The broad overlapping-root fixture and remaining growth/cost failures
are not solved by this slice. No acceptance thresholds were raised. Full
production steady-state qualification remains open.

The common `./scripts/check.sh` core gate passed **677 Rust tests**, **147 Python
tests**, **158 core worker results plus rebuild**, **97.19% production line
coverage**, and dependency/lint checks. Independent final review approved the
implementation, measurements and stated limits. Native results, concurrent DDL,
crash/replay, partial uploads, source ACK ordering and GC protections retain their
existing qualification coverage.

The sizing-cache test initially supplied durable cohorts for a memory-only run;
the normal flush path supplies that run's temporary cohort. Correcting the test
setup verified that selection does not serialize the rows again. Original failed
logs, the targeted passing check and the complete passing gate are retained.
The owned PostgreSQL fixture used tmpfs with no persistent Docker volumes and
was removed. No shared fixture, live deployment or automation was changed.
