# Sliding integral extrema in mixed window sets

E3 of the [engine development roadmap](engine-development-roadmap.md) targets a
measured remaining recomputation cost. M7 optimized COUNT/integral SUM/AVG; a mixed
set containing MIN/MAX forced complete frame scanning. The new path retains a
frame-local integral multiset for each MIN/MAX, alongside M7's linear statistics.
It accepts existing integral domains and expressions, not new PostgreSQL functions.

Monotone ROWS/GROUPS/RANGE frame bounds add entering occurrences and remove departing
ones. A value remains a candidate until its final duplicate departs. NULL and
FILTER exclusion use the same contribution evaluator as the scan reference;
empty frames emit NULL extrema. Checked unsigned multiplicities reject underflow
or overflow. Integer results use canonical native text. Nonintegral extrema,
numeric SUM and other ineligible mixed sets retain the complete scan path.

The multiset is transient, rebuilt from the pinned affected partition each tick.
No arrangement, checkpoint, source binding or public result layout changes. A
scan-created checkpoint can reopen on the optimized executor. Full affected-input
and retained-output reads remain, as do sorting, row expansion and output changes.
This is reduced recomputation, not a claim of smaller object I/O or delta-local
window work.

The B-tree state uses O(distinct frame values) entries and O(log distinct values)
update cost. Charged work counts aggregate record/result visits, not B-tree
comparisons, CPU instructions or allocation bytes. Existing partition occurrence,
output and contribution limits bound this state; it is not an independently
spilling index or an RSS bound; it trades additional transient memory for less
recomputation. FILTER/arguments are reevaluated on departures, as for M7.

## Verification and measurement

- Scan-reference equivalence covers ROWS/GROUPS prefix, bounded, suffix and empty
  frames, duplicate values, weighted peers, NULLs, FILTER and mixed aggregates.
- Increasing prefix sizes 64/256/1024 charge scan work 10,464/164,736/2,625,024,
  versus sliding 576/2,304/9,216. Outputs are identical. These are algorithmic
  counter assertions, not end-to-end latency measurements.
- The complete cold 400-row tick compares an independent source oracle and a
  scan-created checkpoint. Both paths measure 854 block misses, 104 newly written
  objects and 66,909 newly written bytes. This explicitly records unchanged I/O.
- The native live harness covers computed filtered COUNT/SUM/AVG/MIN/MAX in ROWS
  and GROUPS frames through source changes, rollback and cold restart, comparing
  binary bags, output metadata and independent frame slices.
- An ignored release measurement runs the unchanged scan reference and selected
  sliding evaluator on the same 1,000-row bag, with NULL every seventh row and
  nineteen cycling integral values. Three serial scan-then-sliding repeats are
  retained by `scripts/extrema_work_baseline.py`. This is a pure evaluator sample,
  not throughput, capacity, significance or complete-publication evidence.

The [retained release samples](sliding-extrema-samples.json) record source and
binary fingerprints, the exact command and all three repeats. ROWS scan times
were 396.491–397.716 ms versus 4.685–4.948 ms selected; GROUPS times were
11.018–11.147 ms versus 4.166–4.214 ms. This host-local, scan-first ordering is not
a statistical significance claim. The six native live cases additionally passed
for smallint/integer/bigint, each with ROWS and GROUPS frames. Independent review and the common gate passed: 322 Rust tests (three ignored),
45 Python tests, 127 worker cases plus rebuild and 97.38% production line coverage.
The complete release profile was not run. The [completion evidence](engine-development-evidence.json)
records the local qualification and unchanged cold-tick I/O.
