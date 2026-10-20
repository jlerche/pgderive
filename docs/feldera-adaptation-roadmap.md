# Feldera operator adaptation

The active scope is all five structural improvements proposed in the source
comparison. These are independent implementations for Pgderive, not imports of
Feldera's runtime or SQL semantics. All computation remains in-engine; PostgreSQL
remains the metadata/progress authority and ordinary destination table.

| Item | Required implementation and evidence | Current state |
| --- | --- | --- |
| A1 membership | Specialize SELECT DISTINCT representative/threshold maintenance and persist exact right-key counts for EXISTS/NOT EXISTS. Prove stable positive counts avoid hot-left reads, zero crossings preserve multiplicities/NULLs, counters can exceed i64, and state restores safely. | Complete; common gate and independent review passed |
| A2 LEFT JOIN | Direct matching-pair deltas with the simultaneous-input cross term and null-extension transitions; no complete Cartesian-product reconstruction. Prove signed histories, filters, restart, overflow atomicity and hot-key work. | Complete; direct and cursor algorithms passed source/native/restart/work checks |
| A3 cursor interface | Let direct group operators consume pinned old state and a signed-delta overlay without materializing complete old/new groups. Validate every changed identity even when evaluation stops early. Preserve generic recomputation for operators that need it; remove superseded interfaces. | Complete; common gate and independent review passed |
| A4 persistent ordered summaries | Implement a narrowly specified window family using durable ordered/range aggregate summaries across ticks. Compare exact native results and independent recomputation, including retractions, NULL/FILTER, restart and actual read/work counts. Keep unsupported frames on their existing exact evaluator. | Pending; temporary sliding frame state alone does not satisfy this item |
| A5 multi-run qualification | Vary immutable runs, hot-key size and unrelated state independently, with cold caches and signed cancellation. Check exact results before/after compaction and quantify read amplification. Document remaining run-count and output-size costs. | Complete; common gate and independent review passed |

Changing persisted membership state deliberately changes the complete plan
identity. Old existence checkpoints require explicit `query rebuild CONFIG.toml`;
they must not be reopened under the new layout. Other unchanged graph layouts
retain their identities. No shared application state is migrated or reset by
these implementation tests.

The scope remains open until every row has passing current implementation,
independent review and appropriate common/native qualification evidence. Tests
for one optimized window family will not establish universal window complexity.


Current A1/A2/A3/A5 evidence: [qualification manifest](feldera-adaptation-evidence.json).
The common gate passed 329 Rust tests (three ignored), 45 Python tests, 127 worker
cases plus rebuild and 97.39% production line coverage. Formatting, strict Clippy,
file-size and dependency checks passed. Independent review found no actionable
implementation findings. A4 remains pending; this is not completion of the full
goal. Its independent prefix-decomposition design experiment has passed 21,000
comparisons, but does not substitute for a durable runtime/native qualification.
