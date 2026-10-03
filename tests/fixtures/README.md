# Recorded PoC event fixture

`zset-contract-events.jsonl` retains all 120 transaction event lists, transaction
IDs and rollback flags from the accepted PoC `zset-contract-source.jsonl`. Only
the unused SQL strings were removed; commands are never executed.

Original: `/home/jlerche/programming_projects/workspace/try_incremental_pg_cache/object_backed_dataflow/zset-contract-source.jsonl`
Original SHA256: `34614d2cf4baf849f3484495e5ccee5b741482ead1fc19334b0863ecf130f2bb`
Fixture SHA256: `a7486d3b7a943e0ae9de61e1f60745a28cb28a68d98cff262f3af3f7765e258e`

Seed 20261003; initial rows follow `check_zset_contract.py::make_trace`: 10,000
tasks plus 512 fan-out tasks (IDs 20000–20511). Project state is scoped to IDs
1–32, containing every project mutation and destination join key in this trace;
unaffected results for projects 33–10000 are outside this replay. Every task
change is replayed and validated against an independent primary-key source map.
Full-tuple weighted engine operators do not use these primary keys.

The test starts with an explicit in-memory bootstrap, skips rolled-back source
transactions, and compares integrated join/filter/count results with a fresh
source-map computation after each commit. This does not test storage recovery
or PostgreSQL snapshot/CDC bootstrap. Original accepted artifacts are unchanged.
