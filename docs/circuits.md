# Typed executable acyclic circuits

`engine::dataflow::Stream<T>` is a typed graph edge carrying a sequence of values
across logical ticks. It is a handle, not a collection or a clock. `TimedBatch<T>`
is one element of that sequence: a logical time and a complete value. Production
relational edges carry signed, consolidated `Batch<K,V>` deltas. Source LSN/XID
metadata belongs to the adapter/publication layer and never enters the operators.

## Construction and execution

`CircuitBuilder<S>` binds executable callbacks to an existing validated `Plan`.
The plan remains the semantic, schema/codec, arrangement and durable identity
contract. The builder validates that the actual callbacks' typed dependencies
implement that declared topology; a declaration alone still cannot execute.

- `source<T>(id)` creates a typed source edge. `source_with` also prepares an
  immutable arrangement replacement for that source.
- `unary(id, &input, callback)` binds a project/filter or aggregate node.
- `binary(id, (&left, &right), callback)` binds a join node.
- Each method returns `Stream<T>` with the callback's exact output type. Rust
  rejects incompatible callback input types. Handles from another builder,
  duplicate/unknown nodes, wrong arities and mismatched dependencies fail closed.
- `stream.output()` selects a visible output. `Output::zip` selects heterogeneous
  outputs as a tuple. Selection must match the plan's output membership and order.
- `build(output)` freezes the complete executable circuit. Every declared node
  must be bound. Inputs can refer only to previously constructed edges, so the
  schedule is topological. Independent nodes may be bound in either order.

Fan-out shares one immutable `Arc<T>` value, and each operator runs once per tick.
Fan-in, chained joins, multiple source relations, branches and multiple outputs
are supported for the existing source/project/join/aggregate operator families.
Callbacks may await object reads/PUTs, but this initial scheduler executes nodes
sequentially. It retains tick edge values until evaluation completes. It does not
provide recursion, feedback edges, frontiers, parallel scheduling or streaming
physical chunks within a tick. A single scalar or aggregate input may itself
hold a tuple; additional operator families need an explicit plan contract.

## Delta-tick and state contract

`circuit.inputs()` creates the source values for one tick. `insert(&source, value)`
rejects foreign, nonsource and duplicate handles. Every source must be supplied,
even when its delta is empty. Missing inputs are rejected before any node runs.
Wrap the complete input collection in `TimedBatch { time, batch: inputs }`.

Every callback receives `NodeContext<S>` with the same logical time and immutable
committed prior state. Unary/binary inputs carry that same time. A stateful node
returns `NodeOutput::staged(delta, update)`; source state uses `StateUpdate`.
The update only assigns owned values or immutable snapshots to the candidate.
Earlier nodes' staged state is never read as the prior state by later nodes.
For example a chained join reads the prior integrated intermediate relation and
its current delta separately, retaining all simultaneous-input cross terms.

`Circuit::evaluate` validates the prior boundary, evaluates all nodes, collects
outputs, clones the prior snapshot and applies the deferred replacements. All
registered arrangements must then have the incoming tick and exact declared
schemas/membership. It returns the complete candidate and result delta; it never
publishes a root or external SQL effect. `Circuit::engine` wraps this evaluator
in the existing prepare/commit ownership boundary. `engine_at` binds caller-
validated recovered state at its authoritative logical tick.

`S: Clone + State` is an explicit isolation contract: cloning must yield an
independent owned snapshot or share only immutable descriptors. Rust does not
prove this property; shared interior mutation, arbitrary external effects and
mutating a committed trace are forbidden callback behavior. Stateful callbacks
must replace only their declared arrangements, and revisions must identify their
semantics. Schema strings are explicit stable Rust/type/ordering/codec contracts,
not automatically derived proofs of callback correctness. Typed handles use
checked safe type erasure internally; no unsafe code or persistent type erasure
is introduced.

For a supported pure snapshot query Q and a valid, representable input history,
the acceptance invariant at every successful tick is:

```
integrate(output_deltas)[t] = Q(integrate(input_deltas)[t])
```

Tests also check the exact relational output delta against
`Q(DB_after) - Q(DB_before)`. An empty delta tick advances all arrangement clocks
while retaining integrated data. Physical run splits/spills do not define new
ticks. Changing logical batching changes intermediate observations; arbitrary
rebatching is not automatically failure-equivalent under the i64 limits.
Generic batches/joins support signed coefficients. SQL GroupSum requires an
integrated nonnegative bag and has the existing bounded sum/statistics domain.

## Production SQL bridge and durability

`engine::plan::query::Execution` now binds the grouped worker's seven declared
nodes through typed edges and uses this scheduler for bootstrap, CDC and recovery.
The circuit is bound once per execution namespace. Protected uploads get a
separate bound circuit whose immutable writers use the reservation's namespace.
The SQL subset and registered arrangement set remain left/right/sums/output.
The existing serialized plan definition, compiler IR/revision, codecs and
checkpoint format remain identical; the construction refactor preserves their
identity and the query semantics. A changed graph, schema, codec or callback
semantics still requires an incompatible identity and fresh registration.

Object PUTs can precede downstream failure but remain unpublished. PostgreSQL
atomically publishes membership, result deltas and source progress before local
root installation and slot ACK. Uncertain publication requires authoritative
recovery. Compaction/GC remain separate physical operations without a delta tick.

Arbitrary Rust circuit topology does not extend SQL syntax automatically. Custom
state types still explicitly supply arrangement bindings, checkpoint/reopen and
compaction logic; this slice supplies execution scheduling and typed edges, not
an automatic heterogeneous persistent-state codec or a generic SQL sink adapter.

## Evidence and next boundary

The grouped worker's PostgreSQL/memory oracles and crash/restart gate execute the
new scheduler. A separate three-source circuit compares exact output deltas and
integrated results against independent bag recomputation through chained joins,
simultaneous inserts/deletes, duplicate tuples, empty ticks and cold object reads.
Branch tests check shared fan-out, heterogeneous outputs, independent node order,
prior-state isolation and downstream failure/retry. Construction/input tests
cover mismatches; compile-fail doctests cover incompatible edge types and function-pointer
subtyping; they run in the common gate.

The semantic reference is the DBSP paper (§1–§3 and §7), and the local Feldera
checkout `15e74a9dc3d1b1682f7f636bc79aff5461f1e7fd`. Its DBSP input handles accept
weighted batches, and its current logical transaction can span multiple physical
`step` calls. The present one-pass acyclic scheduler does not equate those
physical steps with logical time or claim the full DBSP runtime/compiler.
