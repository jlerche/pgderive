//! Reusable production query composition over registered immutable arrangements.
use super::{Binding, Engine, Plan, State};
use crate::engine::{
    Batch,
    dataflow::{Arrangement, GroupSum, Join, Project, Stream, SumState},
    execution::Limits,
    reader::BatchData,
    trace::TraceSnapshot,
};
use anyhow::{Context, Result};
use object_store::ObjectStore;
use std::sync::Arc;
/// Visible GROUP BY COUNT(*) and nullable SUM row.
pub type AggregateRow = (i64, Option<i64>);
/// Both complete committed source deltas for a typed join query.
pub type Inputs<K, A, B> = (Batch<K, A>, Batch<K, B>);
/// Pure, lifetime-bound project/join/group/measure composition.
pub struct Operators<
    K: BatchData,
    A: BatchData,
    B: BatchData,
    L: BatchData,
    R: BatchData,
    G: BatchData,
    V: BatchData,
> {
    /// Source left projection/filter.
    pub left: Project<K, A, K, L>,
    /// Source right projection/filter.
    pub right: Project<K, B, K, R>,
    /// Join result grouping/projection/filter.
    pub group: Project<K, (L, R), G, V>,
    /// Nullable numeric measure.
    pub sum: GroupSum<G, V>,
}
/// Pinned complete immutable state of a registered grouped join.
pub struct QueryState<K: BatchData, L: BatchData, R: BatchData, G: BatchData> {
    /// Left projected input arrangement.
    pub left: TraceSnapshot<K, L>,
    /// Right projected input arrangement.
    pub right: TraceSnapshot<K, R>,
    /// Grouped sufficient statistics.
    pub sums: TraceSnapshot<G, SumState>,
    /// Integrated COUNT/SUM rows.
    pub output: TraceSnapshot<G, AggregateRow>,
    schemas: [String; 4],
}
impl<K: BatchData, L: BatchData, R: BatchData, G: BatchData> State for QueryState<K, L, R, G> {
    fn bindings(&self) -> Vec<Binding> {
        let times = [self.left.time(), self.right.time(), self.sums.time(), self.output.time()];
        ["left", "right", "sums", "output"]
            .into_iter()
            .zip(&self.schemas)
            .zip(times)
            .map(|((id, schema), time)| Binding { id: id.into(), schema: schema.clone(), time })
            .collect()
    }
}
struct Execution<
    K: BatchData,
    A: BatchData,
    B: BatchData,
    L: BatchData,
    R: BatchData,
    G: BatchData,
    V: BatchData,
> {
    operators: Operators<K, A, B, L, R, G, V>,
    left: Arrangement<K, L>,
    right: Arrangement<K, R>,
    sums: Arrangement<G, SumState>,
    output: Arrangement<G, AggregateRow>,
    schemas: [String; 4],
    limits: Limits,
}
type Output<G> = Batch<G, AggregateRow>;
type SharedExecution<K, A, B, L, R, G, V> = Arc<Execution<K, A, B, L, R, G, V>>;
/// Complete unpublished state/output of a typed grouped join.
pub type Prepared<K, L, R, G> =
    crate::engine::dataflow::PreparedGraph<QueryState<K, L, R, G>, Output<G>>;
type Runtime<K, A, B, L, R, G> = Engine<QueryState<K, L, R, G>, Inputs<K, A, B>, Output<G>>;
/// Production reusable typed project → inner join → grouped COUNT/SUM engine.
/// Registration must declare exactly left/right/sums/output state identities.
pub struct GroupedJoin<
    K: BatchData,
    A: BatchData,
    B: BatchData,
    L: BatchData,
    R: BatchData,
    G: BatchData,
    V: BatchData,
> {
    engine: Runtime<K, A, B, L, R, G>,
    execution: SharedExecution<K, A, B, L, R, G, V>,
}
impl<
    K: BatchData,
    A: BatchData,
    B: BatchData,
    L: BatchData,
    R: BatchData,
    G: BatchData,
    V: BatchData,
> Execution<K, A, B, L, R, G, V>
{
    fn empty(&self) -> QueryState<K, L, R, G> {
        QueryState {
            left: self.left.empty(),
            right: self.right.empty(),
            sums: self.sums.empty(),
            output: self.output.empty(),
            schemas: self.schemas.clone(),
        }
    }
    async fn evaluate(
        &self,
        state: Arc<QueryState<K, L, R, G>>,
        input: Stream<Inputs<K, A, B>>,
    ) -> Result<(QueryState<K, L, R, G>, Output<G>)> {
        let left = self.operators.left.evaluate_with_limits(
            &Stream { time: input.time, batch: input.batch.0 },
            self.limits,
        )?;
        let right = self.operators.right.evaluate_with_limits(
            &Stream { time: input.time, batch: input.batch.1 },
            self.limits,
        )?;
        let joined = Join
            .evaluate_with_limits((&left, &right), (&state.left, &state.right), self.limits)
            .await?;
        let grouped = self.operators.group.evaluate_with_limits(&joined, self.limits)?;
        let delta =
            self.operators.sum.evaluate_with_limits(&grouped, &state.sums, self.limits).await?;
        let output = Project::new(|key: &G, value: &SumState| {
            Ok(Some((key.clone(), (value.rows, (value.non_null != 0).then_some(value.sum)))))
        })
        .evaluate_with_limits(&delta.state, self.limits)?;
        let next = QueryState {
            left: self.left.stage(&state.left, &left).await?,
            right: self.right.stage(&state.right, &right).await?,
            sums: self.sums.stage(&state.sums, &delta.state).await?,
            output: self.output.stage(&state.output, &output).await?,
            schemas: self.schemas.clone(),
        };
        Ok((next, output.batch))
    }
    async fn compact(&self, state: Arc<QueryState<K, L, R, G>>) -> Result<QueryState<K, L, R, G>> {
        Ok(QueryState {
            left: self.left.compact(&state.left).await?,
            right: self.right.compact(&state.right).await?,
            sums: self.sums.compact(&state.sums).await?,
            output: self.output.compact(&state.output).await?,
            schemas: self.schemas.clone(),
        })
    }
}
impl<
    K: BatchData,
    A: BatchData,
    B: BatchData,
    L: BatchData,
    R: BatchData,
    G: BatchData,
    V: BatchData,
> GroupedJoin<K, A, B, L, R, G, V>
{
    /// Bind a registered query to pure typed operators and immutable object storage.
    ///
    /// # Errors
    /// Rejects missing/mismatched registrations or invalid storage configuration.
    pub fn new(
        plan: Plan,
        operators: Operators<K, A, B, L, R, G, V>,
        store: Arc<dyn ObjectStore>,
        block_rows: usize,
    ) -> Result<Self> {
        Self::new_with_limits(plan, operators, store, block_rows, Limits::default())
    }
    /// Bind a query using explicit transaction operator resource limits.
    ///
    /// # Errors
    /// Rejects invalid registrations, storage configuration, or resource budgets.
    pub fn new_with_limits(
        plan: Plan,
        operators: Operators<K, A, B, L, R, G, V>,
        store: Arc<dyn ObjectStore>,
        block_rows: usize,
        limits: Limits,
    ) -> Result<Self> {
        let limits = limits.validate()?;
        validate_shape(&plan)?;
        let schema = |id: &str| {
            plan.definition()
                .arrangements
                .iter()
                .find(|item| item.id == id)
                .map(|item| item.schema.clone())
                .context("missing grouped join arrangement")
        };
        let schemas = [schema("left")?, schema("right")?, schema("sums")?, schema("output")?];
        let execution = Arc::new(Execution {
            operators,
            left: Arrangement::new(store.clone(), schemas[0].clone(), block_rows)?,
            right: Arrangement::new(store.clone(), schemas[1].clone(), block_rows)?,
            sums: Arrangement::new(store.clone(), schemas[2].clone(), block_rows)?,
            output: Arrangement::new(store, schemas[3].clone(), block_rows)?,
            schemas,
            limits,
        });
        let evaluator = execution.clone();
        let engine = Engine::new(plan, execution.empty(), move |state, input| {
            let execution = evaluator.clone();
            async move { execution.evaluate(state, input).await }
        })?;
        Ok(Self { engine, execution })
    }
    /// Registered query identity/schema contract.
    #[must_use]
    pub fn plan(&self) -> &Plan {
        self.engine.plan()
    }
    /// Last committed transaction tick.
    #[must_use]
    pub fn time(&self) -> u64 {
        self.engine.time()
    }
    /// Pin all immutable arrangements at one boundary.
    #[must_use]
    pub fn snapshot(&self) -> Arc<QueryState<K, L, R, G>> {
        self.engine.snapshot()
    }
    /// Stage one complete source transaction.
    ///
    /// # Errors
    /// Returns node, I/O, arithmetic, contract, or tick failures.
    pub async fn prepare(&self, input: Stream<Inputs<K, A, B>>) -> Result<Prepared<K, L, R, G>> {
        self.execution.limits.check_batch(&input.batch.0)?;
        self.execution.limits.check_batch(&input.batch.1)?;
        self.engine.prepare(input).await
    }
    /// Publish prepared state locally; durable PG publication is a later slice.
    ///
    /// # Errors
    /// Rejects stale/foreign preparations.
    pub fn commit(&mut self, prepared: Prepared<K, L, R, G>) -> Result<Stream<Output<G>>> {
        self.engine.commit(prepared)
    }
    /// Replace equivalent physical memberships without advancing logical time.
    ///
    /// # Errors
    /// Returns storage/equivalence/stale errors, retaining the current root.
    pub async fn compact(&mut self) -> Result<()> {
        let execution = self.execution.clone();
        let prepared = self
            .engine
            .maintenance(move |state| async move { execution.compact(state).await })
            .await?;
        self.engine.commit_maintenance(prepared)
    }
}

fn validate_shape(plan: &Plan) -> Result<()> {
    use super::Kind;
    use anyhow::ensure;
    let definition = plan.definition();
    ensure!(
        definition.sources.len() == 2
            && definition.nodes.len() == 7
            && definition.outputs == ["aggregate"],
        "registered graph does not match grouped join executor"
    );
    let find = |id: &str| {
        definition.nodes.iter().find(|node| node.id == id).context("missing grouped join node")
    };
    for (id, kind, inputs) in [
        ("join", Kind::Join, vec!["left", "right"]),
        ("group", Kind::Project, vec!["join"]),
        ("aggregate", Kind::Aggregate, vec!["group"]),
    ] {
        let node = find(id)?;
        ensure!(node.kind == kind && node.inputs == inputs, "grouped join operator mismatch");
    }
    for id in ["left", "right"] {
        let node = find(id)?;
        ensure!(
            node.kind == Kind::Project && find(&node.inputs[0])?.kind == Kind::Source,
            "grouped join source projection mismatch"
        );
        let registration = definition
            .arrangements
            .iter()
            .find(|a| a.id == id)
            .context("missing projected arrangement")?;
        ensure!(
            registration.node == id && registration.schema == node.schema,
            "projection arrangement mismatch"
        );
    }
    ensure!(
        find("left")?.inputs != find("right")?.inputs,
        "grouped join requires two distinct registered sources"
    );
    for id in ["sums", "output"] {
        ensure!(
            definition.arrangements.iter().any(|a| a.id == id && a.node == "aggregate"),
            "aggregate arrangement owner mismatch"
        );
    }
    Ok(())
}
