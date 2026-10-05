//! Durable single-source typed projection/filter circuit with full-tuple bag output.
use super::{Binding, Checkpoint, Engine, Membership, Plan, State, query::Settings};
use crate::{
    catalog::{Protection, Writer},
    engine::{
        Batch,
        dataflow::{
            Arrangement, Circuit, CircuitBuilder, NodeOutput, PreparedGraph, PreparedMaintenance,
            Project, Stream, TimedBatch,
        },
        reader::{BatchData, BlockCache, CacheStats},
        trace::TraceSnapshot,
    },
};
use anyhow::{Context, Result, ensure};
use std::sync::Arc;
use tokio_postgres::Client;

/// Immutable projected full-tuple bag at one committed logical boundary.
#[derive(Clone)]
pub struct ProjectionState<V: BatchData> {
    /// Integrated output arrangement.
    pub output: TraceSnapshot<(), V>,
    schema: String,
}
impl<V: BatchData> State for ProjectionState<V> {
    fn bindings(&self) -> Vec<Binding> {
        vec![Binding { id: "output".into(), schema: self.schema.clone(), time: self.output.time() }]
    }
}
/// Unpublished projection delta and candidate, bound to the exact runtime root.
pub type Prepared<V> = PreparedGraph<ProjectionState<V>, Batch<(), V>>;
/// Unpublished equivalent physical state, without advancing logical time.
pub type Compaction<V> = PreparedMaintenance<ProjectionState<V>>;
type Runtime<A, V> = Engine<ProjectionState<V>, Batch<(), A>, Batch<(), V>>;
type Executable<V> = Circuit<ProjectionState<V>, Batch<(), V>>;
struct Bound<A: BatchData, V: BatchData> {
    circuit: Arc<Executable<V>>,
    source: Stream<Batch<(), A>>,
}
struct Execution<A: BatchData, V: BatchData> {
    plan: Plan,
    project: Arc<Project<(), A, (), V>>,
    output: Arrangement<(), V>,
    schema: String,
    limits: crate::engine::execution::Limits,
    cache: Arc<BlockCache>,
    bound: Bound<A, V>,
}
/// Reusable production projection circuit with explicit checkpoint and publication boundaries.
pub struct Projection<A: BatchData, V: BatchData> {
    engine: Runtime<A, V>,
    execution: Arc<Execution<A, V>>,
}
impl<A: BatchData, V: BatchData> Execution<A, V> {
    fn build(plan: Plan, project: Project<(), A, (), V>, settings: Settings) -> Result<Arc<Self>> {
        validate_shape(&plan)?;
        let schema = plan.definition().arrangements[0].schema.clone();
        let limits = settings.limits.validate()?;
        let cache = Arc::new(BlockCache::new(limits.cache_bytes, limits.cache_entries));
        let output = Arrangement::new(settings.store, schema.clone(), settings.block_rows)?
            .with_cache(cache.clone());
        let project = Arc::new(project);
        let bound = circuit(&plan, project.clone(), output.clone(), limits)?;
        Ok(Arc::new(Self { plan, project, output, schema, limits, cache, bound }))
    }
    fn scoped(&self, protection: &Protection) -> Result<Bound<A, V>> {
        circuit(
            &self.plan,
            self.project.clone(),
            self.output.clone().with_namespace(protection.namespace()?),
            self.limits,
        )
    }
    fn state(&self, output: TraceSnapshot<(), V>) -> ProjectionState<V> {
        ProjectionState { output, schema: self.schema.clone() }
    }
}
fn circuit<A: BatchData, V: BatchData>(
    plan: &Plan,
    project: Arc<Project<(), A, (), V>>,
    output: Arrangement<(), V>,
    limits: crate::engine::execution::Limits,
) -> Result<Bound<A, V>> {
    let mut builder = CircuitBuilder::new(plan.clone());
    let source = builder.source::<Batch<(), A>>("source")?;
    let result = builder.unary(
        "project",
        &source,
        move |context: crate::engine::dataflow::NodeContext<ProjectionState<V>>, delta| {
            let project = project.clone();
            let output = output.clone();
            async move {
                let projected = project.evaluate_with_limits(&delta, limits)?;
                let next = output.stage(&context.prior.output, &projected).await?;
                Ok(NodeOutput::staged(projected.batch, move |state: &mut ProjectionState<V>| {
                    state.output = next;
                }))
            }
        },
    )?;
    Ok(Bound { circuit: Arc::new(builder.build(result.output())?), source })
}
impl<A: BatchData, V: BatchData> Bound<A, V> {
    async fn evaluate(
        &self,
        prior: Arc<ProjectionState<V>>,
        delta: TimedBatch<Batch<(), A>>,
    ) -> Result<(ProjectionState<V>, Batch<(), V>)> {
        let mut inputs = self.circuit.inputs();
        inputs.insert(&self.source, delta.batch)?;
        self.circuit.evaluate(prior, TimedBatch { time: delta.time, batch: inputs }).await
    }
}
impl<A: BatchData, V: BatchData> Projection<A, V> {
    /// Bind the exact two-node source/project plan to immutable output storage.
    ///
    /// # Errors
    /// Rejects incompatible declarations or invalid storage/resource settings.
    pub fn new(plan: Plan, project: Project<(), A, (), V>, settings: Settings) -> Result<Self> {
        let execution = Execution::build(plan, project, settings)?;
        let initial = execution.state(execution.output.empty());
        Self::bind(execution, initial, 0)
    }
    fn bind(execution: Arc<Execution<A, V>>, state: ProjectionState<V>, time: u64) -> Result<Self> {
        let evaluator = execution.clone();
        let engine = Engine::restore(execution.plan.clone(), state, time, move |state, delta| {
            let execution = evaluator.clone();
            async move { execution.bound.evaluate(state, delta).await }
        })?;
        Ok(Self { engine, execution })
    }
    /// Exact source, graph, output layout, codec and semantic identity.
    #[must_use]
    pub fn plan(&self) -> &Plan {
        self.engine.plan()
    }
    /// Current committed logical tick; never a source LSN.
    #[must_use]
    pub fn time(&self) -> u64 {
        self.engine.time()
    }
    /// Pin the committed immutable output arrangement.
    #[must_use]
    pub fn snapshot(&self) -> Arc<ProjectionState<V>> {
        self.engine.snapshot()
    }
    /// Shared bounded block cache statistics.
    ///
    /// # Errors
    /// Returns cache lock poisoning errors.
    pub fn cache_stats(&self) -> Result<CacheStats> {
        self.execution.cache.stats()
    }
    /// Evaluate a complete synchronized source delta in its protected upload namespace.
    ///
    /// # Errors
    /// Returns reservation, resource, tick, operator, arithmetic or storage failures.
    pub async fn prepare_protected(
        &self,
        delta: TimedBatch<Batch<(), A>>,
        protection: &Protection,
    ) -> Result<Prepared<V>> {
        self.execution.limits.check_batch(&delta.batch)?;
        let bound = self.execution.scoped(protection)?;
        self.engine
            .prepare_using(
                delta,
                move |state, delta| async move { bound.evaluate(state, delta).await },
            )
            .await
    }
    /// Evaluate one source delta without publishing local or durable visibility.
    ///
    /// # Errors
    /// Returns resource, tick, operator, arithmetic or storage failures.
    pub async fn prepare(&self, delta: TimedBatch<Batch<(), A>>) -> Result<Prepared<V>> {
        self.execution.limits.check_batch(&delta.batch)?;
        self.engine.prepare(delta).await
    }
    /// Install a prepared candidate only after authoritative publication when durable.
    ///
    /// # Errors
    /// Rejects stale or foreign candidates.
    pub fn commit(&mut self, prepared: Prepared<V>) -> Result<TimedBatch<Batch<(), V>>> {
        self.engine.commit(prepared)
    }
    /// Export current complete object membership; this does not authorize ACK.
    ///
    /// # Errors
    /// Rejects invalid membership, codecs or clocks.
    pub fn checkpoint(&self) -> Result<Checkpoint> {
        self.encode(&self.snapshot(), self.time())
    }
    fn encode(&self, state: &ProjectionState<V>, time: u64) -> Result<Checkpoint> {
        let checkpoint = Checkpoint {
            version: 1,
            plan_identity: self.plan().identity().into(),
            time,
            arrangements: vec![Membership {
                id: "output".into(),
                schema: state.schema.clone(),
                trace: state.output.manifest()?,
            }],
        };
        checkpoint.validate(self.plan())?;
        Ok(checkpoint)
    }
    /// Validate exact ownership before exposing unpublished object membership.
    ///
    /// # Errors
    /// Rejects stale/foreign candidates or invalid checkpoints.
    pub fn prepared_checkpoint(&self, prepared: &Prepared<V>) -> Result<Checkpoint> {
        self.engine.validate_prepared(prepared)?;
        self.encode(&prepared.candidate(), prepared.output().time)
    }
    /// Cold-validate authoritative objects and replace local visibility.
    ///
    /// # Errors
    /// Rejects incompatible or older state, corruption, missing objects or bad codecs.
    pub async fn restore_checkpoint(&mut self, checkpoint: Checkpoint) -> Result<()> {
        checkpoint.validate(self.plan())?;
        ensure!(checkpoint.time >= self.time(), "recovery cannot roll back local time");
        let member = checkpoint.arrangements.first().context("missing projection membership")?;
        let output = self.execution.output.reopen(member.trace.clone()).await?;
        let restored =
            Self::bind(self.execution.clone(), self.execution.state(output), checkpoint.time)?;
        self.engine = restored.engine;
        Ok(())
    }
    /// Stage equivalent immutable output compaction under an upload reservation.
    ///
    /// # Errors
    /// Returns reservation, storage or equivalence failures.
    pub async fn prepare_compaction_protected(
        &self,
        protection: &Protection,
    ) -> Result<Compaction<V>> {
        let writer = self.execution.output.clone().with_namespace(protection.namespace()?);
        let schema = self.execution.schema.clone();
        self.engine
            .maintenance(move |state| async move {
                Ok(ProjectionState { output: writer.compact(&state.output).await?, schema })
            })
            .await
    }
    /// Validate ownership and encode unpublished physical membership at the same tick.
    ///
    /// # Errors
    /// Rejects stale/foreign maintenance or invalid metadata.
    pub fn prepared_compaction_checkpoint(&self, prepared: &Compaction<V>) -> Result<Checkpoint> {
        self.engine.validate_maintenance(prepared)?;
        self.encode(&prepared.candidate(), self.time())
    }
    /// Publish equivalent object membership before changing the local root.
    ///
    /// # Errors
    /// Returns stale/foreign, fencing or PG publication errors; uncertain COMMIT needs recovery.
    pub async fn publish_compaction(
        &mut self,
        sql: &mut Client,
        writer: &mut Writer,
        prepared: Compaction<V>,
    ) -> Result<()> {
        self.engine.validate_maintenance(&prepared)?;
        let durable = writer.confirmed(sql, self.plan()).await?;
        ensure!(
            durable.stored.checkpoint == self.checkpoint()?,
            "compaction base differs from durable membership"
        );
        let checkpoint = self.prepared_compaction_checkpoint(&prepared)?;
        writer.maintain(sql, self.plan(), &checkpoint).await?;
        self.engine.commit_maintenance(prepared)
    }
}
fn validate_shape(plan: &Plan) -> Result<()> {
    use super::Kind;
    let definition = plan.definition();
    ensure!(
        definition.sources.len() == 1
            && definition.nodes.len() == 2
            && definition.arrangements.len() == 1
            && definition.outputs == ["project"],
        "registered graph does not match projection executor"
    );
    let source = &definition.nodes[0];
    let project = &definition.nodes[1];
    let output = &definition.arrangements[0];
    ensure!(
        source.id == "source"
            && source.kind == Kind::Source
            && project.id == "project"
            && project.kind == Kind::Project
            && project.inputs == ["source"]
            && output.id == "output"
            && output.node == "project"
            && output.schema == project.schema,
        "projection declarations mismatch"
    );
    Ok(())
}
