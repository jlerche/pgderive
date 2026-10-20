//! Durable acyclic source/project/join circuits with explicitly registered state.
mod circuit;
mod persistence;
use super::{Binding, Engine, Plan, Settings, State};
use crate::{
    catalog::Protection,
    engine::{
        Batch,
        dataflow::{
            Arrangement, Expand, Group, OrderedSelection, Partition, PreparedGraph,
            PreparedMaintenance, Project, Statistics, TimedBatch,
        },
        reader::{BatchData, BlockCache, CacheStats},
        trace::TraceSnapshot,
    },
};
use anyhow::{Context, Result, ensure};
use std::{collections::BTreeMap, sync::Arc};

type JoinProject<K, V> = Project<K, (V, V), K, V>;

/// One callback with an explicit executable operator family.
#[derive(Clone)]
pub enum Operator<K: BatchData, V: BatchData> {
    /// Stateless unary projection/filter.
    Project(Project<K, V, K, V>),
    /// Stateless expansion.
    Expand(Expand<K, V, K, V>),
    /// Projection of the complete joined pair.
    Join(JoinProject<K, V>),
    /// Affected-partition transformation.
    Partition(Partition<K, V>),
    /// Direct cursor-based affected-group differentiation.
    Group(Group<K, V>),
    /// Exact retained linear statistics.
    Statistics(Statistics<K, V>),
    /// Retained ordered selection.
    Ordered(OrderedSelection<K, V>),
}
impl<K: BatchData, V: BatchData> Operator<K, V> {
    const fn kind(&self) -> super::Kind {
        match self {
            Self::Project(_) => super::Kind::Project,
            Self::Expand(_) => super::Kind::Expand,
            Self::Join(_) => super::Kind::Join,
            Self::Partition(_) | Self::Group(_) => super::Kind::Aggregate,
            Self::Statistics(_) => super::Kind::Statistics,
            Self::Ordered(_) => super::Kind::Ordered,
        }
    }
}
/// Exact callback registry keyed by the declared node identity.
pub type Operators<K, V> = BTreeMap<String, Operator<K, V>>;
#[derive(Clone)]
struct Entry<K: BatchData, V: BatchData> {
    schema: String,
    trace: TraceSnapshot<K, V>,
}
/// All registered immutable arrangements at one synchronized logical boundary.
#[derive(Clone)]
pub struct RelationalState<K: BatchData, V: BatchData> {
    entries: BTreeMap<String, Entry<K, V>>,
}
impl<K: BatchData, V: BatchData> RelationalState<K, V> {
    /// Pin a registered arrangement by its stable name.
    ///
    /// # Errors
    /// Rejects an unknown arrangement name.
    pub fn trace(&self, id: &str) -> Result<&TraceSnapshot<K, V>> {
        self.entries.get(id).map(|entry| &entry.trace).context("unknown arrangement")
    }
}
impl<K: BatchData, V: BatchData> State for RelationalState<K, V> {
    fn bindings(&self) -> Vec<Binding> {
        self.entries
            .iter()
            .map(|(id, entry)| Binding {
                id: id.clone(),
                schema: entry.schema.clone(),
                time: entry.trace.time(),
            })
            .collect()
    }
}
/// Unpublished complete logical candidate and visible weighted delta.
pub type Prepared<K, V> = PreparedGraph<RelationalState<K, V>, Batch<K, V>>;
/// Unpublished equivalent physical state at the same logical boundary.
pub type Compaction<K, V> = PreparedMaintenance<RelationalState<K, V>>;
type Runtime<K, V> = Engine<RelationalState<K, V>, Vec<Batch<K, V>>, Batch<K, V>>;
struct Execution<K: BatchData, V: BatchData> {
    plan: Plan,
    operators: Arc<Operators<K, V>>,
    writers: BTreeMap<String, Arrangement<K, V>>,
    limits: crate::engine::execution::Limits,
    cache: Arc<BlockCache>,
}
/// Reusable durable circuit; source deltas follow declared source-node order.
/// Every source is supplied on every tick, including empty deltas.
pub struct Relational<K: BatchData, V: BatchData> {
    engine: Runtime<K, V>,
    execution: Arc<Execution<K, V>>,
}
impl<K: BatchData, V: BatchData> Relational<K, V> {
    /// Bind validated declarations to executable callbacks and immutable storage.
    ///
    /// # Errors
    /// Rejects unsupported shapes, missing callbacks/state, and invalid resources.
    pub fn new(plan: Plan, operators: Operators<K, V>, settings: &Settings) -> Result<Self> {
        circuit::validate(&plan, &operators)?;
        let limits = settings.limits.validate()?;
        let cache = Arc::new(
            BlockCache::new(limits.cache_bytes, limits.cache_entries)
                .with_encoded(settings.encoded_cache.clone()),
        );
        let writers = plan
            .definition()
            .arrangements
            .iter()
            .map(|member| {
                Ok((
                    member.id.clone(),
                    Arrangement::new(
                        settings.store.clone(),
                        member.schema.clone(),
                        settings.block_rows,
                    )?
                    .with_cache(cache.clone()),
                ))
            })
            .collect::<Result<BTreeMap<_, _>>>()?;
        let entries = plan
            .definition()
            .arrangements
            .iter()
            .map(|member| {
                let writer = writers.get(&member.id).context("missing relational writer")?;
                Ok((
                    member.id.clone(),
                    Entry { schema: member.schema.clone(), trace: writer.empty() },
                ))
            })
            .collect::<Result<BTreeMap<_, _>>>()?;
        let execution =
            Arc::new(Execution { plan, operators: Arc::new(operators), writers, limits, cache });
        Self::bind(execution, RelationalState { entries }, 0)
    }
    fn bind(
        execution: Arc<Execution<K, V>>,
        state: RelationalState<K, V>,
        time: u64,
    ) -> Result<Self> {
        let evaluator = execution.clone();
        let engine = Engine::restore(execution.plan.clone(), state, time, move |state, input| {
            let evaluator = evaluator.clone();
            async move { evaluator.evaluate(state, input, None).await }
        })?;
        Ok(Self { engine, execution })
    }
    /// Exact declaration, native layout, callback and codec identity.
    #[must_use]
    pub const fn plan(&self) -> &Plan {
        self.engine.plan()
    }
    /// Last committed logical tick, separate from source LSNs.
    #[must_use]
    pub fn time(&self) -> u64 {
        self.engine.time()
    }
    /// Maximum live run count across all maintained arrangements.
    #[must_use]
    pub fn run_count(&self) -> usize {
        self.engine
            .snapshot()
            .entries
            .values()
            .map(|entry| entry.trace.run_count())
            .max()
            .unwrap_or(0)
    }
    /// Resident cache usage and physical read counters.
    ///
    /// # Errors
    /// Returns cache lock errors.
    pub fn cache_stats(&self) -> Result<CacheStats> {
        self.execution.cache.stats()
    }
    fn check_inputs(&self, input: &[Batch<K, V>]) -> Result<()> {
        ensure!(
            input.len() == self.plan().definition().sources.len(),
            "incomplete relational source tick"
        );
        for batch in input {
            self.execution.limits.check_batch(batch)?;
        }
        Ok(())
    }
    /// Pin all committed arrangements at one boundary.
    #[must_use]
    pub fn snapshot(&self) -> Arc<RelationalState<K, V>> {
        self.engine.snapshot()
    }
    /// Stage one synchronized delta tick without changing visibility.
    ///
    /// # Errors
    /// Returns input, tick, resource, arithmetic, callback or object errors.
    pub async fn prepare(&self, input: TimedBatch<Vec<Batch<K, V>>>) -> Result<Prepared<K, V>> {
        self.check_inputs(&input.batch)?;
        self.engine.prepare(input).await
    }
    /// Stage immutable objects under a live managed upload protection.
    ///
    /// # Errors
    /// Returns reservation, input, tick, operator or storage errors.
    pub async fn prepare_protected(
        &self,
        input: TimedBatch<Vec<Batch<K, V>>>,
        protection: &Protection,
    ) -> Result<Prepared<K, V>> {
        self.check_inputs(&input.batch)?;
        let execution = self.execution.clone();
        let namespace = protection.namespace()?;
        self.engine
            .prepare_using(input, move |state, input| async move {
                execution.evaluate(state, input, Some(namespace)).await
            })
            .await
    }
    /// Install a candidate after authoritative publication when used durably.
    ///
    /// # Errors
    /// Rejects foreign or stale candidates.
    pub fn commit(&mut self, work: Prepared<K, V>) -> Result<TimedBatch<Batch<K, V>>> {
        self.engine.commit(work)
    }
}

#[cfg(test)]
mod tests;

#[cfg(test)]
mod partition_tests;
