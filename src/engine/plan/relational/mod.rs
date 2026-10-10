//! Durable acyclic source/project/join circuits with explicitly registered state.
mod circuit;
mod persistence;
use super::{Binding, Engine, Plan, State, query::Settings};
use crate::{
    catalog::Protection,
    engine::{
        Batch,
        dataflow::{
            Arrangement, Partition, PreparedGraph, PreparedMaintenance, Project, Statistics,
            TimedBatch,
        },
        reader::{BatchData, BlockCache, CacheStats},
        trace::TraceSnapshot,
    },
};
use anyhow::{Context, Result, ensure};
use std::{collections::BTreeMap, sync::Arc};

type Linear<K, V> = Arc<Statistics<K, V>>;
type Group<K, V> = Arc<Partition<K, V>>;
type Unary<K, V> = Arc<Project<K, V, K, V>>;
type Binary<K, V> = Arc<Project<K, (V, V), K, V>>;
/// Pure callbacks implementing the exact declared project and join semantics.
/// Callback changes require a new semantic plan revision.
pub struct Operators<K: BatchData, V: BatchData> {
    /// Unary filter/map callbacks, keyed by declared node identity.
    pub projects: BTreeMap<String, Unary<K, V>>,
    /// Projection of each complete joined tuple pair, keyed by join identity.
    pub joins: BTreeMap<String, Binary<K, V>>,
    /// Affected-partition callbacks keyed by declared aggregate node identity.
    pub partitions: BTreeMap<String, Group<K, V>>,
    /// Exact additive statistics callbacks, keyed by their retained state node.
    pub statistics: BTreeMap<String, Linear<K, V>>,
}
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
        let cache = Arc::new(BlockCache::new(limits.cache_bytes, limits.cache_entries));
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
        let bound = Arc::new(execution.build_circuit(None)?);
        let engine = Engine::restore(execution.plan.clone(), state, time, move |state, input| {
            let bound = bound.clone();
            async move { bound.evaluate(state, input).await }
        })?;
        Ok(Self { engine, execution })
    }
    /// Exact declaration, native layout, callback and codec identity.
    #[must_use]
    pub fn plan(&self) -> &Plan {
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
        let bound = self.execution.build_circuit(Some(protection))?;
        self.engine
            .prepare_using(
                input,
                move |state, input| async move { bound.evaluate(state, input).await },
            )
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
