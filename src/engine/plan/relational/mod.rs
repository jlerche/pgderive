//! Durable acyclic source/project/join circuits with explicitly registered state.
mod circuit;
mod flush;
mod persistence;
mod streamed;
use super::{Binding, Engine, Plan, Settings, State};
use crate::{
    catalog::Protection,
    engine::{
        Batch,
        dataflow::{
            Arrangement, Binary, Expand, Group, IndexedStatistics, OrderedSelection, Partition,
            PreparedGraph, PreparedMaintenance, Project, Statistics, TimedBatch,
        },
        reader::{BatchData, BlockCache, CacheStats},
        trace::TraceSnapshot,
    },
};
use anyhow::{Context, Result, ensure};
pub use flush::{FrozenFlush, UploadedFlush};
use std::{
    collections::{BTreeMap, BTreeSet},
    sync::Arc,
};

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
    /// Fused retained index summaries.
    IndexedStatistics(IndexedStatistics<K, V>),
    /// Direct indexed window differentiation.
    Window(Binary<K, V>),
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
            Self::Statistics(_) | Self::IndexedStatistics(_) => super::Kind::Statistics,
            Self::Window(_) => super::Kind::Window,
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
type Runtime<K, V> =
    Engine<RelationalState<K, V>, Vec<crate::engine::execution::Sealed<K, V>>, Batch<K, V>>;
struct Execution<K: BatchData, V: BatchData> {
    plan: Plan,
    operators: Arc<Operators<K, V>>,
    writers: BTreeMap<String, Arrangement<K, V>>,
    limits: crate::engine::execution::Limits,
    cache: Arc<BlockCache>,
    source_statistics: BTreeSet<String>,
}
#[derive(Debug)]
pub(crate) struct MemtableFull;
impl std::fmt::Display for MemtableFull {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str("memtable byte limit exceeded; checkpoint required")
    }
}
impl std::error::Error for MemtableFull {}
/// Reusable durable circuit; source deltas follow declared source-node order.
/// Every source is supplied on every tick, including empty deltas.
pub struct Relational<K: BatchData, V: BatchData> {
    engine: Runtime<K, V>,
    incarnation: Arc<()>,
    execution: Arc<Execution<K, V>>,
}
impl<K: BatchData, V: BatchData> Relational<K, V> {
    /// Bind validated declarations to executable callbacks and immutable storage.
    ///
    /// # Errors
    /// Rejects unsupported shapes, missing callbacks/state, and invalid resources.
    pub fn new(plan: Plan, operators: Operators<K, V>, settings: &Settings) -> Result<Self> {
        Self::from_source_keys(plan, operators, settings, BTreeSet::new())
    }
    /// Internal capability boundary: the worker compiler proves complete source
    /// PK preservation from an imported snapshot and committed ordered full CDC.
    /// Arbitrary weighted batches/callbacks must use the strict public constructor.
    pub(crate) fn from_source_keys(
        plan: Plan,
        operators: Operators<K, V>,
        settings: &Settings,
        source_statistics: BTreeSet<String>,
    ) -> Result<Self> {
        circuit::validate(&plan, &operators, &source_statistics)?;
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
        let execution = Arc::new(Execution {
            plan,
            operators: Arc::new(operators),
            writers,
            limits,
            cache,
            source_statistics,
        });
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
            async move { evaluator.evaluate(state, input, circuit::Staging::Objects(None)).await }
        })?;
        Ok(Self { engine, execution, incarnation: Arc::new(()) })
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
    pub(crate) fn memtable_bytes(&self) -> Result<usize> {
        self.snapshot().entries.values().try_fold(0_usize, |bytes, entry| {
            bytes.checked_add(entry.trace.memory_charge()).context("memtable charge overflow")
        })
    }
    pub(crate) fn unsealed_run_count(&self) -> usize {
        self.snapshot()
            .entries
            .values()
            .map(|entry| entry.trace.unsealed_run_count())
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
    fn check_inputs(&self, input: &[crate::engine::execution::Sealed<K, V>]) -> Result<()> {
        ensure!(
            input.len() == self.plan().definition().sources.len(),
            "incomplete relational source tick"
        );
        for batch in input {
            if let Some(batch) = batch.resident() {
                self.execution.limits.check_batch(batch)?;
            }
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
    pub async fn prepare<I: Into<crate::engine::execution::Sealed<K, V>>>(
        &self,
        input: TimedBatch<Vec<I>>,
    ) -> Result<Prepared<K, V>> {
        let input = TimedBatch {
            time: input.time,
            batch: input.batch.into_iter().map(Into::into).collect::<Vec<_>>(),
        };
        self.check_inputs(&input.batch)?;
        let retained = input.batch.clone();
        let result = self.engine.prepare(input).await;
        Self::preserve_failed_input(&retained, result)
    }
    /// Stage immutable objects under a live managed upload protection.
    ///
    /// # Errors
    /// Returns reservation, input, tick, operator or storage errors.
    pub async fn prepare_protected<I: Into<crate::engine::execution::Sealed<K, V>>>(
        &self,
        input: TimedBatch<Vec<I>>,
        protection: &Protection,
    ) -> Result<Prepared<K, V>> {
        let input = TimedBatch {
            time: input.time,
            batch: input.batch.into_iter().map(Into::into).collect::<Vec<_>>(),
        };
        self.check_inputs(&input.batch)?;
        let execution = self.execution.clone();
        let namespace = protection.namespace()?;
        let retained = input.batch.clone();
        let result = self
            .engine
            .prepare_using(input, move |state, input| async move {
                execution.evaluate(state, input, circuit::Staging::Objects(Some(namespace))).await
            })
            .await;
        Self::preserve_failed_input(&retained, result)
    }
    /// Prepare a transaction in memory; retained source WAL supplies crash replay.
    ///
    /// # Errors
    /// Returns input, operator or resource limit failures without changing visibility.
    pub(crate) async fn prepare_memtable<I: Into<crate::engine::execution::Sealed<K, V>>>(
        &self,
        input: TimedBatch<Vec<I>>,
        byte_limit: usize,
    ) -> Result<Prepared<K, V>> {
        self.prepare_memtable_profiled(input, byte_limit, &mut crate::timing::Profile::new(false))
            .await
    }
    pub(crate) async fn prepare_memtable_profiled<
        I: Into<crate::engine::execution::Sealed<K, V>>,
    >(
        &self,
        input: TimedBatch<Vec<I>>,
        byte_limit: usize,
        profile: &mut crate::timing::Profile,
    ) -> Result<Prepared<K, V>> {
        let input = TimedBatch {
            time: input.time,
            batch: input.batch.into_iter().map(Into::into).collect::<Vec<_>>(),
        };
        self.check_inputs(&input.batch)?;
        let execution = self.execution.clone();
        profile.mark("input_validation");
        let retained = input.batch.clone();
        let result = self
            .engine
            .prepare_using(input, move |state, input| async move {
                let (candidate, output) = execution
                    .evaluate_profiled(state, input, circuit::Staging::Memory, profile)
                    .await?;
                let mut bytes = 0_usize;
                for entry in candidate.entries.values() {
                    bytes = bytes
                        .checked_add(entry.trace.memory_charge())
                        .context("memtable charge overflow")?;
                    ensure!(bytes <= byte_limit, MemtableFull);
                }
                profile.mark("memory_accounting");
                Ok((candidate, output))
            })
            .await;
        Self::preserve_failed_input(&retained, result)
    }
    fn preserve_failed_input(
        input: &[crate::engine::execution::Sealed<K, V>],
        result: Result<Prepared<K, V>>,
    ) -> Result<Prepared<K, V>> {
        if result.as_ref().is_err_and(|error| !error.is::<MemtableFull>()) {
            for batch in input {
                batch.retain_failure();
            }
        }
        result
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
