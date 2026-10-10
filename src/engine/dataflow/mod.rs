//! Explicit acyclic, transaction-ordered edges and an object-backed join/count graph.
//!
//! Preparation uploads immutable objects; only local commit changes graph visibility.
//! This is not a `PostgreSQL` durable publication or a recursive DBSP scheduler.
mod arrangement;
mod builder;
mod circuit;
mod join;
mod operators;
mod partition;
mod project;
mod runtime;
mod statistics;
mod stream;
mod sum;
use super::{
    Batch,
    reader::{BatchData, ObjectBatch},
    trace::{Run, TraceSnapshot},
};
use anyhow::{Context, Result, ensure};
pub use arrangement::Arrangement;
pub use builder::CircuitBuilder;
pub use circuit::{Circuit, NodeContext, NodeOutput, StateUpdate};
pub use join::Join;
use object_store::ObjectStore;
pub use partition::{Partition, PartitionWork};
pub use project::Project;
pub use runtime::{Graph, PreparedGraph, PreparedMaintenance};
pub use statistics::Statistics;
use std::sync::Arc;
pub use stream::{CircuitInputs, Output, Stream};
pub use sum::{GroupSum, SumDelta, SumState};

type Grouper<K, L, R, G> = dyn Fn(&K, &L, &R) -> Result<Option<G>> + Send + Sync;

/// One complete logical delta batch at a tick. Empty batches still carry time.
#[derive(Debug, Clone)]
pub struct TimedBatch<B> {
    /// Logical delta-tick index, distinct from source LSNs.
    pub time: u64,
    /// Finalized batch for this tick.
    pub batch: B,
}

/// Pinned prior-state inputs and grouped output of the fixed graph.
#[derive(Debug, Clone)]
pub struct Snapshot<K: BatchData, L: BatchData, R: BatchData, G: BatchData> {
    /// Left arrangement; navigation key does not imply uniqueness.
    pub left: TraceSnapshot<K, L>,
    /// Right arrangement.
    pub right: TraceSnapshot<K, R>,
    /// Count rows `(group, count)` with unit multiplicity.
    pub counts: TraceSnapshot<G, i64>,
}

/// Unpublished full-graph candidate, tied to its exact prior root.
pub struct Prepared<K: BatchData, L: BatchData, R: BatchData, G: BatchData> {
    base: Arc<Snapshot<K, L, R, G>>,
    next: Arc<Snapshot<K, L, R, G>>,
    output: TimedBatch<Batch<G, i64>>,
}

/// Unpublished equivalent physical replacement of the graph's arrangements.
pub struct PreparedCompaction<K: BatchData, L: BatchData, R: BatchData, G: BatchData> {
    base: Arc<Snapshot<K, L, R, G>>,
    next: Arc<Snapshot<K, L, R, G>>,
}

/// Fixed equijoin → fallible filter/group projection → grouped count graph.
///
/// Prior arrangements are probed through readers. Encoding and candidate validation
/// still buffer/scan full batches; bounded-memory execution is not claimed.
pub struct TraceQuery<K: BatchData, L: BatchData, R: BatchData, G: BatchData> {
    root: Arc<Snapshot<K, L, R, G>>,
    store: Arc<dyn ObjectStore>,
    schema: String,
    block_rows: usize,
    group: Arc<Grouper<K, L, R, G>>,
}

impl<K: BatchData, L: BatchData, R: BatchData, G: BatchData> TraceQuery<K, L, R, G> {
    /// Create an empty graph with an explicit codec/type/ordering schema identity.
    /// The classifier is fixed for this graph and must be pure and deterministic.
    /// Changing classification semantics requires rebuilding the graph.
    ///
    /// # Errors
    /// Rejects an empty schema identity or zero block size.
    pub fn new(
        store: Arc<dyn ObjectStore>,
        schema: String,
        block_rows: usize,
        group: impl Fn(&K, &L, &R) -> Result<Option<G>> + Send + Sync + 'static,
    ) -> Result<Self> {
        ensure!(!schema.is_empty() && block_rows > 0, "invalid graph storage configuration");
        Ok(Self {
            root: Arc::new(Snapshot {
                left: TraceSnapshot::empty(),
                right: TraceSnapshot::empty(),
                counts: TraceSnapshot::empty(),
            }),
            store,
            schema,
            block_rows,
            group: Arc::new(group),
        })
    }
    /// Pin all committed arrangements together.
    #[must_use]
    pub fn snapshot(&self) -> Arc<Snapshot<K, L, R, G>> {
        self.root.clone()
    }
    /// Evaluate against prior inputs, upload deltas, and validate the next root.
    /// The lifetime-bound projection must be deterministic and pure; None filters.
    ///
    /// # Errors
    /// Read, callback, overflow, upload, or tick errors leave visibility unchanged.
    /// Uploaded objects may be orphaned; garbage collection is not implemented.
    pub async fn prepare(
        &self,
        left: &TimedBatch<Batch<K, L>>,
        right: &TimedBatch<Batch<K, R>>,
    ) -> Result<Prepared<K, L, R, G>> {
        let time = self.root.left.time().checked_add(1).context("logical time overflow")?;
        ensure!(left.time == time && right.time == time, "out-of-order graph input tick");
        let joined =
            join::join_batches(&left.batch, &right.batch, &self.root.left, &self.root.right)
                .await?;
        let output = operators::count(&joined, &self.root.counts, &*self.group).await?;
        let next_left = self.append(&self.root.left, &left.batch, time, "left").await?;
        let next_right = self.append(&self.root.right, &right.batch, time, "right").await?;
        let next_counts = self.append(&self.root.counts, &output, time, "counts").await?;
        Ok(Prepared {
            base: self.root.clone(),
            next: Arc::new(Snapshot { left: next_left, right: next_right, counts: next_counts }),
            output: TimedBatch { time, batch: output },
        })
    }
    async fn append<A: BatchData, B: BatchData>(
        &self,
        prior: &TraceSnapshot<A, B>,
        delta: &Batch<A, B>,
        time: u64,
        edge: &str,
    ) -> Result<TraceSnapshot<A, B>> {
        let schema = format!("{}:{edge}", self.schema);
        let reference =
            ObjectBatch::write(self.store.clone(), delta, &schema, self.block_rows).await?;
        let run = ObjectBatch::open(self.store.clone(), reference, &schema).await?;
        prior.append(Run::Object(run), time).await
    }
    /// Publish all arrangements with one local root assignment and return the edge.
    ///
    /// # Errors
    /// Rejects stale/foreign candidates without publishing any arrangement.
    pub fn commit(&mut self, prepared: Prepared<K, L, R, G>) -> Result<TimedBatch<Batch<G, i64>>> {
        ensure!(Arc::ptr_eq(&self.root, &prepared.base), "foreign or stale graph preparation");
        self.root = prepared.next;
        Ok(prepared.output)
    }
    /// Upload equivalent compacted arrangements without advancing logical time.
    ///
    /// # Errors
    /// Read, overflow or upload errors leave the current root unchanged.
    pub async fn prepare_compaction(&self) -> Result<PreparedCompaction<K, L, R, G>> {
        let left = self.compact(&self.root.left, "left").await?;
        let right = self.compact(&self.root.right, "right").await?;
        let counts = self.compact(&self.root.counts, "counts").await?;
        Ok(PreparedCompaction {
            base: self.root.clone(),
            next: Arc::new(Snapshot { left, right, counts }),
        })
    }
    async fn compact<A: BatchData, B: BatchData>(
        &self,
        prior: &TraceSnapshot<A, B>,
        edge: &str,
    ) -> Result<TraceSnapshot<A, B>> {
        Arrangement::<A, B>::new(
            self.store.clone(),
            format!("{}:{edge}", self.schema),
            self.block_rows,
        )?
        .compact(prior)
        .await
    }
    /// Publish equivalent physical membership without producing a logical edge.
    ///
    /// # Errors
    /// Rejects stale/foreign compactions without changing the graph root.
    pub fn commit_compaction(&mut self, prepared: PreparedCompaction<K, L, R, G>) -> Result<()> {
        ensure!(Arc::ptr_eq(&self.root, &prepared.base), "foreign or stale graph compaction");
        self.root = prepared.next;
        Ok(())
    }
}

#[cfg(test)]
mod tests;

#[cfg(test)]
#[path = "tests/operators.rs"]
mod operator_tests;

#[cfg(test)]
#[path = "tests/sum.rs"]
mod sum_tests;

#[cfg(test)]
#[path = "tests/budgets.rs"]
mod budget_tests;

#[cfg(test)]
#[path = "tests/circuit.rs"]
mod circuit_tests;

#[cfg(test)]
#[path = "tests/join_probes.rs"]
mod join_probe_tests;
