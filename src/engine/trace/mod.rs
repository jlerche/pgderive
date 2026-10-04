//! Immutable trace generations and in-memory prepare/commit visibility.
mod merged;
use super::{
    Batch, BatchBuilder,
    reader::{BatchData, BatchReader, Cursor, ObjectBatch},
};
use anyhow::{Context, Result, ensure};
pub use merged::MergedCursor;
use std::sync::Arc;

/// One immutable run, in memory or in object storage.
#[derive(Debug, Clone)]
pub enum Run<K: BatchData, V: BatchData> {
    /// Canonical memory batch.
    Memory(Arc<Batch<K, V>>),
    /// Open object batch with resident fences.
    Object(ObjectBatch<K, V>),
}

impl<K: BatchData, V: BatchData> BatchReader for Run<K, V> {
    type Key = K;
    type Val = V;
    type Cursor = Cursor<K, V>;
    async fn cursor(&self) -> Result<Self::Cursor> {
        match self {
            Self::Memory(batch) => batch.cursor().await,
            Self::Object(batch) => batch.cursor().await,
        }
    }
}

/// Pinned immutable manifest. Clones retain every referenced run for the reader.
#[derive(Debug, Clone)]
pub struct TraceSnapshot<K: BatchData, V: BatchData> {
    pub(super) runs: Vec<Run<K, V>>,
    generation: u64,
    time: u64,
}

impl<K: BatchData, V: BatchData> TraceSnapshot<K, V> {
    pub(super) const fn empty() -> Self {
        Self { runs: Vec::new(), generation: 0, time: 0 }
    }
    pub(super) async fn append(&self, run: Run<K, V>, time: u64) -> Result<Self> {
        ensure!(self.time.checked_add(1) == Some(time), "out-of-order trace tick");
        let generation = self.generation.checked_add(1).context("trace generation overflow")?;
        let mut runs = self.runs.clone();
        runs.push(run);
        let next = Self { runs, generation, time };
        next.validate().await?;
        Ok(next)
    }
    pub(super) async fn replace(&self, run: Run<K, V>) -> Result<Self> {
        let generation = self.generation.checked_add(1).context("trace generation overflow")?;
        let next = Self { runs: vec![run], generation, time: self.time };
        ensure!(
            next.materialize().await? == self.materialize().await?,
            "compaction changed logical state"
        );
        Ok(next)
    }
    async fn validate(&self) -> Result<()> {
        let mut cursor = self.cursor().await?;
        while cursor.current().is_some() {
            cursor.advance().await?;
        }
        Ok(())
    }
    /// Physical membership generation; compaction may change it without a tick.
    #[must_use]
    pub const fn generation(&self) -> u64 {
        self.generation
    }
    /// Last committed logical tick, independent of source LSNs.
    #[must_use]
    pub const fn time(&self) -> u64 {
        self.time
    }
    /// Materialize the complete logical state for validation/reference use.
    ///
    /// # Errors
    /// Returns read, integrity, or final coefficient overflow errors.
    pub async fn materialize(&self) -> Result<Batch<K, V>> {
        let mut cursor = self.cursor().await?;
        let mut builder = BatchBuilder::default();
        while let Some((key, value, weight)) = cursor.current() {
            builder.push(key.clone(), value.clone(), weight);
            cursor.advance().await?;
        }
        builder.finish()
    }
}

impl<K: BatchData, V: BatchData> BatchReader for TraceSnapshot<K, V> {
    type Key = K;
    type Val = V;
    type Cursor = MergedCursor<K, V>;
    async fn cursor(&self) -> Result<Self::Cursor> {
        MergedCursor::open(&self.runs).await
    }
}

/// Unpublished candidate trace; uploads referenced here have no commit authority.
pub struct PreparedTrace<K: BatchData, V: BatchData> {
    owner: Arc<()>,
    base: u64,
    next: TraceSnapshot<K, V>,
}

/// Mutable visibility pointer over immutable runs, with local generation checks.
///
/// This is an in-memory contract. `PostgreSQL` publication/recovery is not yet
/// implemented. No objects are deleted; physical GC must respect pinned readers.
#[derive(Debug)]
pub struct Trace<K: BatchData, V: BatchData> {
    owner: Arc<()>,
    snapshot: TraceSnapshot<K, V>,
}

impl<K: BatchData, V: BatchData> Default for Trace<K, V> {
    fn default() -> Self {
        Self {
            owner: Arc::new(()),
            snapshot: TraceSnapshot { runs: Vec::new(), generation: 0, time: 0 },
        }
    }
}

impl<K: BatchData, V: BatchData> Trace<K, V> {
    /// Pin the currently committed run membership.
    #[must_use]
    pub fn snapshot(&self) -> TraceSnapshot<K, V> {
        self.snapshot.clone()
    }
    /// Prepare one next-tick delta and validate the resulting full weighted state.
    ///
    /// # Errors
    /// Returns read/integrity/overflow errors or an out-of-order logical tick.
    pub async fn prepare(&self, delta: Run<K, V>, time: u64) -> Result<PreparedTrace<K, V>> {
        self.prepare_runs(vec![delta], time).await
    }
    /// Prepare one logical delta split into multiple physical runs atomically.
    /// Both its consolidated coefficients and the resulting state must fit i64.
    ///
    /// # Errors
    /// Returns read/integrity/overflow errors or an out-of-order logical tick.
    pub async fn prepare_runs(
        &self,
        deltas: Vec<Run<K, V>>,
        time: u64,
    ) -> Result<PreparedTrace<K, V>> {
        ensure!(self.snapshot.time.checked_add(1) == Some(time), "out-of-order trace tick");
        TraceSnapshot { runs: deltas.clone(), generation: 0, time }.validate().await?;
        let mut runs = self.snapshot.runs.clone();
        runs.extend(deltas);
        self.candidate(runs, time).await
    }
    /// Prepare a physically different but logically identical membership set.
    ///
    /// # Errors
    /// Returns read/overflow errors or unequal replacement weighted state.
    pub async fn prepare_compaction(&self, runs: Vec<Run<K, V>>) -> Result<PreparedTrace<K, V>> {
        let candidate = self.candidate(runs, self.snapshot.time).await?;
        ensure!(
            candidate.next.materialize().await? == self.snapshot.materialize().await?,
            "compaction changed logical state"
        );
        Ok(candidate)
    }
    async fn candidate(&self, runs: Vec<Run<K, V>>, time: u64) -> Result<PreparedTrace<K, V>> {
        let generation =
            self.snapshot.generation.checked_add(1).context("trace generation overflow")?;
        let next = TraceSnapshot { runs, generation, time };
        next.validate().await?;
        Ok(PreparedTrace { owner: self.owner.clone(), base: self.snapshot.generation, next })
    }
    /// Publish a locally prepared generation after validating its origin/base.
    ///
    /// # Errors
    /// Returns foreign/stale preparation errors without changing visibility.
    pub fn commit(&mut self, prepared: PreparedTrace<K, V>) -> Result<()> {
        ensure!(
            Arc::ptr_eq(&self.owner, &prepared.owner) && prepared.base == self.snapshot.generation,
            "foreign or stale trace preparation"
        );
        self.snapshot = prepared.next;
        Ok(())
    }
}

#[cfg(test)]
mod tests;
