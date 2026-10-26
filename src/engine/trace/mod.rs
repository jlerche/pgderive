//! Immutable trace generations, weighted merging and pinned membership.
mod cache;
mod coefficient;
mod flush;
mod manifest;
mod memory;
mod merged;
mod overlap;
mod probes;
mod rebase;
mod tiers;
mod validation;
use super::{
    Batch, BatchBuilder,
    reader::{BatchData, BatchReader, Cursor, ObjectBatch},
};
use anyhow::{Context, Result, ensure};
pub use manifest::Manifest;
pub use merged::{KeyCursor, MergedCursor, RangeCursor};
pub use probes::KeyProbes;
use std::sync::Arc;
pub(crate) use validation::ValidatedBag;

// Bound merge fan-in across durable roots and immutable memory chunks.
// Normal memory fan-in grows logarithmically; unmergeable coefficient subsets
// still consume this explicit budget. Individual uploads remain capped at 128
// roots; independent consolidation prevents accumulated roots exhausting this bound.
// 128 durable base roots + 128 new checkpoint roots + 120 memory suffix runs
// = 376. Reserve eight additional heads before forcing foreground drain.
pub(crate) const MAX_RUNS: usize = 384;
pub(crate) const CONSOLIDATION_ROOTS: usize = 112;

/// One immutable run, in memory or in object storage.
#[derive(Debug, Clone)]
pub enum Run<K: BatchData, V: BatchData> {
    /// Canonical memory batch.
    Memory(Arc<Batch<K, V>>),
    /// Open object batch with resident fences.
    Object(ObjectBatch<K, V>),
}

impl<K: BatchData, V: BatchData> Run<K, V> {
    async fn cursor_for_range(&self, lower: &K, upper: &K) -> Result<Option<Cursor<K, V>>> {
        match self {
            Self::Object(batch) => Cursor::object_range(batch.clone(), lower, upper).await,
            Self::Memory(batch) => {
                let mut cursor = batch.cursor().await?;
                cursor.seek_key(lower).await?;
                cursor.restrict_to_upper(upper);
                Ok(cursor.current().is_some().then_some(cursor))
            }
        }
    }
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
    runs: Vec<Run<K, V>>,
    logical_cache: Option<cache::State>,
    spans: Vec<Option<memory::Span>>,
    // Consecutive object-root counts per logical upload; memory runs are excluded.
    cohorts: Vec<usize>,
    sealed_time: u64,
    memory_bytes: usize,
    generation: u64,
    time: u64,
}

impl<K: BatchData, V: BatchData> TraceSnapshot<K, V> {
    pub(super) const fn empty() -> Self {
        Self {
            runs: Vec::new(),
            logical_cache: None,
            spans: Vec::new(),
            cohorts: Vec::new(),
            sealed_time: 0,
            memory_bytes: 0,
            generation: 0,
            time: 0,
        }
    }
    pub(crate) async fn replace_runs(&self, runs: Vec<Run<K, V>>) -> Result<Self> {
        let generation = self.generation.checked_add(1).context("trace generation overflow")?;
        let mut next = Self::from_runs(runs, generation, self.time)?;
        ensure!(self.equivalent(&next).await?, "compaction changed logical state");
        next.logical_cache.clone_from(&self.logical_cache);
        Ok(next)
    }
    async fn equivalent(&self, other: &Self) -> Result<bool> {
        let mut left = self.cursor().await?;
        let mut right = other.cursor().await?;
        loop {
            match (left.current(), right.current()) {
                (None, None) => return Ok(true),
                (Some(a), Some(b)) if a == b => {
                    left.advance().await?;
                    right.advance().await?;
                }
                _ => return Ok(false),
            }
        }
    }
    fn from_runs(runs: Vec<Run<K, V>>, generation: u64, time: u64) -> Result<Self> {
        let memory_bytes = runs.iter().try_fold(0_usize, |bytes, run| {
            bytes.checked_add(memory_run_charge(run)?).context("memtable charge overflow")
        })?;
        let spans = vec![None; runs.len()];
        let objects = runs.iter().filter(|run| matches!(run, Run::Object(_))).count();
        let cohorts = if objects == 0 { Vec::new() } else { vec![objects] };
        let logical_cache = runs.iter().find_map(|run| match run {
            Run::Object(object) => cache::State::new(object.block_cache()),
            Run::Memory(_) => None,
        });
        Ok(Self {
            runs,
            logical_cache,
            spans,
            cohorts,
            sealed_time: time,
            memory_bytes,
            generation,
            time,
        })
    }
    pub(crate) fn into_runs(self) -> Vec<Run<K, V>> {
        self.runs
    }
    pub(crate) const fn memory_charge(&self) -> usize {
        self.memory_bytes
    }
    pub(crate) fn has_memory(&self) -> bool {
        self.runs.iter().any(|run| matches!(run, Run::Memory(_)))
    }
    /// Number of immutable runs whose decoded heads may be opened by a merge.
    #[must_use]
    pub const fn run_count(&self) -> usize {
        self.runs.len()
    }
    async fn validate(&self) -> Result<()> {
        let mut cursor = self.cursor().await?;
        while cursor.current().is_some() {
            cursor.advance().await?;
        }
        Ok(())
    }
    /// Probe one navigation key, skipping runs whose local fences exclude it.
    /// Returned rows retain full-tuple identity and consolidate every matching run.
    ///
    /// # Errors
    /// Returns selected-block read, integrity, or consolidated arithmetic failures.
    pub async fn key_cursor(&self, key: &K) -> Result<KeyCursor<K, V>> {
        self.probes().cursor(key).await
    }

    /// Open an inclusive navigation-key range, skipping disjoint immutable runs.
    ///
    /// # Errors
    /// Rejects inverted bounds and selected-block integrity/read failures.
    pub async fn range_cursor(&self, lower: &K, upper: &K) -> Result<RangeCursor<K, V>> {
        ensure!(lower <= upper, "inverted trace range");
        RangeCursor::open(&self.runs, lower, upper).await
    }
    /// Batch affected-key probes against this immutable boundary.
    #[must_use]
    pub fn probes(&self) -> KeyProbes<K, V> {
        KeyProbes::new(self.clone())
    }
    /// Read one complete tuple coefficient using object-local full-identity fences.
    ///
    /// # Errors
    /// Returns selected-block integrity/read failures or final coefficient overflow.
    pub async fn tuple_weight(&self, tuple: &(K, V)) -> Result<i64> {
        let mut weight = num_bigint::BigInt::from(0);
        for run in &self.runs {
            weight += match run {
                Run::Memory(batch) => batch.weight(tuple),
                Run::Object(batch) => batch.tuple_weight(tuple).await?,
            };
        }
        i64::try_from(weight).context("final trace coefficient overflow")
    }
    pub(crate) async fn validate_delta(&self, delta: &Batch<K, V>) -> Result<()> {
        // Authenticated writer summaries provide a sufficient triangle bound.
        // Uncertain bounds require exact identity lookup; bag validity always does.
        let bound = self.runs.iter().fold(u128::from(delta.max_weight()), |total, run| {
            total.saturating_add(u128::from(match run {
                Run::Memory(batch) => batch.max_weight(),
                Run::Object(batch) => batch.max_weight(),
            }))
        });
        if bound <= u128::try_from(i64::MAX)? {
            return Ok(());
        }
        self.validate_coefficients(delta, false).await
    }
    pub(crate) async fn validate_bag_delta(&self, delta: &Batch<K, V>) -> Result<()> {
        self.validate_coefficients(delta, true).await
    }
    async fn validate_coefficients(&self, delta: &Batch<K, V>, nonnegative: bool) -> Result<()> {
        use futures_util::{StreamExt as _, TryStreamExt as _, stream};
        let mut totals = vec![num_bigint::BigInt::from(0); delta.iter().count()];
        let tuples = delta.iter().map(|(tuple, _)| tuple).collect::<Vec<_>>();
        for run in &self.runs {
            if let Run::Memory(batch) = run {
                batch.accumulate_weights(delta, &mut totals);
            }
        }
        let mut pages = coefficient::Pages::new(&self.runs, &tuples);
        loop {
            let mut jobs = Vec::new();
            while jobs.len() < crate::engine::reader::READ_CONCURRENCY {
                let Some(page) = pages.next().await else {
                    break;
                };
                jobs.push(page?);
            }
            if jobs.is_empty() {
                break;
            }
            let mut reads = stream::iter(jobs)
                .map(|page| async move {
                    Ok::<_, anyhow::Error>((page.range, page.batch.read(page.ordinal).await?))
                })
                .buffered(crate::engine::reader::READ_CONCURRENCY);
            while let Some((range, rows)) = reads.try_next().await? {
                coefficient::accumulate(&rows, range, &tuples, &mut totals);
            }
        }
        for ((_, weight), total) in delta.iter().zip(totals) {
            let prior = i64::try_from(total).context("final trace coefficient overflow")?;
            let next = prior.checked_add(*weight).context("final state coefficient overflow")?;
            ensure!(!nonnegative || next >= 0, "negative full-tuple input multiplicity");
        }
        Ok(())
    }
    pub(crate) fn append_validated(&self, run: Run<K, V>, time: u64) -> Result<Self> {
        self.append_runs_validated(vec![run], time)
    }
    pub(crate) fn append_runs_validated(&self, added: Vec<Run<K, V>>, time: u64) -> Result<Self> {
        ensure!(self.time.checked_add(1) == Some(time), "out-of-order trace tick");
        let generation = self.generation.checked_add(1).context("trace generation overflow")?;
        let memory_bytes = added.iter().try_fold(self.memory_bytes, |bytes, run| {
            bytes.checked_add(memory_run_charge(run)?).context("memtable charge overflow")
        })?;
        let mut cohorts = self.cohorts.clone();
        let objects = added.iter().filter(|run| matches!(run, Run::Object(_))).count();
        if objects > 0 {
            cohorts.push(objects);
        }
        let added_cache = added.clone();
        let mut runs = self.runs.clone();
        let mut spans = self.spans.clone();
        for run in added {
            spans.push(matches!(&run, Run::Memory(_)).then_some(memory::Span::new(time)));
            runs.push(run);
        }
        let mut next = Self {
            logical_cache: self.logical_cache.clone(),
            runs,
            spans,
            cohorts,
            sealed_time: self.sealed_time,
            memory_bytes,
            generation,
            time,
        };
        next.consolidate_memory()?;
        ensure!(next.runs.len() <= MAX_RUNS, "arrangement run limit requires checkpoint");
        next.logical_cache = self.logical_cache.as_ref().map(|cache| cache.append(&added_cache));
        if next.logical_cache.is_none() {
            next.logical_cache = next.runs.iter().find_map(|run| match run {
                Run::Object(object) => cache::State::new(object.block_cache()),
                Run::Memory(_) => None,
            });
        }
        Ok(next)
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

// Charge only newly retained memory rows. Immutable object/cache bytes have their
// own limits. Keep the serialized-payload-plus-structure formula unchanged.
fn memory_run_charge<K: BatchData, V: BatchData>(run: &Run<K, V>) -> Result<usize> {
    let Run::Memory(batch) = run else {
        return Ok(0);
    };
    let size = batch.encoded_size(usize::MAX, u64::MAX)?;
    let structure = std::mem::size_of::<((K, V), i64)>() + 64;
    usize::try_from(size.bytes)?
        .checked_add(size.entries.checked_mul(structure).context("memtable charge overflow")?)
        .context("memtable charge overflow")
}

impl<K: BatchData, V: BatchData> BatchReader for TraceSnapshot<K, V> {
    type Key = K;
    type Val = V;
    type Cursor = MergedCursor<K, V>;
    async fn cursor(&self) -> Result<Self::Cursor> {
        MergedCursor::open(&self.runs).await
    }
}

#[cfg(test)]
mod tests;

#[cfg(test)]
#[path = "tests/probes.rs"]
mod probe_tests;

#[cfg(test)]
#[path = "tests/manifests.rs"]
mod manifest_tests;

#[cfg(test)]
#[path = "tests/range.rs"]
mod range_tests;

#[cfg(test)]
#[path = "tests/accounting.rs"]
mod accounting_tests;

#[cfg(test)]
#[path = "tests/flush.rs"]
mod flush_tests;

#[cfg(test)]
#[path = "tests/tiers.rs"]
mod tier_tests;

#[cfg(test)]
#[path = "tests/growth.rs"]
mod growth_tests;

#[cfg(test)]
#[path = "tests/weight_bounds.rs"]
mod weight_bound_tests;

#[cfg(test)]
#[path = "tests/logical_cache.rs"]
mod logical_cache_tests;
