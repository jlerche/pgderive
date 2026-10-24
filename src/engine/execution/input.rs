//! One logical input accepts transport chunks and exposes data only after sealing.
use super::{Consolidator, Limits, Sealed};
use crate::engine::{dataflow::TimedBatch, reader::BatchData};
use anyhow::{Context, Result, ensure};
/// Complete source edges at one logical boundary.
pub type SealedInput<K, V> = TimedBatch<Vec<Sealed<K, V>>>;

/// Unpublished bounded input. Dropping it never creates a logical tick or progress.
pub struct OpenInput<K: BatchData, V: BatchData> {
    sources: Vec<Consolidator<(K, V)>>,
}
impl<K: BatchData, V: BatchData> OpenInput<K, V> {
    /// Open all source edges of one complete logical transaction or snapshot.
    ///
    /// # Errors
    /// Returns invalid counts, limits or scratch creation failures.
    pub fn new(sources: usize, limits: Limits) -> Result<Self> {
        ensure!(sources > 0, "logical input requires sources");
        Ok(Self {
            sources: (0..sources).map(|_| Consolidator::new(limits)).collect::<Result<_>>()?,
        })
    }
    /// Append one full-tuple contribution without running any operators.
    ///
    /// # Errors
    /// Returns source, record, scratch or capacity errors.
    pub fn push(&mut self, source: usize, key: K, value: V, weight: i64) -> Result<()> {
        self.sources
            .get_mut(source)
            .context("unknown logical input source")?
            .add((key, value), weight)
    }
    /// Seal every source together at one logical time. No partial input escapes.
    ///
    /// # Errors
    /// Returns resource, I/O, integrity or finalized coefficient errors.
    pub fn seal(self, time: u64) -> Result<SealedInput<K, V>> {
        ensure!(time > 0, "logical input requires positive time");
        Ok(TimedBatch {
            time,
            batch: self.sources.into_iter().map(Consolidator::seal).collect::<Result<_>>()?,
        })
    }
}
