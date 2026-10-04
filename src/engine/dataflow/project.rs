use super::Stream;
use crate::engine::{
    Batch,
    execution::{Consolidator, Limits},
    reader::BatchData,
};
use anyhow::Result;
use std::sync::Arc;
type Mapper<K, V, A, B> = dyn Fn(&K, &V) -> Result<Option<(A, B)>> + Send + Sync;

/// Lifetime-bound pure projection/filter over a typed weighted edge.
pub struct Project<K: BatchData, V: BatchData, A: BatchData, B: BatchData> {
    map: Arc<Mapper<K, V, A, B>>,
}
impl<K: BatchData, V: BatchData, A: BatchData, B: BatchData> Project<K, V, A, B> {
    /// Bind deterministic semantics; None filters, Some projects full identity.
    pub fn new(map: impl Fn(&K, &V) -> Result<Option<(A, B)>> + Send + Sync + 'static) -> Self {
        Self { map: Arc::new(map) }
    }
    /// Preserve the tick and consolidate projected full-tuple collisions exactly.
    ///
    /// # Errors
    /// Returns callback errors or overflow of finalized projected coefficients.
    pub fn evaluate(&self, input: &Stream<Batch<K, V>>) -> Result<Stream<Batch<A, B>>> {
        self.evaluate_with_limits(input, Limits::default())
    }
    /// Evaluate using explicit spill, contribution, and finalized-output budgets.
    ///
    /// # Errors
    /// Returns callback, resource, scratch I/O, or finalized coefficient errors.
    pub fn evaluate_with_limits(
        &self,
        input: &Stream<Batch<K, V>>,
        limits: Limits,
    ) -> Result<Stream<Batch<A, B>>> {
        limits.check_batch(&input.batch)?;
        let mut builder = Consolidator::new(limits)?;
        for ((key, value), weight) in input.batch.iter() {
            if let Some(tuple) = (self.map)(key, value)? {
                builder.add(tuple, *weight)?;
            }
        }
        Ok(Stream { time: input.time, batch: builder.finish_batch()? })
    }
}
