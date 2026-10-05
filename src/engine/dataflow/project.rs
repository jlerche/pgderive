use super::TimedBatch;
use crate::engine::{
    Batch,
    execution::{Consolidator, Limits},
    reader::BatchData,
};
use anyhow::Result;
use std::borrow::Borrow;
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
    pub fn evaluate(&self, input: &TimedBatch<Batch<K, V>>) -> Result<TimedBatch<Batch<A, B>>> {
        self.evaluate_with_limits(input, Limits::default())
    }
    /// Evaluate using explicit spill, contribution, and finalized-output budgets.
    ///
    /// # Errors
    /// Returns callback, resource, scratch I/O, or finalized coefficient errors.
    pub fn evaluate_with_limits<I: Borrow<Batch<K, V>>>(
        &self,
        input: &TimedBatch<I>,
        limits: Limits,
    ) -> Result<TimedBatch<Batch<A, B>>> {
        limits.check_batch(input.batch.borrow())?;
        let mut builder = Consolidator::new(limits)?;
        for ((key, value), weight) in input.batch.borrow().iter() {
            if let Some(tuple) = (self.map)(key, value)? {
                builder.add(tuple, *weight)?;
            }
        }
        Ok(TimedBatch { time: input.time, batch: builder.finish_batch()? })
    }
}
