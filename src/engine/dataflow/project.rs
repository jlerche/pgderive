use super::{Expand, TimedBatch};
use crate::engine::{Batch, execution::Limits, reader::BatchData};
use anyhow::Result;
use std::borrow::Borrow;

/// Lifetime-bound pure projection/filter over a typed weighted edge.
#[derive(Clone)]
pub struct Project<K: BatchData, V: BatchData, A: BatchData, B: BatchData> {
    expansion: Expand<K, V, A, B>,
}
impl<K: BatchData, V: BatchData, A: BatchData, B: BatchData> Project<K, V, A, B> {
    /// Bind deterministic semantics; None filters, Some projects full identity.
    pub fn new(map: impl Fn(&K, &V) -> Result<Option<(A, B)>> + Send + Sync + 'static) -> Self {
        Self {
            expansion: Expand::new(move |key, value, emit| {
                if let Some(tuple) = map(key, value)? {
                    emit(tuple)?;
                }
                Ok(())
            }),
        }
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
        self.expansion.evaluate(input, limits)
    }
}

impl<K: BatchData, V: BatchData, A: BatchData, B: BatchData> Project<K, V, A, B> {
    pub(crate) async fn evaluate_sealed(
        &self,
        input: &crate::engine::execution::Sealed<K, V>,
        limits: Limits,
    ) -> Result<crate::engine::execution::Sealed<A, B>> {
        self.evaluate_sealed_counted(input, limits, &mut 0).await
    }
    pub(crate) async fn evaluate_sealed_counted(
        &self,
        input: &crate::engine::execution::Sealed<K, V>,
        limits: Limits,
        contributions: &mut usize,
    ) -> Result<crate::engine::execution::Sealed<A, B>> {
        self.expansion.evaluate_sealed_counted(input, limits, contributions).await
    }
}
