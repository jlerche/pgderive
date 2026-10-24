//! Exact linear sufficient statistics over complete signed input ticks.
mod ordered;
use super::TimedBatch;
use crate::engine::{
    Batch,
    execution::Limits,
    reader::BatchData,
    trace::{KeyProbes, TraceSnapshot},
    value::Decoded,
};
use anyhow::{Result, ensure};
use num_bigint::BigInt;
use std::{borrow::Borrow, sync::Arc};
mod algebra;
pub use algebra::Additive;
use algebra::{Algebra, FoldState};
type Create<K, V, S> = dyn Fn(Limits) -> Result<FoldState<K, V, S>> + Send + Sync;
/// A producer-validated replacement tied to its exact immutable prior and delta.
/// Only statistics evaluation can construct this capability.
pub struct StatisticsOutput<'a, K: BatchData, V: BatchData> {
    prior: &'a TraceSnapshot<K, V>,
    batch: Arc<crate::engine::execution::Sealed<K, V>>,
}
impl<K: BatchData, V: BatchData> StatisticsOutput<'_, K, V> {
    pub(crate) const fn batch(&self) -> &Arc<crate::engine::execution::Sealed<K, V>> {
        &self.batch
    }
    pub(crate) fn matches(
        &self,
        prior: &TraceSnapshot<K, V>,
        batch: &crate::engine::execution::Sealed<K, V>,
    ) -> bool {
        std::ptr::eq(self.prior, prior) && std::ptr::eq(self.batch.as_ref(), batch)
    }
}
/// A linear map into exact additive statistics, followed by group finalization.
/// Callers maintain valid full-tuple source bags; counts do not prove tuple validity.
#[derive(Clone)]
pub struct Statistics<K: BatchData, V: BatchData, S: BatchData = V> {
    create: Arc<Create<K, V, S>>,
}
impl<K: BatchData, V: BatchData> Statistics<K, V> {
    /// Bind fixed-width exact statistics and their persisted state codec.
    /// Callback semantics and width must bind the owning durable plan identity.
    pub fn new(
        width: usize,
        measure: impl Fn(&V) -> Result<Vec<BigInt>> + Send + Sync + 'static,
        decode: impl Fn(&V) -> Result<Vec<BigInt>> + Send + Sync + 'static,
        encode: impl Fn(&K, &[BigInt]) -> Result<Option<V>> + Send + Sync + 'static,
    ) -> Self {
        Self::with_measure(width, move |_, value| measure(value), decode, encode)
    }
}
impl<K: BatchData, V: BatchData, S: BatchData> Statistics<K, V, S> {
    /// Bind a keyed measure and an independent persisted sufficient-statistics type.
    pub fn with_measure(
        width: usize,
        measure: impl Fn(&K, &V) -> Result<Vec<BigInt>> + Send + Sync + 'static,
        decode: impl Fn(&S) -> Result<Vec<BigInt>> + Send + Sync + 'static,
        encode: impl Fn(&K, &[BigInt]) -> Result<Option<S>> + Send + Sync + 'static,
    ) -> Self {
        Self::with_algebra(width, measure, move |row| Ok(Decoded::transient(decode(row)?)), encode)
    }
    /// Bind a reversible additive scalar algebra to the shared statistics traversal.
    /// Its canonical persisted codec and all interpretation parameters must bind
    /// the owning durable plan identity.
    pub fn with_algebra<A: Additive>(
        width: usize,
        measure: impl Fn(&K, &V) -> Result<Vec<A>> + Send + Sync + 'static,
        decode: impl Fn(&S) -> Result<Decoded<Vec<A>>> + Send + Sync + 'static,
        encode: impl Fn(&K, &[A]) -> Result<Option<S>> + Send + Sync + 'static,
    ) -> Self {
        let algebra = Arc::new(Algebra::new(width, measure, decode, encode));
        Self { create: Arc::new(move |limits| algebra.create(limits)) }
    }
    /// Combine signed contributions with one prior statistic per changed group.
    ///
    /// # Errors
    /// Rejects clock, codec, callback, read, resource and final arithmetic failures.
    pub async fn evaluate<B: Borrow<Batch<K, V>> + Sync>(
        &self,
        input: &TimedBatch<B>,
        prior: &TraceSnapshot<K, S>,
        limits: Limits,
    ) -> Result<TimedBatch<Batch<K, S>>> {
        ensure!(prior.time().checked_add(1) == Some(input.time), "out-of-order statistics tick");
        limits.check_batch(input.batch.borrow())?;
        self.evaluate_ordered(input, prior, limits).await
    }
}
async fn read<K: BatchData, V: BatchData>(
    probes: &mut KeyProbes<K, V>,
    key: &K,
) -> Result<Option<V>> {
    let mut cursor = probes.cursor(key).await?;
    let Some((_, value, weight)) = cursor.current() else {
        return Ok(None);
    };
    ensure!(weight == 1, "invalid statistics state multiplicity");
    let value = value.clone();
    cursor.advance().await?;
    ensure!(cursor.current().is_none(), "multiple statistics states for group");
    Ok(Some(value))
}

#[cfg(test)]
mod tests;
