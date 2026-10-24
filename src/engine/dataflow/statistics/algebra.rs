//! Typed reversible scalar folds erased only at the operator registry boundary.
use crate::engine::{
    execution::{Limits, weight_size},
    reader::BatchData,
    value::Decoded,
};
use anyhow::{Context, Result, ensure};
use num_bigint::BigInt;
use serde::Serialize;
use std::sync::Arc;

/// Exact signed addition. Intermediate values may contain negative counts;
/// SQL validity and narrowing belong to complete-group finalization.
pub trait Additive: Clone + Default + Serialize + Send + Sync + 'static {
    /// Add another scalar multiplied by a signed full-tuple coefficient.
    ///
    /// # Errors
    /// Returns incompatible scalar domains or explicit arithmetic limits.
    fn add_scaled(&mut self, other: &Self, weight: i64) -> Result<()>;
    /// Whether every hidden contribution is zero, including scale/special counts.
    fn is_zero(&self) -> bool;
    /// Charge the complete scalar representation against a bounded record.
    ///
    /// # Errors
    /// Returns serialization or record-capacity failures.
    fn record_size(&self, tuple: &impl Serialize, limit: usize) -> Result<usize> {
        crate::engine::execution::record_size(&(tuple, self), limit)
    }
}
impl Additive for BigInt {
    fn add_scaled(&mut self, other: &Self, weight: i64) -> Result<()> {
        *self += other * weight;
        Ok(())
    }
    fn is_zero(&self) -> bool {
        self == &Self::default()
    }
    fn record_size(&self, tuple: &impl Serialize, limit: usize) -> Result<usize> {
        weight_size(tuple, self, limit)
    }
}
type Measure<K, V, A> = dyn Fn(&K, &V) -> Result<Vec<A>> + Send + Sync;
type Decode<S, A> = dyn Fn(&S) -> Result<Decoded<Vec<A>>> + Send + Sync;
type Encode<K, S, A> = dyn Fn(&K, &[A]) -> Result<Option<S>> + Send + Sync;
pub(super) type FoldState<K, V, S> = Box<dyn Accumulator<K, V, S>>;
pub(super) struct Algebra<K, V, S, A> {
    width: usize,
    measure: Arc<Measure<K, V, A>>,
    decode: Arc<Decode<S, A>>,
    encode: Arc<Encode<K, S, A>>,
}
impl<K: BatchData, V: BatchData, S: BatchData, A: Additive> Algebra<K, V, S, A> {
    pub(super) fn new(
        width: usize,
        measure: impl Fn(&K, &V) -> Result<Vec<A>> + Send + Sync + 'static,
        decode: impl Fn(&S) -> Result<Decoded<Vec<A>>> + Send + Sync + 'static,
        encode: impl Fn(&K, &[A]) -> Result<Option<S>> + Send + Sync + 'static,
    ) -> Self {
        Self {
            width,
            measure: Arc::new(measure),
            decode: Arc::new(decode),
            encode: Arc::new(encode),
        }
    }
    pub(super) fn create(self: &Arc<Self>, limits: Limits) -> Result<FoldState<K, V, S>> {
        ensure!((1..=129).contains(&self.width), "statistics width unsupported");
        Ok(Box::new(Fold {
            algebra: self.clone(),
            values: vec![A::default(); self.width],
            budget: Budget { limits, contributions: 0, entries: 0, bytes: 0 },
        }))
    }
}
pub(super) trait Accumulator<K, V, S>: Send {
    fn reset(&mut self);
    fn observe(&mut self, key: &K, row: &V, weight: i64) -> Result<()>;
    fn finish_delta(&mut self, key: &K) -> Result<bool>;
    fn restore(&mut self, row: &S) -> Result<()>;
    fn encode(&self, key: &K) -> Result<Option<S>>;
}
struct Fold<K, V, S, A> {
    algebra: Arc<Algebra<K, V, S, A>>,
    values: Vec<A>,
    budget: Budget,
}
impl<K: BatchData, V: BatchData, S: BatchData, A: Additive> Accumulator<K, V, S>
    for Fold<K, V, S, A>
{
    fn reset(&mut self) {
        self.values.fill(A::default());
    }
    fn observe(&mut self, key: &K, row: &V, weight: i64) -> Result<()> {
        let values = (self.algebra.measure)(key, row)?;
        ensure!(values.len() == self.values.len(), "statistics measure width mismatch");
        for (column, (total, value)) in self.values.iter_mut().zip(values).enumerate() {
            total.add_scaled(&value, weight)?;
            self.budget.add(key, column, total)?;
        }
        Ok(())
    }
    fn finish_delta(&mut self, key: &K) -> Result<bool> {
        self.budget.finish(key, &self.values)?;
        Ok(!self.values.iter().all(Additive::is_zero))
    }
    fn restore(&mut self, row: &S) -> Result<()> {
        let values = (self.algebra.decode)(row)?;
        ensure!(values.len() == self.values.len(), "persisted statistics width mismatch");
        for (total, value) in self.values.iter_mut().zip(values.iter()) {
            total.add_scaled(value, 1)?;
        }
        Ok(())
    }
    fn encode(&self, key: &K) -> Result<Option<S>> {
        (self.algebra.encode)(key, &self.values)
    }
}
struct Budget {
    limits: Limits,
    contributions: usize,
    entries: usize,
    bytes: u64,
}
impl Budget {
    fn add<K: BatchData, A: Additive>(&mut self, key: &K, column: usize, value: &A) -> Result<()> {
        ensure!(
            self.contributions < self.limits.contributions,
            crate::engine::execution::CapacityExceeded("contribution limit exceeded")
        );
        self.contributions += 1;
        value.record_size(&(key, column), self.limits.record_bytes)?;
        Ok(())
    }
    fn finish<K: BatchData, A: Additive>(&mut self, key: &K, values: &[A]) -> Result<()> {
        for (column, value) in values.iter().enumerate().filter(|(_, value)| !value.is_zero()) {
            self.entries = self.entries.checked_add(1).context("output entry count overflow")?;
            ensure!(
                self.entries <= self.limits.output_entries,
                crate::engine::execution::CapacityExceeded("output entry limit exceeded")
            );
            self.bytes = self
                .bytes
                .checked_add(u64::try_from(
                    value.record_size(&(key, column), self.limits.record_bytes)?,
                )?)
                .context("output byte count overflow")?;
            ensure!(
                self.bytes <= self.limits.output_bytes,
                crate::engine::execution::CapacityExceeded("output byte limit exceeded")
            );
        }
        Ok(())
    }
}
