//! Exact linear sufficient statistics over complete signed input ticks.
mod ordered;
use super::TimedBatch;
use crate::engine::{
    Batch,
    execution::{Consolidator, Limits},
    reader::BatchData,
    trace::{KeyProbes, TraceSnapshot},
    value::Decoded,
};
use anyhow::{Context, Result, ensure};
use num_bigint::BigInt;
use std::{borrow::Borrow, collections::BTreeMap, sync::Arc};
type Decode<V> = dyn Fn(&V) -> Result<Decoded<Vec<BigInt>>> + Send + Sync;
type Measure<K, V> = dyn Fn(&K, &V) -> Result<Vec<BigInt>> + Send + Sync;
type Encode<K, V> = dyn Fn(&K, &[BigInt]) -> Result<Option<V>> + Send + Sync;
/// A linear map into exact additive statistics, followed by group finalization.
/// Callers maintain valid full-tuple source bags; counts do not prove tuple validity.
#[derive(Clone)]
pub struct Statistics<K: BatchData, V: BatchData, S: BatchData = V> {
    width: usize,
    measure: Arc<Measure<K, V>>,
    decode: Arc<Decode<S>>,
    encode: Arc<Encode<K, S>>,
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
        Self {
            width,
            measure: Arc::new(measure),
            decode: Arc::new(move |row| Ok(Decoded::transient(decode(row)?))),
            encode: Arc::new(encode),
        }
    }
    /// Bind an immutable shared decoded accumulator, avoiding repeated decoding.
    ///
    /// This is the same linear operator and durable state contract as `with_measure`.
    pub fn with_shared_decode(
        width: usize,
        measure: impl Fn(&K, &V) -> Result<Vec<BigInt>> + Send + Sync + 'static,
        decode: impl Fn(&S) -> Result<Decoded<Vec<BigInt>>> + Send + Sync + 'static,
        encode: impl Fn(&K, &[BigInt]) -> Result<Option<S>> + Send + Sync + 'static,
    ) -> Self {
        Self {
            width,
            measure: Arc::new(measure),
            decode: Arc::new(decode),
            encode: Arc::new(encode),
        }
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
        ensure!((1..=129).contains(&self.width), "statistics width unsupported");
        limits.check_batch(input.batch.borrow())?;
        if self.width <= limits.resident_entries
            && self
                .width
                .checked_mul(limits.record_bytes)
                .is_some_and(|bytes| bytes <= limits.resident_bytes)
        {
            return self.evaluate_ordered(input, prior, limits).await;
        }
        let mut contributions = Consolidator::new(limits)?;
        for ((key, row), weight) in input.batch.borrow().iter() {
            let values = (self.measure)(key, row)?;
            ensure!(values.len() == self.width, "statistics measure width mismatch");
            for (column, value) in values.into_iter().enumerate() {
                contributions.add((key.clone(), column), value * weight)?;
            }
        }
        let mut totals = BTreeMap::<K, Vec<BigInt>>::new();
        contributions.finish(|(key, column), value| {
            let total = totals.entry(key).or_insert_with(|| vec![BigInt::from(0); self.width]);
            *total.get_mut(column).context("statistics column out of range")? += value;
            Ok(())
        })?;
        let mut changes = Consolidator::new(limits)?;
        let mut probes = prior.probes();
        for (key, mut total) in totals {
            let before = read(&mut probes, &key).await?;
            if let Some(before) = &before {
                let values = (self.decode)(before)?;
                ensure!(values.len() == self.width, "persisted statistics width mismatch");
                for (total, value) in total.iter_mut().zip(values.iter()) {
                    *total += value;
                }
            }
            let after = (self.encode)(&key, &total)?;
            if before == after {
                continue;
            }
            if let Some(before) = before {
                changes.add((key.clone(), before), -1)?;
            }
            if let Some(after) = after {
                changes.add((key, after), 1)?;
            }
        }
        Ok(TimedBatch { time: input.time, batch: changes.finish_batch()? })
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
