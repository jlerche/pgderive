//! Exact linear sufficient statistics over complete signed input ticks.
use super::TimedBatch;
use crate::engine::{
    Batch,
    execution::{Consolidator, Limits},
    reader::BatchData,
    trace::TraceSnapshot,
};
use anyhow::{Context, Result, ensure};
use num_bigint::BigInt;
use std::{collections::BTreeMap, sync::Arc};
type Decode<V> = dyn Fn(&V) -> Result<Vec<BigInt>> + Send + Sync;
type Encode<K, V> = dyn Fn(&K, &[BigInt]) -> Result<Option<V>> + Send + Sync;
/// A linear map into exact additive statistics, followed by group finalization.
/// Callers maintain valid full-tuple source bags; counts do not prove tuple validity.
pub struct Statistics<K: BatchData, V: BatchData> {
    width: usize,
    measure: Arc<Decode<V>>,
    decode: Arc<Decode<V>>,
    encode: Arc<Encode<K, V>>,
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
    pub async fn evaluate(
        &self,
        input: &TimedBatch<Batch<K, V>>,
        prior: &TraceSnapshot<K, V>,
        limits: Limits,
    ) -> Result<TimedBatch<Batch<K, V>>> {
        ensure!(prior.time().checked_add(1) == Some(input.time), "out-of-order statistics tick");
        ensure!((1..=129).contains(&self.width), "statistics width unsupported");
        limits.check_batch(&input.batch)?;
        let mut contributions = Consolidator::new(limits)?;
        for ((key, row), weight) in input.batch.iter() {
            let values = (self.measure)(row)?;
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
        for (key, mut total) in totals {
            let before = read(prior, &key).await?;
            if let Some(before) = &before {
                let values = (self.decode)(before)?;
                ensure!(values.len() == self.width, "persisted statistics width mismatch");
                for (total, value) in total.iter_mut().zip(values) {
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
    prior: &TraceSnapshot<K, V>,
    key: &K,
) -> Result<Option<V>> {
    let mut cursor = prior.key_cursor(key).await?;
    let Some((_, value, weight)) = cursor.current() else {
        return Ok(None);
    };
    ensure!(weight == 1, "invalid statistics state multiplicity");
    let value = value.clone();
    cursor.advance().await?;
    ensure!(cursor.current().is_none(), "multiple statistics states for group");
    Ok(Some(value))
}
