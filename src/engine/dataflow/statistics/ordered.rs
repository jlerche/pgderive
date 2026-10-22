//! One bounded accumulator vector over the already grouped input delta.
use super::{Statistics, read};
use crate::engine::{
    Batch,
    dataflow::TimedBatch,
    execution::{Consolidator, Limits, weight_size},
    reader::BatchData,
    trace::TraceSnapshot,
};
use anyhow::{Context, Result, ensure};
use num_bigint::BigInt;
use std::borrow::Borrow;

struct Budget {
    limits: Limits,
    contributions: usize,
    entries: usize,
    bytes: u64,
}
impl Budget {
    fn add<K: BatchData>(&mut self, key: &K, column: usize, value: &BigInt) -> Result<()> {
        ensure!(self.contributions < self.limits.contributions, "contribution limit exceeded");
        self.contributions += 1;
        weight_size(&(key, column), value, self.limits.record_bytes)?;
        Ok(())
    }
    fn finish<K: BatchData>(&mut self, key: &K, values: &[BigInt]) -> Result<()> {
        for (column, value) in
            values.iter().enumerate().filter(|(_, value)| **value != BigInt::default())
        {
            self.entries = self.entries.checked_add(1).context("output entry count overflow")?;
            ensure!(self.entries <= self.limits.output_entries, "output entry limit exceeded");
            self.bytes = self
                .bytes
                .checked_add(u64::try_from(weight_size(
                    &(key, column),
                    value,
                    self.limits.record_bytes,
                )?)?)
                .context("output byte count overflow")?;
            ensure!(self.bytes <= self.limits.output_bytes, "output byte limit exceeded");
        }
        Ok(())
    }
}
impl<K: BatchData, V: BatchData, S: BatchData> Statistics<K, V, S> {
    pub(super) async fn evaluate_ordered<B: Borrow<Batch<K, V>> + Sync>(
        &self,
        input: &TimedBatch<B>,
        prior: &TraceSnapshot<K, S>,
        limits: Limits,
    ) -> Result<TimedBatch<Batch<K, S>>> {
        let mut rows = input.batch.borrow().iter().peekable();
        let mut budget = Budget { limits, contributions: 0, entries: 0, bytes: 0 };
        let mut changes = Consolidator::new(limits)?;
        let mut probes = prior.probes();
        let mut totals = vec![BigInt::default(); self.width];
        while let Some(((key, _), _)) = rows.peek().copied() {
            for total in &mut totals {
                total.clone_from(&BigInt::default());
            }
            while let Some(((found, row), weight)) = rows.next_if(|((found, _), _)| found == key) {
                let values = (self.measure)(found, row)?;
                ensure!(values.len() == self.width, "statistics measure width mismatch");
                for (column, (total, value)) in totals.iter_mut().zip(values).enumerate() {
                    *total += value * *weight;
                    budget.add(key, column, total)?;
                }
            }
            budget.finish(key, &totals)?;
            if totals.iter().all(|value| *value == BigInt::default()) {
                continue;
            }
            let before = read(&mut probes, key).await?;
            if let Some(before) = &before {
                let values = (self.decode)(before)?;
                ensure!(values.len() == self.width, "persisted statistics width mismatch");
                for (total, value) in totals.iter_mut().zip(values) {
                    *total += value;
                }
            }
            let after = (self.encode)(key, &totals)?;
            if before == after {
                continue;
            }
            if let Some(before) = before {
                changes.add((key.clone(), before), -1)?;
            }
            if let Some(after) = after {
                changes.add((key.clone(), after), 1)?;
            }
        }
        Ok(TimedBatch { time: input.time, batch: changes.finish_batch()? })
    }
}
