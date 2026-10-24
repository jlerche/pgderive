//! One bounded accumulator vector over the already grouped input delta.
use super::{Statistics, StatisticsOutput, read};
use crate::engine::{
    Batch,
    dataflow::TimedBatch,
    execution::{Consolidator, Limits},
    reader::BatchData,
    trace::TraceSnapshot,
};
use anyhow::Result;
use std::{borrow::Borrow, sync::Arc};

impl<K: BatchData, V: BatchData, S: BatchData> Statistics<K, V, S> {
    pub(super) async fn evaluate_ordered<B: Borrow<Batch<K, V>> + Sync>(
        &self,
        input: &TimedBatch<B>,
        prior: &TraceSnapshot<K, S>,
        limits: Limits,
    ) -> Result<TimedBatch<Batch<K, S>>> {
        let sealed = crate::engine::execution::Sealed::from(input.batch.borrow().clone());
        Ok(TimedBatch {
            time: input.time,
            batch: self
                .evaluate_sealed(&sealed, prior, limits)
                .await?
                .batch()
                .materialize(limits)
                .await?,
        })
    }
    pub(crate) async fn evaluate_sealed<'a>(
        &self,
        input: &crate::engine::execution::Sealed<K, V>,
        prior: &'a TraceSnapshot<K, S>,
        limits: Limits,
    ) -> Result<StatisticsOutput<'a, K, S>> {
        use crate::engine::reader::{BatchCursor as _, BatchReader as _};
        let mut accumulator = (self.create)(limits)?;
        let mut rows = input.cursor().await?;
        let mut changes = Consolidator::new(limits)?;
        let mut probes = prior.probes();
        while let Some((key, _, _)) = rows.current() {
            let key = key.clone();
            accumulator.reset();
            while let Some((found, row, weight)) = rows.current() {
                if *found != key {
                    break;
                }
                accumulator.observe(found, row, weight)?;
                rows.advance().await?;
            }
            if !accumulator.finish_delta(&key)? {
                continue;
            }
            let before = read(&mut probes, &key).await?;
            if let Some(before) = &before {
                accumulator.restore(before)?;
            }
            let after = accumulator.encode(&key)?;
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
        // Each affected key was absent or a unique unit state. Replacing exactly
        // that value preserves full-tuple multiplicity; no second lookup is needed.
        Ok(StatisticsOutput { prior, batch: Arc::new(changes.seal()?) })
    }
}
