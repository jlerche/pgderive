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
        while rows.current().is_some() {
            let mut pending = Vec::new();
            let mut bytes = 0_usize;
            let mut plans = Vec::new();
            while pending.len() < limits.resident_entries
                && !crate::engine::reader::ReadAhead::ready(&plans)
                && bytes < limits.resident_bytes
            {
                let Some((key, delta)) = next_delta(&mut accumulator, &mut rows).await? else {
                    break;
                };
                bytes = bytes
                    .checked_add(delta.charge())
                    .and_then(|bytes| bytes.checked_add(std::mem::size_of::<K>()))
                    .ok_or_else(|| anyhow::anyhow!("statistics lookahead charge overflow"))?;
                let admitted = probes.plan_key(&key, &mut plans)?;
                pending.push((key, delta));
                if !admitted {
                    break;
                }
            }
            // Only nonzero measured deltas are prefetched. No speculative errors
            // or reads for groups whose complete signed contribution cancels.
            crate::engine::reader::ReadAhead::load(plans).await?;
            for (key, delta) in pending {
                let before = read(&mut probes, &key).await?;
                let after = delta.apply(&key, before.as_ref())?;
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
        }
        // Each affected key was absent or a unique unit state. Replacing exactly
        // that value preserves full-tuple multiplicity; no second lookup is needed.
        Ok(StatisticsOutput { prior, batch: Arc::new(changes.seal()?) })
    }
}

async fn next_delta<K: BatchData, V: BatchData, S: BatchData>(
    accumulator: &mut super::algebra::FoldState<K, V, S>,
    rows: &mut crate::engine::execution::SealedCursor<K, V>,
) -> Result<Option<(K, super::algebra::Delta<K, S>)>> {
    use crate::engine::reader::BatchCursor as _;
    while let Some((key, _, _)) = rows.current() {
        let key = key.clone();
        while let Some((found, row, weight)) = rows.current() {
            if *found != key {
                break;
            }
            accumulator.observe(found, row, weight)?;
            rows.advance().await?;
        }
        if let Some(delta) = accumulator.finish_delta(&key)? {
            return Ok(Some((key, delta)));
        }
    }
    Ok(None)
}
