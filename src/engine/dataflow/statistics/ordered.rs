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
use futures_util::{StreamExt as _, TryStreamExt as _, stream};
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
                let group = gather_deltas(
                    &mut accumulator,
                    &mut rows,
                    (limits.resident_entries - pending.len(), limits.resident_bytes),
                    &mut bytes,
                )
                .await?;
                if group.is_empty() {
                    break;
                }
                let probes = &probes;
                let keys = group.iter().map(|(key, _)| key.clone()).collect::<Vec<_>>();
                // The byte target is soft by at most one READ_CONCURRENCY-sized
                // metadata group plus one resident budget of optional key-slice pins.
                // Each key spends its allowance across all selected runs.
                let prepared = stream::iter(keys)
                    .map(|key| async move {
                        probes
                            .prepare_key(
                                &key,
                                limits.resident_bytes / crate::engine::reader::READ_CONCURRENCY,
                            )
                            .await
                    })
                    .buffered(crate::engine::reader::READ_CONCURRENCY)
                    .try_collect::<Vec<_>>()
                    .await?;
                let mut admitted = true;
                for ((key, delta), selected) in group.into_iter().zip(prepared) {
                    bytes = bytes
                        .checked_add(selected.charge()?)
                        .ok_or_else(|| anyhow::anyhow!("statistics metadata charge overflow"))?;
                    admitted &= probes.plan_key(&selected, &mut plans)?;
                    pending.push((key, delta, selected));
                }
                if !admitted {
                    break;
                }
            }
            // Only nonzero measured deltas are prefetched. No speculative errors
            // or reads for groups whose complete signed contribution cancels.
            let _read_ahead: crate::engine::reader::ReadGuard =
                crate::engine::reader::ReadAhead::load(plans).await?;
            for (key, delta, selected) in pending {
                let before = read(probes.cursor_prepared(&selected).await?).await?;
                let after = delta.apply(&key, before.as_ref())?;
                replace(&mut changes, key, before, after)?;
            }
        }
        // Each affected key was absent or a unique unit state. Replacing exactly
        // that value preserves full-tuple multiplicity; no second lookup is needed.
        Ok(StatisticsOutput { prior, batch: Arc::new(changes.seal()?) })
    }
}

fn replace<K: BatchData, S: BatchData>(
    changes: &mut Consolidator<(K, S)>,
    key: K,
    before: Option<S>,
    after: Option<S>,
) -> Result<()> {
    if before == after {
        return Ok(());
    }
    if let Some(before) = before {
        changes.add((key.clone(), before), -1)?;
    }
    if let Some(after) = after {
        changes.add((key, after), 1)?;
    }
    Ok(())
}

async fn gather_deltas<K: BatchData, V: BatchData, S: BatchData>(
    accumulator: &mut super::algebra::FoldState<K, V, S>,
    rows: &mut crate::engine::execution::SealedCursor<K, V>,
    capacity: (usize, usize),
    bytes: &mut usize,
) -> Result<Vec<(K, super::algebra::Delta<K, S>)>> {
    let mut group = Vec::new();
    while group.len() < crate::engine::reader::READ_CONCURRENCY.min(capacity.0)
        && *bytes < capacity.1
    {
        let Some((key, delta)) = next_delta(accumulator, rows).await? else { break };
        *bytes = bytes
            .checked_add(delta.charge())
            .and_then(|bytes| bytes.checked_add(std::mem::size_of::<K>()))
            .ok_or_else(|| anyhow::anyhow!("statistics lookahead charge overflow"))?;
        group.push((key, delta));
    }
    Ok(group)
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
