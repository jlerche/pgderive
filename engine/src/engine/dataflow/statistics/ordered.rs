//! One bounded accumulator vector over the already grouped input delta.
use super::{Statistics, StatisticsOutput, read};
use crate::engine::{
    Batch,
    dataflow::TimedBatch,
    execution::{Consolidator, Limits},
    reader::{BatchData, READ_CONCURRENCY},
    trace::TraceSnapshot,
};
use anyhow::Result;
use futures_util::{StreamExt as _, stream::FuturesOrdered};
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
        let allowance = limits.resident_bytes / READ_CONCURRENCY;
        // Prepared tickets allocate no collector. Consumption is sequential, so
        // reserve exactly one bounded active cursor's optional workspace.
        let pending_budget = limits.resident_bytes.saturating_sub(allowance.min(64 * 1024)).max(1);
        let mut pending = Vec::new();
        let mut group = Vec::new();
        while rows.current().is_some() {
            let mut bytes = 0_usize;
            let mut plans = Vec::new();
            let prefetch_probes = &probes;
            let prepare = |key, delta| async move {
                let selected = prefetch_probes.prepare_key(&key, allowance).await?;
                Ok::<_, anyhow::Error>((key, delta, selected))
            };
            while pending.len() < limits.resident_entries
                && !crate::engine::reader::ReadAhead::ready(&plans)
                && bytes < pending_budget
            {
                gather_deltas(
                    &mut accumulator,
                    &mut rows,
                    (limits.resident_entries - pending.len(), limits.resident_bytes),
                    (&mut group, &mut bytes),
                )
                .await?;
                if group.is_empty() {
                    break;
                }
                // The byte target is soft by at most one READ_CONCURRENCY-sized
                // metadata group plus one resident budget of optional key-slice pins.
                // Actual complete-key pins remain charged to each prepared key;
                // the one active collector is reserved outside pending metadata.
                let start = pending.len();
                let mut prepared = FuturesOrdered::new();
                #[expect(
                    clippy::iter_with_drain,
                    reason = "reuse the emptied group allocation for the next bounded metadata batch"
                )]
                for (key, delta) in group.drain(..) {
                    prepared.push_back(prepare(key, delta));
                }
                while let Some(result) = prepared.next().await {
                    pending.push(result?);
                }
                let mut admitted = true;
                for (_, _, selected) in &pending[start..] {
                    bytes = bytes
                        .checked_add(selected.charge()?)
                        .ok_or_else(|| anyhow::anyhow!("statistics metadata charge overflow"))?;
                    admitted &= probes.plan_key(selected, &mut plans)?;
                }
                if !admitted {
                    break;
                }
            }
            // Only nonzero measured deltas are prefetched. No speculative errors
            // or reads for groups whose complete signed contribution cancels.
            let _read_ahead: crate::engine::reader::ReadGuard =
                crate::engine::reader::ReadAhead::load(plans).await?;
            #[expect(
                clippy::iter_with_drain,
                reason = "reuse the emptied pending allocation for the next read-ahead window"
            )]
            for (key, delta, selected) in pending.drain(..) {
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
    match (before, after) {
        (Some(before), Some(after)) => {
            changes.add((key.clone(), before), -1)?;
            changes.add((key, after), 1)?;
        }
        (Some(before), None) => changes.add((key, before), -1)?,
        (None, Some(after)) => changes.add((key, after), 1)?,
        (None, None) => {}
    }
    Ok(())
}

type DeltaGroup<K, S> = Vec<(K, super::algebra::Delta<K, S>)>;

async fn gather_deltas<K: BatchData, V: BatchData, S: BatchData>(
    accumulator: &mut super::algebra::FoldState<K, V, S>,
    rows: &mut crate::engine::execution::SealedCursor<K, V>,
    capacity: (usize, usize),
    output: (&mut DeltaGroup<K, S>, &mut usize),
) -> Result<()> {
    let (group, bytes) = output;
    while group.len() < READ_CONCURRENCY.min(capacity.0) && *bytes < capacity.1 {
        let Some((key, delta)) = next_delta(accumulator, rows).await? else { break };
        *bytes = bytes
            .checked_add(delta.charge())
            .and_then(|bytes| bytes.checked_add(std::mem::size_of::<K>()))
            .ok_or_else(|| anyhow::anyhow!("statistics lookahead charge overflow"))?;
        group.push((key, delta));
    }
    Ok(())
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
