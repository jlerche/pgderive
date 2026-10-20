//! Direct group differentiation over pinned cursors and complete signed deltas.
use super::{PartitionWork, TimedBatch};
use crate::engine::{
    Batch,
    execution::{Consolidator, Limits},
    reader::BatchData,
    trace::{KeyCursor, TraceSnapshot},
};
use anyhow::{Context, Result, ensure};
use std::{borrow::Borrow, collections::BTreeMap, future::Future, pin::Pin, sync::Arc};
/// Asynchronous result of one affected group's direct delta evaluation.
pub type GroupFuture<'a, V> = Pin<Box<dyn Future<Output = Result<Batch<(), V>>> + Send + 'a>>;
type Transform<K, V> =
    dyn for<'a> Fn(GroupInput<'a, K, V>, &'a mut PartitionWork) -> GroupFuture<'a, V> + Send + Sync;
/// Pinned old state and complete changes for one navigation key.
pub struct GroupInput<'a, K: BatchData, V: BatchData> {
    /// Affected navigation key; full values retain independent tuple identity.
    pub key: &'a K,
    /// Complete consolidated changes for this key at this committed boundary.
    pub delta: &'a Batch<(), V>,
    /// Shared resource limits.
    pub limits: Limits,
    prior: &'a TraceSnapshot<K, V>,
}
impl<K: BatchData, V: BatchData> GroupInput<'_, K, V> {
    /// Open old state without materializing the group.
    ///
    /// # Errors
    /// Returns selected-block read, integrity or arithmetic failures.
    pub async fn before(&self) -> Result<GroupCursor<K, V>> {
        Ok(GroupCursor {
            prior: self.prior.key_cursor(self.key).await?,
            delta: Arc::new(Vec::new()),
            index: 0,
        })
    }
    /// Overlay the signed tick delta on old state, without rebuilding the old bag.
    /// Changed identities have already passed nonnegative membership validation.
    ///
    /// # Errors
    /// Returns selected-block read, integrity or arithmetic failures.
    pub async fn after(&self) -> Result<GroupCursor<K, V>> {
        Ok(GroupCursor {
            prior: self.prior.key_cursor(self.key).await?,
            delta: Arc::new(
                self.delta.iter().map(|(((), value), weight)| (value.clone(), *weight)).collect(),
            ),
            index: 0,
        })
    }
}
/// Forward-only merge of retained full identities and a bounded tick delta.
#[derive(Clone)]
pub struct GroupCursor<K: BatchData, V: BatchData> {
    prior: KeyCursor<K, V>,
    delta: Arc<Vec<(V, i64)>>,
    index: usize,
}
impl<K: BatchData, V: BatchData> GroupCursor<K, V> {
    /// Read the next nonzero full identity, charging every candidate visited.
    ///
    /// # Errors
    /// Returns read/integrity, resource, negative multiplicity or overflow errors.
    pub async fn next(&mut self, work: &mut PartitionWork) -> Result<Option<(V, i64)>> {
        let mut next = self.clone();
        let row = next.next_inner(work).await?;
        *self = next;
        Ok(row)
    }
    async fn next_inner(&mut self, work: &mut PartitionWork) -> Result<Option<(V, i64)>> {
        loop {
            let old = self.prior.current().map(|(_, value, weight)| (value.clone(), weight));
            let changed = self.delta.get(self.index);
            let value = old
                .as_ref()
                .map(|entry| &entry.0)
                .into_iter()
                .chain(changed.map(|entry| &entry.0))
                .min()
                .cloned();
            let Some(value) = value else { return Ok(None) };
            work.charge(1)?;
            let mut weight = 0_i64;
            if let Some((candidate, coefficient)) = &old
                && candidate == &value
            {
                weight = *coefficient;
                self.prior.advance().await?;
            }
            if let Some((candidate, coefficient)) = changed
                && candidate == &value
            {
                weight = weight.checked_add(*coefficient).context("group coefficient overflow")?;
                self.index += 1;
            }
            ensure!(weight >= 0, "negative group membership");
            if weight != 0 {
                return Ok(Some((value, weight)));
            }
        }
    }
}
/// Cursor-based incremental group operator. Callbacks emit Q(new)-Q(old).
/// Every changed identity is validated before callbacks may stop reading early.
#[derive(Clone)]
pub struct Group<K: BatchData, V: BatchData> {
    transform: Arc<Transform<K, V>>,
}
impl<K: BatchData, V: BatchData> Group<K, V> {
    /// Bind a direct incremental evaluator; empty input must map to empty output.
    #[must_use]
    pub fn new(
        transform: impl for<'a> Fn(GroupInput<'a, K, V>, &'a mut PartitionWork) -> GroupFuture<'a, V>
        + Send
        + Sync
        + 'static,
    ) -> Self {
        Self { transform: Arc::new(transform) }
    }
    /// Differentiate affected keys against one old committed boundary.
    ///
    /// # Errors
    /// Rejects clocks, invalid deltas, resource excesses, callback and I/O failures.
    pub async fn evaluate<B: Borrow<Batch<K, V>> + Sync>(
        &self,
        input: &TimedBatch<B>,
        prior: &TraceSnapshot<K, V>,
        limits: Limits,
    ) -> Result<TimedBatch<Batch<K, V>>> {
        ensure!(prior.time().checked_add(1) == Some(input.time), "out-of-order group tick");
        limits.check_batch(input.batch.borrow())?;
        prior.validate_bag_delta(input.batch.borrow()).await?;
        let mut groups = BTreeMap::<K, Vec<(((), V), i64)>>::new();
        for ((key, value), weight) in input.batch.borrow().iter() {
            groups.entry(key.clone()).or_default().push((((), value.clone()), *weight));
        }
        let mut work = PartitionWork::new(limits.contributions);
        let mut output = Consolidator::new(limits)?;
        for (key, changes) in groups {
            work.charge(changes.len())?;
            let delta = Batch::from_updates(changes)?;
            let result =
                (self.transform)(GroupInput { key: &key, delta: &delta, prior, limits }, &mut work)
                    .await?;
            limits.check_batch(&result)?;
            for (((), value), weight) in result.iter() {
                output.add((key.clone(), value.clone()), *weight)?;
            }
        }
        Ok(TimedBatch { time: input.time, batch: output.finish_batch()? })
    }
}
