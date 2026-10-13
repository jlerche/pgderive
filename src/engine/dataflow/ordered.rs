//! Retractable first-value selection over immutable full-identity ordered runs.
use super::{Project, TimedBatch};
use crate::engine::{
    Batch,
    execution::{Consolidator, Limits},
    reader::BatchData,
    trace::TraceSnapshot,
};
use anyhow::{Context, Result, ensure};
use std::collections::BTreeMap;
/// Select the first positive full identity per key in the codec's physical order.
///
/// SQL callers explicitly encode their order and bind that codec to plan identity.
/// Every changed identity is validated before early termination is permitted.
pub struct OrderedFirst<K: BatchData, V: BatchData> {
    project: Project<K, V, K, V>,
}
impl<K: BatchData, V: BatchData> OrderedFirst<K, V> {
    /// Bind a pure selected-value projection. It must preserve the input key.
    #[must_use]
    pub const fn new(project: Project<K, V, K, V>) -> Self {
        Self { project }
    }
    /// Merge the complete delta with ordered prior heads, stopping at the first
    /// surviving identity. Deleted prefixes and cancelling runs may require scans.
    /// The prior input must already be a nonnegative integrated bag and output
    /// its exact previous selection at the same boundary. Successful evaluation
    /// preserves this inductive contract; authoritative cold restore must retain
    /// the validated published lineage. Untouched tails are deliberately not scanned.
    ///
    /// # Errors
    /// Rejects clocks, negative changed identities or visited prior entries,
    /// invalid retained output multiplicities, I/O and resource errors.
    pub async fn evaluate(
        &self,
        input: &TimedBatch<Batch<K, V>>,
        prior: &TraceSnapshot<K, V>,
        output: &TraceSnapshot<K, V>,
        limits: Limits,
    ) -> Result<TimedBatch<Batch<K, V>>> {
        ensure!(
            prior.time() == output.time() && prior.time().checked_add(1) == Some(input.time),
            "out-of-order ordered tick"
        );
        limits.check_batch(&input.batch)?;
        prior.validate_bag_delta(&input.batch).await?;
        let mut groups = BTreeMap::<K, BTreeMap<V, i64>>::new();
        for ((key, value), weight) in input.batch.iter() {
            groups.entry(key.clone()).or_default().insert(value.clone(), *weight);
        }
        let mut work = limits.contributions;
        let mut changes = Consolidator::new(limits)?;
        for (key, delta) in groups {
            let before = retained(output, &key).await?;
            let selected = first(prior, &key, &delta, &mut work).await?;
            let selected =
                Batch::from_updates(selected.into_iter().map(|value| ((key.clone(), value), 1)))?;
            let after = self
                .project
                .evaluate_with_limits(&TimedBatch { time: input.time, batch: selected }, limits)?
                .batch;
            ensure!(
                after.iter().count() <= 1
                    && after.iter().all(|((selected, _), weight)| selected == &key && *weight == 1),
                "ordered projection must preserve key and unit membership"
            );
            for ((_, value), weight) in before.iter() {
                changes.add((key.clone(), value.clone()), -*weight)?;
            }
            for (tuple, weight) in after.iter() {
                changes.add(tuple.clone(), *weight)?;
            }
        }
        Ok(TimedBatch { time: input.time, batch: changes.finish_batch()? })
    }
}
async fn retained<K: BatchData, V: BatchData>(
    trace: &TraceSnapshot<K, V>,
    key: &K,
) -> Result<Batch<K, V>> {
    let mut cursor = trace.key_cursor(key).await?;
    let mut rows = Vec::new();
    if let Some((_, value, weight)) = cursor.current() {
        ensure!(weight == 1, "ordered retained output multiplicity");
        rows.push(((key.clone(), value.clone()), weight));
        cursor.advance().await?;
        ensure!(cursor.current().is_none(), "multiple ordered retained outputs");
    }
    Batch::from_updates(rows)
}
async fn first<K: BatchData, V: BatchData>(
    prior: &TraceSnapshot<K, V>,
    key: &K,
    delta: &BTreeMap<V, i64>,
    work: &mut usize,
) -> Result<Option<V>> {
    let mut cursor = prior.key_cursor(key).await?;
    let mut changed = delta.iter().peekable();
    loop {
        *work = work.checked_sub(1).context("ordered tick work limit exceeded")?;
        let old = cursor.current().map(|(_, value, weight)| (value.clone(), weight));
        let new = changed.peek().map(|(value, weight)| ((*value).clone(), **weight));
        let Some(value) = old
            .as_ref()
            .map(|entry| &entry.0)
            .into_iter()
            .chain(new.as_ref().map(|entry| &entry.0))
            .min()
            .cloned()
        else {
            return Ok(None);
        };
        let mut weight = 0_i64;
        if let Some((old_value, old_weight)) = &old {
            ensure!(*old_weight > 0, "negative ordered prior multiplicity");
            if old_value == &value {
                weight = *old_weight;
            }
        }
        if let Some((new_value, new_weight)) = &new
            && new_value == &value
        {
            weight = weight.checked_add(*new_weight).context("ordered coefficient overflow")?;
            changed.next();
        }
        ensure!(weight >= 0, "negative ordered candidate multiplicity");
        if weight > 0 {
            return Ok(Some(value));
        }
        if old.as_ref().is_some_and(|entry| entry.0 == value) {
            cursor.advance().await?;
        }
    }
}
