//! Affected-partition transformation against pinned full-tuple bags.
use super::TimedBatch;
use crate::engine::{
    Batch,
    execution::{Consolidator, Limits},
    reader::BatchData,
    trace::TraceSnapshot,
};
use anyhow::{Context, Result, ensure};
use std::{collections::BTreeMap, sync::Arc};
/// Per-tick record/evaluation work budget shared across affected partitions.
pub struct PartitionWork {
    remaining: usize,
}
impl PartitionWork {
    /// Charge record visits or scalar aggregate evaluations before doing the work.
    ///
    /// # Errors
    /// Rejects work exceeding the configured complete operator-tick budget.
    pub fn charge(&mut self, amount: usize) -> Result<()> {
        self.remaining =
            self.remaining.checked_sub(amount).context("partition tick work limit exceeded")?;
        Ok(())
    }
}
type Transform<K, V> =
    dyn Fn(&K, &Batch<(), V>, &mut PartitionWork) -> Result<Batch<(), V>> + Send + Sync;
/// Pure group transformation, differentiated at each complete input boundary.
///
/// The callback receives row occurrences as weighted full-tuple identities.
/// Callback semantics and codecs must be included in the owning plan revision.
pub struct Partition<K: BatchData, V: BatchData> {
    transform: Arc<Transform<K, V>>,
}
impl<K: BatchData, V: BatchData> Partition<K, V> {
    /// Bind a deterministic partition evaluator; output must be a nonnegative bag.
    pub fn new(
        transform: impl Fn(&K, &Batch<(), V>, &mut PartitionWork) -> Result<Batch<(), V>>
        + Send
        + Sync
        + 'static,
    ) -> Self {
        Self { transform: Arc::new(transform) }
    }
    /// Recompute only affected partitions and emit new output minus prior output.
    ///
    /// # Errors
    /// Rejects invalid ticks/bags, resource limits, callback or trace read failures.
    pub async fn evaluate(
        &self,
        input: &TimedBatch<Batch<K, V>>,
        prior: &TraceSnapshot<K, V>,
        limits: Limits,
    ) -> Result<TimedBatch<Batch<K, V>>> {
        ensure!(prior.time().checked_add(1) == Some(input.time), "out-of-order partition tick");
        limits.check_batch(&input.batch)?;
        let mut changes = BTreeMap::<K, Vec<(((), V), i64)>>::new();
        for ((key, row), weight) in input.batch.iter() {
            changes.entry(key.clone()).or_default().push((((), row.clone()), *weight));
        }
        let mut work = PartitionWork { remaining: limits.contributions };
        let mut updates = Consolidator::new(limits)?;
        for (key, changes) in changes {
            let before = read(prior, &key, limits).await?;
            work.charge(before.iter().count() + changes.len())?;
            let after = Batch::from_updates(
                before.iter().map(|(tuple, weight)| (tuple.clone(), *weight)).chain(changes),
            )?;
            limits.check_batch(&after)?;
            ensure!(
                after.iter().all(|(_, weight)| *weight > 0),
                "negative partition row multiplicity"
            );
            let old_output = (self.transform)(&key, &before, &mut work)?;
            let new_output = (self.transform)(&key, &after, &mut work)?;
            for output in [&old_output, &new_output] {
                limits.check_batch(output)?;
                ensure!(
                    output.iter().all(|(_, weight)| *weight > 0),
                    "negative partition output multiplicity"
                );
            }
            for (((), value), weight) in old_output.iter() {
                updates.add((key.clone(), value.clone()), -*weight)?;
            }
            for (((), value), weight) in new_output.iter() {
                updates.add((key.clone(), value.clone()), *weight)?;
            }
        }
        let batch = updates.finish_batch()?;
        limits.check_batch(&batch)?;
        Ok(TimedBatch { time: input.time, batch })
    }
}
async fn read<K: BatchData, V: BatchData>(
    prior: &TraceSnapshot<K, V>,
    key: &K,
    limits: Limits,
) -> Result<Batch<(), V>> {
    let mut cursor = prior.key_cursor(key).await?;
    let mut rows = BTreeMap::new();
    let mut bytes = 0_u64;
    while let Some((_, value, weight)) = cursor.current() {
        ensure!(weight > 0, "negative prior partition multiplicity");
        ensure!(
            rows.len() < limits.output_entries && rows.len() < limits.contributions,
            "partition entry limit exceeded"
        );
        bytes = bytes
            .checked_add(u64::try_from(crate::engine::execution::record_size(
                &(((), value), weight),
                limits.record_bytes,
            )?)?)
            .context("partition byte overflow")?;
        ensure!(bytes <= limits.output_bytes, "partition byte limit exceeded");
        rows.insert(((), value.clone()), weight);
        cursor.advance().await?;
    }
    Batch::from_updates(rows)
}
