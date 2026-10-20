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
    pub(super) const fn new(remaining: usize) -> Self {
        Self { remaining }
    }
    #[cfg(test)]
    pub(crate) const fn for_test(remaining: usize) -> Self {
        Self { remaining }
    }
    #[cfg(test)]
    pub(crate) const fn remaining_for_test(&self) -> usize {
        self.remaining
    }
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
#[derive(Clone)]
pub struct Partition<K: BatchData, V: BatchData> {
    transform: Arc<Transform<K, V>>,
    zero_preserving: bool,
}
impl<K: BatchData, V: BatchData> Partition<K, V> {
    /// Bind a deterministic partition evaluator; output must be a nonnegative bag.
    pub fn new(
        transform: impl Fn(&K, &Batch<(), V>, &mut PartitionWork) -> Result<Batch<(), V>>
        + Send
        + Sync
        + 'static,
    ) -> Self {
        Self { transform: Arc::new(transform), zero_preserving: false }
    }
    /// Bind a partition evaluator with empty input mapped to empty output.
    /// The supplied callback is never invoked for an empty bag. This contract
    /// enables exact output retention; it is unsuitable for unseeded global aggregates.
    pub fn new_zero_preserving(
        transform: impl Fn(&K, &Batch<(), V>, &mut PartitionWork) -> Result<Batch<(), V>>
        + Send
        + Sync
        + 'static,
    ) -> Self {
        Self {
            transform: Arc::new(move |key, rows, work| {
                if rows.iter().next().is_none() {
                    Batch::from_updates([])
                } else {
                    transform(key, rows, work)
                }
            }),
            zero_preserving: true,
        }
    }
    /// Whether this operator enforces the empty-input contract for retained output.
    #[must_use]
    pub const fn supports_retained_output(&self) -> bool {
        self.zero_preserving
    }
    /// Evaluate new affected partitions once and subtract their retained output.
    /// Input and output must come from the same prior logical boundary; output
    /// retains the operator's full tuples and partition keys before projection.
    ///
    /// # Errors
    /// Rejects generic callbacks, inconsistent clocks/bags, reads and limits.
    pub async fn evaluate_retained(
        &self,
        input: &TimedBatch<Batch<K, V>>,
        prior: &TraceSnapshot<K, V>,
        output: &TraceSnapshot<K, V>,
        limits: Limits,
    ) -> Result<TimedBatch<Batch<K, V>>> {
        ensure!(self.zero_preserving, "retained partition requires zero-preserving callback");
        ensure!(prior.time() == output.time(), "partition prior trace times differ");
        self.evaluate_with_output(input, prior, Some(output), limits).await
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
        self.evaluate_with_output(input, prior, None, limits).await
    }
    async fn evaluate_with_output(
        &self,
        input: &TimedBatch<Batch<K, V>>,
        prior: &TraceSnapshot<K, V>,
        retained: Option<&TraceSnapshot<K, V>>,
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
            let before = read(prior, &key, limits, &mut work).await?;
            work.charge(changes.len())?;
            let after = Batch::from_updates(
                before.iter().map(|(tuple, weight)| (tuple.clone(), *weight)).chain(changes),
            )?;
            limits.check_batch(&after)?;
            ensure!(
                after.iter().all(|(_, weight)| *weight > 0),
                "negative partition row multiplicity"
            );
            let old_output = if let Some(retained) = retained {
                let output = read(retained, &key, limits, &mut work).await?;
                ensure!(
                    before.iter().next().is_some() || output.iter().next().is_none(),
                    "retained output for empty partition"
                );
                output
            } else {
                (self.transform)(&key, &before, &mut work)?
            };
            let new_output = (self.transform)(&key, &after, &mut work)?;
            for output in [&old_output, &new_output] {
                limits.check_batch(output)?;
                ensure!(
                    output.iter().all(|(_, weight)| *weight > 0),
                    "negative partition output multiplicity"
                );
            }
            if retained.is_some() {
                work.charge(old_output.iter().count())?;
                work.charge(new_output.iter().count())?;
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
    work: &mut PartitionWork,
) -> Result<Batch<(), V>> {
    let mut cursor = prior.key_cursor(key).await?;
    let mut rows = BTreeMap::new();
    let mut bytes = 0_u64;
    while let Some((_, value, weight)) = cursor.current() {
        work.charge(1)?;
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
