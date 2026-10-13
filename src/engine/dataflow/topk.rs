//! Retractable ordered selection counting row occurrences, including cutoff weights.
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
type Selection<K, V> = dyn Fn(&K, &V, usize) -> Result<(K, V)> + Send + Sync;
/// Retractable first K row occurrences per key in physical codec order.
/// SQL callers bind explicit ordering and the codec to durable plan identity.
pub struct OrderedTopK<K: BatchData, V: BatchData> {
    limit: usize,
    select: Arc<Selection<K, V>>,
}
impl<K: BatchData, V: BatchData> OrderedTopK<K, V> {
    /// Bind a pure selection that preserves the input key and receives a
    /// one-based position for each selected occurrence.
    #[must_use]
    pub fn new(
        limit: usize,
        select: impl Fn(&K, &V, usize) -> Result<(K, V)> + Send + Sync + 'static,
    ) -> Self {
        Self { limit, select: Arc::new(select) }
    }
    /// Differentiate complete synchronized input changes against retained selection.
    /// Prior input is a nonnegative bag and retained output is the exact previous
    /// selection at the same boundary. Successful evaluation preserves this
    /// inductive contract. Cold restore validates its published lineage; unchanged
    /// unselected tails are not scanned. Every changed identity is validated before
    /// selection can stop early, including for a zero cutoff.
    ///
    /// # Errors
    /// Rejects clocks, invalid memberships, I/O, projection and resource failures.
    pub async fn evaluate(
        &self,
        input: &TimedBatch<Batch<K, V>>,
        prior: &TraceSnapshot<K, V>,
        output: &TraceSnapshot<K, V>,
        limits: Limits,
    ) -> Result<TimedBatch<Batch<K, V>>> {
        ensure!(
            prior.time() == output.time() && prior.time().checked_add(1) == Some(input.time),
            "out-of-order top-k tick"
        );
        limits.check_batch(&input.batch)?;
        prior.validate_bag_delta(&input.batch).await?;
        let mut groups = BTreeMap::<K, BTreeMap<V, i64>>::new();
        for ((key, value), weight) in input.batch.iter() {
            groups.entry(key.clone()).or_default().insert(value.clone(), *weight);
        }
        let mut progress =
            Progress { remaining: limits.contributions, changes: Consolidator::new(limits)? };
        for (key, delta) in groups {
            let mut cursor = output.key_cursor(&key).await?;
            let mut retained = 0usize;
            while let Some((_, value, weight)) = cursor.current() {
                charge(&mut progress.remaining)?;
                ensure!(weight > 0, "negative retained top-k membership");
                retained = retained
                    .checked_add(usize::try_from(weight)?)
                    .context("top-k retained count overflow")?;
                ensure!(retained <= self.limit, "retained top-k exceeds limit");
                progress.changes.add((key.clone(), value.clone()), -weight)?;
                cursor.advance().await?;
            }
            self.selected(prior, &key, &delta, &mut progress).await?;
        }
        Ok(TimedBatch { time: input.time, batch: progress.changes.finish_batch()? })
    }
    async fn selected(
        &self,
        prior: &TraceSnapshot<K, V>,
        key: &K,
        delta: &BTreeMap<V, i64>,
        progress: &mut Progress<K, V>,
    ) -> Result<()> {
        let mut cursor = prior.key_cursor(key).await?;
        let mut changed = delta.iter().peekable();
        let mut position = 0;
        while position < self.limit {
            charge(&mut progress.remaining)?;
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
                break;
            };
            let mut coefficient = BigInt::from(0);
            if old.as_ref().is_some_and(|entry| entry.0 == value) {
                let weight = old.as_ref().context("missing old top-k row")?.1;
                ensure!(weight > 0, "negative prior top-k membership");
                coefficient += weight;
                cursor.advance().await?;
            }
            if new.as_ref().is_some_and(|entry| entry.0 == value) {
                coefficient += new.as_ref().context("missing changed top-k row")?.1;
                changed.next();
            }
            ensure!(coefficient >= BigInt::from(0), "negative top-k membership");
            let count = usize::try_from(coefficient.min(BigInt::from(self.limit - position)))?;
            for _ in 0..count {
                charge(&mut progress.remaining)?;
                position += 1;
                let selected = (self.select)(key, &value, position)?;
                ensure!(&selected.0 == key, "top-k selection must preserve key");
                progress.changes.add(selected, 1)?;
            }
        }
        Ok(())
    }
}
struct Progress<K: BatchData, V: BatchData> {
    changes: Consolidator<(K, V)>,
    remaining: usize,
}
fn charge(work: &mut usize) -> Result<()> {
    *work = work.checked_sub(1).context("top-k tick work limit exceeded")?;
    Ok(())
}
