use super::TimedBatch;
use crate::engine::{
    Batch,
    execution::{Consolidator, Limits},
    reader::BatchData,
    trace::TraceSnapshot,
};
use anyhow::{Result, ensure};
use num_bigint::BigInt;
use std::borrow::Borrow;
type JoinWeights<K, L, R> = Consolidator<(K, (L, R))>;
type Joined<K, L, R> = Batch<K, (L, R)>;
type Inputs<'a, L, R> = (&'a TimedBatch<L>, &'a TimedBatch<R>);
type Priors<'a, K, L, R> = (&'a TraceSnapshot<K, L>, &'a TraceSnapshot<K, R>);

/// Stateless incremental equijoin; both arrangements are pinned to prior time.
#[derive(Debug, Default)]
pub struct Join;
impl Join {
    /// Evaluate all three transaction delta terms without publishing either input.
    ///
    /// # Errors
    /// Returns tick, read, integrity or finalized coefficient overflow errors.
    pub async fn evaluate<K: BatchData, L: BatchData, R: BatchData>(
        &self,
        left: &TimedBatch<Batch<K, L>>,
        right: &TimedBatch<Batch<K, R>>,
        prior_left: &TraceSnapshot<K, L>,
        prior_right: &TraceSnapshot<K, R>,
    ) -> Result<TimedBatch<Joined<K, L, R>>> {
        self.evaluate_with_limits((left, right), (prior_left, prior_right), Limits::default()).await
    }
    /// Evaluate with explicit limits; physical spills preserve exact cross terms.
    ///
    /// # Errors
    /// Returns clock, read, scratch, resource, or final arithmetic failures.
    pub async fn evaluate_with_limits<
        K: BatchData,
        L: BatchData,
        R: BatchData,
        LB: Borrow<Batch<K, L>> + Sync,
        RB: Borrow<Batch<K, R>> + Sync,
    >(
        &self,
        inputs: Inputs<'_, LB, RB>,
        priors: Priors<'_, K, L, R>,
        limits: Limits,
    ) -> Result<TimedBatch<Joined<K, L, R>>> {
        let (left, right) = inputs;
        let (prior_left, prior_right) = priors;
        ensure!(
            left.time == right.time
                && prior_left.time() == prior_right.time()
                && prior_left.time().checked_add(1) == Some(left.time),
            "inconsistent join boundary"
        );
        Ok(TimedBatch {
            time: left.time,
            batch: join_bounded(
                left.batch.borrow(),
                right.batch.borrow(),
                prior_left,
                prior_right,
                limits,
            )
            .await?,
        })
    }
}
pub(super) async fn join_batches<K: BatchData, L: BatchData, R: BatchData>(
    left: &Batch<K, L>,
    right: &Batch<K, R>,
    prior_left: &TraceSnapshot<K, L>,
    prior_right: &TraceSnapshot<K, R>,
) -> Result<Joined<K, L, R>> {
    join_bounded(left, right, prior_left, prior_right, Limits::default()).await
}
async fn join_bounded<K: BatchData, L: BatchData, R: BatchData>(
    left: &Batch<K, L>,
    right: &Batch<K, R>,
    prior_left: &TraceSnapshot<K, L>,
    prior_right: &TraceSnapshot<K, R>,
    limits: Limits,
) -> Result<Joined<K, L, R>> {
    limits.check_batch(left)?;
    limits.check_batch(right)?;
    let mut output = Consolidator::new(limits)?;
    prior_join(left, prior_right, &mut output, |left, right| (left.clone(), right.clone())).await?;
    prior_join(right, prior_left, &mut output, |right, left| (left.clone(), right.clone())).await?;
    cross(left, right, &mut output)?;
    output.finish_batch()
}

// Delta identities are ordered by key, then full value. Scan each selected prior
// key once, even when many delta values share it. No resident prior-key bag or
// extra trace state is retained; cloned iterators only borrow the bounded delta.
async fn prior_join<K: BatchData, D: BatchData, P: BatchData, L: BatchData, R: BatchData>(
    delta: &Batch<K, D>,
    prior: &TraceSnapshot<K, P>,
    output: &mut JoinWeights<K, L, R>,
    pair: impl Fn(&D, &P) -> (L, R),
) -> Result<()> {
    let mut entries = delta.iter().peekable();
    while let Some(((key, _), _)) = entries.peek().copied() {
        let group = entries.clone().take_while(|((candidate, _), _)| candidate == key);
        let mut cursor = prior.key_cursor(key).await?;
        while let Some((other_key, other, other_weight)) = cursor.current() {
            if other_key != key {
                break;
            }
            for ((_, value), weight) in group.clone() {
                output
                    .add((key.clone(), pair(value, other)), BigInt::from(*weight) * other_weight)?;
            }
            cursor.advance().await?;
        }
        while entries.peek().is_some_and(|((candidate, _), _)| candidate == key) {
            entries.next();
        }
    }
    Ok(())
}

fn cross<K: BatchData, L: BatchData, R: BatchData>(
    left: &Batch<K, L>,
    right: &Batch<K, R>,
    output: &mut JoinWeights<K, L, R>,
) -> Result<()> {
    let right = right.iter().collect::<Vec<_>>();
    for ((key, value), weight) in left.iter() {
        let start = right.partition_point(|((other, _), _)| other < key);
        let end = right.partition_point(|((other, _), _)| other <= key);
        for ((_, other), other_weight) in &right[start..end] {
            output.add(
                (key.clone(), (value.clone(), other.clone())),
                BigInt::from(*weight) * **other_weight,
            )?;
        }
    }
    Ok(())
}
