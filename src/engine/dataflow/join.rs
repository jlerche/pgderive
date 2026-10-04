use super::Stream;
use crate::engine::{
    Batch,
    reader::{BatchData, BatchReader},
    trace::TraceSnapshot,
    weights::Accumulator,
};
use anyhow::{Result, ensure};
use num_bigint::BigInt;
type JoinWeights<K, L, R> = Accumulator<(K, (L, R))>;
type Joined<K, L, R> = Batch<K, (L, R)>;

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
        left: &Stream<Batch<K, L>>,
        right: &Stream<Batch<K, R>>,
        prior_left: &TraceSnapshot<K, L>,
        prior_right: &TraceSnapshot<K, R>,
    ) -> Result<Stream<Joined<K, L, R>>> {
        ensure!(
            left.time == right.time
                && prior_left.time() == prior_right.time()
                && prior_left.time().checked_add(1) == Some(left.time),
            "inconsistent join boundary"
        );
        Ok(Stream {
            time: left.time,
            batch: join_batches(&left.batch, &right.batch, prior_left, prior_right).await?,
        })
    }
}
pub(super) async fn join_batches<K: BatchData, L: BatchData, R: BatchData>(
    left: &Batch<K, L>,
    right: &Batch<K, R>,
    prior_left: &TraceSnapshot<K, L>,
    prior_right: &TraceSnapshot<K, R>,
) -> Result<Joined<K, L, R>> {
    let mut output = Accumulator::default();
    let mut right_cursor = prior_right.cursor().await?;
    for ((key, value), weight) in left.iter() {
        right_cursor.seek_key(key).await?;
        while let Some((other_key, other, other_weight)) = right_cursor.current() {
            if other_key != key {
                break;
            }
            output.add(
                (key.clone(), (value.clone(), other.clone())),
                BigInt::from(*weight) * other_weight,
            );
            right_cursor.advance().await?;
        }
    }
    let mut left_cursor = prior_left.cursor().await?;
    for ((key, value), weight) in right.iter() {
        left_cursor.seek_key(key).await?;
        while let Some((other_key, other, other_weight)) = left_cursor.current() {
            if other_key != key {
                break;
            }
            output.add(
                (key.clone(), (other.clone(), value.clone())),
                BigInt::from(*weight) * other_weight,
            );
            left_cursor.advance().await?;
        }
    }
    cross(left, right, &mut output);
    Batch::from_updates(output.finish()?)
}

fn cross<K: BatchData, L: BatchData, R: BatchData>(
    left: &Batch<K, L>,
    right: &Batch<K, R>,
    output: &mut JoinWeights<K, L, R>,
) {
    for ((key, value), weight) in left.iter() {
        for ((other_key, other), other_weight) in right.iter() {
            if key == other_key {
                output.add(
                    (key.clone(), (value.clone(), other.clone())),
                    BigInt::from(*weight) * other_weight,
                );
            }
        }
    }
}
