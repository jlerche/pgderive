use super::Snapshot;
use crate::engine::{
    Batch, BatchBuilder,
    reader::{BatchData, BatchReader},
    trace::TraceSnapshot,
    weights::Accumulator,
};
use anyhow::{Result, ensure};
use num_bigint::BigInt;
use std::collections::{BTreeMap, BTreeSet};

type JoinWeights<K, L, R> = Accumulator<(K, (L, R))>;

type Joined<K, L, R> = Batch<K, (L, R)>;

pub(super) async fn join<K: BatchData, L: BatchData, R: BatchData, G: BatchData>(
    left: &Batch<K, L>,
    right: &Batch<K, R>,
    prior: &Snapshot<K, L, R, G>,
) -> Result<Joined<K, L, R>> {
    let mut output = Accumulator::default();
    let mut right_cursor = prior.right.cursor().await?;
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
    let mut left_cursor = prior.left.cursor().await?;
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

pub(super) async fn count<K: BatchData, L: BatchData, R: BatchData, G: BatchData>(
    joined: &Joined<K, L, R>,
    prior: &TraceSnapshot<G, i64>,
    group: impl Fn(&K, &L, &R) -> Result<Option<G>>,
) -> Result<Batch<G, i64>> {
    let mut totals = Accumulator::default();
    let mut affected = BTreeSet::new();
    for ((key, (left, right)), weight) in joined.iter() {
        if let Some(group) = group(key, left, right)? {
            affected.insert(group.clone());
            totals.add(group, *weight);
        }
    }
    let mut old = BTreeMap::new();
    let mut cursor = prior.cursor().await?;
    for key in &affected {
        cursor.seek_key(key).await?;
        if let Some((group, count, weight)) = cursor.current()
            && group == key
        {
            ensure!(weight == 1 && *count != 0, "invalid grouped count arrangement");
            old.insert(key.clone(), *count);
            totals.add(key.clone(), *count);
            cursor.advance().await?;
            ensure!(
                cursor.current().is_none_or(|(group, _, _)| group != key),
                "multiple count rows for group"
            );
        }
    }
    let next = totals.finish()?;
    let mut output = BatchBuilder::default();
    for key in affected {
        let before = old.get(&key).copied().unwrap_or_default();
        let after = next.get(&key).copied().unwrap_or_default();
        if before == after {
            continue;
        }
        if before != 0 {
            output.push(key.clone(), before, -1);
        }
        if after != 0 {
            output.push(key, after, 1);
        }
    }
    output.finish()
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
