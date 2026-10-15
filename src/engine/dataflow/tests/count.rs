use super::{Arrangement, Graph, Statistics, TimedBatch};
use crate::engine::{Batch, execution::Limits, trace::TraceSnapshot};
use anyhow::{Context, Result, ensure};
use object_store::memory::InMemory;
use std::{collections::BTreeMap, sync::Arc};
type Input<K, V> = TimedBatch<(Batch<K, V>, bool)>;
type Counter<K, V> = Graph<TraceSnapshot<K, i64>, (Batch<K, V>, bool), Batch<K, i64>>;
fn counter<K: crate::engine::reader::BatchData, V: crate::engine::reader::BatchData>()
-> Result<Counter<K, V>> {
    let writer = Arrangement::new(Arc::new(InMemory::new()), "signed-count-v1".into(), 64)?;
    let operator = Arc::new(Statistics::with_measure(
        1,
        |_, _: &V| Ok(vec![1.into()]),
        |count: &i64| Ok(vec![(*count).into()]),
        |_: &K, values| {
            let count = i64::try_from(values[0].clone()).context("count overflow")?;
            Ok((count != 0).then_some(count))
        },
    ));
    Ok(Graph::new(writer.empty(), move |prior: Arc<TraceSnapshot<K, i64>>, input: Input<K, V>| {
        let writer = writer.clone();
        let operator = operator.clone();
        async move {
            let delta = operator
                .evaluate(
                    &TimedBatch { time: input.time, batch: input.batch.0 },
                    &prior,
                    Limits::default(),
                )
                .await?;
            let candidate = writer.stage(&prior, &delta).await?;
            ensure!(!input.batch.1, "downstream failure");
            Ok((candidate, delta.batch))
        }
    }))
}
fn tick<K: Ord + Clone, V: Ord + Clone>(
    time: u64,
    rows: impl IntoIterator<Item = ((K, V), i64)>,
) -> Result<Input<K, V>> {
    Ok(TimedBatch { time, batch: (Batch::from_updates(rows)?, false) })
}
#[tokio::test]
async fn groups_are_replaced_and_removed_with_signed_weights() -> Result<()> {
    let mut count = counter()?;
    let first = count.commit(
        count.prepare(tick(1, [((Some(1), "a".to_owned()), 3), ((None, "b".into()), 2)])?).await?,
    )?;
    assert_eq!(first.batch, Batch::from_updates([((Some(1), 3), 1), ((None, 2), 1)])?);
    let second = count.commit(
        count.prepare(tick(2, [((Some(1), "a".into()), -3), ((None, "b".into()), -1)])?).await?,
    )?;
    assert_eq!(
        second.batch,
        Batch::from_updates([((Some(1), 3), -1), ((None, 2), -1), ((None, 1), 1)])?
    );
    let unchanged = count.commit(
        count.prepare(tick(3, [((None, "x".into()), 1), ((None, "y".into()), -1)])?).await?,
    )?;
    assert_eq!(unchanged.batch, Batch::from_updates([])?);
    let signed = count.commit(count.prepare(tick(4, [((Some(2), "c".into()), -2)])?).await?)?;
    assert_eq!(signed.batch, Batch::from_updates([((Some(2), -2), 1)])?);
    Ok(())
}
#[tokio::test]
async fn signed_histories_match_independent_group_oracle() -> Result<()> {
    for seed in 1..=128_i64 {
        let mut count = counter()?;
        let mut totals = BTreeMap::<i64, i64>::new();
        for tick_index in 0..16_i64 {
            let raw = (0..8)
                .map(|n| (((n + tick_index) % 5, n), (seed + n + tick_index) % 5 - 2))
                .collect::<Vec<_>>();
            for ((key, _), weight) in &raw {
                *totals.entry(*key).or_default() += weight;
            }
            count.commit(count.prepare(tick(u64::try_from(tick_index)? + 1, raw)?).await?)?;
            let expected = Batch::from_updates(
                totals
                    .iter()
                    .filter(|(_, count)| **count != 0)
                    .map(|(key, count)| ((*key, *count), 1)),
            )?;
            assert_eq!(
                count.snapshot().materialize().await?,
                expected,
                "seed={seed}, tick={tick_index}"
            );
        }
    }
    Ok(())
}
#[tokio::test]
async fn group_overflow_and_circuit_failure_can_be_retried() -> Result<()> {
    let mut count = counter()?;
    count.commit(count.prepare(tick(1, [((1, 1), i64::MAX)])?).await?)?;
    let before = count.snapshot();
    assert!(count.prepare(tick(2, [((0, 0), 1), ((1, 1), 1)])?).await.is_err());
    assert!(Arc::ptr_eq(&count.snapshot(), &before));
    assert!(count.prepare(tick(2, [((2, 0), i64::MAX), ((2, 1), 1)])?).await.is_err());
    assert!(Arc::ptr_eq(&count.snapshot(), &before));
    let mut input = tick(2, [((1, 1), -1)])?;
    input.batch.1 = true;
    assert!(count.prepare(input).await.is_err());
    assert!(Arc::ptr_eq(&count.snapshot(), &before));
    let retried = count.commit(count.prepare(tick(2, [((1, 1), -1)])?).await?)?;
    assert_eq!(retried.time, 2);
    assert_eq!(retried.batch, Batch::from_updates([((1, i64::MAX), -1), ((1, i64::MAX - 1), 1)])?);
    Ok(())
}
#[tokio::test]
async fn count_narrows_after_combining_prior_state_and_delta() -> Result<()> {
    let mut count = counter()?;
    count.commit(count.prepare(tick(1, [((1, 0), i64::MIN)])?).await?)?;
    let output = count.commit(count.prepare(tick(2, [((1, 1), i64::MAX), ((1, 2), 1)])?).await?)?;
    assert_eq!(output.batch, Batch::from_updates([((1, i64::MIN), -1)])?);
    Ok(())
}
