use super::{Arrangement, GroupSum, Stream, SumState};
use crate::engine::{Batch, ZSet};
use anyhow::{Result, bail};
use object_store::memory::InMemory;
use std::{collections::BTreeMap, sync::Arc};
type Value = (i64, Option<i64>);
type Updates = Vec<((i64, Value), i64)>;
fn edge(time: u64, updates: Updates) -> Result<Stream<Batch<i64, Value>>> {
    Ok(Stream { time, batch: Batch::from_updates(updates)? })
}
#[tokio::test]
async fn zero_null_empty_and_duplicate_groups_have_distinct_semantics() -> Result<()> {
    let writer = Arrangement::new(Arc::new(InMemory::new()), "sum-statistics-v1".into(), 1)?;
    let sum = GroupSum::new(|_: &i64, value: &Value| Ok(value.1));
    let mut state = writer.empty();
    let mut output = ZSet::default();
    let transactions = [
        vec![((0, (1, Some(5))), 2), ((0, (2, Some(-10))), 1), ((1, (3, None)), 1)],
        vec![((0, (1, Some(5))), -2), ((0, (2, Some(-10))), -1)],
        vec![((1, (3, None)), -1), ((1, (3, Some(0))), 1)],
        vec![((1, (3, Some(0))), -1)],
    ];
    let expected = [
        vec![((0, Some(0)), 1), ((1, None), 1)],
        vec![((1, None), 1)],
        vec![((1, Some(0)), 1)],
        vec![],
    ];
    for (ordinal, updates) in transactions.into_iter().enumerate() {
        let input = edge(u64::try_from(ordinal)? + 1, updates)?;
        let delta = sum.evaluate(&input, &state).await?;
        let pinned = state.clone();
        let next = writer.stage(&state, &delta.state).await?;
        output.apply(&ZSet::from_updates(delta.output.batch.iter().map(|(row, w)| (*row, *w)))?)?;
        state = next;
        assert_eq!(output, ZSet::from_updates(expected[ordinal].clone())?);
        assert_eq!(pinned.time(), u64::try_from(ordinal)?);
    }
    assert_eq!(state.materialize().await?, Batch::from_updates([])?);
    Ok(())
}
#[tokio::test]
async fn exact_products_prior_cancellation_and_failures_preserve_snapshot() -> Result<()> {
    let writer = Arrangement::new(Arc::new(InMemory::new()), "boundary-statistics-v1".into(), 1)?;
    let sum = GroupSum::new(|_: &i64, value: &Value| Ok(value.1));
    let initial = edge(1, vec![((0, (0, Some(i64::MIN))), 1)])?;
    let first = sum.evaluate(&initial, &writer.empty()).await?;
    let state = writer.stage(&writer.empty(), &first.state).await?;
    let next = edge(2, vec![((0, (1, Some(i64::MAX))), 1), ((0, (2, Some(1))), 1)])?;
    let delta = sum.evaluate(&next, &state).await?;
    assert_eq!(
        delta.output.batch,
        Batch::from_updates([((0, Some(i64::MIN)), -1), ((0, Some(0)), 1)])?
    );
    let state = writer.stage(&state, &delta.state).await?;
    let too_large = edge(3, vec![((0, (3, Some(i64::MAX))), 2)])?;
    assert!(sum.evaluate(&too_large, &state).await.is_err());
    assert_eq!(state.time(), 2);
    let invalid = edge(3, vec![((1, (1, Some(1))), -1)])?;
    assert!(sum.evaluate(&invalid, &state).await.is_err());
    assert!(sum.evaluate(&edge(2, vec![])?, &state).await.is_err());
    let failing = GroupSum::new(|_: &i64, _: &Value| bail!("measure failed"));
    assert!(failing.evaluate(&edge(3, vec![((0, (4, None)), 1)])?, &state).await.is_err());
    let large = edge(
        1,
        vec![((0, (0, Some(i64::MAX))), i64::MAX / 2), ((0, (1, Some(-i64::MAX))), i64::MAX / 2)],
    )?;
    let delta = sum.evaluate(&large, &writer.empty()).await?;
    assert_eq!(
        delta.state.batch,
        Batch::from_updates([(
            (0, SumState { rows: i64::MAX - 1, non_null: i64::MAX - 1, sum: 0 }),
            1
        )])?
    );
    Ok(())
}
#[tokio::test]
async fn object_sum_histories_match_independent_source_bag() -> Result<()> {
    let writer = Arrangement::new(Arc::new(InMemory::new()), "bag-statistics-v1".into(), 2)?;
    let sum = GroupSum::new(|_: &i64, value: &Value| Ok(value.1));
    let mut state = writer.empty();
    let mut output = ZSet::default();
    let mut source = BTreeMap::<i64, (i64, Value)>::new();
    for time in 1_i64..=64 {
        let id = time % 11;
        let mut updates = Vec::new();
        if let Some(old) = source.remove(&id) {
            updates.push((old, -1));
        }
        if time % 4 != 0 {
            let value = (time % 3, (id, (time % 5 != 0).then_some(time % 9 - 4)));
            updates.push((value, 1));
            source.insert(id, value);
        }
        let delta = sum.evaluate(&edge(u64::try_from(time)?, updates)?, &state).await?;
        state = writer.stage(&state, &delta.state).await?;
        output.apply(&ZSet::from_updates(delta.output.batch.iter().map(|(row, w)| (*row, *w)))?)?;
        let mut oracle = BTreeMap::<i64, (i64, i64, i64)>::new();
        for (group, (_, value)) in source.values() {
            let accumulated = oracle.entry(*group).or_default();
            accumulated.0 += 1;
            if let Some(value) = value {
                accumulated.1 += 1;
                accumulated.2 += value;
            }
        }
        let expected = ZSet::from_updates(
            oracle.iter().map(|(key, (_, count, sum))| ((*key, (*count != 0).then_some(*sum)), 1)),
        )?;
        assert_eq!(output, expected, "tick {time}");
        assert_eq!(
            state.materialize().await?,
            Batch::from_updates(
                oracle.into_iter().map(|(key, (rows, non_null, sum))| (
                    (key, SumState { rows, non_null, sum }),
                    1
                ))
            )?
        );
    }
    Ok(())
}
