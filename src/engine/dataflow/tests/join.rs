use super::{Arrangement, Graph, Join, TimedBatch};
use crate::engine::{Batch, ZSet, reader::BatchData, trace::TraceSnapshot};
use anyhow::Result;
use object_store::memory::InMemory;
use std::{collections::BTreeMap, sync::Arc};
type Tick<K, L, R> = TimedBatch<(Batch<K, L>, Batch<K, R>)>;
type Output = BTreeMap<(i64, (i64, i64)), i64>;
type State<K, L, R> = (TraceSnapshot<K, L>, TraceSnapshot<K, R>);
type Query<K, L, R> = Graph<State<K, L, R>, (Batch<K, L>, Batch<K, R>), Batch<K, (L, R)>>;
fn graph<K: BatchData, L: BatchData, R: BatchData>() -> Result<Query<K, L, R>> {
    let store = Arc::new(InMemory::new());
    let left = Arrangement::new(store.clone(), "join-left-v1".into(), 16)?;
    let right = Arrangement::new(store, "join-right-v1".into(), 16)?;
    Ok(Graph::new(
        (left.empty(), right.empty()),
        move |prior: Arc<State<K, L, R>>, input: Tick<K, L, R>| {
            let left = left.clone();
            let right = right.clone();
            async move {
                let a = TimedBatch { time: input.time, batch: input.batch.0 };
                let b = TimedBatch { time: input.time, batch: input.batch.1 };
                let joined = Join.evaluate(&a, &b, &prior.0, &prior.1).await?;
                Ok((
                    (left.stage(&prior.0, &a).await?, right.stage(&prior.1, &b).await?),
                    joined.batch,
                ))
            }
        },
    ))
}
fn tick<K: Ord + Clone, L: Ord + Clone, R: Ord + Clone>(
    time: u64,
    left: impl IntoIterator<Item = ((K, L), i64)>,
    right: impl IntoIterator<Item = ((K, R), i64)>,
) -> Result<Tick<K, L, R>> {
    Ok(TimedBatch { time, batch: (Batch::from_updates(left)?, Batch::from_updates(right)?) })
}
type Input = ((i64, i64), i64);
fn oracle(left: &[Input], right: &[Input]) -> Output {
    let mut output = BTreeMap::new();
    for ((key, left), lw) in left {
        for ((other, right), rw) in right {
            if key == other {
                *output.entry((*key, (*left, *right))).or_default() += lw * rw;
            }
        }
    }
    output.retain(|_, weight| *weight != 0);
    output
}
#[tokio::test]
async fn simultaneous_signed_inputs_match_independent_oracle() -> Result<()> {
    for seed in 1..=128_i64 {
        let mut engine = graph()?;
        let mut left_history = Vec::new();
        let mut right_history = Vec::new();
        let mut output = BTreeMap::new();
        for ordinal in 1..=16_i64 {
            let left = [((ordinal % 3, seed % 5), (ordinal + seed) % 5 - 2)];
            let right = [(((ordinal + seed) % 3, ordinal % 4), (ordinal * seed) % 5 - 2)];
            let step = engine
                .commit(engine.prepare(tick(u64::try_from(ordinal)?, left, right)?).await?)?;
            assert_eq!(step.time, u64::try_from(ordinal)?);
            for (tuple, weight) in step.batch.iter() {
                *output.entry(*tuple).or_default() += weight;
            }
            output.retain(|_, weight| *weight != 0);
            left_history.extend(left);
            right_history.extend(right);
            assert_eq!(
                output,
                oracle(&left_history, &right_history),
                "seed={seed}, tick={ordinal}"
            );
        }
    }
    Ok(())
}
#[tokio::test]
async fn multiplicity_cross_term_and_full_tuple_retraction() -> Result<()> {
    let mut engine = graph()?;
    let first = engine.commit(
        engine
            .prepare(tick(1, [((1, "old".to_owned()), 3)], [((1, "right".to_owned()), 2)])?)
            .await?,
    )?;
    assert_eq!(first.batch, Batch::from_updates([((1, ("old".into(), "right".into())), 6)])?);
    let second = engine.commit(
        engine
            .prepare(tick(
                2,
                [((1, "old".into()), -3), ((1, "new".into()), 3)],
                [((1, "right".into()), -2), ((1, "other".into()), 2)],
            )?)
            .await?,
    )?;
    assert_eq!(
        second.batch,
        Batch::from_updates([
            ((1, ("old".into(), "right".into())), -6),
            ((1, ("new".into(), "other".into())), 6)
        ])?
    );
    Ok(())
}
#[tokio::test]
async fn overflow_keeps_state_and_time_unchanged() -> Result<()> {
    let mut engine = graph::<i32, i32, i32>()?;
    engine.commit(engine.prepare(tick(1, [((1, 1), i64::MAX)], [])?).await?)?;
    let before = engine.snapshot();
    assert!(engine.prepare(tick(2, [], [((1, 1), 2)])?).await.is_err());
    assert!(engine.prepare(tick(2, [((1, 1), 1)], [])?).await.is_err());
    assert!(Arc::ptr_eq(&engine.snapshot(), &before));
    assert_eq!(engine.time(), 1);
    let empty = engine.commit(engine.prepare(tick(2, [], [])?).await?)?;
    assert_eq!(empty.time, 2);
    let overflow = Graph::at_boundary((), u64::MAX, |_, _: TimedBatch<()>| async { Ok(((), ())) });
    assert!(overflow.prepare(TimedBatch { time: 0, batch: () }).await.is_err());
    assert_eq!(overflow.time(), u64::MAX);
    Ok(())
}
#[test]
fn integration_overflow_is_atomic() -> Result<()> {
    let mut state = ZSet::from_updates([(1, i64::MAX)])?;
    let before = state.clone();
    assert!(state.apply(&ZSet::from_updates([(0, 1), (1, 1)])?).is_err());
    assert_eq!(state, before);
    state.apply(&ZSet::from_updates([(1, -i64::MAX)])?)?;
    assert_eq!(state, ZSet::default());
    Ok(())
}
#[tokio::test]
async fn failed_right_integration_does_not_commit_staged_left() -> Result<()> {
    let mut engine = graph::<i32, i32, i32>()?;
    engine.commit(engine.prepare(tick(1, [], [((1, 1), i64::MAX)])?).await?)?;
    let before = engine.snapshot();
    assert!(engine.prepare(tick(2, [((2, 2), 1)], [((1, 1), 1)])?).await.is_err());
    assert!(Arc::ptr_eq(&engine.snapshot(), &before));
    assert_eq!(engine.time(), 1);
    let recovered = engine.commit(engine.prepare(tick(2, [((1, 3), 1)], [])?).await?)?;
    assert_eq!(recovered.time, 2);
    assert_eq!(recovered.batch, Batch::from_updates([((1, (3, 1)), i64::MAX)])?);
    Ok(())
}
#[tokio::test]
async fn join_narrows_after_all_cross_terms() -> Result<()> {
    let mut engine = graph::<i32, i32, i32>()?;
    engine.commit(engine.prepare(tick(1, [((1, 1), i64::MAX)], [])?).await?)?;
    let step =
        engine.commit(engine.prepare(tick(2, [((1, 1), 1 - i64::MAX)], [((1, 1), 2)])?).await?)?;
    assert_eq!(step.batch, Batch::from_updates([((1, (1, 1)), 2)])?);
    Ok(())
}
