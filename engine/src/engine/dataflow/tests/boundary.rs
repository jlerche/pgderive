use super::{Arrangement, Graph, Join, Project, TimedBatch};
use crate::engine::{Batch, trace::TraceSnapshot};
use anyhow::{Result, ensure};
use object_store::memory::InMemory;
use std::sync::Arc;
type Rows = Batch<i32, i32>;
type State = (TraceSnapshot<i32, i32>, TraceSnapshot<i32, i32>, TraceSnapshot<i32, ()>);
type Input = (Rows, Rows, bool);
type Query = Graph<State, Input, Batch<i32, ()>>;
fn graph() -> Result<Query> {
    let store = Arc::new(InMemory::new());
    let left = Arrangement::new(store.clone(), "boundary-left".into(), 1)?;
    let right = Arrangement::new(store.clone(), "boundary-right".into(), 1)?;
    let output = Arrangement::new(store, "boundary-output".into(), 1)?;
    Ok(Graph::new(
        (left.empty(), right.empty(), output.empty()),
        move |prior: Arc<State>, input: TimedBatch<Input>| {
            let left = left.clone();
            let right = right.clone();
            let output = output.clone();
            async move {
                let a = TimedBatch { time: input.time, batch: input.batch.0 };
                let b = TimedBatch { time: input.time, batch: input.batch.1 };
                let joined = Join.evaluate(&a, &b, &prior.0, &prior.1).await?;
                let delta = Project::new(|key: &i32, (left, _): &(i32, i32)| {
                    Ok((*left >= 0).then_some((*key, ())))
                })
                .evaluate(&joined)?;
                let next = (
                    left.stage(&prior.0, &a).await?,
                    right.stage(&prior.1, &b).await?,
                    output.stage(&prior.2, &delta).await?,
                );
                ensure!(!input.batch.2, "failure after all nodes");
                Ok((next, delta.batch))
            }
        },
    ))
}
fn input(
    time: u64,
    left: impl IntoIterator<Item = ((i32, i32), i64)>,
    right: impl IntoIterator<Item = ((i32, i32), i64)>,
    fail: bool,
) -> Result<TimedBatch<Input>> {
    Ok(TimedBatch { time, batch: (Batch::from_updates(left)?, Batch::from_updates(right)?, fail) })
}
#[tokio::test]
async fn downstream_failure_rolls_back_entire_graph_and_retry() -> Result<()> {
    let mut circuit = graph()?;
    assert!(circuit.prepare(input(1, [((1, 1), 1)], [((1, 2), 1)], true)?).await.is_err());
    assert_eq!(circuit.snapshot().2.materialize().await?, Batch::from_updates([])?);
    assert_eq!(circuit.time(), 0);
    let first =
        circuit.commit(circuit.prepare(input(1, [((1, 1), 1)], [((1, 2), 1)], false)?).await?)?;
    assert_eq!(first.time, 1);
    assert_eq!(first.batch, Batch::from_updates([((1, ()), 1)])?);
    let empty = circuit.commit(circuit.prepare(input(2, [], [], false)?).await?)?;
    assert_eq!(empty.time, 2);
    assert_eq!(empty.batch, Batch::from_updates([])?);
    let delete = circuit.commit(circuit.prepare(input(3, [((1, 1), -1)], [], false)?).await?)?;
    assert_eq!(delete.batch, Batch::from_updates([((1, ()), -1)])?);
    Ok(())
}
#[tokio::test]
async fn projected_overflow_does_not_advance_join_or_output() -> Result<()> {
    let mut circuit = graph()?;
    assert!(
        circuit
            .prepare(input(1, [((1, 1), i64::MAX), ((1, 2), 1)], [((1, 1), 1)], false)?)
            .await
            .is_err()
    );
    assert_eq!(circuit.time(), 0);
    assert_eq!(circuit.snapshot().2.materialize().await?, Batch::from_updates([])?);
    let retry =
        circuit.commit(circuit.prepare(input(1, [((1, 1), 1)], [((1, 1), 1)], false)?).await?)?;
    assert_eq!(retry.batch, Batch::from_updates([((1, ()), 1)])?);
    Ok(())
}
#[tokio::test]
async fn tick_overflow_prevents_evaluation() -> Result<()> {
    let circuit = Graph::at_boundary(0, u64::MAX, |_, _: TimedBatch<()>| async { Ok((1, ())) });
    assert!(circuit.prepare(TimedBatch { time: 0, batch: () }).await.is_err());
    assert_eq!(*circuit.snapshot(), 0);
    assert_eq!(circuit.time(), u64::MAX);
    Ok(())
}
