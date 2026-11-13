use super::{Arrangement, Graph, Join, Project, TimedBatch};
use crate::engine::{Batch, trace::TraceSnapshot};
use anyhow::{Result, bail};
use object_store::memory::InMemory;
use std::sync::Arc;
type Updates = Vec<((i64, i64), i64)>;
type Rows = Batch<i64, i64>;
type Inputs = (Rows, Rows);
type State = (TraceSnapshot<i64, i64>, TraceSnapshot<i64, i64>, TraceSnapshot<i64, (i64, i64)>);
type Runtime = Graph<State, Inputs, Batch<i64, (i64, i64)>>;
fn graph() -> Result<Runtime> {
    let store = Arc::new(InMemory::new());
    let left = Arrangement::new(store.clone(), "graph-left-v1".into(), 1)?;
    let right = Arrangement::new(store.clone(), "graph-right-v1".into(), 1)?;
    let output = Arrangement::new(store, "graph-joined-v1".into(), 1)?;
    let initial = (left.empty(), right.empty(), output.empty());
    let writers = Arc::new((left, right, output));
    let projection = Arc::new(Project::new(|key: &i64, value: &i64| {
        if *value == 99 {
            bail!("projection failure");
        }
        Ok(Some((*key, *value % 2)))
    }));
    Ok(Graph::new(initial, move |state: Arc<State>, input: TimedBatch<Inputs>| {
        let writers = writers.clone();
        let projection = projection.clone();
        async move {
            let left =
                projection.evaluate(&TimedBatch { time: input.time, batch: input.batch.0 })?;
            let right = TimedBatch { time: input.time, batch: input.batch.1 };
            let joined = Join.evaluate(&left, &right, &state.0, &state.1).await?;
            let next_left = writers.0.stage(&state.0, &left).await?;
            let next_right = writers.1.stage(&state.1, &right).await?;
            let next_output = writers.2.stage(&state.2, &joined).await?;
            Ok(((next_left, next_right, next_output), joined.batch))
        }
    }))
}
fn input(time: u64, left: Updates, right: Updates) -> Result<TimedBatch<Inputs>> {
    Ok(TimedBatch { time, batch: (Batch::from_updates(left)?, Batch::from_updates(right)?) })
}
#[tokio::test]
async fn typed_project_join_graph_publishes_every_arrangement_together() -> Result<()> {
    let mut graph = graph()?;
    let first =
        input(1, vec![((1, 2), 1), ((1, 4), 2), ((1, 3), 1)], vec![((1, 8), 2), ((1, 9), 1)])?;
    let prepared = graph.prepare(first).await?;
    let pinned = graph.snapshot();
    assert_eq!(graph.time(), 0);
    graph.validate_prepared(&prepared)?;
    assert_eq!(prepared.base_time(), 0);
    assert_eq!(prepared.candidate().2.time(), 1);
    let staged_output = prepared.output().batch.clone();
    let result = graph.commit(prepared)?;
    assert_eq!(result.batch, staged_output);
    assert_eq!(
        result.batch,
        Batch::from_updates([
            ((1, (0, 8)), 6),
            ((1, (0, 9)), 3),
            ((1, (1, 8)), 2),
            ((1, (1, 9)), 1)
        ])?
    );
    assert_eq!(pinned.0.time(), 0);
    let change = input(2, vec![((1, 2), -1)], vec![((1, 8), -1)])?;
    graph.commit(graph.prepare(change).await?)?;
    let snapshot = graph.snapshot();
    assert_eq!(
        snapshot.2.materialize().await?,
        Batch::from_updates([
            ((1, (0, 8)), 2),
            ((1, (0, 9)), 2),
            ((1, (1, 8)), 1),
            ((1, (1, 9)), 1)
        ])?
    );
    let before = graph.snapshot();
    assert!(graph.prepare(input(3, vec![((1, 99), 1)], vec![])?).await.is_err());
    assert!(Arc::ptr_eq(&before, &graph.snapshot()));
    graph.commit(graph.prepare(input(3, vec![], vec![])?).await?)?;
    assert_eq!(graph.time(), 3);
    Ok(())
}
#[tokio::test]
async fn graph_rejects_clock_stale_foreign_and_join_boundary_errors() -> Result<()> {
    let mut first = graph()?;
    let mut second = graph()?;
    assert!(first.prepare(input(0, vec![], vec![])?).await.is_err());
    let a = first.prepare(input(1, vec![], vec![])?).await?;
    let b = first.prepare(input(1, vec![], vec![])?).await?;
    let c = first.prepare(input(1, vec![], vec![])?).await?;
    assert!(second.validate_prepared(&a).is_err());
    assert!(second.commit(a).is_err());
    first.commit(b)?;
    assert!(first.validate_prepared(&c).is_err());
    assert!(first.commit(c).is_err());
    let state = first.snapshot();
    let left = TimedBatch { time: 2, batch: Batch::from_updates([])? };
    let right = TimedBatch { time: 3, batch: Batch::from_updates([])? };
    assert!(Join.evaluate(&left, &right, &state.0, &state.1).await.is_err());
    let store = Arc::new(InMemory::new());
    assert!(Arrangement::<i64, i64>::new(store.clone(), String::new(), 1).is_err());
    assert!(Arrangement::<i64, i64>::new(store.clone(), "x".into(), 0).is_err());
    let writer = Arrangement::<i64, i64>::new(store, "x".into(), 1)?;
    let compact = writer.compact(&state.0).await?;
    assert_eq!(compact.time(), state.0.time());
    assert_eq!(compact.materialize().await?, state.0.materialize().await?);
    let project = Project::new(|_: &i64, _: &i64| Ok(Some((0_i64, 0_i64))));
    let edge = TimedBatch {
        time: 1,
        batch: Batch::from_updates([((0, 0), i64::MAX), ((1, 0), 1), ((2, 0), -1)])?,
    };
    assert_eq!(project.evaluate(&edge)?.batch, Batch::from_updates([((0, 0), i64::MAX)])?);
    Ok(())
}

#[tokio::test]
async fn maintenance_changes_physical_boundary_without_advancing_time() -> Result<()> {
    let mut first = graph()?;
    let mut other = graph()?;
    let pending = first.prepare(input(1, vec![], vec![])?).await?;
    let retained = first.snapshot();
    let candidate = first.prepare_maintenance(|state| async move { Ok((*state).clone()) }).await?;
    let stale = first.prepare_maintenance(|state| async move { Ok((*state).clone()) }).await?;
    let foreign = first.prepare_maintenance(|state| async move { Ok((*state).clone()) }).await?;
    assert!(other.commit_maintenance(foreign).is_err());
    first.commit_maintenance(candidate)?;
    assert_eq!(first.time(), 0);
    assert!(first.commit(pending).is_err());
    assert!(first.commit_maintenance(stale).is_err());
    let pinned = first.snapshot();
    assert!(first.prepare_maintenance(|_| async { bail!("maintenance failed") }).await.is_err());
    assert!(Arc::ptr_eq(&pinned, &first.snapshot()));
    assert_eq!(retained.0.time(), 0);
    first.commit(first.prepare(input(1, vec![], vec![])?).await?)?;
    Ok(())
}
