use crate::engine::{
    Batch,
    dataflow::{Arrangement, GroupSum, Join, Project, Stream, SumState},
    execution::Limits,
    plan::{
        Definition, Kind, Node, Plan, Source,
        query::{GroupedJoin, Operators},
    },
};
use anyhow::Result;
use object_store::{ObjectStore, memory::InMemory};
use std::sync::Arc;

fn limits() -> Limits {
    Limits { resident_entries: 1, ..Limits::default() }
}
fn stream(
    time: u64,
    rows: impl IntoIterator<Item = ((i64, i64), i64)>,
) -> Result<Stream<Batch<i64, i64>>> {
    Ok(Stream { time, batch: Batch::from_updates(rows)? })
}
fn plan() -> Result<Plan> {
    Plan::new(Definition {
        revision: "budget-test-v1".into(),
        sources: ["as", "bs"]
            .into_iter()
            .map(|id| Source { id: id.into(), schema: "int".into() })
            .collect(),
        nodes: [
            ("a", Kind::Source, vec!["as"], "int"),
            ("b", Kind::Source, vec!["bs"], "int"),
            ("left", Kind::Project, vec!["a"], "int"),
            ("right", Kind::Project, vec!["b"], "int"),
            ("join", Kind::Join, vec!["left", "right"], "pair"),
            ("group", Kind::Project, vec!["join"], "int"),
            ("aggregate", Kind::Aggregate, vec!["group"], "sum"),
        ]
        .into_iter()
        .map(|(id, kind, inputs, schema)| Node {
            id: id.into(),
            kind,
            inputs: inputs.into_iter().map(str::to_owned).collect(),
            schema: schema.into(),
        })
        .collect(),
        arrangements: [
            ("left", "left", "int"),
            ("right", "right", "int"),
            ("sums", "aggregate", "sum"),
            ("output", "aggregate", "visible"),
        ]
        .into_iter()
        .map(|(id, node, schema)| crate::engine::plan::Arrangement {
            id: id.into(),
            node: node.into(),
            schema: schema.into(),
        })
        .collect(),
        outputs: vec!["aggregate".into()],
    })
}
#[tokio::test]
async fn join_budget_failure_retains_complete_query_root_and_can_retry() -> Result<()> {
    let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let identity = || Project::new(|key: &i64, value: &i64| Ok(Some((*key, *value))));
    let operators = Operators {
        left: identity(),
        right: identity(),
        group: Project::new(|key: &i64, value: &(i64, i64)| Ok(Some((*key, value.0 + value.1)))),
        sum: GroupSum::new(|_: &i64, value: &i64| Ok(Some(*value))),
    };
    let mut query = GroupedJoin::new_with_limits(
        plan()?,
        operators,
        store,
        1,
        Limits { output_entries: 3, ..limits() },
    )?;
    let before = query.snapshot();
    let left = stream(1, [((1, 1), 1), ((1, 2), 1)])?;
    let right = stream(1, [((1, 3), 1), ((1, 4), 1)])?;
    assert!(
        query.prepare(Stream { time: 1, batch: (left.batch, right.batch.clone()) }).await.is_err()
    );
    assert!(Arc::ptr_eq(&before, &query.snapshot()));
    assert_eq!(query.time(), 0);
    let prepared = query
        .prepare(Stream { time: 1, batch: (stream(1, [((1, 1), 1)])?.batch, right.batch) })
        .await?;
    query.commit(prepared)?;
    assert_eq!(query.time(), 1);
    assert!(query.cache_stats()?.bytes <= Limits::default().cache_bytes);
    assert_eq!(
        query.snapshot().output.materialize().await?,
        Batch::from_updates([((1, (2, Some(9))), 1)])?
    );
    Ok(())
}
#[tokio::test]
async fn spilled_projection_join_and_aggregate_preserve_exact_cancellation() -> Result<()> {
    let projection = Project::new(|_: &i64, _: &i64| Ok(Some((0, 0))));
    let input =
        stream(1, (0..20).map(|n| ((n, n), if n % 2 == 0 { i64::MAX } else { -i64::MAX })))?;
    assert_eq!(projection.evaluate_with_limits(&input, limits())?.batch.iter().count(), 0);
    let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let writer = Arrangement::<i64, i64>::new(store.clone(), "int".into(), 1)?;
    let left = stream(1, [((1, 1), i64::MAX)])?;
    let right = stream(1, [((1, 2), 2)])?;
    let old_left = writer.stage(&writer.empty(), &left).await?;
    let old_right = writer.stage(&writer.empty(), &right).await?;
    let next_left = stream(2, [((1, 1), -i64::MAX)])?;
    let next_right = stream(2, [((1, 2), -1)])?;
    let joined = Join
        .evaluate_with_limits((&next_left, &next_right), (&old_left, &old_right), limits())
        .await;
    // Complete join delta is -2*MAX and must fail only at finalization.
    assert!(joined.is_err());
    let initial_left = stream(1, [((1, 1), i64::MAX), ((2, 1), i64::MAX)])?;
    let initial_right = stream(1, [((1, 2), 1), ((2, 2), 1)])?;
    let prior_left = writer.stage(&writer.empty(), &initial_left).await?;
    let prior_right = writer.stage(&writer.empty(), &initial_right).await?;
    let retract_left = stream(2, [((1, 1), -i64::MAX), ((2, 1), -i64::MAX)])?;
    let retract_right = stream(2, [((1, 2), -2), ((2, 2), -2)])?;
    let joined = Join
        .evaluate_with_limits(
            (&retract_left, &retract_right),
            (&prior_left, &prior_right),
            limits(),
        )
        .await?;
    assert_eq!(
        joined.batch,
        Batch::from_updates([((1, (1, 2)), -i64::MAX), ((2, (1, 2)), -i64::MAX)])?
    );
    let sums = Arrangement::<i64, SumState>::new(store, "sum".into(), 1)?;
    let prior = sums
        .stage(
            &sums.empty(),
            &Stream {
                time: 1,
                batch: Batch::from_updates([((1, SumState { rows: 2, non_null: 2, sum: 0 }), 1)])?,
            },
        )
        .await?;
    let aggregate = GroupSum::new(|_: &i64, value: &i64| Ok(Some(*value)));
    let delta = aggregate
        .evaluate_with_limits(
            &stream(2, [((1, i64::MAX), 2), ((1, -i64::MAX), 2)])?,
            &prior,
            limits(),
        )
        .await?;
    assert_eq!(
        delta.state.batch,
        Batch::from_updates([
            ((1, SumState { rows: 2, non_null: 2, sum: 0 }), -1),
            ((1, SumState { rows: 6, non_null: 6, sum: 0 }), 1),
        ])?
    );
    Ok(())
}

type TestOperators = Operators<i64, i64, i64, i64, i64, i64, i64>;
fn test_operators() -> TestOperators {
    let identity = || Project::new(|key: &i64, value: &i64| Ok(Some((*key, *value))));
    Operators {
        left: identity(),
        right: identity(),
        group: Project::new(|key: &i64, values: &(i64, i64)| Ok(Some((*key, values.0 + values.1)))),
        sum: GroupSum::new(|_: &i64, value: &i64| Ok(Some(*value))),
    }
}
#[tokio::test]
async fn reopened_query_restores_all_arrangements_and_continues_next_tick() -> Result<()> {
    use crate::engine::plan::query::Settings;
    let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let mut original = GroupedJoin::new(plan()?, test_operators(), store.clone(), 1)?;
    let bootstrap = original.checkpoint()?;
    original.commit(
        original
            .prepare(Stream {
                time: 1,
                batch: (stream(1, [((1, 2), 1)])?.batch, stream(1, [((1, 3), 1)])?.batch),
            })
            .await?,
    )?;
    let checkpoint = original.checkpoint()?;
    let checkpoint = serde_json::from_slice(&serde_json::to_vec(&checkpoint)?)?;
    let mut reopened = GroupedJoin::reopen(
        plan()?,
        test_operators(),
        Settings { store, block_rows: 1, limits: Limits::default() },
        checkpoint,
    )
    .await?;
    assert_eq!(reopened.time(), 1);
    assert_eq!(reopened.checkpoint()?, original.checkpoint()?);
    let foreign = original
        .prepare(Stream { time: 2, batch: (stream(2, [])?.batch, stream(2, [])?.batch) })
        .await?;
    assert!(reopened.prepared_checkpoint(&foreign).is_err());
    assert!(reopened.commit(foreign).is_err());
    let pending = reopened
        .prepare(Stream { time: 2, batch: (stream(2, [])?.batch, stream(2, [])?.batch) })
        .await?;
    reopened.restore_checkpoint(original.checkpoint()?).await?;
    assert!(reopened.commit(pending).is_err());
    let pinned = reopened.snapshot();
    assert!(reopened.restore_checkpoint(bootstrap).await.is_err());
    assert!(Arc::ptr_eq(&pinned, &reopened.snapshot()));
    let staged = reopened
        .prepare(Stream {
            time: 2,
            batch: (
                stream(2, [((1, 2), -1), ((1, 4), 1)])?.batch,
                stream(2, [((1, 3), -1), ((1, 7), 1)])?.batch,
            ),
        })
        .await?;
    let staged_checkpoint = reopened.prepared_checkpoint(&staged)?;
    assert_eq!(staged_checkpoint.time, 2);
    assert_eq!(reopened.time(), 1);
    reopened.commit(staged)?;
    assert_eq!(reopened.checkpoint()?, staged_checkpoint);
    assert_eq!(reopened.time(), 2);
    assert_eq!(
        reopened.snapshot().output.materialize().await?,
        Batch::from_updates([((1, (1, Some(11))), 1)])?
    );
    assert_eq!(original.time(), 1);
    Ok(())
}
#[tokio::test]
async fn incompatible_checkpoint_memberships_fail_before_reopening() -> Result<()> {
    use crate::engine::plan::query::Settings;
    let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let original = GroupedJoin::new(plan()?, test_operators(), store.clone(), 1)?;
    let checkpoint = original.checkpoint()?;
    let settings = Settings { store, block_rows: 1, limits: Limits::default() };
    let mut incompatible = checkpoint.clone();
    incompatible.version = 9;
    assert!(
        GroupedJoin::reopen(plan()?, test_operators(), settings.clone(), incompatible)
            .await
            .is_err()
    );
    let mut incompatible = checkpoint.clone();
    incompatible.plan_identity.push('x');
    assert!(
        GroupedJoin::reopen(plan()?, test_operators(), settings.clone(), incompatible)
            .await
            .is_err()
    );
    let mut incompatible = checkpoint.clone();
    incompatible.arrangements.pop();
    assert!(
        GroupedJoin::reopen(plan()?, test_operators(), settings.clone(), incompatible)
            .await
            .is_err()
    );
    let mut incompatible = checkpoint.clone();
    incompatible.arrangements[1] = incompatible.arrangements[0].clone();
    assert!(
        GroupedJoin::reopen(plan()?, test_operators(), settings.clone(), incompatible)
            .await
            .is_err()
    );
    let mut incompatible = checkpoint.clone();
    incompatible.arrangements[0].trace.time = 1;
    assert!(
        GroupedJoin::reopen(plan()?, test_operators(), settings.clone(), incompatible)
            .await
            .is_err()
    );
    let mut incompatible = checkpoint.clone();
    incompatible.arrangements[0].schema.push('x');
    assert!(
        GroupedJoin::reopen(plan()?, test_operators(), settings.clone(), incompatible)
            .await
            .is_err()
    );
    let mut incompatible = checkpoint;
    incompatible.arrangements[0].trace.version = 2;
    assert!(GroupedJoin::reopen(plan()?, test_operators(), settings, incompatible).await.is_err());
    Ok(())
}
