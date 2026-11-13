use crate::engine::{
    Batch,
    dataflow::{Arrangement, OrderedSelection, Project, TimedBatch},
    execution::Limits,
};
use anyhow::Result;
use object_store::memory::InMemory;
use std::sync::Arc;
#[tokio::test]
async fn ordered_first_retracts_prefix_merges_complete_delta_and_reopens() -> Result<()> {
    let store = Arc::new(InMemory::new());
    let inputs = Arrangement::<i64, i64>::new(store.clone(), "ordered-input-v1".into(), 1)?;
    let outputs = Arrangement::<i64, i64>::new(store, "ordered-output-v1".into(), 1)?;
    let operator =
        OrderedSelection::first(Project::new(|key: &i64, value: &i64| Ok(Some((*key, *value)))));
    let mut input = inputs.empty();
    let mut output = outputs.empty();
    let history = [
        (vec![((0, 1), 1), ((0, 2), 2), ((0, 3), 1)], Some(1)),
        (vec![((0, 1), -1), ((0, 0), 1)], Some(0)),
        (vec![((0, 0), -1), ((0, 2), -1)], Some(2)),
        (vec![((0, 2), -1), ((0, 3), -1)], None),
        (vec![], None),
        (vec![((0, -1), 1)], Some(-1)),
    ];
    for (index, (updates, selected)) in history.into_iter().enumerate() {
        let delta =
            TimedBatch { time: u64::try_from(index)? + 1, batch: Batch::from_updates(updates)? };
        let change = operator.evaluate(&delta, &input, &output, Limits::default()).await?;
        input = inputs.reopen(inputs.stage(&input, &delta).await?.manifest()?).await?;
        output = outputs.reopen(outputs.stage(&output, &change).await?.manifest()?).await?;
        assert_eq!(
            output.materialize().await?,
            Batch::from_updates(selected.into_iter().map(|value| ((0, value), 1)))?
        );
    }
    Ok(())
}
#[tokio::test]
async fn ordered_first_rejects_bad_clocks_weights_projection_state_and_work() -> Result<()> {
    let writer =
        Arrangement::<i64, i64>::new(Arc::new(InMemory::new()), "ordered-error-v1".into(), 1)?;
    let empty = writer.empty();
    let delta = TimedBatch { time: 1, batch: Batch::from_updates([((0, 1), 1)])? };
    let state = writer.stage(&empty, &delta).await?;
    let operator =
        OrderedSelection::first(Project::new(|key: &i64, value: &i64| Ok(Some((*key, *value)))));
    assert!(operator.evaluate(&delta, &empty, &state, Limits::default()).await.is_err());
    let wrong_time = TimedBatch { time: 0, batch: delta.batch.clone() };
    assert!(operator.evaluate(&wrong_time, &empty, &empty, Limits::default()).await.is_err());
    let negative =
        TimedBatch { time: 2, batch: Batch::from_updates([((0, 999), -1), ((0, 998), 1)])? };
    assert!(operator.evaluate(&negative, &state, &state, Limits::default()).await.is_err());
    let invalid =
        OrderedSelection::first(Project::new(|_: &i64, value: &i64| Ok(Some((1, *value)))));
    assert!(invalid.evaluate(&delta, &empty, &empty, Limits::default()).await.is_err());
    let heavy = writer
        .stage(&empty, &TimedBatch { time: 1, batch: Batch::from_updates([((0, 1), 2)])? })
        .await?;
    let next = TimedBatch { time: 2, batch: Batch::from_updates([((0, 0), 1)])? };
    assert!(operator.evaluate(&next, &state, &heavy, Limits::default()).await.is_err());
    let multi = writer
        .stage(
            &empty,
            &TimedBatch { time: 1, batch: Batch::from_updates([((0, 1), 1), ((0, 2), 1)])? },
        )
        .await?;
    assert!(operator.evaluate(&next, &state, &multi, Limits::default()).await.is_err());
    let limits = Limits { contributions: 1, ..Limits::default() };
    let removal = TimedBatch { time: 2, batch: Batch::from_updates([((0, 1), -1)])? };
    assert!(operator.evaluate(&removal, &state, &state, limits).await.is_err());
    let maximum = writer
        .stage(&empty, &TimedBatch { time: 1, batch: Batch::from_updates([((0, 1), i64::MAX)])? })
        .await?;
    let overflow = TimedBatch { time: 2, batch: delta.batch };
    assert!(operator.evaluate(&overflow, &maximum, &state, Limits::default()).await.is_err());
    Ok(())
}

#[tokio::test]
async fn ordered_first_does_not_read_unselected_tail_after_cutoff() -> Result<()> {
    use crate::engine::reader::ObjectBatch;
    let store = Arc::new(InMemory::new());
    let inputs = Arrangement::new(store.clone(), "ordered-tail-input".into(), 1)?;
    let outputs = Arrangement::new(store.clone(), "ordered-tail-output".into(), 1)?;
    let prior = inputs
        .stage(
            &inputs.empty(),
            &TimedBatch {
                time: 1,
                batch: Batch::from_updates([((0, 1), 1), ((0, 2), 1), ((0, 3), 1)])?,
            },
        )
        .await?;
    let retained = outputs
        .stage(
            &outputs.empty(),
            &TimedBatch { time: 1, batch: Batch::from_updates([((0, 1), 1)])? },
        )
        .await?;
    let reference = prior.manifest()?.objects[0].clone();
    let object =
        ObjectBatch::<i32, i32>::open(store.clone(), reference, "ordered-tail-input").await?;
    crate::engine::tests::store::corrupt_block(store.as_ref(), &object, 1).await?;
    let operator =
        OrderedSelection::first(Project::new(|key: &i32, value: &i32| Ok(Some((*key, *value)))));
    let delta = operator
        .evaluate(
            &TimedBatch { time: 2, batch: Batch::from_updates([((0, 3), -1), ((0, 4), 1)])? },
            &prior,
            &retained,
            Limits::default(),
        )
        .await?;
    assert_eq!(delta.batch, Batch::from_updates([])?);
    Ok(())
}

#[tokio::test]
async fn ordered_first_filters_the_selected_identity_without_falling_through() -> Result<()> {
    let writer = Arrangement::new(Arc::new(InMemory::new()), "ordered-filter".into(), 1)?;
    let prior = writer
        .stage(
            &writer.empty(),
            &TimedBatch { time: 1, batch: Batch::from_updates([((0, 1), 2), ((0, 2), 1)])? },
        )
        .await?;
    let retained = writer
        .stage(&writer.empty(), &TimedBatch { time: 1, batch: Batch::from_updates([])? })
        .await?;
    let input = TimedBatch { time: 2, batch: Batch::from_updates([((0, 3), 1)])? };
    let operator = OrderedSelection::first(Project::new(|key: &i32, value: &i32| {
        Ok((*value != 1).then_some((*key, *value)))
    }));
    let limits = Limits { contributions: 1, ..Limits::default() };
    assert_eq!(
        operator.evaluate(&input, &prior, &retained, limits).await?.batch,
        Batch::from_updates([])?
    );
    Ok(())
}
