use crate::engine::{
    Batch,
    dataflow::{Arrangement, Partition, TimedBatch},
    execution::Limits,
};
use anyhow::Result;
use object_store::memory::InMemory;
use std::{
    collections::BTreeMap,
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
};

#[tokio::test]
async fn retained_partition_histories_restore_and_callback_work() -> Result<()> {
    let store = Arc::new(InMemory::new());
    let inputs = Arrangement::<i64, i64>::new(store.clone(), "input-v1".into(), 1)?;
    let outputs = Arrangement::<i64, i64>::new(store, "output-v1".into(), 1)?;
    let calls = Arc::new(AtomicUsize::new(0));
    let visits = Arc::new(AtomicUsize::new(0));
    let measured_calls = calls.clone();
    let measured_visits = visits.clone();
    let operator = Partition::new_zero_preserving(move |_: &i64, bag: &Batch<(), i64>, work| {
        measured_calls.fetch_add(1, Ordering::Relaxed);
        measured_visits.fetch_add(bag.iter().count(), Ordering::Relaxed);
        work.charge(bag.iter().count())?;
        Batch::from_updates(
            bag.iter()
                .filter(|(tuple, _)| tuple.1 >= 0)
                .map(|(((), value), weight)| (((), value % 2), *weight)),
        )
    });
    let mut input = inputs.empty();
    let mut output = outputs.empty();
    let mut memory = BTreeMap::<(i64, i64), i64>::new();
    let history = [
        vec![((0, 2), 2), ((0, 4), 1), ((1, -1), 1)],
        vec![((0, 2), -1), ((0, 3), 2), ((1, -1), -1)],
        vec![((0, 2), -1), ((0, 4), -1), ((0, 3), -2)],
        vec![],
        vec![((0, 6), 3), ((1, 5), 1)],
    ];
    let mut expected_calls = 0;
    let mut expected_visits = 0;
    for (index, changes) in history.into_iter().enumerate() {
        let changed: std::collections::BTreeSet<_> =
            changes.iter().map(|((key, _), _)| *key).collect();
        for (tuple, weight) in &changes {
            *memory.entry(*tuple).or_default() += weight;
        }
        memory.retain(|_, weight| *weight != 0);
        for key in changed {
            let entries = memory.keys().filter(|(group, _)| *group == key).count();
            expected_calls += usize::from(entries != 0);
            expected_visits += entries;
        }
        let delta =
            TimedBatch { time: u64::try_from(index)? + 1, batch: Batch::from_updates(changes)? };
        let update = operator.evaluate_retained(&delta, &input, &output, Limits::default()).await?;
        let next_input = inputs.stage(&input, &delta).await?;
        let next_output = outputs.stage(&output, &update).await?;
        let expected = Batch::from_updates(
            memory
                .iter()
                .filter(|((_, value), _)| *value >= 0)
                .map(|((key, value), weight)| ((*key, value % 2), *weight)),
        )?;
        assert_eq!(next_output.materialize().await?, expected);
        assert_eq!(calls.load(Ordering::Relaxed), expected_calls);
        assert_eq!(visits.load(Ordering::Relaxed), expected_visits);
        input = inputs.reopen(next_input.manifest()?).await?;
        output = outputs.reopen(next_output.manifest()?).await?;
    }
    Ok(())
}

#[tokio::test]
async fn generic_empty_callback_keeps_legacy_semantics() -> Result<()> {
    let inputs = Arrangement::<i64, i64>::new(Arc::new(InMemory::new()), "generic-v1".into(), 1)?;
    let operator = Partition::new(|_: &i64, bag: &Batch<(), i64>, _| {
        Batch::from_updates([(((), i64::try_from(bag.iter().count())?), 1)])
    });
    let delta = TimedBatch { time: 1, batch: Batch::from_updates([((0, 7), 1)])? };
    let output = operator.evaluate(&delta, &inputs.empty(), Limits::default()).await?;
    assert_eq!(output.batch, Batch::from_updates([((0, 0), -1), ((0, 1), 1)])?);
    assert!(
        operator
            .evaluate_retained(&delta, &inputs.empty(), &inputs.empty(), Limits::default())
            .await
            .is_err()
    );
    Ok(())
}

#[tokio::test]
async fn retained_partition_rejects_invalid_boundaries_and_bags() -> Result<()> {
    let writer = Arrangement::<i64, i64>::new(Arc::new(InMemory::new()), "errors-v1".into(), 1)?;
    let operator =
        Partition::new_zero_preserving(|_: &i64, rows: &Batch<(), i64>, _| Ok(rows.clone()));
    let empty = writer.empty();
    let first = TimedBatch { time: 1, batch: Batch::from_updates([((0, 1), 1)])? };
    let nonempty = writer.stage(&empty, &first).await?;
    assert!(
        operator.evaluate_retained(&first, &empty, &nonempty, Limits::default()).await.is_err()
    );
    let stale = TimedBatch { time: 0, batch: first.batch.clone() };
    assert!(operator.evaluate_retained(&stale, &empty, &empty, Limits::default()).await.is_err());
    let negative = TimedBatch { time: 1, batch: Batch::from_updates([((0, 1), -1)])? };
    assert!(
        operator.evaluate_retained(&negative, &empty, &empty, Limits::default()).await.is_err()
    );
    let empty_tick = TimedBatch { time: 1, batch: Batch::from_updates([])? };
    let empty_at_one = writer.stage(&empty, &empty_tick).await?;
    let second = TimedBatch { time: 2, batch: first.batch.clone() };
    assert!(
        operator
            .evaluate_retained(&second, &empty_at_one, &nonempty, Limits::default())
            .await
            .is_err()
    );
    let failing = Partition::new_zero_preserving(|_: &i64, _: &Batch<(), i64>, _| {
        anyhow::bail!("transform failed")
    });
    assert!(failing.evaluate_retained(&first, &empty, &empty, Limits::default()).await.is_err());
    let many = Partition::new_zero_preserving(|_: &i64, _: &Batch<(), i64>, _| {
        Batch::from_updates([(((), 1), 1), (((), 2), 1)])
    });
    assert!(
        many.evaluate_retained(
            &first,
            &empty,
            &empty,
            Limits { output_entries: 1, ..Limits::default() }
        )
        .await
        .is_err()
    );
    let two = TimedBatch { time: 1, batch: Batch::from_updates([((0, 1), 1), ((0, 2), 1)])? };
    let two = writer.stage(&empty, &two).await?;
    assert!(
        operator
            .evaluate_retained(
                &second,
                &two,
                &two,
                Limits { contributions: 1, ..Limits::default() }
            )
            .await
            .is_err()
    );
    assert_eq!(empty.time(), 0);
    assert_eq!(two.time(), 1);
    Ok(())
}

#[tokio::test]
async fn retained_output_reduces_callback_visits_against_legacy() -> Result<()> {
    let store = Arc::new(InMemory::new());
    let input_writer = Arrangement::<i64, i64>::new(store.clone(), "measure-input".into(), 32)?;
    let output_writer = Arrangement::<i64, i64>::new(store, "measure-output".into(), 1)?;
    let legacy_visits = Arc::new(AtomicUsize::new(0));
    let retained_visits = Arc::new(AtomicUsize::new(0));
    let transform = |visits: Arc<AtomicUsize>| {
        move |_: &i64, rows: &Batch<(), i64>, work: &mut crate::engine::dataflow::PartitionWork| {
            visits.fetch_add(rows.iter().count(), Ordering::Relaxed);
            work.charge(rows.iter().count())?;
            let maximum = rows.iter().map(|(((), row), _)| *row).max();
            Batch::from_updates(maximum.map(|maximum| (((), maximum), 1)))
        }
    };
    let legacy = Partition::new(transform(legacy_visits.clone()));
    let retained = Partition::new_zero_preserving(transform(retained_visits.clone()));
    let bootstrap =
        TimedBatch { time: 1, batch: Batch::from_updates((0..1000).map(|row| ((0, row), 1)))? };
    let first = retained
        .evaluate_retained(
            &bootstrap,
            &input_writer.empty(),
            &output_writer.empty(),
            Limits::default(),
        )
        .await?;
    let input = input_writer.stage(&input_writer.empty(), &bootstrap).await?;
    let output = output_writer.stage(&output_writer.empty(), &first).await?;
    retained_visits.store(0, Ordering::Relaxed);
    let delta = TimedBatch { time: 2, batch: Batch::from_updates([((0, 1000), 1)])? };
    let old = legacy.evaluate(&delta, &input, Limits::default()).await?;
    let new = retained.evaluate_retained(&delta, &input, &output, Limits::default()).await?;
    assert_eq!(old.batch, new.batch);
    assert_eq!(new.batch, Batch::from_updates([((0, 999), -1), ((0, 1000), 1)])?);
    assert_eq!(legacy_visits.load(Ordering::Relaxed), 2001);
    assert_eq!(retained_visits.load(Ordering::Relaxed), 1001);
    Ok(())
}

#[tokio::test]
async fn direct_group_delta_skips_recomputation_and_reopens_legacy_state() -> Result<()> {
    let store = Arc::new(InMemory::new());
    let inputs = Arrangement::<i64, i64>::new(store.clone(), "direct-input".into(), 1)?;
    let outputs = Arrangement::<i64, i64>::new(store, "direct-output".into(), 1)?;
    let fallback =
        Partition::new_zero_preserving(|_: &i64, rows: &Batch<(), i64>, _| Ok(rows.clone()));
    let initial = TimedBatch { time: 1, batch: Batch::from_updates([((0, 2), 3)])? };
    let old = fallback
        .evaluate_retained(&initial, &inputs.empty(), &outputs.empty(), Limits::default())
        .await?;
    let input = inputs.stage(&inputs.empty(), &initial).await?;
    let output = outputs.stage(&outputs.empty(), &old).await?;
    let input = inputs.reopen(input.manifest()?).await?;
    let output = outputs.reopen(output.manifest()?).await?;
    let visits = Arc::new(AtomicUsize::new(0));
    let measured = visits.clone();
    let direct = Partition::new_zero_preserving(|_: &i64, _: &Batch<(), i64>, _| {
        anyhow::bail!("full callback must not run")
    })
    .with_incremental(move |_, before, after, delta, work| {
        assert_eq!(before, &Batch::from_updates([(((), 2), 3)])?);
        assert_eq!(after, &Batch::from_updates([(((), 2), 2), (((), 4), 1)])?);
        work.charge(delta.iter().count())?;
        measured.fetch_add(1, Ordering::Relaxed);
        Ok(delta.clone())
    });
    let delta = TimedBatch { time: 2, batch: Batch::from_updates([((0, 2), -1), ((0, 4), 1)])? };
    // Three input/delta visits and two callback visits: no old-output read charge.
    let limits = Limits { contributions: 5, ..Limits::default() };
    let next = direct.evaluate_retained(&delta, &input, &output, limits).await?;
    assert_eq!(next.batch, delta.batch);
    assert_eq!(visits.load(Ordering::Relaxed), 1);
    let invalid = TimedBatch { time: 2, batch: Batch::from_updates([((0, 2), -4)])? };
    assert!(direct.evaluate_retained(&invalid, &input, &output, limits).await.is_err());
    assert_eq!(visits.load(Ordering::Relaxed), 1);
    assert_eq!(input.materialize().await?, initial.batch);
    Ok(())
}
