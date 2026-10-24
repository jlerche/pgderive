//! Cursor overlays are exact, bounded by selected reads, and validate skipped tails.
use crate::engine::{
    Batch,
    dataflow::{Arrangement, Group, PartitionWork, TimedBatch},
    execution::Limits,
    reader::{BlockCache, ObjectBatch},
    trace::{Run, TraceSnapshot},
};
use anyhow::Result;
use object_store::{ObjectStore, memory::InMemory};
use std::sync::{
    Arc,
    atomic::{AtomicUsize, Ordering},
};
#[tokio::test]
async fn cursor_overlay_matches_weighted_recomputation_and_cold_restore() -> Result<()> {
    let writer = Arrangement::<i64, i64>::new(Arc::new(InMemory::new()), "group-v1".into(), 1)?;
    let operator = Group::new(|input, work| {
        Box::pin(async move {
            let mut output = Vec::new();
            let old = drain(input.before().await?, work, -1).await?;
            output.extend(old);
            output.extend(drain(input.after().await?, work, 1).await?);
            Batch::from_updates(output)
        })
    });
    let mut prior = writer.empty();
    for (index, changes) in [
        vec![((0, 2), 3), ((0, 4), 1)],
        vec![((0, 2), -3), ((0, 1), 2), ((0, 4), 1)],
        vec![((0, 1), -1), ((0, 4), -2)],
        vec![],
        vec![((0, 1), -1)],
    ]
    .into_iter()
    .enumerate()
    {
        let delta =
            TimedBatch { time: u64::try_from(index)? + 1, batch: Batch::from_updates(changes)? };
        let output = operator.evaluate(&delta, &prior, Limits::default()).await?;
        assert_eq!(output.batch, delta.batch);
        prior = writer.stage(&prior, &delta).await?;
        prior = writer.reopen(prior.manifest()?).await?;
    }
    let invalid = TimedBatch { time: 6, batch: Batch::from_updates([((0, 1), -1)])? };
    assert!(operator.evaluate(&invalid, &prior, Limits::default()).await.is_err());
    assert_eq!(prior.time(), 5);
    Ok(())
}
#[tokio::test]
async fn stopped_cursor_validates_changed_tail_and_ignores_hot_group_size() -> Result<()> {
    for size in [8, 128, 1024] {
        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let cache = Arc::new(BlockCache::new(0, 0));
        let rows = Batch::from_updates((0..size).map(|value| ((0, value), 1)))?;
        let object = ObjectBatch::write(store.clone(), &rows, "early-v1", 1).await?;
        let object = ObjectBatch::open_with_cache(store, object, "early-v1", cache.clone()).await?;
        let prior = TraceSnapshot::empty().append_validated(Run::Object(object), 1)?;
        let calls = Arc::new(AtomicUsize::new(0));
        let measured = calls.clone();
        let operator = Group::new(move |input, work| {
            let measured = measured.clone();
            Box::pin(async move {
                measured.fetch_add(1, Ordering::Relaxed);
                let mut after = input.after().await?;
                let first = after.next(work).await?;
                assert_eq!(first, Some((0, 1)));
                // Resource failure preserves cursor position for a retry.
                let mut exhausted = PartitionWork::new(0);
                assert!(after.next(&mut exhausted).await.is_err());
                assert_eq!(after.next(work).await?, Some((1, 1)));
                Batch::from_updates([])
            })
        });
        let before = cache.stats()?.misses;
        let delta = TimedBatch { time: 2, batch: Batch::from_updates([((0, size - 1), 1)])? };
        let limits = Limits { contributions: 4, ..Limits::default() };
        let result = operator.evaluate(&delta, &prior, limits).await?;
        assert_eq!(result.batch, Batch::from_updates([])?);
        let reads = cache.stats()?.misses - before;
        assert!(reads <= 5, "{size}: {reads}");
        let invalid = TimedBatch { time: 2, batch: Batch::from_updates([((0, size - 1), -2)])? };
        assert!(operator.evaluate(&invalid, &prior, limits).await.is_err());
        assert_eq!(calls.load(Ordering::Relaxed), 1);
        let wrong = TimedBatch { time: 3, batch: Batch::from_updates([])? };
        assert!(operator.evaluate(&wrong, &prior, limits).await.is_err());
    }
    Ok(())
}

async fn drain(
    mut cursor: crate::engine::dataflow::GroupCursor<i64, i64>,
    work: &mut PartitionWork,
    sign: i64,
) -> Result<Vec<(((), i64), i64)>> {
    let mut output = Vec::new();
    while let Some((value, weight)) = cursor.next(work).await? {
        output.push((((), value), sign * weight));
    }
    Ok(output)
}
