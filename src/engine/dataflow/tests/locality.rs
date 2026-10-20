//! Audit two independent axes: unrelated state and the affected key's cardinality.
use crate::engine::{
    Batch,
    dataflow::{Join, Partition, TimedBatch},
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

async fn snapshot(
    rows: &Batch<i64, i64>,
    cache: Arc<BlockCache>,
) -> Result<TraceSnapshot<i64, i64>> {
    let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let reference = ObjectBatch::write(store.clone(), rows, "locality-v1", 1).await?;
    let object = ObjectBatch::open_with_cache(store, reference, "locality-v1", cache).await?;
    TraceSnapshot::empty().append_validated(Run::Object(object), 1)
}
fn input(hot: i64, unrelated: i64) -> Result<Batch<i64, i64>> {
    Batch::from_updates(
        (0..hot)
            .map(|value| ((7, value), value % 3 + 1))
            .chain((0..unrelated).map(|key| ((100 + key, 0), 1))),
    )
}

#[tokio::test]
async fn join_reads_scale_with_matching_key_and_ignore_unrelated_state() -> Result<()> {
    for hot in [1_i64, 8, 32] {
        for unrelated in [0_i64, 64, 1024] {
            let cache = Arc::new(BlockCache::new(0, 0));
            let prior = snapshot(&input(hot, unrelated)?, cache.clone()).await?;
            let empty = TraceSnapshot::empty()
                .append_validated(Run::Memory(Arc::new(Batch::<i64, i64>::from_updates([])?)), 1)?;
            let before = cache.stats()?.misses;
            let delta = TimedBatch { time: 2, batch: Batch::from_updates([((7, -1), 2)])? };
            let unchanged = TimedBatch { time: 2, batch: Batch::from_updates([])? };
            let result = Join.evaluate(&delta, &unchanged, &empty, &prior).await?;
            let reads = cache.stats()?.misses - before;
            assert!(reads <= u64::try_from(hot)? + 1, "{hot}/{unrelated}: {reads}");
            assert_eq!(
                result.batch,
                Batch::from_updates((0..hot).map(|value| ((7, (-1, value)), 2 * (value % 3 + 1))))?
            );
            assert_eq!(prior.time(), 1);
            assert_eq!(cache.stats()?.bytes, 0);
        }
    }
    Ok(())
}

#[tokio::test]
async fn retained_partition_recomputes_hot_bag_but_not_unrelated_groups() -> Result<()> {
    for hot in [1_i64, 8, 32] {
        for unrelated in [0_i64, 64, 1024] {
            let cache = Arc::new(BlockCache::new(0, 0));
            let prior = snapshot(&input(hot, unrelated)?, cache.clone()).await?;
            let retained = snapshot(
                &Batch::from_updates(
                    std::iter::once(((7, hot - 1), 1))
                        .chain((0..unrelated).map(|key| ((100 + key, 0), 1))),
                )?,
                cache.clone(),
            )
            .await?;
            let visits = Arc::new(AtomicUsize::new(0));
            let measured = visits.clone();
            let operator =
                Partition::new_zero_preserving(move |_: &i64, bag: &Batch<(), i64>, work| {
                    measured.fetch_add(bag.iter().count(), Ordering::Relaxed);
                    work.charge(bag.iter().count())?;
                    let maximum = bag.iter().map(|(((), value), _)| *value).max();
                    Batch::from_updates(maximum.map(|value| (((), value), 1)))
                });
            let before = cache.stats()?.misses;
            let delta = TimedBatch { time: 2, batch: Batch::from_updates([((7, hot), 1)])? };
            let result =
                operator.evaluate_retained(&delta, &prior, &retained, Limits::default()).await?;
            let reads = cache.stats()?.misses - before;
            assert!(reads <= u64::try_from(hot)? + 3, "{hot}/{unrelated}: {reads}");
            assert_eq!(visits.load(Ordering::Relaxed), usize::try_from(hot)? + 1);
            assert_eq!(result.batch, Batch::from_updates([((7, hot - 1), -1), ((7, hot), 1)])?);
            assert_eq!(prior.time(), 1);
            assert_eq!(retained.time(), 1);
        }
    }
    Ok(())
}
