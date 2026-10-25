use crate::engine::tests::store::Store;
use crate::engine::{
    Batch,
    reader::{BlockCache, ObjectBatch},
    trace::{Run, TraceSnapshot},
};
use anyhow::Result;
use std::sync::{Arc, atomic::Ordering};

async fn object(
    store: Arc<Store>,
    rows: Vec<crate::engine::reader::Entry<i64, i64>>,
) -> Result<Run<i64, i64>> {
    let batch = Batch::from_updates(rows)?;
    let root = ObjectBatch::write(store.clone(), &batch, "weight-bounds", 128).await?;
    Ok(Run::Object(
        ObjectBatch::open_with_cache(store, root, "weight-bounds", Arc::new(BlockCache::new(0, 0)))
            .await?,
    ))
}
#[tokio::test]
async fn signed_certificate_skips_cold_pages_but_never_proves_bag_validity() -> Result<()> {
    let store = Arc::new(Store::unpaced());
    let mut runs = Vec::new();
    for n in 0..16 {
        runs.push(
            object(store.clone(), vec![((1, 1), if n % 2 == 0 { 1 } else { -1 }), ((2, n), 1)])
                .await?,
        );
    }
    runs.push(Run::Memory(Arc::new(Batch::from_updates([((1, 1), 1)])?)));
    let trace = TraceSnapshot::from_runs(runs, 1, 1)?;
    let before = store.reads.load(Ordering::SeqCst);
    trace.validate_delta(&Batch::from_updates([((1, 1), -2)])?).await?;
    assert_eq!(store.reads.load(Ordering::SeqCst), before);
    assert!(trace.validate_bag_delta(&Batch::from_updates([((1, 1), -2)])?).await.is_err());
    assert_eq!(store.reads.load(Ordering::SeqCst) - before, 16);
    Ok(())
}
#[tokio::test]
async fn uncertain_bounds_preserve_duplicates_cancellation_minimum_and_overflow() -> Result<()> {
    let store = Arc::new(Store::unpaced());
    let positive = object(store.clone(), vec![((1, 1), i64::MAX)]).await?;
    let negative = object(store.clone(), vec![((1, 1), -i64::MAX)]).await?;
    let trace = TraceSnapshot::from_runs(vec![positive.clone(), positive, negative], 1, 1)?;
    let before = store.reads.load(Ordering::SeqCst);
    trace.validate_delta(&Batch::from_updates([((1, 1), -i64::MAX)])?).await?;
    assert_eq!(store.reads.load(Ordering::SeqCst) - before, 3);
    assert!(trace.validate_delta(&Batch::from_updates([((1, 1), 1)])?).await.is_err());
    let minimum = TraceSnapshot::from_runs(
        vec![object(store.clone(), vec![((1, 1), i64::MIN)]).await?],
        1,
        1,
    )?;
    minimum.validate_delta(&Batch::from_updates([((1, 1), 1)])?).await?;
    assert!(minimum.validate_delta(&Batch::from_updates([((1, 1), -1)])?).await.is_err());
    let unrelated = TraceSnapshot::from_runs(
        vec![object(store.clone(), vec![((99, 99), i64::MAX)]).await?],
        1,
        1,
    )?;
    let before = store.reads.load(Ordering::SeqCst);
    unrelated.validate_delta(&Batch::from_updates([((1, 1), 1)])?).await?;
    assert_eq!(store.reads.load(Ordering::SeqCst), before);
    let empty = TraceSnapshot::<i64, i64>::empty();
    empty.validate_delta(&Batch::default()).await?;
    Ok(())
}

#[test]
fn memory_bounds_follow_consolidation_apply_and_failed_apply() -> Result<()> {
    let mut batch = Batch::from_updates([((1, 1), i64::MAX), ((1, 1), -i64::MAX), ((2, 2), -7)])?;
    assert_eq!(batch.max_weight(), 7);
    assert_eq!(batch.clone().max_weight(), 7);
    batch.apply(&Batch::from_updates([((2, 2), 7)])?)?;
    assert_eq!(batch.max_weight(), 0);
    let mut minimum = Batch::from_canonical(vec![((1, 1), i64::MIN)])?;
    assert_eq!(minimum.max_weight(), i64::MIN.unsigned_abs());
    assert!(minimum.apply(&Batch::from_updates([((1, 1), -1)])?).is_err());
    assert_eq!(minimum.max_weight(), i64::MIN.unsigned_abs());
    assert_eq!(minimum.weight(&(1, 1)), i64::MIN);
    Ok(())
}
#[tokio::test]
async fn object_bound_is_maximum_across_blocks_and_full_tuple_values() -> Result<()> {
    let store = Arc::new(Store::unpaced());
    let batch = Batch::from_updates((0..8).map(|value| ((1_i64, value), i64::MAX / 4)))?;
    let root = ObjectBatch::write(store.clone(), &batch, "weight-bounds", 1).await?;
    let object = ObjectBatch::open_with_cache(
        store.clone(),
        root,
        "weight-bounds",
        Arc::new(BlockCache::new(0, 0)),
    )
    .await?;
    let trace = TraceSnapshot::from_runs(vec![Run::Object(object)], 1, 1)?;
    let before = store.reads.load(Ordering::SeqCst);
    trace.validate_delta(&Batch::from_updates([((1, 0), 1)])?).await?;
    assert_eq!(store.reads.load(Ordering::SeqCst), before);
    Ok(())
}
