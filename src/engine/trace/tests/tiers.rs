use super::{Run, TraceSnapshot};
use crate::engine::{
    Batch,
    dataflow::Arrangement,
    reader::{BlockCache, ObjectBatch},
};
use anyhow::{Context, Result};
use object_store::{ObjectStore, ObjectStoreExt, memory::InMemory, path::Path};
use std::sync::Arc;

#[tokio::test]
async fn growing_tiny_flushes_never_rewrite_large_base() -> Result<()> {
    let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let writer = Arrangement::<i64, i64>::new(store.clone(), "tiers".into(), 65536)?;
    let base = Batch::from_updates((0..65536).map(|key| ((key, key), 1)))?;
    let pending = writer.empty().append_validated(Run::Memory(Arc::new(base)), 1)?;
    let mut trace = writer.flush(&pending).await?;
    let root = trace.manifest()?.objects[0].clone();
    let object = ObjectBatch::<i64, i64>::open(store.clone(), root.clone(), "tiers").await?;
    let block = Path::from(object.block_range(0).context("missing block")?.0);
    let bytes = store.get(&block).await?.bytes().await?;
    store.delete(&block).await?;
    for time in 2..=257 {
        let key = 65536 + i64::try_from(time)?;
        let delta = Batch::from_updates([((key, key), 1)])?;
        trace = writer.flush(&trace.append_validated(Run::Memory(Arc::new(delta)), time)?).await?;
        assert_eq!(trace.manifest()?.objects[0], root);
        assert!(trace.cohorts.len() < 32);
        assert_eq!(trace.unsealed_run_count(), 0);
    }
    store.put(&block, bytes.into()).await?;
    let fresh = Batch::from_updates([((0, 0), 1)])?;
    let pending = trace.append_validated(Run::Memory(Arc::new(fresh)), 258)?;
    assert_eq!(pending.unsealed_run_count(), 1);
    assert!(pending.run_count() > pending.unsealed_run_count());
    let restored = writer.reopen(trace.manifest()?).await?;
    assert_eq!(restored.manifest()?, trace.manifest()?);
    assert_eq!(restored.materialize().await?.iter().count(), 65536 + 256);
    Ok(())
}

#[tokio::test]
async fn multipack_cohort_survives_tiny_flushes_and_restart() -> Result<()> {
    let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let writer = Arrangement::<i64, i64>::new(store.clone(), "packs".into(), 16)?;
    let mut roots = Vec::new();
    for part in 0..8 {
        let batch = Batch::from_updates((part * 16..(part + 1) * 16).map(|key| ((key, key), 1)))?;
        let root = ObjectBatch::write(store.clone(), &batch, "packs", 16).await?;
        roots.push(Run::Object(ObjectBatch::open(store.clone(), root, "packs").await?));
    }
    let mut trace = TraceSnapshot::from_runs(roots, 1, 1)?;
    let base = trace.manifest()?.objects;
    for time in 2..=65 {
        let delta = Batch::from_updates([((0, 0), 1)])?;
        trace = writer.flush(&trace.append_validated(Run::Memory(Arc::new(delta)), time)?).await?;
        assert_eq!(&trace.manifest()?.objects[..8], base.as_slice());
        trace = TraceSnapshot::reopen(
            store.clone(),
            trace.manifest()?,
            "packs",
            Arc::new(BlockCache::new(0, 0)),
        )
        .await?;
        assert_eq!(trace.cohorts[0], 8);
    }
    assert_eq!(trace.tuple_weight(&(0, 0)).await?, 65);
    Ok(())
}

#[tokio::test]
async fn tier_subset_overflow_consolidates_only_complete_state() -> Result<()> {
    let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let writer = Arrangement::<i64, i64>::new(store.clone(), "overflow-tier".into(), 128)?;
    let base =
        Batch::from_updates((100..228).map(|key| ((key, key), 1)).chain([((1, 1), -i64::MAX)]))?;
    let mut trace =
        writer.flush(&writer.empty().append_validated(Run::Memory(Arc::new(base)), 1)?).await?;
    for time in 2..=8 {
        let (key, weight) = if time < 4 { (1, i64::MAX) } else { (i64::try_from(time)?, 1) };
        let batch = Batch::from_updates([((key, key), weight)])?;
        let root = ObjectBatch::write(store.clone(), &batch, "overflow-tier", 128).await?;
        trace = trace.append_validated(
            Run::Object(ObjectBatch::open(store.clone(), root, "overflow-tier").await?),
            time,
        )?;
    }
    let delta = Batch::from_updates([((9, 9), 1)])?;
    let pending = trace.append_validated(Run::Memory(Arc::new(delta)), 9)?;
    let flushed = writer.flush(&pending).await?;
    assert_eq!(flushed.materialize().await?, pending.materialize().await?);
    assert_eq!(flushed.cohorts.len(), 1);
    assert_eq!(flushed.tuple_weight(&(1, 1)).await?, i64::MAX);
    Ok(())
}

#[tokio::test]
async fn empty_cohorts_survive_growth_cancellation_and_cold_restore() -> Result<()> {
    let writer = Arrangement::<i64, i64>::new(Arc::new(InMemory::new()), "empty-tier".into(), 16)?;
    let mut trace = writer.compact(&writer.empty()).await?;
    assert_eq!(trace.run_count(), 1);
    let mut oracle = Batch::default();
    for time in 1..=32 {
        let weight = if time % 2 == 1 { 1 } else { -1 };
        let delta = Batch::from_updates([((1, 1), weight)])?;
        oracle.apply(&delta)?;
        trace = writer.flush(&trace.append_validated(Run::Memory(Arc::new(delta)), time)?).await?;
        trace = writer.reopen(trace.manifest()?).await?;
        assert_eq!(trace.materialize().await?, oracle);
    }
    Ok(())
}
