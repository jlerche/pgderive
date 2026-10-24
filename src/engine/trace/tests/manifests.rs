use crate::engine::{
    Batch,
    dataflow::{Arrangement, TimedBatch},
    reader::{BlockCache, ObjectBatch},
    trace::{Manifest, Run, TraceSnapshot},
};
use anyhow::{Context, Result};
use object_store::{ObjectStore, ObjectStoreExt, memory::InMemory, path::Path};
use std::sync::Arc;

#[tokio::test]
async fn manifest_reopen_preserves_repeated_run_multiplicity_and_physical_generation() -> Result<()>
{
    let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let writer = Arrangement::<i64, i64>::new(store.clone(), "ints".into(), 1)?;
    let batch = Batch::from_updates([((1, 1), 1), ((2, 2), -1)])?;
    let first =
        writer.stage(&writer.empty(), &TimedBatch { time: 1, batch: batch.clone() }).await?;
    let second = writer.stage(&first, &TimedBatch { time: 2, batch }).await?;
    let manifest = second.manifest()?;
    assert_eq!(manifest.objects[0], manifest.objects[1]);
    let reopened = writer.reopen(serde_json::from_slice(&serde_json::to_vec(&manifest)?)?).await?;
    assert_eq!(reopened.manifest()?, manifest);
    assert_eq!(reopened.materialize().await?, Batch::from_updates([((1, 1), 2), ((2, 2), -2)])?);
    let compacted = writer.compact(&reopened).await?;
    let reopened = writer.reopen(compacted.manifest()?).await?;
    assert_eq!(reopened.time(), 2);
    assert_eq!(reopened.generation(), 3);
    assert_eq!(reopened.materialize().await?, compacted.materialize().await?);
    let mut invalid = manifest.clone();
    invalid.version = 2;
    assert!(writer.reopen(invalid).await.is_err());
    let mut invalid = manifest;
    invalid.generation = 1;
    assert!(writer.reopen(invalid).await.is_err());
    Ok(())
}
#[tokio::test]
async fn cold_manifest_reopen_does_not_accept_warm_cached_missing_or_corrupt_objects() -> Result<()>
{
    let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let cache = Arc::new(BlockCache::new(1024, 8));
    let writer = Arrangement::new(store.clone(), "ints".into(), 1)?.with_cache(cache);
    let batch = Batch::from_updates([((1, 1), 1), ((2, 2), 1)])?;
    let state = writer.stage(&writer.empty(), &TimedBatch { time: 1, batch }).await?;
    state.materialize().await?;
    let manifest = state.manifest()?;
    let root = manifest.objects.first().context("missing root")?;
    let object = ObjectBatch::<i64, i64>::open(store.clone(), root.clone(), "ints").await?;
    let path = Path::from(object.block_range(1).map(|(path, _)| path).context("missing block")?);
    let bytes = store.get(&path).await?.bytes().await?;
    store.delete(&path).await?;
    assert!(writer.reopen(manifest.clone()).await.is_err());
    // The old pinned reader may legitimately still have valid bytes in its cache.
    assert_eq!(state.materialize().await?.iter().count(), 2);
    let mut corrupt = bytes.to_vec();
    corrupt[0] ^= 1;
    store.put(&path, corrupt.into()).await?;
    assert!(writer.reopen(manifest.clone()).await.is_err());
    store.put(&path, bytes.into()).await?;
    assert_eq!(writer.reopen(manifest.clone()).await?.time(), 1);
    store.delete(&Path::from(root.path())).await?;
    assert!(writer.reopen(manifest).await.is_err());
    Ok(())
}
#[tokio::test]
async fn memory_and_invalid_logical_membership_cannot_be_recovered() -> Result<()> {
    let mut trace = TraceSnapshot::<i64, i64>::empty();
    trace =
        trace.append_validated(Run::Memory(Arc::new(Batch::from_updates([((1, 1), 1)])?)), 1)?;
    assert!(trace.manifest().is_err());
    let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let reference = ObjectBatch::write(
        store.clone(),
        &Batch::from_updates([((1_i64, 1_i64), i64::MAX)])?,
        "ints",
        1,
    )
    .await?;
    let invalid = Manifest {
        version: 1,
        time: 1,
        generation: 1,
        objects: vec![reference.clone(), reference],
    };
    assert!(
        TraceSnapshot::<i64, i64>::reopen(
            store.clone(),
            invalid.clone(),
            "ints",
            Arc::new(BlockCache::new(0, 0))
        )
        .await
        .is_err()
    );
    assert!(
        TraceSnapshot::<i64, i64>::reopen(store, invalid, "other", Arc::new(BlockCache::new(0, 0)))
            .await
            .is_err()
    );
    Ok(())
}

#[tokio::test]
async fn oversized_full_compaction_preserves_hot_cache_and_admits_output_only_when_read()
-> Result<()> {
    let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let cache = Arc::new(BlockCache::new(4096, 1));
    let writer =
        Arrangement::<i64, i64>::new(store.clone(), "scan".into(), 1)?.with_cache(cache.clone());
    let batch = Batch::from_updates((0..32).map(|key| ((key, key), 1)))?;
    let cold_writer = Arrangement::<i64, i64>::new(store, "scan".into(), 1)?;
    let state = cold_writer.stage(&cold_writer.empty(), &TimedBatch { time: 1, batch }).await?;
    let state = writer.reopen(state.manifest()?).await?;
    // Materialization leaves the final block hot in this one-entry cache.
    state.materialize().await?;
    let before = cache.stats()?;
    let compacted = writer.compact(&state).await?;
    let after = cache.stats()?;
    assert_eq!((before.hits, before.misses, before.bytes), (after.hits, after.misses, after.bytes));
    assert_eq!(compacted.time(), state.time());
    assert_eq!(compacted.generation(), state.generation() + 1);
    let delta = Batch::from_updates([((31, 31), -1)])?;
    state.validate_delta(&delta).await?;
    assert_eq!(cache.stats()?.misses, before.misses);
    let delta = Batch::from_updates([((0, 0), -1)])?;
    compacted.validate_delta(&delta).await?;
    let loaded = cache.stats()?;
    assert_eq!(loaded.misses, before.misses + 1);
    compacted.validate_delta(&delta).await?;
    assert_eq!(cache.stats()?.misses, loaded.misses);
    assert!(cache.stats()?.hits > loaded.hits);
    assert_eq!(compacted.materialize().await?, state.materialize().await?);
    Ok(())
}

#[tokio::test]
async fn trusted_compaction_coalesces_cold_pages_and_rejects_corruption() -> Result<()> {
    use std::sync::atomic::Ordering;
    let store = Arc::new(crate::engine::tests::store::Store::default());
    let writer = Arrangement::<i64, i64>::new(store.clone(), "once".into(), 1)?;
    let batch = Batch::from_updates((0..8).map(|key| ((key, key), 1)))?;
    let state = writer.stage(&writer.empty(), &TimedBatch { time: 1, batch }).await?;
    let before = store.reads.load(Ordering::SeqCst);
    let compacted =
        writer.clone().with_namespace("pgderive/upload-v1/compaction-once").compact(&state).await?;
    assert_eq!(store.reads.load(Ordering::SeqCst) - before, 1);
    assert_eq!(compacted.materialize().await?, state.materialize().await?);
    let manifest = state.manifest()?;
    let root = manifest.objects.first().context("missing root")?;
    let object = ObjectBatch::<i64, i64>::open(store.clone(), root.clone(), "once").await?;
    let path = Path::from(object.block_range(3).map(|(path, _)| path).context("missing block")?);
    store.put(&path, b"corrupt".to_vec().into()).await?;
    assert!(writer.compact(&state).await.is_err());
    assert_eq!(state.manifest()?, manifest);
    Ok(())
}

#[tokio::test]
async fn fitting_rewritten_checkpoint_pages_need_no_remote_reads_after_compaction() -> Result<()> {
    use std::sync::atomic::Ordering;
    let store = Arc::new(crate::engine::tests::store::Store::unpaced());
    let cache = Arc::new(BlockCache::new(16 * 1024, 16));
    let writer =
        Arrangement::<i64, i64>::new(store.clone(), "handoff".into(), 1)?.with_cache(cache.clone());
    let initial = Batch::from_updates((0..8).map(|key| ((key, key), 1)))?;
    let state = writer.stage(&writer.empty(), &TimedBatch { time: 1, batch: initial }).await?;
    let replacement =
        Batch::from_updates((0..8).flat_map(|key| [((key, key), -1), ((key, key + 100), 1)]))?;
    let pending = state.append_validated(Run::Memory(Arc::new(replacement)), 2)?;
    let compacted = writer.compact(&pending).await?;
    assert_ne!(state.manifest()?.objects, compacted.manifest()?.objects);
    let reads = store.reads.load(Ordering::SeqCst);
    assert_eq!(
        compacted.materialize().await?,
        Batch::from_updates((0..8).map(|key| ((key, key + 100), 1)))?
    );
    assert_eq!(store.reads.load(Ordering::SeqCst), reads);
    assert!(cache.stats()?.bytes <= 16 * 1024);
    let cold = TraceSnapshot::<i64, i64>::reopen(
        store,
        compacted.manifest()?,
        "handoff",
        Arc::new(BlockCache::new(0, 0)),
    )
    .await?;
    assert_eq!(cold.materialize().await?, compacted.materialize().await?);
    Ok(())
}
