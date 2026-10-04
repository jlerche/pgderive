use crate::engine::{
    Batch,
    dataflow::{Arrangement, Stream},
    reader::{BlockCache, ObjectBatch},
    trace::{Manifest, Run, Trace, TraceSnapshot},
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
    let first = writer.stage(&writer.empty(), &Stream { time: 1, batch: batch.clone() }).await?;
    let second = writer.stage(&first, &Stream { time: 2, batch }).await?;
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
    let state = writer.stage(&writer.empty(), &Stream { time: 1, batch }).await?;
    state.materialize().await?;
    let manifest = state.manifest()?;
    let root = manifest.objects.first().context("missing root")?;
    let object = ObjectBatch::<i64, i64>::open(store.clone(), root.clone(), "ints").await?;
    let path = Path::from(object.block_path(1).context("missing block")?);
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
    let mut trace = Trace::<i64, i64>::default();
    trace.commit(
        trace.prepare(Run::Memory(Arc::new(Batch::from_updates([((1, 1), 1)])?)), 1).await?,
    )?;
    assert!(trace.snapshot().manifest().is_err());
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
