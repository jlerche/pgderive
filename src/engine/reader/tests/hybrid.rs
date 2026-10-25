use super::{CacheConfig, EncodedCache, hash};
use anyhow::{Result, bail};
use std::time::Duration;

#[tokio::test]
async fn disk_reopen_integrity_and_exclusive_shutdown() -> Result<()> {
    let directory = tempfile::tempdir()?;
    let settings = CacheConfig {
        directory: directory.path().into(),
        memory_bytes: 1024 * 1024,
        disk_bytes: 64 * 1024 * 1024,
    };
    let cache = settings.open().await?;
    assert!(settings.open().await.is_err());
    let payload = vec![42; 1024];
    let digest = hash(&payload);
    cache.insert("block".into(), payload.clone());
    assert_eq!(cache.get("block", &digest).await, Some(payload.clone()));
    cache.insert("bad".into(), vec![0]);
    assert!(cache.get("bad", &digest).await.is_none());
    assert!(cache.get("missing", &digest).await.is_none());
    // No explicit close: the directory lease must survive asynchronous drop.
    drop(cache);
    let reopened = reopen(&settings).await?;
    assert_eq!(reopened.get("block", &digest).await, Some(payload));
    reopened.close().await?;
    drop(reopened);
    let mut invalid = settings;
    invalid.memory_bytes = 0;
    if invalid.open().await.is_ok() {
        bail!("accepted invalid cache limits");
    }
    Ok(())
}

#[tokio::test]
async fn disk_hit_never_masks_missing_or_corrupt_durable_state() -> Result<()> {
    use crate::engine::{
        Batch,
        reader::{BatchReader as _, BlockCache, ObjectBatch},
        trace::{Manifest, TraceSnapshot},
    };
    use object_store::{ObjectStore, ObjectStoreExt as _, memory::InMemory, path::Path};
    use std::sync::Arc;
    let directory = tempfile::tempdir()?;
    let settings = CacheConfig {
        directory: directory.path().into(),
        memory_bytes: 1024 * 1024,
        disk_bytes: 64 * 1024 * 1024,
    };
    let encoded = Arc::new(settings.open().await?);
    let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let batch = Batch::from_updates([((1_i64, "payload".repeat(100)), 3)])?;
    let reference = ObjectBatch::write(store.clone(), &batch, "cache-fixture", 1).await?;
    let child = reference.dependencies(&store).await?[0].clone();
    let original = store.get(&Path::from(child.clone())).await?.bytes().await?;
    let cache = Arc::new(BlockCache::new(0, 0).with_encoded(Some(encoded.clone())));
    let object = ObjectBatch::<i64, String>::open_with_cache(
        store.clone(),
        reference.clone(),
        "cache-fixture",
        cache,
    )
    .await?;
    assert_eq!(object.cursor().await?.current().map(|(_, _, weight)| weight), Some(3));
    drop(object);
    encoded.close().await?;
    drop(encoded);
    let encoded = Arc::new(reopen(&settings).await?);
    let cache = Arc::new(BlockCache::new(0, 0).with_encoded(Some(encoded.clone())));
    let object = ObjectBatch::<i64, String>::open_with_cache(
        store.clone(),
        reference.clone(),
        "cache-fixture",
        cache.clone(),
    )
    .await?;
    store.delete(&Path::from(child.clone())).await?;
    // Decoded retention is disabled, so this must come from the reopened disk cache.
    assert_eq!(object.cursor().await?.current().map(|(_, _, weight)| weight), Some(3));
    let manifest =
        Manifest { version: 2, time: 0, generation: 0, cohorts: vec![1], objects: vec![reference] };
    assert!(
        TraceSnapshot::<i64, String>::reopen(
            store.clone(),
            manifest.clone(),
            "cache-fixture",
            cache.clone()
        )
        .await
        .is_err()
    );
    store.put(&Path::from(child.clone()), vec![0; original.len()].into()).await?;
    assert!(
        TraceSnapshot::<i64, String>::reopen(
            store.clone(),
            manifest.clone(),
            "cache-fixture",
            cache.clone()
        )
        .await
        .is_err()
    );
    store.put(&Path::from(child), original.into()).await?;
    assert!(
        TraceSnapshot::<i64, String>::reopen(store, manifest, "cache-fixture", cache).await.is_ok()
    );
    encoded.close().await?;
    Ok(())
}

async fn reopen(settings: &CacheConfig) -> Result<EncodedCache> {
    Ok(tokio::time::timeout(Duration::from_secs(15), async {
        loop {
            if let Ok(cache) = settings.open().await {
                return cache;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await?)
}

#[tokio::test]
async fn canceled_close_keeps_shutdown_owned() -> Result<()> {
    use std::sync::Arc;
    let directory = tempfile::tempdir()?;
    let settings = CacheConfig {
        directory: directory.path().into(),
        memory_bytes: 1024 * 1024,
        disk_bytes: 64 * 1024 * 1024,
    };
    let cache = Arc::new(settings.open().await?);
    cache.insert("pending".into(), vec![42; 512 * 1024]);
    let closing = tokio::spawn(async move { cache.close().await });
    tokio::task::yield_now().await;
    closing.abort();
    let _canceled = closing.await;
    let reopened = reopen(&settings).await?;
    reopened.close().await?;
    Ok(())
}

#[tokio::test]
async fn point_prefetch_respects_encoded_cache_without_redundant_object_reads() -> Result<()> {
    use crate::engine::{
        Batch,
        reader::{BatchReader as _, BlockCache, ObjectBatch},
        tests::store::Store,
        trace::{Run, TraceSnapshot},
    };
    use std::sync::{Arc, atomic::Ordering};
    let directory = tempfile::tempdir()?;
    let encoded = Arc::new(
        CacheConfig {
            directory: directory.path().into(),
            memory_bytes: 1024 * 1024,
            disk_bytes: 64 * 1024 * 1024,
        }
        .open()
        .await?,
    );
    let store = Arc::new(Store::unpaced());
    let reference = ObjectBatch::write(
        store.clone(),
        &Batch::from_updates([((1_i64, 7_i64), 1), ((2, 8), 1)])?,
        "prefetch-encoded",
        1,
    )
    .await?;
    let warm = ObjectBatch::<i64, i64>::open_with_cache(
        store.clone(),
        reference.clone(),
        "prefetch-encoded",
        Arc::new(BlockCache::new(0, 0).with_encoded(Some(encoded.clone()))),
    )
    .await?;
    let mut cursor = warm.cursor().await?;
    cursor.advance().await?;
    let repacked = ObjectBatch::write(
        store.clone(),
        &Batch::from_updates([((0_i64, 6_i64), 1), ((1, 7), 1), ((2, 8), 1)])?,
        "prefetch-encoded",
        1,
    )
    .await?;
    assert_ne!(repacked, reference);
    let cold = ObjectBatch::<i64, i64>::open_with_cache(
        store.clone(),
        repacked,
        "prefetch-encoded",
        Arc::new(BlockCache::new(16 * 1024 * 1024, 256).with_encoded(Some(encoded.clone()))),
    )
    .await?;
    let trace = TraceSnapshot::empty().append_validated(Run::Object(cold), 1)?;
    let reads = store.reads.load(Ordering::SeqCst);
    let mut probes = trace.probes();
    let _read_ahead = probes.prefetch(&[&1, &2]).await?;
    assert_eq!(probes.cursor(&1).await?.current(), Some((&1, &7, 1)));
    assert_eq!(probes.cursor(&2).await?.current(), Some((&2, &8, 1)));
    assert_eq!(store.reads.load(Ordering::SeqCst), reads);
    encoded.close().await?;
    Ok(())
}
