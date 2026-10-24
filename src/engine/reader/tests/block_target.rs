use crate::engine::{
    Batch,
    reader::{
        BatchReader as _, BlockCache, ObjectBatch, WriteLimits, bounded,
        streaming::TARGET_BLOCK_BYTES,
    },
    trace::{Manifest, TraceSnapshot},
};
use anyhow::{Context as _, Result};
use object_store::{ObjectStore, ObjectStoreExt as _, memory::InMemory, path::Path};
use std::sync::Arc;

#[tokio::test]
async fn shared_writers_split_wide_groups_and_cold_reads_validate_every_block() -> Result<()> {
    let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let batch = Batch::from_updates(
        (0..200).map(|id| ((7_i64, (id, "x".repeat(1024))), if id % 2 == 0 { 2 } else { -3 })),
    )?;
    let root = ObjectBatch::write(store.clone(), &batch, "wide-group", 4096).await?;
    let object =
        ObjectBatch::<i64, (i32, String)>::open(store.clone(), root.clone(), "wide-group").await?;
    assert!(object.blocks.len() > 1);
    for ordinal in 0..object.blocks.len() {
        assert!(bounded::encode(&*object.read(ordinal).await?)?.len() <= TARGET_BLOCK_BYTES);
    }
    let roots = ObjectBatch::write_stream(
        store.clone(),
        &mut object.cursor().await?,
        "wide-group",
        WriteLimits::default(),
    )
    .await?;
    assert_eq!(roots, vec![root.clone()]);
    let snapshot = TraceSnapshot::<i64, (i32, String)>::reopen(
        store.clone(),
        Manifest { version: 1, time: 0, generation: 0, objects: roots },
        "wide-group",
        Arc::new(BlockCache::new(0, 0)),
    )
    .await?;
    assert_eq!(snapshot.materialize().await?, batch);
    let last = object.blocks.len() - 1;
    let path = object.block_range(last).map(|(path, _)| path).context("last block missing")?;
    store.put(&Path::from(path), vec![0_u8; 3].into()).await?;
    assert!(object.read(last).await.is_err());
    Ok(())
}

#[tokio::test]
async fn soft_target_preserves_large_rows_empty_roots_and_hard_budgets() -> Result<()> {
    let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let batch = Batch::from_updates([
        ((0_i64, "x".repeat(TARGET_BLOCK_BYTES + 1)), 1),
        ((1, "small".into()), -2),
        ((2, "y".repeat(TARGET_BLOCK_BYTES + 1)), 3),
    ])?;
    let root = ObjectBatch::write(store.clone(), &batch, "large-rows", 4096).await?;
    let object = ObjectBatch::<i64, String>::open(store.clone(), root, "large-rows").await?;
    assert_eq!(object.blocks.iter().map(|block| block.rows).collect::<Vec<_>>(), vec![1, 1, 1]);
    let mut actual = Vec::new();
    for ordinal in 0..object.blocks.len() {
        actual.extend(object.read(ordinal).await?.iter().cloned());
    }
    assert_eq!(Batch::from_updates(actual)?, batch);
    let limits = WriteLimits { block_bytes: TARGET_BLOCK_BYTES, ..WriteLimits::default() };
    assert!(
        ObjectBatch::write_stream(store.clone(), &mut batch.cursor().await?, "large-rows", limits)
            .await
            .is_err()
    );
    let root =
        ObjectBatch::write(store.clone(), &Batch::<i64, String>::default(), "empty", 4096).await?;
    let empty = ObjectBatch::<i64, String>::open(store, root, "empty").await?;
    assert!(empty.blocks.is_empty());
    assert!(empty.cursor().await?.current().is_none());
    Ok(())
}

#[tokio::test]
async fn decode_counters_measure_cold_payloads_not_cache_hits() -> Result<()> {
    let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let batch = Batch::from_updates([((1_i64, "a".to_owned()), 1), ((2, "b".to_owned()), 1)])?;
    let reference = ObjectBatch::write(store.clone(), &batch, "decode-counters", 1).await?;
    let cache = Arc::new(BlockCache::new(4096, 8));
    let object = ObjectBatch::<i64, String>::open_with_cache(
        store,
        reference,
        "decode-counters",
        cache.clone(),
    )
    .await?;
    let rows = object.read(0).await?;
    let bytes = u64::try_from(bounded::encode(&*rows)?.len())?;
    assert_eq!(cache.stats()?.decoded_bytes, bytes);
    assert_eq!(cache.stats()?.decoded_rows, 1);
    object.read(0).await?;
    assert_eq!(cache.stats()?.decoded_bytes, bytes);
    assert_eq!(cache.stats()?.decoded_rows, 1);
    object.read(1).await?;
    assert_eq!(cache.stats()?.decoded_rows, 2);
    Ok(())
}
