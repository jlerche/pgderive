use super::WriteLimits;
use crate::engine::{
    Batch,
    dataflow::Arrangement,
    reader::{BatchReader, ObjectBatch},
    trace::{Manifest, TraceSnapshot},
};
use anyhow::Result;
use object_store::{ObjectStore, ObjectStoreExt, memory::InMemory, path::Path};
use std::sync::Arc;

#[tokio::test]
async fn streamed_roots_preserve_full_identity_and_enforce_each_budget() -> Result<()> {
    let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let batch = Batch::from_updates(
        (0..40).map(|value| ((value / 10, value), if value % 2 == 0 { 2 } else { -3 })),
    )?;
    let limits = WriteLimits {
        block_bytes: 128,
        block_rows: 3,
        index_bytes: 1024,
        index_blocks: 1,
        output_roots: 32,
    };
    let roots =
        ObjectBatch::write_stream(store.clone(), &mut batch.cursor().await?, "i64-pair-v1", limits)
            .await?;
    assert!(roots.len() > 1);
    let mut actual = Vec::new();
    for root in &roots {
        assert!(store.head(&Path::from(root.path())).await?.size <= 1024);
        let object =
            ObjectBatch::<i64, i64>::open(store.clone(), root.clone(), "i64-pair-v1").await?;
        assert_eq!(object.blocks.len(), 1);
        for block in object.blocks.iter() {
            assert!(block.length <= 128);
            assert!(block.rows <= 3);
        }
        let mut cursor = object.cursor().await?;
        while let Some((key, value, weight)) = cursor.current() {
            actual.push(((*key, *value), weight));
            cursor.advance().await?;
        }
    }
    assert_eq!(Batch::from_updates(actual)?, batch);
    let too_few = WriteLimits { output_roots: 1, ..limits };
    assert!(
        ObjectBatch::write_stream(
            store.clone(),
            &mut batch.cursor().await?,
            "i64-pair-v1",
            too_few
        )
        .await
        .is_err()
    );
    let too_small = WriteLimits { block_bytes: 3, ..limits };
    assert!(
        ObjectBatch::write_stream(
            store.clone(),
            &mut batch.cursor().await?,
            "i64-pair-v1",
            too_small
        )
        .await
        .is_err()
    );
    let no_fence = WriteLimits { index_bytes: 32, ..limits };
    assert!(
        ObjectBatch::write_stream(store, &mut batch.cursor().await?, "i64-pair-v1", no_fence)
            .await
            .is_err()
    );
    Ok(())
}
#[tokio::test]
async fn full_trace_compaction_consolidates_before_physical_splitting() -> Result<()> {
    let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let schema = "exact-i64-v1";
    let positive = Batch::from_updates([((1, 1), i64::MAX), ((1, 2), 1)])?;
    let negative = Batch::from_updates([((1, 1), -i64::MAX)])?;
    let a = ObjectBatch::write(store.clone(), &positive, schema, 1).await?;
    let b = ObjectBatch::write(store.clone(), &negative, schema, 1).await?;
    let prior = TraceSnapshot::<i64, i64>::reopen(
        store.clone(),
        Manifest { version: 1, time: 1, generation: 1, objects: vec![a.clone(), a, b] },
        schema,
        Arc::new(crate::engine::reader::BlockCache::new(0, 0)),
    )
    .await?;
    let pinned = prior.clone();
    let compacted = Arrangement::<i64, i64>::new(store, schema.into(), 1)?.compact(&prior).await?;
    assert_eq!(compacted.time(), 1);
    assert_eq!(compacted.generation(), 2);
    assert_eq!(
        compacted.materialize().await?,
        Batch::from_updates([((1, 1), i64::MAX), ((1, 2), 2)])?
    );
    assert_eq!(pinned.materialize().await?, compacted.materialize().await?);
    assert_eq!(compacted.run_count(), 1);
    Ok(())
}

#[tokio::test]
async fn reachability_preserves_shared_blocks_and_rejects_corrupt_roots() -> Result<()> {
    let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let batch = Batch::from_updates([((1_i64, 2_i64), 3)])?;
    let a = ObjectBatch::write(store.clone(), &batch, "schema-a", 1).await?;
    let b = ObjectBatch::write(store.clone(), &batch, "schema-b", 1).await?;
    assert_ne!(a.path(), b.path());
    let children = a.dependencies(&store).await?;
    assert_eq!(children, b.dependencies(&store).await?);
    assert_eq!(children.len(), 1);
    store.delete(&Path::from(a.path())).await?;
    assert!(a.dependencies(&store).await.is_err());
    assert_eq!(b.dependencies(&store).await?, children);
    let path = Path::from(b.path());
    let mut bytes = store.get(&path).await?.bytes().await?.to_vec();
    bytes[0] ^= 1;
    store.put(&path, bytes.into()).await?;
    assert!(b.dependencies(&store).await.is_err());
    Ok(())
}

#[tokio::test]
async fn closed_namespace_deletes_cannot_hit_recreated_equal_content() -> Result<()> {
    let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let batch = Batch::from_updates([((1_i64, 2_i64), 3)])?;
    let old = ObjectBatch::write_namespaced(
        store.clone(),
        &batch,
        "i64-pair-v1",
        ("pgderive/upload-v1/old-1", 1),
    )
    .await?;
    let new = ObjectBatch::write_namespaced(
        store.clone(),
        &batch,
        "i64-pair-v1",
        ("pgderive/upload-v1/new-2", 1),
    )
    .await?;
    assert_ne!(old.path(), new.path());
    let old_blocks = old.dependencies(&store).await?;
    let new_blocks = new.dependencies(&store).await?;
    assert_ne!(old_blocks, new_blocks);
    for path in old_blocks {
        store.delete(&Path::from(path)).await?;
    }
    store.delete(&Path::from(old.path())).await?;
    let object = ObjectBatch::<i64, i64>::open(store.clone(), new.clone(), "i64-pair-v1").await?;
    let mut cursor = object.cursor().await?;
    assert_eq!(cursor.current(), Some((&1, &2, 3)));
    cursor.advance().await?;
    assert!(cursor.current().is_none());
    assert_eq!(object.block_path(0), new_blocks.first().map(String::as_str));
    let roots = ObjectBatch::write_stream_namespaced(
        store.clone(),
        &mut batch.cursor().await?,
        "i64-pair-v1",
        ("pgderive/upload-v1/stream-3", WriteLimits::default()),
    )
    .await?;
    let streamed = ObjectBatch::<i64, i64>::open(store, roots[0].clone(), "i64-pair-v1").await?;
    assert_eq!(streamed.cursor().await?.current(), Some((&1, &2, 3)));
    Ok(())
}
