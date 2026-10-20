use crate::engine::{
    Batch,
    reader::{BlockCache, ObjectBatch},
    trace::{Run, TraceSnapshot},
};
use anyhow::{Context, Result};
use object_store::{ObjectStore, ObjectStoreExt, memory::InMemory, path::Path};
use std::sync::Arc;
#[tokio::test]
async fn bounded_ranges_skip_disjoint_blocks_consolidate_and_retry() -> Result<()> {
    let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let cache = Arc::new(BlockCache::new(0, 0));
    let batch = Batch::from_updates((0..10).map(|key| ((key, key), 2)))?;
    let reference = ObjectBatch::write(store.clone(), &batch, "range-v1", 1).await?;
    let object =
        ObjectBatch::open_with_cache(store.clone(), reference, "range-v1", cache.clone()).await?;
    let mut trace = TraceSnapshot::empty().append_validated(Run::Object(object.clone()), 1)?;
    trace = trace.append_validated(
        Run::Memory(Arc::new(Batch::from_updates([((4, 4), -2), ((5, 5), 1), ((6, 7), 3)])?)),
        2,
    )?;
    for ordinal in [0, 1, 2, 7, 8, 9] {
        store
            .delete(&Path::from(object.block_path(ordinal).context("missing fixture block")?))
            .await?;
    }
    let failed = Path::from(object.block_path(5).context("missing fixture block")?);
    let bytes = store.get(&failed).await?.bytes().await?;
    let mut cursor = trace.range_cursor(&3, &6).await?;
    assert_eq!(cursor.current(), Some((&3, &3, 2)));
    store.delete(&failed).await?;
    assert!(cursor.advance().await.is_err());
    assert_eq!(cursor.current(), Some((&3, &3, 2)));
    store.put(&failed, bytes.into()).await?;
    cursor.advance().await?;
    assert_eq!(cursor.current(), Some((&5, &5, 3)));
    cursor.advance().await?;
    assert_eq!(cursor.current(), Some((&6, &6, 2)));
    cursor.advance().await?;
    assert_eq!(cursor.current(), Some((&6, &7, 3)));
    cursor.advance().await?;
    assert!(cursor.current().is_none());
    assert!(trace.range_cursor(&8, &3).await.is_err());
    // Gaps at the lower bound may select a later key, unlike exact-key probes.
    let memory = TraceSnapshot::empty().append_validated(
        Run::Memory(Arc::new(Batch::from_updates([((1, 0), 1), ((7, 0), 2)])?)),
        1,
    )?;
    assert_eq!(memory.range_cursor(&2, &8).await?.current(), Some((&7, &0, 2)));
    assert!(memory.range_cursor(&2, &6).await?.current().is_none());
    assert!(trace.materialize().await.is_err());
    Ok(())
}
