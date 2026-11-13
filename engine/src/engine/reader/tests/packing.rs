use crate::engine::{
    Batch,
    reader::{
        BatchReader as _, BlockCache, ObjectBatch, WriteLimits, bounded,
        format::{TARGET_OBJECT_BYTES, hash, header_bytes, pack_bytes},
    },
    tests::store::Store,
    trace::{Manifest, TraceSnapshot},
};
use anyhow::{Context as _, Result};
use futures_util::TryStreamExt as _;
use object_store::{ObjectStore as _, ObjectStoreExt as _};
use std::sync::{Arc, atomic::Ordering};

type Rows = Batch<i32, (i32, String)>;
fn batch() -> Result<Rows> {
    Batch::from_updates(
        (0..600).map(|n| ((7, (n, "x".repeat(1024))), if n % 2 == 0 { 2 } else { -3 })),
    )
}

#[tokio::test]
async fn one_pack_put_cold_reads_only_the_selected_authenticated_range() -> Result<()> {
    let store = Arc::new(Store::unpaced());
    let batch = batch()?;
    let root = ObjectBatch::write(store.clone(), &batch, "packing", 4096).await?;
    assert_eq!(store.puts.load(Ordering::SeqCst), 3);
    assert_eq!(store.reads.load(Ordering::SeqCst), 0);
    let object = ObjectBatch::<i32, (i32, String)>::open(store.clone(), root, "packing").await?;
    assert_eq!(object.test_packs().await?.len(), 1);
    assert!(object.block_count() > 2);
    let (_, range) = object.block_range(1).await?.context("missing middle page")?;
    let pack = object.test_packs().await?.first().cloned().context("missing pack")?;
    assert!(range.start > 0 && range.end < pack.bytes);
    let reads = store.reads.load(Ordering::SeqCst);
    let requested = store.range_bytes.load(Ordering::SeqCst);
    let rows = object.read(1).await?;
    assert!(!rows.is_empty());
    assert_eq!(store.reads.load(Ordering::SeqCst) - reads, 1);
    assert_eq!(store.range_start.load(Ordering::SeqCst), range.start);
    assert_eq!(store.range_end.load(Ordering::SeqCst), range.end);
    assert_eq!(store.range_bytes.load(Ordering::SeqCst) - requested, range.end - range.start);
    Ok(())
}

#[tokio::test]
async fn pack_and_index_failures_never_return_membership_and_retry_cached_rows_exactly()
-> Result<()> {
    for fail_at in [0, 1, 2] {
        let mut store = Store::unpaced();
        store.fail_at = Some(fail_at);
        let store = Arc::new(store);
        let batch = batch()?;
        let cache = Arc::new(BlockCache::new(1024 * 1024, 256));
        assert!(
            ObjectBatch::write_resident(
                store.clone(),
                &batch,
                "packing",
                (None, 4096),
                cache.clone()
            )
            .await
            .is_err()
        );
        assert_eq!(store.puts.load(Ordering::SeqCst), fail_at + 1);
        assert_eq!(store.reads.load(Ordering::SeqCst), 0);
        let objects = store.list(None).try_collect::<Vec<_>>().await?;
        assert!(
            !objects.iter().any(|object| object.location.as_ref().starts_with("pgderive/batch/"))
        );
        let object = ObjectBatch::write_resident(
            store.clone(),
            &batch,
            "packing",
            (None, 4096),
            cache.clone(),
        )
        .await?;
        let reads = store.reads.load(Ordering::SeqCst);
        let mut cursor = object.cursor().await?;
        let mut actual = Vec::new();
        while let Some((key, value, weight)) = cursor.current() {
            actual.push(((*key, value.clone()), weight));
            cursor.advance().await?;
        }
        assert_eq!(Batch::from_updates(actual)?, batch);
        let metadata_reads = cache.stats()?.index_reads;
        assert!(store.reads.load(Ordering::SeqCst) - reads <= usize::try_from(metadata_reads)?);
        assert_eq!(cache.stats()?.foreground_io.gets + cache.stats()?.scan_io.gets, 0);
    }
    Ok(())
}

#[tokio::test]
async fn one_root_spans_bounded_packs_and_index_splits_reset_pack_offsets() -> Result<()> {
    let store = Arc::new(Store::unpaced());
    let batch = Batch::from_updates((0..10_000).map(|n| {
        let payload =
            (0..16).map(|part| hash(format!("{n}-{part}").as_bytes())).collect::<String>();
        ((n, payload), 1)
    }))?;
    let root = ObjectBatch::write(store.clone(), &batch, "large-pack", 4096).await?;
    let object =
        ObjectBatch::<i32, String>::open(store.clone(), root.clone(), "large-pack").await?;
    assert!(object.test_packs().await?.len() > 1);
    assert_eq!(store.puts.load(Ordering::SeqCst), object.test_packs().await?.len() + 2);
    let target = u64::try_from(TARGET_OBJECT_BYTES)?;
    assert!(object.test_packs().await?.iter().all(|pack| pack.bytes <= target));
    // Keep many forced index splits within the unchanged 128-root bound.
    let limits = WriteLimits { index_blocks: 8, ..WriteLimits::default() };
    let roots =
        ObjectBatch::write_stream(store.clone(), &mut object.cursor().await?, "large-pack", limits)
            .await?;
    assert!(roots.len() > 1);
    let store: Arc<dyn object_store::ObjectStore> = store;
    let snapshot = TraceSnapshot::<i32, String>::reopen(
        store,
        Manifest { version: 2, time: 0, generation: 0, cohorts: vec![roots.len()], objects: roots },
        "large-pack",
        Arc::new(BlockCache::new(0, 0)),
    )
    .await?;
    assert_eq!(snapshot.materialize().await?, batch);
    Ok(())
}

#[tokio::test]
async fn tight_index_byte_budget_rebinds_each_page_to_a_fresh_pack() -> Result<()> {
    let store = Arc::new(Store::unpaced());
    let expected = batch()?;
    let root = ObjectBatch::write(store.clone(), &expected, "packing", 4096).await?;
    let object = ObjectBatch::<i32, (i32, String)>::open(store.clone(), root, "packing").await?;
    let mut fence_bytes = 0;
    for original in &object.test_blocks().await? {
        let mut block = original.clone();
        block.pack = 0;
        block.offset = 0;
        fence_bytes = fence_bytes.max(bounded::encode(&block)?.len());
    }
    let index_bytes = header_bytes("packing")? + pack_bytes()? + fence_bytes + 2;
    let limits = WriteLimits { index_bytes, index_blocks: 1, ..WriteLimits::default() };
    let roots =
        ObjectBatch::write_stream(store.clone(), &mut object.cursor().await?, "packing", limits)
            .await?;
    assert_eq!(roots.len(), object.block_count());
    for root in &roots {
        let packed =
            ObjectBatch::<i32, (i32, String)>::open(store.clone(), root.clone(), "packing").await?;
        assert_eq!(packed.block_count(), 1);
        assert_eq!(packed.leaf(0).await?.block.pack, 0);
        assert_eq!(packed.leaf(0).await?.block.offset, 0);
        assert!(
            store.head(&object_store::path::Path::from(root.path())).await?.size
                <= u64::try_from(index_bytes)?
        );
    }
    let snapshot = TraceSnapshot::<i32, (i32, String)>::reopen(
        store,
        Manifest { version: 2, time: 0, generation: 0, cohorts: vec![roots.len()], objects: roots },
        "packing",
        Arc::new(BlockCache::new(0, 0)),
    )
    .await?;
    assert_eq!(snapshot.materialize().await?, expected);
    Ok(())
}
