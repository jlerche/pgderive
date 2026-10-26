use super::{BlockCache, Cursor, KeyIdentity, ObjectBatch};
use crate::engine::{Batch, tests::store::Store};
use anyhow::{Context as _, Result};
use std::{sync::Arc, time::Duration};

#[tokio::test]
async fn scans_do_not_use_or_admit_foreground_complete_key_entries() -> Result<()> {
    let store = Arc::new(Store::unpaced());
    let cache = Arc::new(BlockCache::new(64 * 1024, 32));
    let batch = Batch::from_updates((0..10_i64).map(|key| ((key, key), 1)))?;
    let reference = ObjectBatch::write(store.clone(), &batch, "point-scan", 10).await?;
    let object = ObjectBatch::<i64, i64>::open_with_cache(
        store.clone(),
        reference.clone(),
        "point-scan",
        cache.clone(),
    )
    .await?;
    let page = object.key_page(&1).await?.context("missing key")?;
    Cursor::object_key_page(object.clone(), &KeyIdentity::new(&1)?, page, None).await?;
    let before = cache.stats()?;
    assert_eq!(before.point_entries, 0);
    let scan = ObjectBatch::<i64, i64>::open_with_cache(
        store,
        reference,
        "point-scan",
        BlockCache::scan(cache.clone()),
    )
    .await?;
    let page = scan.key_page(&2).await?.context("missing scan key")?;
    Cursor::object_key_page(scan, &KeyIdentity::new(&2)?, page, None).await?;
    let after = cache.stats()?;
    assert_eq!(
        (before.point_bytes, before.point_entries, before.point_hits),
        (after.point_bytes, after.point_entries, after.point_hits)
    );
    Ok(())
}

#[tokio::test(start_paused = true)]
async fn cancelled_page_read_has_no_logical_admission_and_retries_exactly() -> Result<()> {
    let mut store = Store::unpaced();
    store.read_delay = Duration::from_millis(20);
    let store = Arc::new(store);
    let cache = Arc::new(BlockCache::new(64 * 1024, 32));
    let batch = Batch::from_updates([((1_i64, 7_i64), -3), ((1, 8), 2)])?;
    let root = ObjectBatch::write(store.clone(), &batch, "point-cancel", 10).await?;
    let object = ObjectBatch::open_with_cache(store, root, "point-cancel", cache.clone()).await?;
    let identity = KeyIdentity::new(&1)?;
    let page = object.key_page(&1).await?.context("missing key")?;
    assert!(
        tokio::time::timeout(
            Duration::from_millis(5),
            Cursor::object_key_page(object.clone(), &identity, page.clone(), None)
        )
        .await
        .is_err()
    );
    assert_eq!(cache.stats()?.point_entries, 0);
    let cursor =
        Cursor::object_key_page(object, &identity, page, None).await?.context("missing cursor")?;
    assert_eq!(cursor.current(), Some((&1, &7, -3)));
    assert_eq!(cache.stats()?.point_entries, 0);
    Ok(())
}
