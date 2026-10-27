use super::super::GcLimits;
use super::GcCache;
use crate::engine::{
    Batch,
    reader::{ObjectBatch, ObjectRef},
    tests::store::Store,
};
use anyhow::Result;
use object_store::ObjectStore;
use std::sync::{Arc, atomic::Ordering};

async fn root(store: Arc<Store>, key: i64, rows: i64) -> Result<ObjectRef> {
    let batch = Batch::from_updates((key..key + rows).map(|key| ((key, "x".repeat(1024)), 1)))?;
    ObjectBatch::write(store, &batch, "gc-cache", 1).await
}
#[tokio::test]
async fn verified_dependencies_are_reused_and_only_current_roots_are_marked() -> Result<()> {
    let store = Arc::new(Store::unpaced());
    let reference = root(store.clone(), 0, 128).await?;
    let remote: Arc<dyn ObjectStore> = store.clone();
    let mut cache = GcCache::default();
    let before = store.reads.load(Ordering::SeqCst);
    let first = cache.mark_snapshot(&remote, vec![reference.clone()], GcLimits::default()).await?;
    // Root, packed index, and one data pack. Authentication HEADs and reads the root,
    // then authenticates every branch/leaf from one bounded index-pack GET.
    assert_eq!(first.len(), 3);
    assert_eq!(store.reads.load(Ordering::SeqCst) - before, 3);
    for _ in 0..10 {
        assert_eq!(
            cache
                .mark_snapshot(
                    &remote,
                    vec![reference.clone(), reference.clone()],
                    GcLimits::default()
                )
                .await?,
            first
        );
    }
    assert_eq!(store.reads.load(Ordering::SeqCst) - before, 3);
    assert!(cache.bytes <= cache.limit);
    assert!(cache.mark_snapshot(&remote, vec![], GcLimits::default()).await?.is_empty());
    assert_eq!(cache.bytes, 0);
    assert!(cache.entries.is_empty());
    assert_eq!(cache.mark_snapshot(&remote, vec![reference], GcLimits::default()).await?, first);
    assert_eq!(store.reads.load(Ordering::SeqCst) - before, 6);
    Ok(())
}
#[tokio::test]
async fn reference_store_failures_and_budget_changes_never_reuse_unverified_paths() -> Result<()> {
    let store = Arc::new(Store::unpaced());
    let reference = root(store.clone(), 0, 8).await?;
    let remote: Arc<dyn ObjectStore> = store.clone();
    let mut cache = GcCache::default();
    cache.mark_snapshot(&remote, vec![reference.clone()], GcLimits::default()).await?;
    let tiny = GcLimits { metadata_bytes: 1, ..GcLimits::default() };
    let before = store.reads.load(Ordering::SeqCst);
    assert!(cache.mark_snapshot(&remote, vec![reference.clone()], tiny).await.is_err());
    assert!(cache.mark_snapshot(&remote, vec![reference.clone()], tiny).await.is_err());
    assert_eq!(store.reads.load(Ordering::SeqCst), before);
    assert!(cache.resident_bytes() <= cache.limit);
    cache.mark_snapshot(&remote, vec![reference.clone()], GcLimits::default()).await?;
    let mut forged = serde_json::to_value(&reference)?;
    forged["schema"] = "different".into();
    let forged: ObjectRef = serde_json::from_value(forged)?;
    assert!(cache.mark_snapshot(&remote, vec![forged], GcLimits::default()).await.is_err());
    assert!(cache.entries.is_empty());
    cache.mark_snapshot(&remote, vec![reference.clone()], GcLimits::default()).await?;
    let other = Arc::new(Store::unpaced());
    let other_remote: Arc<dyn ObjectStore> = other.clone();
    assert!(
        cache
            .mark_snapshot(&other_remote, vec![reference.clone()], GcLimits::default())
            .await
            .is_err()
    );
    assert!(cache.entries.is_empty());
    assert_eq!(root(other.clone(), 0, 8).await?, reference);
    cache.mark_snapshot(&other_remote, vec![reference.clone()], GcLimits::default()).await?;
    let small = GcLimits { objects: 1, ..GcLimits::default() };
    assert!(cache.mark_snapshot(&other_remote, vec![reference], small).await.is_err());
    Ok(())
}
#[tokio::test]
async fn repeated_wide_index_marking_has_bounded_network_work() -> Result<()> {
    let store = Arc::new(Store::unpaced());
    let reference = root(store.clone(), 0, 512).await?;
    let remote: Arc<dyn ObjectStore> = store.clone();
    let before = store.range_bytes.load(Ordering::SeqCst);
    for _ in 0..10 {
        GcCache::default()
            .mark_snapshot(&remote, vec![reference.clone()], GcLimits::default())
            .await?;
    }
    let uncached = store.range_bytes.load(Ordering::SeqCst) - before;
    let before = store.range_bytes.load(Ordering::SeqCst);
    let mut cache = GcCache::default();
    for _ in 0..10 {
        cache.mark_snapshot(&remote, vec![reference.clone()], GcLimits::default()).await?;
    }
    let cached = store.range_bytes.load(Ordering::SeqCst) - before;
    // The wide fence tree still authenticates completely, but its packed pages
    // are compressed; memoization then eliminates all repeat metadata reads.
    assert!(cached > 0 && cached < 512 * 1024);
    assert_eq!(uncached, cached * 10);
    println!(
        "{}",
        serde_json::json!({"roots":1,"collections":10,"uncached_bytes":uncached,"cached_bytes":cached,"cache_charge":cache.bytes})
    );
    Ok(())
}

#[tokio::test]
async fn admission_eviction_keeps_complete_marks_with_a_one_entry_budget() -> Result<()> {
    let store = Arc::new(Store::unpaced());
    let first = root(store.clone(), 1, 1).await?;
    let second = root(store.clone(), 2, 1).await?;
    let remote: Arc<dyn ObjectStore> = store.clone();
    let mut cache = GcCache::default();
    cache.mark_snapshot(&remote, vec![first.clone()], GcLimits::default()).await?;
    let budget = cache.bytes;
    let limits = GcLimits { metadata_bytes: u64::try_from(budget)?, ..GcLimits::default() };
    let expected =
        cache.mark_snapshot(&remote, vec![first.clone(), second.clone()], limits).await?;
    assert_eq!(expected.len(), 4);
    assert_eq!(cache.entries.len(), 1);
    assert!(cache.bytes <= budget);
    assert_eq!(cache.mark_snapshot(&remote, vec![first, second], limits).await?, expected);
    assert!(cache.bytes <= budget);
    Ok(())
}

#[tokio::test]
async fn snapshot_limits_are_checked_before_object_io() -> Result<()> {
    let store = Arc::new(Store::unpaced());
    let reference = root(store.clone(), 0, 1).await?;
    let remote: Arc<dyn ObjectStore> = store.clone();
    let mut cache = GcCache::default();
    let before = store.reads.load(Ordering::SeqCst);
    for limits in [
        GcLimits { roots: 0, ..GcLimits::default() },
        GcLimits { objects: 0, ..GcLimits::default() },
        GcLimits { metadata_bytes: 1, ..GcLimits::default() },
    ] {
        assert!(cache.mark_snapshot(&remote, vec![reference.clone()], limits).await.is_err());
    }
    assert!(
        cache
            .mark_snapshot(
                &remote,
                vec![reference.clone(), reference],
                GcLimits { roots: 1, ..GcLimits::default() }
            )
            .await
            .is_err()
    );
    assert_eq!(store.reads.load(Ordering::SeqCst), before);
    assert_eq!(cache.resident_bytes(), 0);
    Ok(())
}

#[tokio::test]
async fn extents_share_verified_physical_dependencies_without_losing_packs() -> Result<()> {
    let store = Arc::new(Store::unpaced());
    let reference = root(store.clone(), 0, 128).await?;
    let full =
        ObjectBatch::<i64, String>::open(store.clone(), reference.clone(), "gc-cache").await?;
    let left = full.slice(0..32).await?.reference().clone();
    let right = full.slice(96..128).await?.reference().clone();
    let remote: Arc<dyn ObjectStore> = store.clone();
    let mut cache = GcCache::default();
    let expected = cache.mark_snapshot(&remote, vec![reference], GcLimits::default()).await?;
    let before = store.reads.load(Ordering::SeqCst);
    for _ in 0..3 {
        assert_eq!(
            cache
                .mark_snapshot(&remote, vec![left.clone(), right.clone()], GcLimits::default())
                .await?,
            expected
        );
    }
    assert_eq!(store.reads.load(Ordering::SeqCst), before);
    Ok(())
}
