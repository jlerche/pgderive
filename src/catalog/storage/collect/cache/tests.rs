use super::super::{GcLimits, mark};
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
    let first = mark(&remote, vec![reference.clone()], GcLimits::default(), &mut cache).await?;
    assert_eq!(first.len(), 2);
    assert_eq!(store.reads.load(Ordering::SeqCst) - before, 2);
    for _ in 0..10 {
        assert_eq!(
            mark(
                &remote,
                vec![reference.clone(), reference.clone()],
                GcLimits::default(),
                &mut cache
            )
            .await?,
            first
        );
    }
    assert_eq!(store.reads.load(Ordering::SeqCst) - before, 2);
    assert!(cache.bytes <= cache.limit);
    assert!(mark(&remote, vec![], GcLimits::default(), &mut cache).await?.is_empty());
    assert_eq!(cache.bytes, 0);
    assert!(cache.entries.is_empty());
    assert_eq!(mark(&remote, vec![reference], GcLimits::default(), &mut cache).await?, first);
    assert_eq!(store.reads.load(Ordering::SeqCst) - before, 4);
    Ok(())
}
#[tokio::test]
async fn reference_store_failures_and_budget_changes_never_reuse_unverified_paths() -> Result<()> {
    let store = Arc::new(Store::unpaced());
    let reference = root(store.clone(), 0, 8).await?;
    let remote: Arc<dyn ObjectStore> = store.clone();
    let mut cache = GcCache::default();
    mark(&remote, vec![reference.clone()], GcLimits::default(), &mut cache).await?;
    let tiny = GcLimits { metadata_bytes: 1, ..GcLimits::default() };
    let before = store.reads.load(Ordering::SeqCst);
    mark(&remote, vec![reference.clone()], tiny, &mut cache).await?;
    mark(&remote, vec![reference.clone()], tiny, &mut cache).await?;
    assert_eq!(store.reads.load(Ordering::SeqCst) - before, 4);
    assert_eq!(cache.bytes, 0);
    mark(&remote, vec![reference.clone()], GcLimits::default(), &mut cache).await?;
    let mut forged = serde_json::to_value(&reference)?;
    forged["schema"] = "different".into();
    let forged: ObjectRef = serde_json::from_value(forged)?;
    assert!(mark(&remote, vec![forged], GcLimits::default(), &mut cache).await.is_err());
    assert!(cache.entries.is_empty());
    mark(&remote, vec![reference.clone()], GcLimits::default(), &mut cache).await?;
    let other = Arc::new(Store::unpaced());
    let other_remote: Arc<dyn ObjectStore> = other.clone();
    assert!(
        mark(&other_remote, vec![reference.clone()], GcLimits::default(), &mut cache)
            .await
            .is_err()
    );
    assert!(cache.entries.is_empty());
    assert_eq!(root(other.clone(), 0, 8).await?, reference);
    mark(&other_remote, vec![reference.clone()], GcLimits::default(), &mut cache).await?;
    let small = GcLimits { objects: 1, ..GcLimits::default() };
    assert!(mark(&other_remote, vec![reference], small, &mut cache).await.is_err());
    Ok(())
}
#[tokio::test]
async fn repeated_wide_index_marking_has_bounded_network_work() -> Result<()> {
    let store = Arc::new(Store::unpaced());
    let reference = root(store.clone(), 0, 512).await?;
    let remote: Arc<dyn ObjectStore> = store.clone();
    let before = store.range_bytes.load(Ordering::SeqCst);
    for _ in 0..10 {
        mark(&remote, vec![reference.clone()], GcLimits::default(), &mut GcCache::default())
            .await?;
    }
    let uncached = store.range_bytes.load(Ordering::SeqCst) - before;
    let before = store.range_bytes.load(Ordering::SeqCst);
    let mut cache = GcCache::default();
    for _ in 0..10 {
        mark(&remote, vec![reference.clone()], GcLimits::default(), &mut cache).await?;
    }
    let cached = store.range_bytes.load(Ordering::SeqCst) - before;
    assert!(cached > 1024 * 1024);
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
    mark(&remote, vec![first.clone()], GcLimits::default(), &mut cache).await?;
    let budget = cache.bytes;
    let limits = GcLimits { metadata_bytes: u64::try_from(budget)?, ..GcLimits::default() };
    let expected = mark(&remote, vec![first.clone(), second.clone()], limits, &mut cache).await?;
    assert_eq!(expected.len(), 4);
    assert_eq!(cache.entries.len(), 1);
    assert!(cache.bytes <= budget);
    assert_eq!(mark(&remote, vec![first, second], limits, &mut cache).await?, expected);
    assert!(cache.bytes <= budget);
    Ok(())
}
