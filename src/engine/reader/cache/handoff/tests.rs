use super::super::BlockCache;
use anyhow::Result;
use std::sync::Arc;
fn insert(cache: &BlockCache, schema: &str, hash: &str, bytes: usize) -> Result<()> {
    cache.insert(schema, hash.into(), Arc::new(vec![((1_i64, 2_i64), 1)]), bytes)
}
#[test]
fn handoff_refreshes_duplicates_without_evicting_unrelated_pages() -> Result<()> {
    let parent = Arc::new(BlockCache::new(8192, 2));
    insert(&parent, "s", "a", 1)?;
    insert(&parent, "s", "b", 1)?;
    let staged = BlockCache::output(parent.clone());
    assert!(Arc::ptr_eq(&staged, &BlockCache::output(staged.clone())));
    insert(&staged, "s", "a", 1)?;
    insert(&staged, "s", "c", 1)?;
    assert!(parent.peek::<i64, i64>("s", "c")?.is_none());
    assert_eq!(parent.stats()?.filter_entries, 2);
    assert_eq!(staged.stats()?.filter_entries, 2);
    staged.promote()?;
    assert!(parent.peek::<i64, i64>("s", "a")?.is_some());
    assert!(parent.peek::<i64, i64>("s", "c")?.is_none());
    assert_eq!(parent.stats()?.filter_entries, 3);
    assert!(parent.peek::<i64, i64>("s", "b")?.is_some());
    assert!(parent.promote().is_err());
    Ok(())
}
#[test]
fn sibling_overflow_keeps_a_bounded_prefix_without_evicting_foreground() -> Result<()> {
    let parent = Arc::new(BlockCache::new(8192, 3));
    insert(&parent, "hot", "hot", 1)?;
    let staged = BlockCache::output(parent.clone());
    for hash in ["one", "two", "three", "four"] {
        insert(&staged, "output", hash, 1)?;
    }
    assert!(!staged.retains_output()?);
    assert_eq!(staged.stats()?.entries, 3);
    staged.promote()?;
    assert!(parent.peek::<i64, i64>("hot", "hot")?.is_some());
    assert_eq!(parent.stats()?.entries, 3);
    assert!(parent.stats()?.bytes <= 8192);
    assert_eq!(staged.stats()?.entries, 0);
    for (bytes, entries) in [(0, 4), (8192, 0), (1, 4)] {
        let parent = Arc::new(BlockCache::new(bytes, entries));
        let staged = BlockCache::output(parent.clone());
        insert(&staged, "output", "large", 100)?;
        assert!(!staged.retains_output()?);
        staged.promote()?;
        assert_eq!(parent.stats()?.entries, 0);
    }
    Ok(())
}

#[test]
fn fitting_handoff_replaces_large_duplicate_capacity_before_admission() -> Result<()> {
    let parent = Arc::new(BlockCache::new(8192, 3));
    let mut spare = Vec::with_capacity(200);
    spare.push(((1_i64, 2_i64), 1));
    parent.insert("s", "a".into(), Arc::new(spare), 1)?;
    insert(&parent, "s", "b", 2000)?;
    let staged = BlockCache::output(parent.clone());
    insert(&staged, "s", "a", 1)?;
    insert(&staged, "s", "c", 5000)?;
    staged.promote()?;
    assert!(parent.peek::<i64, i64>("s", "a")?.is_some());
    assert!(parent.peek::<i64, i64>("s", "c")?.is_some());
    assert!(parent.stats()?.bytes <= 8192);
    Ok(())
}

#[test]
fn handoff_tracks_replaced_pins_and_keeps_recency_consistent() -> Result<()> {
    let parent = Arc::new(BlockCache::new(8192, 2));
    let old = Arc::new(vec![((1_i64, 2_i64), 1)]);
    parent.insert("s", "a".into(), old.clone(), 8)?;
    insert(&parent, "s", "b", 8)?;
    let staged = BlockCache::output(parent.clone());
    insert(&staged, "s", "a", 8)?;
    staged.promote()?;
    assert_eq!(parent.stats()?.pins, 1);
    assert!(parent.stats()?.live_bytes > parent.stats()?.bytes);
    insert(&parent, "s", "c", 8)?;
    assert!(parent.peek::<i64, i64>("s", "b")?.is_none());
    assert!(parent.peek::<i64, i64>("s", "a")?.is_some());
    drop(old);
    assert_eq!(parent.stats()?.pins, 0);
    assert_eq!(parent.stats()?.live_bytes, parent.stats()?.bytes);
    let state = parent.state.lock().map_err(|_| anyhow::anyhow!("cache poisoned"))?;
    assert_eq!(state.recency.len(), state.blocks.len());
    drop(state);
    Ok(())
}

#[test]
fn larger_duplicate_cannot_displace_an_unrelated_page() -> Result<()> {
    let parent = Arc::new(BlockCache::new(1024, 2));
    insert(&parent, "s", "a", 1)?;
    insert(&parent, "s", "b", 900)?;
    let before = parent.stats()?;
    let staged = BlockCache::output(parent.clone());
    insert(&staged, "s", "a", 900)?;
    staged.promote()?;
    assert_eq!(parent.stats()?.bytes, before.bytes);
    assert!(parent.peek::<i64, i64>("s", "b")?.is_some());
    assert!(parent.peek::<i64, i64>("s", "a")?.is_some());
    // Skipping the oversized replacement must not change the old LRU order.
    insert(&parent, "s", "c", 1)?;
    assert!(parent.peek::<i64, i64>("s", "a")?.is_none());
    assert!(parent.peek::<i64, i64>("s", "b")?.is_some());
    Ok(())
}

#[test]
fn abandoned_workspace_and_typed_pages_cannot_cross_hit() -> Result<()> {
    let parent = Arc::new(BlockCache::new(8192, 4));
    {
        let staged = BlockCache::output(parent.clone());
        insert(&staged, "s", "a", 1)?;
    }
    assert_eq!(parent.stats()?.entries, 0);
    let staged = BlockCache::output(parent.clone());
    insert(&staged, "s", "a", 1)?;
    staged.insert("s", "a".into(), Arc::new(vec![((1_u64, 3_u64), 1)]), 1)?;
    staged.promote()?;
    assert_eq!(parent.peek::<i64, i64>("s", "a")?.as_deref(), Some(&vec![((1, 2), 1)]));
    assert_eq!(parent.peek::<u64, u64>("s", "a")?.as_deref(), Some(&vec![((1, 3), 1)]));
    Ok(())
}

#[test]
fn byte_overflow_keeps_complete_prefix_and_stops_future_admission() -> Result<()> {
    let parent = Arc::new(BlockCache::new(128, 4));
    let staged = BlockCache::output(parent.clone());
    insert(&staged, "s", "a", 1)?;
    insert(&staged, "s", "b", 120)?;
    insert(&staged, "s", "c", 1)?;
    assert!(!staged.retains_output()?);
    assert_eq!(staged.stats()?.entries, 1);
    staged.promote()?;
    assert!(parent.peek::<i64, i64>("s", "a")?.is_some());
    assert!(parent.peek::<i64, i64>("s", "b")?.is_none());
    assert!(parent.peek::<i64, i64>("s", "c")?.is_none());
    Ok(())
}

#[test]
fn unused_generated_pages_are_recycled_but_foreground_hits_are_protected() -> Result<()> {
    let parent = Arc::new(BlockCache::new(8192, 2));
    let first = BlockCache::output(parent.clone());
    insert(&first, "s", "a", 1)?;
    insert(&first, "s", "b", 1)?;
    first.promote()?;
    assert!(parent.get::<i64, i64>("s", "a")?.is_some());
    let next = BlockCache::output(parent.clone());
    insert(&next, "s", "c", 1)?;
    insert(&next, "s", "d", 1)?;
    next.promote()?;
    assert!(parent.peek::<i64, i64>("s", "a")?.is_some());
    assert!(parent.peek::<i64, i64>("s", "b")?.is_none());
    assert_eq!(parent.stats()?.entries, 2);
    assert_ne!(
        parent.peek::<i64, i64>("s", "c")?.is_some(),
        parent.peek::<i64, i64>("s", "d")?.is_some()
    );
    Ok(())
}
