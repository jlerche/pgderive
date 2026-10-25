use super::super::BlockCache;
use anyhow::Result;
use std::sync::Arc;
fn insert(cache: &BlockCache, schema: &str, hash: &str, bytes: usize) -> Result<()> {
    cache.insert(schema, hash.into(), Arc::new(vec![((1_i64, 2_i64), 1)]), bytes)
}
#[test]
fn complete_handoff_refreshes_existing_output_before_evicting_unrelated_pages() -> Result<()> {
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
    assert!(parent.peek::<i64, i64>("s", "c")?.is_some());
    assert_eq!(parent.stats()?.filter_entries, 3);
    assert!(parent.peek::<i64, i64>("s", "b")?.is_none());
    assert!(parent.promote().is_err());
    Ok(())
}
#[test]
fn sibling_schemas_share_one_budget_and_oversized_outputs_never_evict_foreground() -> Result<()> {
    for (bytes, entries, oversized) in [(8192, 2, 1), (128, 4, 120), (0, 4, 1), (8192, 0, 1)] {
        let parent = Arc::new(BlockCache::new(bytes, entries));
        insert(&parent, "hot", "hot", 1)?;
        let before = parent.stats()?;
        let staged = BlockCache::output(parent.clone());
        insert(&staged, "first", "one", 1)?;
        insert(&staged, "second", "two", 1)?;
        insert(&staged, "third", "three", oversized)?;
        insert(&staged, "later", "four", 1)?;
        assert!(!staged.retains_output()?);
        assert_eq!(staged.stats()?.entries, 0);
        assert_eq!(staged.stats()?.filter_entries, 0);
        staged.promote()?;
        let after = parent.stats()?;
        assert_eq!(
            (before.bytes, before.entries, before.hits, before.misses, before.filter_entries),
            (after.bytes, after.entries, after.hits, after.misses, after.filter_entries)
        );
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
