use super::super::*;
use crate::engine::capacity::{Pool, Reclaim};
use std::sync::Arc;

fn cache(pool: &Arc<Pool>, tenant: &str) -> Result<Arc<BlockCache>> {
    let admission = pool.tenant(tenant);
    let cache = Arc::new(BlockCache::new(8192, 8).with_capacity(Some(admission.clone()))?);
    let owner: Arc<dyn Reclaim> = cache.clone();
    admission.register(&owner)?;
    Ok(cache)
}

#[test]
fn identical_keys_from_different_tenants_never_share_values() -> Result<()> {
    let pool = Pool::new(4096, 4096)?;
    let alice = cache(&pool, "alice")?;
    let bob = cache(&pool, "bob")?;
    alice.insert("same", "same".into(), Arc::new(vec![((1_i64, 10_i64), 1)]), 1024)?;
    bob.insert("same", "same".into(), Arc::new(vec![((1_i64, 20_i64), 1)]), 1024)?;
    assert_eq!(alice.get::<i64, i64>("same", "same")?.as_deref(), Some(&vec![((1, 10), 1)]));
    assert_eq!(bob.get::<i64, i64>("same", "same")?.as_deref(), Some(&vec![((1, 20), 1)]));
    assert_eq!(pool.stats()?.tenants.len(), 2);
    Ok(())
}

#[test]
fn evicted_cache_rows_keep_credit_until_the_last_pin_drops() -> Result<()> {
    let pool = Pool::new(2048, 2048)?;
    let alice = cache(&pool, "alice")?;
    let bob = cache(&pool, "bob")?;
    let pin = Arc::new(vec![((1_i64, 10_i64), 1)]);
    alice.insert("same", "same".into(), pin.clone(), 1024)?;
    let value = Arc::new(vec![((1_i64, 20_i64), 1)]);
    bob.insert("same", "same".into(), value.clone(), 1024)?;
    assert!(bob.get::<i64, i64>("same", "same")?.is_none());
    assert!(pool.stats()?.tenants.contains_key("alice"));
    drop(pin);
    bob.insert("same", "same".into(), value, 1024)?;
    assert!(bob.get::<i64, i64>("same", "same")?.is_some());
    assert!(!pool.stats()?.tenants.contains_key("alice"));
    assert!(pool.stats()?.peak <= 2048);
    Ok(())
}

#[test]
fn filters_can_reclaim_their_own_previous_entries() -> Result<()> {
    let probe = Pool::new(8192, 8192)?;
    let first = cache(&probe, "alice")?;
    first.filters.remember(
        ("schema", "first".into(), "frame".into(), std::any::TypeId::of::<(i64, i64)>()),
        &[((1_i64, 2_i64), 1)],
    )?;
    let charge = probe.stats()?.used;
    assert!(charge > 0);
    let pool = Pool::new(charge, charge)?;
    let owner = cache(&pool, "alice")?;
    owner.filters.remember(
        ("schema", "first".into(), "frame".into(), std::any::TypeId::of::<(i64, i64)>()),
        &[((1_i64, 2_i64), 1)],
    )?;
    owner.filters.remember(
        ("schema", "other".into(), "frame".into(), std::any::TypeId::of::<(i64, i64)>()),
        &[((3_i64, 4_i64), 1)],
    )?;
    assert_eq!(owner.filters.stats()?.1, 1);
    assert_eq!(pool.stats()?.used, charge);
    Ok(())
}
