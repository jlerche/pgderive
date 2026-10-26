//! Bounded complete logical-key slices, scoped by owning lineage identities.
use super::{BatchData, Entry, SharedRows};
use anyhow::{Context as _, Result, ensure};
use sha2::{Digest as _, Sha256};
use std::{any::TypeId, sync::Arc};

// Owning pointer identity prevents scope reuse while cached entries remain.
#[derive(Clone, Debug)]
pub struct CacheScope(Arc<()>);
impl CacheScope {
    pub(crate) fn new() -> Self {
        Self(Arc::new(()))
    }
}
impl PartialEq for CacheScope {
    fn eq(&self, other: &Self) -> bool {
        Arc::ptr_eq(&self.0, &other.0)
    }
}
impl Eq for CacheScope {}
impl std::hash::Hash for CacheScope {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        std::hash::Hash::hash(&Arc::as_ptr(&self.0), state);
    }
}
type Identity = (CacheScope, [u8; 32], TypeId);
#[derive(Clone)]
pub struct KeyIdentity<K: BatchData> {
    key: K,
    digest: [u8; 32],
    charge: usize,
}
impl<K: BatchData> KeyIdentity<K> {
    pub(crate) fn new(key: &K) -> Result<Self> {
        let bytes = serde_json::to_vec(key)?;
        let digest = <[u8; 32]>::from(Sha256::digest(&bytes));
        let charge = bytes
            .len()
            .checked_mul(2)
            .and_then(|bytes| {
                bytes.checked_add(std::mem::size_of::<Self>() + 2 * std::mem::size_of::<usize>())
            })
            .context("key identity charge overflow")?;
        Ok(Self { key: key.clone(), digest, charge })
    }
    pub(crate) const fn charge(&self) -> usize {
        self.charge
    }
    pub(crate) const fn key(&self) -> &K {
        &self.key
    }
}
#[derive(Clone, Debug)]
pub struct PointRows<K: BatchData, V: BatchData> {
    pub(crate) rows: SharedRows<K, V>,
    pub(crate) charge: usize,
}
#[derive(Debug)]
struct Cached {
    rows: Arc<dyn std::any::Any + Send + Sync>,
    charge: usize,
}
#[derive(Debug)]
pub struct Points {
    cache: foyer::Cache<Identity, Cached>,
    bytes: usize,
    hits: std::sync::atomic::AtomicU64,
}
impl Points {
    pub(super) fn new(bytes: usize) -> Self {
        let cache = foyer::Cache::builder(bytes.max(1))
            .with_shards(1)
            .with_eviction_config(foyer::S3FifoConfig::default())
            .with_weighter(|_: &Identity, entry: &Cached| entry.charge)
            .build();
        Self { cache, bytes, hits: std::sync::atomic::AtomicU64::new(0) }
    }
    pub(super) fn stats(&self) -> (usize, usize, u64) {
        (
            self.cache.usage(),
            self.cache.entries(),
            self.hits.load(std::sync::atomic::Ordering::Relaxed),
        )
    }
    pub(crate) fn record_hit(&self) {
        self.hits.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
    }
    fn identity<K: BatchData, V: BatchData>(scope: &CacheScope, key: &KeyIdentity<K>) -> Identity {
        (scope.clone(), key.digest, TypeId::of::<Vec<Entry<K, V>>>())
    }
    pub(crate) fn get<K: BatchData, V: BatchData>(
        &self,
        scope: &CacheScope,
        key: &KeyIdentity<K>,
        allowance: usize,
    ) -> Result<Option<PointRows<K, V>>> {
        if self.bytes == 0 || allowance < 512 {
            return Ok(None);
        }
        self.cache
            .get(&Self::identity::<K, V>(scope, key))
            .filter(|entry| entry.value().charge <= allowance)
            .map(|entry| {
                let rows = entry
                    .value()
                    .rows
                    .clone()
                    .downcast::<Vec<Entry<K, V>>>()
                    .map_err(|_| anyhow::anyhow!("point cache type mismatch"))?;
                ensure!(
                    rows.first().is_some_and(|row| &row.0.0 == key.key()),
                    "point cache key mismatch"
                );
                Ok(PointRows { rows, charge: entry.value().charge })
            })
            .transpose()
    }
    pub(crate) fn insert<K: BatchData, V: BatchData>(
        &self,
        scope: &CacheScope,
        key: &KeyIdentity<K>,
        rows: SharedRows<K, V>,
    ) -> Result<bool> {
        let identity = Self::identity::<K, V>(scope, key);
        if rows.is_empty() {
            drop(self.cache.remove(&identity));
            return Ok(false);
        }
        if self.bytes == 0 {
            return Ok(false);
        }
        let Ok(payload) = crate::engine::execution::record_size(&rows.as_slice(), 64 * 1024) else {
            drop(self.cache.remove(&identity));
            return Ok(false);
        };
        let charge = rows
            .capacity()
            .checked_mul(std::mem::size_of::<Entry<K, V>>())
            .and_then(|charge| charge.checked_add(payload))
            .and_then(|charge| {
                charge.checked_add(
                    std::mem::size_of::<Identity>() + std::mem::size_of::<Cached>() + 128,
                )
            })
            .context("point cache charge overflow")?
            .max(512);
        if charge > 64 * 1024 || charge > self.bytes {
            drop(self.cache.remove(&identity));
            return Ok(false);
        }
        super::super::format::validate(&rows)?;
        ensure!(rows.iter().all(|row| &row.0.0 == key.key()), "incomplete point cache key slice");
        drop(self.cache.insert(identity, Cached { rows, charge }));
        Ok(true)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    fn insert<K: BatchData, V: BatchData>(
        points: &Points,
        scope: &CacheScope,
        key: &KeyIdentity<K>,
        rows: &[Entry<K, V>],
    ) -> Result<()> {
        points.insert(scope, key, Arc::new(rows.to_vec())).map(|_| ())
    }
    #[test]
    fn typed_rows_keep_scope_identity_bounds_allowance_and_pinned_ownership() -> Result<()> {
        let points = Points::new(4096);
        let scope = CacheScope::new();
        let other = CacheScope::new();
        assert_ne!(scope, other);
        let key = KeyIdentity::new(&1_i64)?;
        let rows = Arc::new(vec![((1, 7_i64), -3), ((1, 8), 4)]);
        points.insert(&scope, &key, rows.clone())?;
        let retained = points.get::<i64, i64>(&scope, &key, 4096)?.context("missing point")?;
        assert!(Arc::ptr_eq(&retained.rows, &rows));
        assert!(points.get::<i64, i64>(&scope, &key, retained.charge - 1)?.is_none());
        assert!(points.get::<i64, String>(&scope, &key, 4096)?.is_none());
        assert!(points.get::<i64, i64>(&other, &key, 4096)?.is_none());
        let mut forged = KeyIdentity::new(&2_i64)?;
        forged.digest = key.digest;
        assert!(points.get::<i64, i64>(&scope, &forged, 4096).is_err());
        insert::<i64, i64>(&points, &scope, &key, &[])?;
        assert!(points.get::<i64, i64>(&scope, &key, 4096)?.is_none());
        assert_eq!(retained.rows.as_slice(), rows.as_slice());
        assert!(insert(&points, &scope, &key, &[((2, 3_i64), 1)]).is_err());
        assert!(insert(&points, &scope, &key, &[((1, 3_i64), 0)]).is_err());
        for value in 0..100 {
            insert(&points, &scope, &KeyIdentity::new(&value)?, &[((value, value), 1)])?;
            assert!(points.stats().0 <= 4096);
        }
        let disabled = Points::new(0);
        insert(&disabled, &scope, &key, &[((1, 3_i64), 1)])?;
        assert_eq!(disabled.stats(), (0, 0, 0));
        Ok(())
    }
    #[test]
    fn oversized_replacements_evict_and_wrong_dynamic_types_are_rejected() -> Result<()> {
        let points = Points::new(4096);
        let scope = CacheScope::new();
        let key = KeyIdentity::new(&1_i64)?;
        insert(&points, &scope, &key, &[((1, "small".to_owned()), -2)])?;
        let retained = points.get::<i64, String>(&scope, &key, 4096)?.context("missing point")?;
        insert(&points, &scope, &key, &[((1, "x".repeat(70 * 1024)), 1)])?;
        assert!(points.get::<i64, String>(&scope, &key, usize::MAX)?.is_none());
        assert_eq!(retained.rows.as_slice(), &[((1, "small".to_owned()), -2)]);
        drop(points.cache.insert(
            Points::identity::<i64, String>(&scope, &key),
            Cached { rows: Arc::new(17_i64), charge: 512 },
        ));
        assert!(points.get::<i64, String>(&scope, &key, 511)?.is_none());
        assert!(points.get::<i64, String>(&scope, &key, 512).is_err());
        Ok(())
    }
    #[test]
    fn frequent_scopes_survive_churn_and_old_popularity_ages() -> Result<()> {
        let points = Points::new(16 * 1024);
        let key = KeyIdentity::new(&1_i64)?;
        let scopes = (0..132).map(|_| CacheScope::new()).collect::<Vec<_>>();
        for scope in &scopes[..4] {
            insert(&points, scope, &key, &[((1, 7_i64), -2)])?;
            for _ in 0..64 {
                assert!(points.get::<i64, i64>(scope, &key, 1024)?.is_some());
            }
        }
        for scope in &scopes[4..68] {
            insert(&points, scope, &key, &[((1, 8_i64), 3)])?;
        }
        for scope in &scopes[..4] {
            assert!(points.get::<i64, i64>(scope, &key, 1024)?.is_some());
        }
        for _ in 0..512 {
            for scope in &scopes[100..132] {
                if points.get::<i64, i64>(scope, &key, 1024)?.is_none() {
                    insert(&points, scope, &key, &[((1, 9_i64), 4)])?;
                }
            }
            let (bytes, entries, _) = points.stats();
            assert!(bytes <= 16 * 1024 && entries <= 32);
        }
        assert!(
            scopes[..4]
                .iter()
                .all(|scope| !points.cache.contains(&Points::identity::<i64, i64>(scope, &key)))
        );
        Ok(())
    }
}
