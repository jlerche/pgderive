//! Complete immutable key slices; no partial keys or mutable trace identities.
use super::{BatchData, Entry, Rows, SharedRows};
use anyhow::{Context as _, Result, ensure};
use sha2::{Digest as _, Sha256};
use std::{any::TypeId, sync::Arc};

// The authenticated root digest includes the exact schema and full fences.
type Identity = ([u8; 32], [u8; 32], TypeId);
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
#[derive(Clone, Debug)]
struct Cached {
    rows: Arc<Rows>,
    charge: usize,
}
#[derive(Debug)]
pub(in crate::engine::reader) struct Points {
    cache: foyer::Cache<Identity, Cached>,
    bytes: usize,
    hits: std::sync::atomic::AtomicU64,
}
impl Points {
    pub(super) fn new(bytes: usize) -> Self {
        let cache = foyer::Cache::builder(bytes.max(1))
            .with_shards(1)
            .with_eviction_config(foyer::LruConfig::default())
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
    fn identity<K: BatchData, V: BatchData>(root: &str, key: &KeyIdentity<K>) -> Identity {
        (
            <[u8; 32]>::from(Sha256::digest(root.as_bytes())),
            key.digest,
            TypeId::of::<Vec<Entry<K, V>>>(),
        )
    }
    pub(in crate::engine::reader) fn get<K: BatchData, V: BatchData>(
        &self,
        root: &str,
        key: &KeyIdentity<K>,
        allowance: usize,
    ) -> Result<Option<PointRows<K, V>>> {
        if self.bytes == 0 || allowance < 512 {
            return Ok(None);
        }
        self.cache
            .get(&Self::identity::<K, V>(root, key))
            .filter(|entry| entry.value().charge <= allowance)
            .map(|entry| {
                let rows: SharedRows<K, V> = entry
                    .value()
                    .rows
                    .clone()
                    .downcast()
                    .map_err(|_| anyhow::anyhow!("point cache type mismatch"))?;
                ensure!(
                    rows.first().is_some_and(|row| &row.0.0 == key.key()),
                    "point cache key mismatch"
                );
                self.hits.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                Ok(PointRows { rows, charge: entry.value().charge })
            })
            .transpose()
    }
    pub(in crate::engine::reader) fn insert<K: BatchData, V: BatchData>(
        &self,
        root: &str,
        key: &KeyIdentity<K>,
        rows: &[Entry<K, V>],
    ) -> Result<()> {
        if self.bytes == 0 || rows.is_empty() {
            return Ok(());
        }
        super::super::format::validate(rows)?;
        ensure!(rows.iter().all(|row| &row.0.0 == key.key()), "incomplete point cache key slice");
        let identity = Self::identity::<K, V>(root, key);
        let encoded_bytes = serde_json::to_vec(rows)?.len();
        let charge = rows
            .len()
            .checked_mul(std::mem::size_of::<Entry<K, V>>())
            .and_then(|bytes| bytes.checked_add(encoded_bytes))
            .and_then(|bytes| bytes.checked_add(std::mem::size_of::<Identity>() + 256))
            .context("point cache charge overflow")?
            .max(512);
        if charge <= self.bytes.min(64 * 1024) {
            drop(self.cache.insert(identity, Cached { rows: Arc::new(rows.to_vec()), charge }));
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn complete_signed_slices_are_scoped_bounded_and_owned_after_eviction() -> Result<()> {
        let points = Points::new(1024);
        let key = KeyIdentity::new(&1_i64)?;
        let rows = [((1, 7_i64), -3), ((1, 8), 4)];
        points.insert("root", &key, &rows)?;
        let retained = points.get::<i64, i64>("root", &key, 1024)?.context("missing point")?;
        let weak = Arc::downgrade(&retained.rows);
        assert_eq!(retained.rows.as_slice(), rows);
        assert!(points.get::<i64, i64>("root", &key, 0)?.is_none());
        assert!(points.get::<i64, i64>("other-root", &key, 1024)?.is_none());
        assert!(points.get::<i64, String>("root", &key, 1024)?.is_none());
        let mut forged = KeyIdentity::new(&2)?;
        forged.digest.clone_from(&key.digest);
        assert!(points.get::<i64, i64>("root", &forged, 1024).is_err());
        for value in 2..32 {
            points.insert("root", &KeyIdentity::new(&value)?, &[((value, value), 1)])?;
            let (bytes, entries, _) = points.stats();
            assert!(bytes <= 1024 && entries <= 2);
        }
        assert!(points.get::<i64, i64>("root", &key, 1024)?.is_none());
        assert_eq!(retained.rows.as_slice(), rows);
        drop(retained);
        assert!(weak.upgrade().is_none());
        Ok(())
    }
    #[test]
    fn invalid_partial_or_oversized_admissions_never_replace_valid_data() -> Result<()> {
        let points = Points::new(128 * 1024);
        let key = KeyIdentity::new(&1_i64)?;
        assert!(points.insert("root", &key, &[((2, 3_i64), 1)]).is_err());
        assert!(points.insert("root", &key, &[((1, 3_i64), 0)]).is_err());
        assert!(points.insert("root", &key, &[((1, 4_i64), 1), ((1, 3), 1)]).is_err());
        points.insert("root", &key, &[((1, "x".repeat(70 * 1024)), 1)])?;
        assert_eq!(points.stats(), (0, 0, 0));
        let disabled = Points::new(0);
        disabled.insert("root", &key, &[((1, 3_i64), 1)])?;
        assert!(disabled.get::<i64, i64>("root", &key, usize::MAX)?.is_none());
        Ok(())
    }
}
