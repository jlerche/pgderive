use super::{BatchData, Entry};
use anyhow::{Context, Result};
use std::{
    any::{Any, TypeId},
    collections::BTreeMap,
    sync::{Arc, Mutex, Weak},
};

type SharedRows<K, V> = Arc<Vec<Entry<K, V>>>;

type Rows = dyn Any + Send + Sync;
// Row identity is canonical JSON after decompression.
const ROW_CODEC: &str = "canonical-json-rows-v1";
type Identity = (&'static str, String, String, TypeId);

/// Shared verified decoded blocks. Charges include vector storage and encoded
/// payload length as a heap estimate; they are not an allocator or RSS bound.
#[derive(Debug)]
pub struct BlockCache {
    bytes: usize,
    entries: usize,
    pub(super) ranges: Arc<range::Ranges>,
    parent: Option<Arc<Self>>,
    staged: bool,
    state: Mutex<State>,
    pub(super) encoded: Option<Arc<super::EncodedCache>>,
}
#[derive(Debug, Default)]
struct State {
    blocks: BTreeMap<Identity, Cached>,
    live: Vec<(Weak<Rows>, usize)>,
    stats: CacheStats,
    clock: u64,
    overflowed: bool,
}
#[derive(Debug)]
struct Cached {
    rows: Arc<Rows>,
    charge: usize,
    accessed: u64,
}
/// Cache counters. Charges exclude allocator overhead and cannot bound RSS.
#[derive(Debug, Clone, Default)]
pub struct CacheStats {
    /// Retained decoded block charges (vector capacity plus encoded payload).
    pub bytes: usize,
    /// Number of retained decoded blocks.
    pub entries: usize,
    /// Live decoded block charges, including evicted blocks pinned by cursors.
    pub live_bytes: usize,
    /// Cursor/reader strong references, excluding the cache's own references.
    pub pins: usize,
    /// Successful decoded block lookups.
    pub hits: u64,
    /// Decoded-cache misses, served by Foyer or object storage before decoding.
    pub misses: u64,
}
impl BlockCache {
    /// Set decoded-charge and entry caps; either zero disables retention.
    #[must_use]
    pub fn new(bytes: usize, entries: usize) -> Self {
        Self {
            bytes,
            entries,
            ranges: Arc::new(range::Ranges::new(bytes / 4, entries)),
            parent: None,
            staged: false,
            state: Mutex::new(State::default()),
            encoded: None,
        }
    }
    /// Reuse foreground blocks without admitting scan data or promoting its LRU.
    pub(crate) fn scan(parent: Arc<Self>) -> Arc<Self> {
        let parent = parent.parent.clone().unwrap_or(parent);
        Arc::new(Self {
            encoded: parent.encoded.clone(),
            ranges: parent.ranges.clone(),
            parent: Some(parent),
            ..Self::new(0, 0)
        })
    }
    pub(super) const fn admits_encoded(&self) -> bool {
        self.parent.is_none()
    }
    fn peek<K: BatchData, V: BatchData>(
        &self,
        schema: &str,
        hash: &str,
    ) -> Result<Option<SharedRows<K, V>>> {
        let state = self.state.lock().map_err(|_| anyhow::anyhow!("cache lock poisoned"))?;
        let identity =
            (ROW_CODEC, schema.to_owned(), hash.to_owned(), TypeId::of::<Vec<Entry<K, V>>>());
        let rows = state.blocks.get(&identity).map(|entry| entry.rows.clone());
        drop(state);
        rows.map(|rows| rows.downcast().map_err(|_| anyhow::anyhow!("cache type mismatch")))
            .transpose()
    }
    /// Attach a shared Foyer cache for encoded immutable blocks.
    #[must_use]
    pub fn with_encoded(mut self, cache: Option<Arc<super::EncodedCache>>) -> Self {
        self.encoded = cache;
        self
    }
    /// Read usage including active pins and evicted live decoded blocks.
    ///
    /// # Errors
    /// Returns an error if a prior thread poisoned the cache lock.
    pub fn stats(&self) -> Result<CacheStats> {
        let mut state = self.state.lock().map_err(|_| anyhow::anyhow!("cache lock poisoned"))?;
        state.live.retain(|(rows, _)| rows.strong_count() > 0);
        let mut counters = state.stats.clone();
        counters.live_bytes = state.live.iter().map(|(_, charge)| charge).sum();
        let references: usize = state.live.iter().map(|(rows, _)| rows.strong_count()).sum();
        counters.pins = references.saturating_sub(state.blocks.len());
        drop(state);
        Ok(counters)
    }
    pub(super) fn get<K: BatchData, V: BatchData>(
        &self,
        schema: &str,
        hash: &str,
    ) -> Result<Option<SharedRows<K, V>>> {
        if let Some(parent) = &self.parent {
            return parent.peek(schema, hash);
        }
        let mut state = self.state.lock().map_err(|_| anyhow::anyhow!("cache lock poisoned"))?;
        state.clock = state.clock.saturating_add(1);
        let clock = state.clock;
        let identity =
            (ROW_CODEC, schema.to_owned(), hash.to_owned(), TypeId::of::<Vec<Entry<K, V>>>());
        let rows = state.blocks.get_mut(&identity).map(|entry| {
            entry.accessed = clock;
            entry.rows.clone()
        });
        if rows.is_some() {
            state.stats.hits = state.stats.hits.saturating_add(1);
        } else {
            state.stats.misses = state.stats.misses.saturating_add(1);
        }
        drop(state);
        rows.map(|rows| rows.downcast().map_err(|_| anyhow::anyhow!("cache type mismatch")))
            .transpose()
    }
    pub(super) fn insert<K: BatchData, V: BatchData>(
        &self,
        schema: &str,
        hash: String,
        rows: Arc<Vec<Entry<K, V>>>,
        encoded_bytes: usize,
    ) -> Result<()> {
        let charge = rows
            .capacity()
            .checked_mul(std::mem::size_of::<Entry<K, V>>())
            .and_then(|bytes| bytes.checked_add(encoded_bytes))
            .context("decoded cache charge overflow")?;
        let mut state = self.state.lock().map_err(|_| anyhow::anyhow!("cache lock poisoned"))?;
        let identity = (ROW_CODEC, schema.to_owned(), hash, TypeId::of::<Vec<Entry<K, V>>>());
        if self.staged {
            if state.overflowed {
                return Ok(());
            }
            if state.blocks.contains_key(&identity) {
                return Ok(());
            }
            if charge > self.bytes
                || state.stats.bytes > self.bytes - charge
                || state.blocks.len() >= self.entries
                || self.bytes == 0
            {
                state.blocks.clear();
                state.live.clear();
                state.stats.bytes = 0;
                state.stats.entries = 0;
                state.overflowed = true;
                return Ok(());
            }
        }
        state.admit(identity, rows, charge, (self.bytes, self.entries))
    }
}
impl State {
    fn admit(
        &mut self,
        identity: Identity,
        rows: Arc<Rows>,
        charge: usize,
        limits: (usize, usize),
    ) -> Result<()> {
        self.live.retain(|(rows, _)| rows.strong_count() > 0);
        self.live.push((Arc::downgrade(&rows), charge));
        if charge > limits.0
            || limits.1 == 0
            || limits.0 == 0
            || self.blocks.contains_key(&identity)
        {
            return Ok(());
        }
        while self.stats.bytes > limits.0 - charge || self.blocks.len() >= limits.1 {
            let oldest = self
                .blocks
                .iter()
                .min_by_key(|(_, entry)| entry.accessed)
                .map(|(identity, _)| identity.clone())
                .context("cache eviction without entries")?;
            let evicted = self.blocks.remove(&oldest).context("cache eviction lost entry")?;
            self.stats.bytes -= evicted.charge;
        }
        self.clock = self.clock.saturating_add(1);
        self.stats.bytes += charge;
        self.blocks.insert(identity, Cached { rows, charge, accessed: self.clock });
        self.stats.entries = self.blocks.len();
        Ok(())
    }
}

mod handoff;
mod range;

#[cfg(test)]
mod tests {
    use super::BlockCache;
    use anyhow::Result;
    use std::sync::Arc;

    #[test]
    fn scan_preserves_foreground_lru_counters_and_tracks_its_own_pins() -> Result<()> {
        let cache = Arc::new(BlockCache::new(4096, 2));
        for hash in ["a", "b"] {
            cache.insert("s", hash.into(), Arc::new(vec![((1_i64, 2_i64), 1)]), 8)?;
        }
        let scan = BlockCache::scan(cache.clone());
        assert!(scan.get::<i64, i64>("s", "a")?.is_some());
        assert!(scan.get::<i64, i64>("s", "missing")?.is_none());
        let cold = Arc::new(vec![((3_i64, 4_i64), 1)]);
        scan.insert("s", "cold".into(), cold.clone(), 8)?;
        assert_eq!(scan.stats()?.entries, 0);
        assert_eq!(scan.stats()?.pins, 1);
        assert_eq!(cache.stats()?.hits, 0);
        assert_eq!(cache.stats()?.misses, 0);
        cache.insert("s", "c".into(), Arc::new(vec![((5_i64, 6_i64), 1)]), 8)?;
        assert!(cache.get::<i64, i64>("s", "a")?.is_none());
        assert!(cache.get::<i64, i64>("s", "b")?.is_some());
        assert!(cache.get::<i64, i64>("s", "cold")?.is_none());
        assert!(!scan.admits_encoded());
        assert!(cache.admits_encoded());
        drop(cold);
        assert_eq!(scan.stats()?.live_bytes, 0);
        Ok(())
    }

    #[test]
    fn decoded_sharing_tracks_evicted_and_disabled_pins() -> Result<()> {
        let cache = BlockCache::new(1024, 1);
        let rows = Arc::new(vec![((1_i64, "payload".to_owned()), 1)]);
        cache.insert("schema", "first".into(), rows.clone(), 20)?;
        let shared = cache
            .get::<i64, String>("schema", "first")?
            .ok_or_else(|| anyhow::anyhow!("missing block"))?;
        assert!(Arc::ptr_eq(&rows, &shared));
        assert!(cache.get::<i64, String>("other-schema", "first")?.is_none());
        assert!(cache.get::<i64, i64>("schema", "first")?.is_none());
        assert_eq!(cache.stats()?.pins, 2);
        cache.insert("schema", "second".into(), Arc::new(vec![((2_i64, String::new()), 1)]), 10)?;
        let stats = cache.stats()?;
        assert_eq!(stats.entries, 1);
        assert!(stats.live_bytes > stats.bytes);
        assert_eq!(stats.pins, 2);
        drop(shared);
        drop(rows);
        let stats = cache.stats()?;
        assert_eq!(stats.live_bytes, stats.bytes);
        assert_eq!(stats.pins, 0);
        let disabled = BlockCache::new(0, 0);
        let rows = Arc::new(vec![((1_i64, 2_i64), 1)]);
        disabled.insert("schema", "hash".into(), rows.clone(), 8)?;
        assert_eq!(disabled.stats()?.bytes, 0);
        assert_eq!(disabled.stats()?.pins, 1);
        drop(rows);
        assert_eq!(disabled.stats()?.live_bytes, 0);
        Ok(())
    }
}
