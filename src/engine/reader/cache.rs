use anyhow::{Context, Result};
use std::{
    collections::BTreeMap,
    sync::{Arc, Mutex},
};

/// Shared cache of checksum-verified immutable encoded block bytes.
/// Decoded cursor blocks are separately pinned by active readers.
#[derive(Debug)]
pub struct BlockCache {
    bytes: usize,
    entries: usize,
    state: Mutex<State>,
}
#[derive(Debug, Default)]
struct State {
    blocks: BTreeMap<String, Cached>,
    stats: CacheStats,
    clock: u64,
}
#[derive(Debug)]
struct Cached {
    bytes: Arc<[u8]>,
    accessed: u64,
}
/// Cache counters and current resident encoded bytes.
#[derive(Debug, Clone, Default)]
pub struct CacheStats {
    /// Current encoded byte count, excluding active decoded cursor blocks.
    pub bytes: usize,
    /// Number of cached blocks.
    pub entries: usize,
    /// Successful cache lookups.
    pub hits: u64,
    /// Lookups requiring an object-store read.
    pub misses: u64,
}
impl BlockCache {
    /// Set exact encoded-byte and entry caps; either zero disables retention.
    #[must_use]
    pub fn new(bytes: usize, entries: usize) -> Self {
        Self { bytes, entries, state: Mutex::new(State::default()) }
    }
    /// Read current cache usage and request counters.
    ///
    /// # Errors
    /// Returns an error if a prior thread poisoned the cache lock.
    pub fn stats(&self) -> Result<CacheStats> {
        Ok(self.state.lock().map_err(|_| anyhow::anyhow!("cache lock poisoned"))?.stats.clone())
    }
    pub(super) fn get(&self, hash: &str) -> Result<Option<Arc<[u8]>>> {
        let mut state = self.state.lock().map_err(|_| anyhow::anyhow!("cache lock poisoned"))?;
        state.clock = state.clock.saturating_add(1);
        let clock = state.clock;
        let bytes = state.blocks.get_mut(hash).map(|entry| {
            entry.accessed = clock;
            entry.bytes.clone()
        });
        if bytes.is_some() {
            state.stats.hits = state.stats.hits.saturating_add(1);
        } else {
            state.stats.misses = state.stats.misses.saturating_add(1);
        }
        drop(state);
        Ok(bytes)
    }
    pub(super) fn insert(&self, hash: String, bytes: Arc<[u8]>) -> Result<()> {
        if bytes.len() > self.bytes || self.entries == 0 || self.bytes == 0 {
            return Ok(());
        }
        let mut state = self.state.lock().map_err(|_| anyhow::anyhow!("cache lock poisoned"))?;
        if state.blocks.contains_key(&hash) {
            return Ok(());
        }
        while state.stats.bytes > self.bytes - bytes.len() || state.blocks.len() >= self.entries {
            let oldest = state
                .blocks
                .iter()
                .min_by_key(|(_, entry)| entry.accessed)
                .map(|(hash, _)| hash.clone())
                .context("cache eviction without entries")?;
            let evicted = state.blocks.remove(&oldest).context("cache eviction lost entry")?;
            state.stats.bytes -= evicted.bytes.len();
        }
        state.clock = state.clock.saturating_add(1);
        let clock = state.clock;
        state.stats.bytes += bytes.len();
        state.blocks.insert(hash, Cached { bytes, accessed: clock });
        state.stats.entries = state.blocks.len();
        drop(state);
        Ok(())
    }
}
