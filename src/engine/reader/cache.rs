use super::{BatchData, Entry};
use anyhow::{Context, Result};
use std::{
    any::{Any, TypeId},
    collections::{BTreeMap, BTreeSet},
    sync::{Arc, Mutex, Weak},
};

pub use range::{ReadAhead, ReadGuard};

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
    filters: Arc<filter::Filters>,
    pub(super) points: Arc<points::Points>,
    parent: Option<Arc<Self>>,
    staged: bool,
    state: Arc<Mutex<State>>,
    pub(super) encoded: Option<Arc<super::EncodedCache>>,
    pub(super) index: Arc<super::index::Cache>,
}
#[derive(Debug, Default)]
struct State {
    blocks: BTreeMap<Identity, Cached>,
    recency: BTreeSet<(u64, Identity)>,
    // Only decoded allocations no longer owned by `blocks` need weak tracking.
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
    /// Resident complete logical-key slices, separate from full decoded pages.
    pub point_bytes: usize,
    /// Retained typed key slices; each has at least a 512-byte charge.
    pub point_entries: usize,
    /// Complete-key cache hits, avoiding data and index page reads.
    pub point_hits: u64,
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
    /// Successful cold page payload bytes deserialized, excluding cache hits.
    pub decoded_bytes: u64,
    /// Successful cold page rows deserialized, including unrelated rows.
    pub decoded_rows: u64,
    /// Resident negative-key synopsis charges, separate from decoded pages.
    pub filter_bytes: usize,
    /// Resident page-key synopses.
    pub filter_entries: usize,
    /// Key probes excluded without reading or decoding a page.
    pub filter_rejections: u64,
    /// CLOCK candidates examined while making room for new key synopses.
    pub filter_eviction_steps: u64,
    /// Foreground compressed-page requests and returned range work.
    pub foreground_io: ReadStats,
    /// Private scan requests and returned range work.
    pub scan_io: ReadStats,
    /// Successful compactions by floor(log2(input rows)), including signed rows.
    pub merges: Vec<u64>,
    /// Combined foreground decoded-node and weighted encoded-frame charge.
    /// Private scan workspace is separate.
    pub index_bytes: usize,
    /// Foreground retained authenticated decoded index nodes.
    pub index_entries: usize,
    /// Decoded index-node charges included in `index_bytes`.
    pub index_decoded_bytes: usize,
    /// Weighted foreground encoded-frame charge included in `index_bytes`.
    pub index_encoded_bytes: usize,
    /// Encoded authenticated index-frame count.
    pub index_encoded_entries: usize,
    /// Attempted index-page GETs, including private scan reads.
    pub index_reads: u64,
    /// Encoded bytes returned by index-page GETs, including private scans.
    pub index_read_bytes: u64,
    /// Requested compressed index-frame bytes, including cache hits and scans.
    pub index_requested_bytes: u64,
}
impl BlockCache {
    #[cfg(test)]
    pub(crate) fn enable_page_probes(&self) {
        self.ranges.enable_probes();
    }
    #[cfg(test)]
    pub(crate) fn range_diagnostics(&self) -> Result<serde_json::Value> {
        self.ranges.take_diagnostics()
    }
    #[cfg(test)]
    pub(crate) fn fix_range_width(&self, width: usize) -> Result<()> {
        self.ranges.fix_width(width)
    }

    /// Set decoded-charge and entry caps; either zero disables decoded retention.
    /// Complete-key reuse reserves an additional bytes/4, like index navigation.
    /// Index navigation retains a shared bounded 8 MiB navigation budget when
    /// decoded retention is disabled; enabled caches reserve bytes/4 for indexes.
    #[must_use]
    pub fn new(bytes: usize, entries: usize) -> Self {
        Self {
            bytes,
            entries,
            index: Arc::new(super::index::Cache::new(
                if bytes == 0 || entries == 0 { super::bounded::MAX_BYTES } else { bytes / 4 },
                if bytes == 0 || entries == 0 { 17 } else { entries },
            )),
            ranges: Arc::new(range::Ranges::new(
                bytes / 4,
                (bytes / 4 / range::MIN_WINDOW_BYTES).max(entries),
            )),
            filters: Arc::new(filter::Filters::new(bytes, entries)),
            points: Arc::new(points::Points::new(if entries == 0 { 0 } else { bytes / 4 })),
            parent: None,
            staged: false,
            state: Arc::new(Mutex::new(State::default())),
            encoded: None,
        }
    }
    #[cfg(test)]
    pub(crate) fn disable_index_retention(&mut self) {
        self.index = Arc::new(super::index::Cache::new(0, 0));
    }
    /// Reuse foreground decoded blocks without admission or LRU promotion.
    /// Each cold scan owns bounded private data and index compressed windows;
    /// index navigation additionally retains at most 256 KiB/17 decoded nodes.
    pub(crate) fn scan(parent: Arc<Self>) -> Arc<Self> {
        let parent = parent.parent.clone().unwrap_or(parent);
        Arc::new(Self {
            index: Arc::new(super::index::Cache::scan(parent.index.clone())),
            encoded: parent.encoded.clone(),
            points: parent.points.clone(),
            ranges: Arc::new(range::Ranges::scan(parent.ranges.metrics.clone())),
            filters: parent.filters.clone(),
            parent: Some(parent),
            ..Self::new(0, 0)
        })
    }
    pub(super) fn cursor(parent: Arc<Self>) -> Arc<Self> {
        if parent.parent.is_some() {
            return Self::scan(parent);
        }
        if parent.bytes > 0 && parent.entries > 0 {
            return parent;
        }
        // A disabled decoded cache still needs bounded cold-scan read-ahead.
        // Preserve its encoded admission policy; ordinary scans can warm Foyer.
        Arc::new(Self {
            index: parent.index.clone(),
            encoded: parent.encoded.clone(),
            points: parent.points.clone(),
            ranges: Arc::new(range::Ranges::scan(parent.ranges.metrics.clone())),
            state: parent.state.clone(),
            ..Self::new(0, 0)
        })
    }
    pub(crate) const fn admits_points(&self) -> bool {
        self.bytes > 0 && self.entries > 0 && (self.parent.is_none() || self.staged)
    }
    pub(crate) fn logical_points(&self) -> Arc<points::Points> {
        self.points.clone()
    }
    pub(super) const fn admits_encoded(&self) -> bool {
        self.parent.is_none()
    }
    pub(super) fn peek<K: BatchData, V: BatchData>(
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
    fn filter_identity<K: BatchData, V: BatchData>(schema: &str, hash: &str) -> Identity {
        (ROW_CODEC, schema.into(), hash.into(), TypeId::of::<Vec<Entry<K, V>>>())
    }
    pub(super) fn may_contain_key<K: BatchData, V: BatchData>(
        &self,
        schema: &str,
        block: &super::format::Block<(K, V)>,
        key: &K,
    ) -> Result<bool> {
        if !self.filters.enabled() {
            return Ok(true);
        }
        self.filters.contains(&Self::filter_identity::<K, V>(schema, &block.hash), key, block)
    }
    pub(crate) fn record_merge(&self, rows: usize) -> Result<()> {
        self.ranges.metrics.merged(rows)
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
        counters.live_bytes =
            counters.bytes + state.live.iter().map(|(_, charge)| charge).sum::<usize>();
        counters.pins = state
            .blocks
            .values()
            .map(|entry| Arc::strong_count(&entry.rows).saturating_sub(1))
            .sum::<usize>()
            + state.live.iter().map(|(rows, _)| rows.strong_count()).sum::<usize>();
        drop(state);
        (
            counters.filter_bytes,
            counters.filter_entries,
            counters.filter_rejections,
            counters.filter_eviction_steps,
        ) = self.filters.stats()?;
        (counters.foreground_io, counters.scan_io, counters.merges) =
            self.ranges.metrics.snapshot();
        (counters.point_bytes, counters.point_entries, counters.point_hits) = self.points.stats();
        let index = self.index.stats()?;
        counters.index_bytes = index
            .bytes
            .checked_add(index.encoded_bytes)
            .context("index cache byte sum overflow")?;
        counters.index_entries = index.entries;
        counters.index_decoded_bytes = index.bytes;
        counters.index_encoded_bytes = index.encoded_bytes;
        counters.index_encoded_entries = index.encoded_entries;
        counters.index_reads = index.reads;
        counters.index_read_bytes = index.read_bytes;
        counters.index_requested_bytes = index.requested_bytes;
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
        let mut previous = None;
        let rows = state.blocks.get_mut(&identity).map(|entry| {
            previous = Some(entry.accessed);
            entry.accessed = clock;
            entry.rows.clone()
        });
        if let Some(accessed) = previous {
            let prior = (accessed, identity);
            state.recency.remove(&prior);
            state.recency.insert((clock, prior.1));
        }
        if rows.is_some() {
            state.stats.hits = state.stats.hits.saturating_add(1);
        } else {
            state.stats.misses = state.stats.misses.saturating_add(1);
        }
        drop(state);
        rows.map(|rows| rows.downcast().map_err(|_| anyhow::anyhow!("cache type mismatch")))
            .transpose()
    }
    pub(super) fn record_decode(&self, bytes: usize, rows: usize) -> Result<()> {
        let mut state = self.state.lock().map_err(|_| anyhow::anyhow!("cache lock poisoned"))?;
        state.stats.decoded_bytes = state.stats.decoded_bytes.saturating_add(u64::try_from(bytes)?);
        state.stats.decoded_rows = state.stats.decoded_rows.saturating_add(u64::try_from(rows)?);
        drop(state);
        Ok(())
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
                state.recency.clear();
                state.live.clear();
                state.stats.bytes = 0;
                state.stats.entries = 0;
                state.overflowed = true;
                self.filters.clear()?;
                return Ok(());
            }
        }
        if self.bytes > 0
            && self.entries > 0
            && (self.parent.is_none() || self.staged)
            && !rows.is_empty()
        {
            self.filters.remember(Self::filter_identity::<K, V>(schema, &identity.2), &rows)?;
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
        if self.blocks.get(&identity).is_some_and(|entry| Arc::ptr_eq(&entry.rows, &rows)) {
            return Ok(());
        }
        if charge > limits.0
            || limits.1 == 0
            || limits.0 == 0
            || self.blocks.contains_key(&identity)
        {
            self.track(&rows, charge);
            return Ok(());
        }
        while self.stats.bytes > limits.0 - charge || self.blocks.len() >= limits.1 {
            let oldest = self
                .recency
                .pop_first()
                .map(|(_, identity)| identity)
                .context("cache eviction without entries")?;
            self.remove(oldest)?;
        }
        self.clock = self.clock.saturating_add(1);
        self.live.retain(|(live, _)| {
            live.strong_count() > 0 && !std::ptr::addr_eq(live.as_ptr(), Arc::as_ptr(&rows))
        });
        self.stats.bytes += charge;
        self.recency.insert((self.clock, identity.clone()));
        self.blocks.insert(identity, Cached { rows, charge, accessed: self.clock });
        self.stats.entries = self.blocks.len();
        Ok(())
    }
    fn track(&mut self, rows: &Arc<Rows>, charge: usize) {
        if Arc::strong_count(rows) > 1 {
            self.live.retain(|(rows, _)| rows.strong_count() > 0);
            if !self
                .live
                .iter()
                .any(|(live, _)| std::ptr::addr_eq(live.as_ptr(), Arc::as_ptr(rows)))
            {
                self.live.push((Arc::downgrade(rows), charge));
            }
        }
    }
    fn remove(&mut self, identity: Identity) -> Result<()> {
        let entry = self.blocks.remove(&identity).context("cache eviction lost entry")?;
        self.recency.remove(&(entry.accessed, identity));
        self.stats.bytes -= entry.charge;
        self.stats.entries = self.blocks.len();
        self.track(&entry.rows, entry.charge);
        Ok(())
    }
}

mod filter;
pub(in crate::engine::reader) mod metrics;
pub(super) mod points;
pub use metrics::ReadStats;
mod handoff;
pub(super) mod range;

#[cfg(test)]
mod tests {
    use super::BlockCache;
    use anyhow::Result;
    use std::sync::Arc;

    #[test]
    fn ordered_eviction_matches_lru_including_saturated_clock_ties() -> Result<()> {
        let cache = BlockCache::new(4096, 2);
        for hash in ["a", "b"] {
            cache.insert("s", hash.into(), Arc::new(vec![((1_i64, 2_i64), 1)]), 8)?;
        }
        assert!(cache.get::<i64, i64>("s", "a")?.is_some());
        cache.insert("s", "c".into(), Arc::new(vec![((1_i64, 2_i64), 1)]), 8)?;
        assert!(cache.peek::<i64, i64>("s", "b")?.is_none());
        {
            let mut state = cache.state.lock().map_err(|_| anyhow::anyhow!("cache poisoned"))?;
            state.clock = u64::MAX;
        }
        assert!(cache.get::<i64, i64>("s", "c")?.is_some());
        assert!(cache.get::<i64, i64>("s", "a")?.is_some());
        cache.insert("s", "d".into(), Arc::new(vec![((1_i64, 2_i64), 1)]), 8)?;
        // Saturating timestamps preserve the old identity-order tie break.
        assert!(cache.peek::<i64, i64>("s", "a")?.is_none());
        assert!(cache.peek::<i64, i64>("s", "c")?.is_some());
        let state = cache.state.lock().map_err(|_| anyhow::anyhow!("cache poisoned"))?;
        assert_eq!(state.recency.len(), state.blocks.len());
        assert!(state.live.is_empty());
        drop(state);
        Ok(())
    }

    #[test]
    fn duplicate_allocations_and_readmitted_pins_are_charged_once() -> Result<()> {
        let cache = BlockCache::new(4096, 1);
        let first = Arc::new(vec![((1_i64, 2_i64), 1)]);
        let duplicate = Arc::new(vec![((1_i64, 2_i64), 1)]);
        cache.insert("s", "a".into(), first.clone(), 8)?;
        let charge = cache.stats()?.bytes;
        cache.insert("s", "a".into(), first.clone(), 8)?;
        assert_eq!(cache.stats()?.pins, 1);
        cache.insert("s", "a".into(), duplicate.clone(), 8)?;
        cache.insert("s", "a".into(), duplicate.clone(), 8)?;
        assert_eq!(cache.stats()?.live_bytes, 2 * charge);
        assert_eq!(cache.stats()?.pins, 2);
        cache.insert("s", "b".into(), Arc::new(vec![((3_i64, 4_i64), 1)]), 8)?;
        assert_eq!(cache.stats()?.live_bytes, 3 * charge);
        cache.insert("s", "a".into(), first.clone(), 8)?;
        assert_eq!(cache.stats()?.live_bytes, 2 * charge);
        assert_eq!(cache.stats()?.pins, 2);
        drop(duplicate);
        assert_eq!(cache.stats()?.live_bytes, charge);
        assert_eq!(cache.stats()?.pins, 1);
        drop(first);
        assert_eq!(cache.stats()?.pins, 0);
        Ok(())
    }

    #[test]
    fn scan_preserves_foreground_lru_counters_and_tracks_its_own_pins() -> Result<()> {
        let cache = Arc::new(BlockCache::new(4096, 2));
        for hash in ["a", "b"] {
            cache.insert("s", hash.into(), Arc::new(vec![((1_i64, 2_i64), 1)]), 8)?;
        }
        let before_filters = cache.stats()?.filter_entries;
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
        assert_eq!(cache.stats()?.filter_entries, before_filters + 1);
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
