//! Bounded compressed windows amortize reads without pinning decoded pages.
use crate::engine::reader::format::Pack;
use anyhow::{Result, ensure};
use object_store::{ObjectStore, ObjectStoreExt as _, path::Path};
use std::{
    collections::BTreeMap,
    ops::Range,
    sync::{Arc, Mutex, Weak},
};

mod pins;
#[cfg(test)]
mod probes;
pub use pins::ReadGuard;
mod prefetch;
pub use prefetch::ReadAhead;

const WINDOW_BYTES: usize = 256 * 1024;
const STRIPES: usize = 256;
pub(super) const MIN_WINDOW_BYTES: usize = 4 * 1024;
const WIDTH_LEVELS: usize = 7;
// Keep cells for independent run probes; adjacent cells still share one GET.
const TARGET_WINDOWS: usize = 16;
type Identity = (String, String, u64, u64, u64);
type CachedRange = (u64, Arc<Vec<u8>>);
#[derive(Clone)]
struct Window {
    bytes: Arc<Vec<u8>>,
}
struct State {
    windows: BTreeMap<Identity, Window>,
    bytes: usize,
    policy: foyer::Cache<Identity, usize>,
    width: usize,
    #[cfg(test)]
    probes: probes::Probes,
    pins: Vec<Weak<pins::Pinned>>,
    #[cfg(test)]
    fetched: BTreeMap<(String, u64, u64), usize>,
    #[cfg(test)]
    flight_wait_us: u128,
    #[cfg(test)]
    flight_groups: usize,
    #[cfg(test)]
    flight_stripes: usize,
}
pub(in crate::engine::reader) struct Ranges {
    bytes: usize,
    entries: usize,
    #[cfg(test)]
    coalesce: bool,
    #[cfg(test)]
    probe_enabled: std::sync::atomic::AtomicBool,
    pub(in crate::engine::reader) metrics: Arc<super::metrics::Metrics>,
    scan: bool,
    state: Mutex<State>,
    flights: [tokio::sync::Mutex<()>; STRIPES],
}
impl std::fmt::Debug for Ranges {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Ranges")
            .field("bytes", &self.bytes)
            .field("entries", &self.entries)
            .finish_non_exhaustive()
    }
}
impl Ranges {
    pub(in crate::engine::reader) fn new(bytes: usize, entries: usize) -> Self {
        Self {
            bytes,
            entries,
            #[cfg(test)]
            coalesce: true,
            #[cfg(test)]
            probe_enabled: std::sync::atomic::AtomicBool::new(false),
            metrics: Arc::default(),
            scan: false,
            state: Mutex::new(State::new(bytes, entries)),
            flights: std::array::from_fn(|_| tokio::sync::Mutex::new(())),
        }
    }
    pub(in crate::engine::reader) fn snapshot(&self) -> Result<(usize, usize, super::ReadStats)> {
        let state = self.state.lock().map_err(|_| anyhow::anyhow!("range cache lock poisoned"))?;
        let retained = (state.policy.usage(), state.windows.len());
        drop(state);
        Ok((retained.0, retained.1, self.metrics.snapshot().0))
    }
    pub(in crate::engine::reader) fn scan(metrics: Arc<super::metrics::Metrics>) -> Self {
        Self { metrics, scan: true, ..Self::new(WINDOW_BYTES + 4096, 1) }
    }
    #[cfg(test)]
    pub(in crate::engine::reader) const fn disable_coalescing(&mut self) {
        self.coalesce = false;
    }
    #[cfg_attr(
        not(test),
        expect(
            clippy::unused_self,
            reason = "the instance flag exists only for the paired test control"
        )
    )]
    const fn coalesces(&self) -> bool {
        #[cfg(test)]
        {
            self.coalesce
        }
        #[cfg(not(test))]
        {
            true
        }
    }
    pub(in crate::engine::reader) async fn read(
        &self,
        store: &Arc<dyn ObjectStore>,
        pack: &Pack,
        range: Range<u64>,
    ) -> Result<Vec<u8>> {
        ensure!(range.start < range.end, "empty cached pack range");
        ensure!(range.end <= pack.bytes, "cached range exceeds pack");
        self.metrics.requested(self.scan, range.end - range.start);
        if self.bytes == 0
            || self.entries == 0
            || range.end - range.start > u64::try_from(self.bytes)?
        {
            return self.load(store, &pack.path, range).await;
        }
        let mut result = Vec::with_capacity(usize::try_from(range.end - range.start)?);
        let mut position = range.start;
        while position < range.end {
            let requested = position..range.end;
            let (start, bytes) = if let Some(cached) = self.cached(pack, &requested)? {
                cached
            } else if let Some(pinned) = self.pinned(pack, &requested)? {
                pinned
            } else {
                let maximum = u64::try_from(
                    self.state
                        .lock()
                        .map_err(|_| anyhow::anyhow!("range cache lock poisoned"))?
                        .width,
                )?;
                let Some(width) = self.fitting_width(pack, position, maximum)? else {
                    result.extend(self.load(store, &pack.path, position..range.end).await?);
                    return Ok(result);
                };
                let start = position / width * width;
                let end = start.saturating_add(width).min(pack.bytes);
                let identity = (pack.path.clone(), pack.hash.clone(), start, pack.bytes, width);
                (start, self.fetch(store, identity, end).await?)
            };
            let stop = range.end.min(start + u64::try_from(bytes.len())?);
            result.extend_from_slice(
                &bytes[usize::try_from(position - start)?..usize::try_from(stop - start)?],
            );
            position = stop;
        }
        Ok(result)
    }
    pub(in crate::engine::reader) fn invalidate(
        &self,
        pack: &Pack,
        range: &Range<u64>,
    ) -> Result<()> {
        let mut state =
            self.state.lock().map_err(|_| anyhow::anyhow!("range cache lock poisoned"))?;
        let keys = state
            .windows
            .keys()
            .filter(|key| {
                key.0 == pack.path
                    && key.1 == pack.hash
                    && key.3 == pack.bytes
                    && key.2 < range.end
                    && key.2.saturating_add(key.4) > range.start
            })
            .cloned()
            .collect::<Vec<_>>();
        for key in keys {
            if let Some(window) = state.windows.remove(&key) {
                state.bytes -= window.bytes.len();
                drop(state.policy.remove(&key));
            }
        }
        for pin in state.pins.iter().filter_map(Weak::upgrade) {
            pin.windows.lock().map_err(|_| anyhow::anyhow!("pinned range lock poisoned"))?.retain(
                |key, _| {
                    !(key.0 == pack.path
                        && key.1 == pack.hash
                        && key.3 == pack.bytes
                        && key.2 < range.end
                        && key.2.saturating_add(key.4) > range.start)
                },
            );
        }
        drop(state);
        Ok(())
    }
    #[cfg(test)]
    pub(in crate::engine::reader) fn enable_probes(&self) {
        self.probe_enabled.store(true, std::sync::atomic::Ordering::Relaxed);
    }
    #[cfg(test)]
    pub(in crate::engine::reader) fn record_probe<K: super::BatchData, V: super::BatchData>(
        &self,
        schema: &str,
        pack: &str,
        leaf: &crate::engine::reader::index::Leaf<(K, V)>,
        key: &K,
    ) -> Result<()> {
        if !self.probe_enabled.load(std::sync::atomic::Ordering::Relaxed) {
            return Ok(());
        }
        self.state
            .lock()
            .map_err(|_| anyhow::anyhow!("range cache lock poisoned"))?
            .probes
            .record(schema, pack, leaf, key)
    }
    #[cfg(test)]
    pub(in crate::engine::reader) fn fix_width(&self, width: usize) -> Result<()> {
        ensure!(self.widths().contains(&width), "invalid test window width");
        let mut state =
            self.state.lock().map_err(|_| anyhow::anyhow!("range cache lock poisoned"))?;
        state.width = width;
        drop(state);
        Ok(())
    }
    #[cfg(test)]
    pub(in crate::engine::reader) fn take_diagnostics(&self) -> Result<serde_json::Value> {
        let mut state =
            self.state.lock().map_err(|_| anyhow::anyhow!("range cache lock poisoned"))?;
        let probes = state.probes.take();
        let fetched = std::mem::take(&mut state.fetched);
        let flight_wait_us = std::mem::take(&mut state.flight_wait_us);
        let flight_groups = std::mem::take(&mut state.flight_groups);
        let flight_stripes = std::mem::take(&mut state.flight_stripes);
        let (width, resident_bytes, resident_windows, resident_charge) =
            (state.width, state.bytes, state.windows.len(), state.policy.usage());
        let mut active_batches = 0;
        let mut pinned_bytes = 0;
        let mut pinned_windows = 0;
        for pin in state.pins.iter().filter_map(Weak::upgrade) {
            let windows =
                pin.windows.lock().map_err(|_| anyhow::anyhow!("pinned range lock poisoned"))?;
            active_batches += 1;
            pinned_bytes += windows.values().map(|window| window.bytes.len()).sum::<usize>();
            pinned_windows += windows.len();
        }
        drop(state);
        let (total, unique) = crate::engine::tests::store::range_totals(&fetched)?;
        let mut lengths = BTreeMap::<u64, usize>::new();
        for ((_, start, end), count) in &fetched {
            *lengths.entry(end - start).or_default() += count;
        }
        Ok(serde_json::json!({
            "probes":probes,
            "range_lengths": lengths,
            "fetched_ranges": fetched.iter().map(|((path, start, end), count)| (path, start, end, count)).collect::<Vec<_>>(),
            "active_batches": active_batches,
            "pinned_bytes": pinned_bytes,
            "pinned_windows": pinned_windows,
            "width": width,
            "resident_bytes": resident_bytes,
            "resident_charge": resident_charge,
            "resident_windows": resident_windows,
            "unique_ranges": fetched.len(),
            "flight_wait_us":flight_wait_us,"flight_groups":flight_groups,"flight_stripes":flight_stripes,
            "repeated_ranges": fetched.values().map(|count| count - 1).sum::<usize>(),
            "repeated_bytes": total - unique,
            "unique_bytes": unique
        }))
    }
    fn fitting_width(&self, pack: &Pack, position: u64, maximum: u64) -> Result<Option<u64>> {
        for width in self.widths().into_iter().rev() {
            let width = u64::try_from(width)?;
            if width == 0 || width > maximum {
                continue;
            }
            let start = position / width * width;
            let length = usize::try_from(start.saturating_add(width).min(pack.bytes) - start)?;
            let identity = (pack.path.clone(), pack.hash.clone(), start, pack.bytes, width);
            if Window::charge(&identity, length) <= self.bytes {
                return Ok(Some(width));
            }
        }
        Ok(None)
    }
    fn widths(&self) -> [usize; WIDTH_LEVELS] {
        window_widths(self.bytes)
    }
    async fn load(
        &self,
        store: &Arc<dyn ObjectStore>,
        path: &str,
        range: Range<u64>,
    ) -> Result<Vec<u8>> {
        #[cfg(test)]
        {
            let mut state =
                self.state.lock().map_err(|_| anyhow::anyhow!("range cache lock poisoned"))?;
            *state.fetched.entry((path.to_owned(), range.start, range.end)).or_default() += 1;
        }
        self.metrics.fetching(self.scan);
        let bytes = store.get_range(&Path::from(path), range).await?;
        self.metrics.fetched(self.scan, u64::try_from(bytes.len())?);
        Ok(bytes.to_vec())
    }
    fn cached(&self, pack: &Pack, requested: &Range<u64>) -> Result<Option<CachedRange>> {
        let mut state =
            self.state.lock().map_err(|_| anyhow::anyhow!("range cache lock poisoned"))?;
        state.pins.retain(|pin| pin.strong_count() > 0);
        for width in self.widths().into_iter().rev() {
            let width = u64::try_from(width)?;
            let start = requested.start / width * width;
            let identity = (pack.path.clone(), pack.hash.clone(), start, pack.bytes, width);
            if let Some(bytes) = state.get(&identity) {
                return Ok(Some((start, bytes)));
            }
        }
        Ok(None)
    }
    async fn fetch(
        &self,
        store: &Arc<dyn ObjectStore>,
        identity: Identity,
        end: u64,
    ) -> Result<Arc<Vec<u8>>> {
        let stripe = Self::stripe(&identity)?;
        let _flight = self.flights[stripe].lock().await;
        let cached = self
            .state
            .lock()
            .map_err(|_| anyhow::anyhow!("range cache lock poisoned"))?
            .get_or_cover(&identity)?;
        if let Some(bytes) = cached {
            return Ok(bytes);
        }
        let bytes = self.load(store, &identity.0, identity.2..end).await?;
        ensure!(
            u64::try_from(bytes.len())? == end - identity.2,
            "cached pack range length mismatch"
        );
        let bytes = Arc::new(bytes);
        self.retain(identity, Window::new(bytes.clone()))?;
        Ok(bytes)
    }
    fn stripe(identity: &Identity) -> Result<usize> {
        use std::hash::{Hash as _, Hasher as _};
        let mut hash = std::collections::hash_map::DefaultHasher::new();
        identity.hash(&mut hash);
        Ok(usize::try_from(hash.finish() % u64::try_from(STRIPES)?)?)
    }
    fn retain(&self, identity: Identity, window: Window) -> Result<()> {
        if self.entries == 0 || Window::charge(&identity, window.bytes.len()) > self.bytes {
            return Ok(());
        }
        let mut state =
            self.state.lock().map_err(|_| anyhow::anyhow!("range cache lock poisoned"))?;
        if state.windows.contains_key(&identity) {
            return Ok(());
        }
        // Budget-fitting planning may slice a resident wider window. Retain its
        // coverage once; charging both copies can evict useful bytes at the cap.
        if state.covered(&identity, &window)? {
            return Ok(());
        }
        state.remove_contained(&identity, window.bytes.len())?;
        drop(state.policy.insert(identity.clone(), window.bytes.len()));
        state.bytes += window.bytes.len();
        state.windows.insert(identity, window);
        let mut evicted = 0;
        let policy = state.policy.clone();
        state.windows.retain(|key, window| {
            if policy.contains(key) {
                return true;
            }
            evicted += window.bytes.len();
            false
        });
        state.bytes -= evicted;
        drop(state);
        Ok(())
    }
}

fn window_widths(bytes: usize) -> [usize; WIDTH_LEVELS] {
    let mut widths = [bytes.min(MIN_WINDOW_BYTES); WIDTH_LEVELS];
    for index in 1..widths.len() {
        widths[index] = widths[index - 1].saturating_mul(2).min(bytes.min(WINDOW_BYTES));
    }
    widths
}

impl Window {
    // Charge both identity copies (payload map and Foyer policy), the vector,
    // and a conservative fixed node/record overhead. This is an estimate, not RSS.
    const fn charge(identity: &Identity, length: usize) -> usize {
        length
            .saturating_add(std::mem::size_of::<Self>())
            .saturating_add(std::mem::size_of::<Vec<u8>>())
            .saturating_add(std::mem::size_of::<Identity>().saturating_mul(2))
            .saturating_add(
                identity.0.capacity().saturating_add(identity.1.capacity()).saturating_mul(2),
            )
            .saturating_add(256)
    }
    const fn new(bytes: Arc<Vec<u8>>) -> Self {
        Self { bytes }
    }
}
impl State {
    fn new(bytes: usize, entries: usize) -> Self {
        let minimum = bytes.div_ceil(entries.max(1)).max(1);
        let target = (bytes / entries.clamp(1, TARGET_WINDOWS)).max(MIN_WINDOW_BYTES);
        let width = window_widths(bytes)
            .into_iter()
            .rev()
            .find(|width| *width <= target)
            .unwrap_or_else(|| bytes.min(MIN_WINDOW_BYTES));
        let policy = foyer::Cache::builder(bytes.max(1))
            .with_shards(1)
            .with_eviction_config(foyer::LruConfig::default())
            .with_weighter(move |key: &Identity, length: &usize| {
                Window::charge(key, *length).max(minimum)
            })
            .build();
        Self {
            windows: BTreeMap::new(),
            bytes: 0,
            policy,
            width,
            #[cfg(test)]
            probes: probes::Probes::default(),
            pins: Vec::new(),
            #[cfg(test)]
            fetched: BTreeMap::new(),
            #[cfg(test)]
            flight_wait_us: 0,
            #[cfg(test)]
            flight_groups: 0,
            #[cfg(test)]
            flight_stripes: 0,
        }
    }
    fn covered(&self, identity: &Identity, window: &Window) -> Result<bool> {
        let end = identity.2 + u64::try_from(window.bytes.len())?;
        for (key, existing) in &self.windows {
            if same_pack(key, identity)
                && key.2 <= identity.2
                && key.2 + u64::try_from(existing.bytes.len())? >= end
            {
                return Ok(true);
            }
        }
        Ok(false)
    }
    fn remove_contained(&mut self, identity: &Identity, length: usize) -> Result<()> {
        let end = identity.2 + u64::try_from(length)?;
        let mut removed = 0;
        let policy = self.policy.clone();
        self.windows.retain(|key, window| {
            let keep = !(same_pack(key, identity)
                && key.2 >= identity.2
                && u64::try_from(window.bytes.len()).is_ok_and(|length| key.2 + length <= end));
            if !keep {
                removed += window.bytes.len();
                drop(policy.remove(key));
            }
            keep
        });
        self.bytes -= removed;
        Ok(())
    }
    fn get(&self, identity: &Identity) -> Option<Arc<Vec<u8>>> {
        drop(self.policy.get(identity));
        self.windows.get(identity).map(|entry| entry.bytes.clone())
    }
    fn get_or_cover(&self, identity: &Identity) -> Result<Option<Arc<Vec<u8>>>> {
        if let Some(bytes) = self.get(identity) {
            return Ok(Some(bytes));
        }

        let end = identity.2.saturating_add(identity.4).min(identity.3);
        let pack = Pack { path: identity.0.clone(), hash: identity.1.clone(), bytes: identity.3 };
        if let Some((key, window)) = self.pinned_window(&pack, &(identity.2..end), true)? {
            if key.2 == identity.2 && u64::try_from(window.bytes.len())? == end - identity.2 {
                return Ok(Some(window.bytes));
            }
            let start = usize::try_from(identity.2 - key.2)?;
            let stop = usize::try_from(end - key.2)?;
            return Ok(Some(Arc::new(window.bytes[start..stop].to_vec())));
        }
        for (key, entry) in &self.windows {
            if same_pack(key, identity)
                && key.2 <= identity.2
                && key.2 + u64::try_from(entry.bytes.len())? >= end
            {
                drop(self.policy.get(key));
                if key.2 == identity.2 && u64::try_from(entry.bytes.len())? == end - identity.2 {
                    return Ok(Some(entry.bytes.clone()));
                }
                // A wider cover may arrive while a narrow request waits on its
                // flight stripe. Return its slice instead of fetching it again.
                let start = usize::try_from(identity.2 - key.2)?;
                let stop = usize::try_from(end - key.2)?;
                return Ok(Some(Arc::new(entry.bytes[start..stop].to_vec())));
            }
        }
        Ok(None)
    }
}

fn same_pack(left: &Identity, right: &Identity) -> bool {
    left.0 == right.0 && left.1 == right.1 && left.3 == right.3
}

#[cfg(test)]
mod tests;
