//! Bounded compressed windows amortize reads without pinning decoded pages.
use crate::engine::reader::format::Pack;
use anyhow::{Context, Result, ensure};
use object_store::{ObjectStore, ObjectStoreExt as _, path::Path};
use std::{
    collections::BTreeMap,
    ops::Range,
    sync::{Arc, Mutex},
};

mod prefetch;
pub use prefetch::ReadAhead;

const WINDOW_BYTES: usize = 256 * 1024;
const STRIPES: usize = 16;
const MIN_WINDOW_BYTES: usize = 16 * 1024;
type Identity = (String, String, u64, u64, u64);
type CachedRange = (u64, Arc<Vec<u8>>);
#[derive(Clone)]
struct Window {
    bytes: Arc<Vec<u8>>,
    accessed: u64,
    used: u64,
}
#[derive(Default)]
struct State {
    windows: BTreeMap<Identity, Window>,
    bytes: usize,
    clock: u64,
    width: usize,
}
pub(in crate::engine::reader) struct Ranges {
    bytes: usize,
    entries: usize,
    pub(super) metrics: Arc<super::metrics::Metrics>,
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
    pub(super) fn new(bytes: usize, entries: usize) -> Self {
        Self {
            bytes,
            entries,
            metrics: Arc::default(),
            scan: false,
            state: Mutex::new(State { width: bytes.min(WINDOW_BYTES), ..State::default() }),
            flights: std::array::from_fn(|_| tokio::sync::Mutex::new(())),
        }
    }
    pub(super) fn scan(parent: &Self) -> Self {
        Self { metrics: parent.metrics.clone(), scan: true, ..Self::new(WINDOW_BYTES, 1) }
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
            } else {
                let width = u64::try_from(
                    self.state
                        .lock()
                        .map_err(|_| anyhow::anyhow!("range cache lock poisoned"))?
                        .width,
                )?;
                let start = position / width * width;
                let end = start.saturating_add(width).min(pack.bytes);
                let identity = (pack.path.clone(), pack.hash.clone(), start, pack.bytes, width);
                (start, self.fetch(store, identity, end, &requested).await?)
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
            }
        }
        drop(state);
        Ok(())
    }
    fn widths(&self) -> [usize; 5] {
        let mut widths = [self.bytes.min(MIN_WINDOW_BYTES); 5];
        for index in 1..widths.len() {
            widths[index] = widths[index - 1].saturating_mul(2).min(self.bytes.min(WINDOW_BYTES));
        }
        widths
    }
    fn adapt(&self, state: &mut State, used: u32) {
        if used < 16 {
            state.width = self
                .widths()
                .into_iter()
                .filter(|width| *width < state.width)
                .max()
                .unwrap_or_else(|| self.bytes.min(MIN_WINDOW_BYTES));
        } else if used > 48 {
            state.width = self
                .widths()
                .into_iter()
                .filter(|width| *width > state.width)
                .min()
                .unwrap_or_else(|| self.bytes.min(WINDOW_BYTES));
        }
    }
    async fn load(
        &self,
        store: &Arc<dyn ObjectStore>,
        path: &str,
        range: Range<u64>,
    ) -> Result<Vec<u8>> {
        self.metrics.fetching(self.scan);
        let bytes = store.get_range(&Path::from(path), range).await?;
        self.metrics.fetched(self.scan, u64::try_from(bytes.len())?);
        Ok(bytes.to_vec())
    }
    fn cached(&self, pack: &Pack, requested: &Range<u64>) -> Result<Option<CachedRange>> {
        let mut state =
            self.state.lock().map_err(|_| anyhow::anyhow!("range cache lock poisoned"))?;
        for width in self.widths().into_iter().rev() {
            let width = u64::try_from(width)?;
            let start = requested.start / width * width;
            let identity = (pack.path.clone(), pack.hash.clone(), start, pack.bytes, width);
            if let Some(bytes) = state.get(&identity, requested)? {
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
        requested: &Range<u64>,
    ) -> Result<Arc<Vec<u8>>> {
        use std::hash::{Hash as _, Hasher as _};
        let mut hash = std::collections::hash_map::DefaultHasher::new();
        identity.hash(&mut hash);
        let stripe = usize::try_from(hash.finish() % u64::try_from(STRIPES)?)?;
        let _flight = self.flights[stripe].lock().await;
        let cached = self
            .state
            .lock()
            .map_err(|_| anyhow::anyhow!("range cache lock poisoned"))?
            .get(&identity, requested)?;
        if let Some(bytes) = cached {
            return Ok(bytes);
        }
        let bytes = self.load(store, &identity.0, identity.2..end).await?;
        ensure!(
            u64::try_from(bytes.len())? == end - identity.2,
            "cached pack range length mismatch"
        );
        let bytes = Arc::new(bytes);
        let mut window = Window { bytes: bytes.clone(), accessed: 0, used: 0 };
        window.mark(identity.2, requested)?;
        self.retain(identity, window)?;
        Ok(bytes)
    }
    fn retain(&self, identity: Identity, mut window: Window) -> Result<()> {
        let mut state =
            self.state.lock().map_err(|_| anyhow::anyhow!("range cache lock poisoned"))?;
        state.clock = state.clock.saturating_add(1);
        window.accessed = state.clock;
        if let Some(existing) = state.windows.get_mut(&identity) {
            existing.used |= window.used;
            existing.accessed = window.accessed;
            return Ok(());
        }
        while state.bytes > self.bytes - window.bytes.len() || state.windows.len() >= self.entries {
            let oldest = state
                .windows
                .iter()
                .min_by_key(|(_, entry)| entry.accessed)
                .map(|(key, _)| key.clone())
                .context("range eviction without entries")?;
            let old = state.windows.remove(&oldest).context("range eviction lost entry")?;
            state.bytes -= old.bytes.len();
            if !self.scan {
                self.adapt(&mut state, old.used.count_ones());
            }
        }
        state.bytes += window.bytes.len();
        state.windows.insert(identity, window);
        drop(state);
        Ok(())
    }
}

impl Window {
    fn mark(&mut self, start: u64, requested: &Range<u64>) -> Result<()> {
        let length = u64::try_from(self.bytes.len())?;
        let unit = length.div_ceil(64);
        let first = (requested.start - start) / unit;
        let end = (requested.end.min(start + length) - start).div_ceil(unit).min(64);
        ensure!(first < end && end <= 64, "invalid cached demand range");
        self.used |= (u64::MAX << first) & (u64::MAX >> (64 - end));
        Ok(())
    }
}
impl State {
    fn get(&mut self, identity: &Identity, requested: &Range<u64>) -> Result<Option<Arc<Vec<u8>>>> {
        self.clock = self.clock.saturating_add(1);
        let clock = self.clock;
        self.windows
            .get_mut(identity)
            .map(|entry| {
                entry.accessed = clock;
                entry.mark(identity.2, requested)?;
                Ok(entry.bytes.clone())
            })
            .transpose()
    }
}

#[cfg(test)]
mod tests;
