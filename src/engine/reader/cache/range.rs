//! Bounded compressed windows amortize reads without pinning decoded pages.
use crate::engine::reader::format::Pack;
use anyhow::{Context, Result, ensure};
use object_store::{ObjectStore, ObjectStoreExt as _, path::Path};
use std::{
    collections::BTreeMap,
    ops::Range,
    sync::{Arc, Mutex},
};

const WINDOW_BYTES: usize = 256 * 1024;
const STRIPES: usize = 16;
type Identity = (String, String, u64, u64);
struct Window {
    bytes: Arc<Vec<u8>>,
    accessed: u64,
}
#[derive(Default)]
struct State {
    windows: BTreeMap<Identity, Window>,
    bytes: usize,
    clock: u64,
}
pub(in crate::engine::reader) struct Ranges {
    bytes: usize,
    entries: usize,
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
            state: Mutex::default(),
            flights: std::array::from_fn(|_| tokio::sync::Mutex::new(())),
        }
    }
    pub(super) fn scan() -> Self {
        Self::new(WINDOW_BYTES, 1)
    }
    pub(in crate::engine::reader) async fn read(
        &self,
        store: &Arc<dyn ObjectStore>,
        pack: &Pack,
        range: Range<u64>,
    ) -> Result<Vec<u8>> {
        ensure!(range.start < range.end, "empty cached pack range");
        ensure!(range.end <= pack.bytes, "cached range exceeds pack");
        if self.bytes == 0
            || self.entries == 0
            || range.end - range.start > u64::try_from(self.bytes)?
        {
            return Ok(store.get_range(&Path::from(pack.path.clone()), range).await?.to_vec());
        }
        let width = u64::try_from(self.bytes.min(WINDOW_BYTES))?;
        let mut result = Vec::with_capacity(usize::try_from(range.end - range.start)?);
        let mut position = range.start;
        while position < range.end {
            let start = position / width * width;
            let end = start.saturating_add(width).min(pack.bytes);
            let identity = (pack.path.clone(), pack.hash.clone(), start, pack.bytes);
            let bytes = match self.get(&identity)? {
                Some(bytes) => bytes,
                None => self.fetch(store, identity, end).await?,
            };
            let stop = range.end.min(end);
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
        let width = u64::try_from(self.bytes.min(WINDOW_BYTES))?;
        let keys = state
            .windows
            .keys()
            .filter(|key| {
                key.0 == pack.path
                    && key.1 == pack.hash
                    && key.3 == pack.bytes
                    && key.2 < range.end
                    && key.2.saturating_add(width) > range.start
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
    fn get(&self, identity: &Identity) -> Result<Option<Arc<Vec<u8>>>> {
        let mut state =
            self.state.lock().map_err(|_| anyhow::anyhow!("range cache lock poisoned"))?;
        state.clock = state.clock.saturating_add(1);
        let clock = state.clock;
        Ok(state.windows.get_mut(identity).map(|entry| {
            entry.accessed = clock;
            entry.bytes.clone()
        }))
    }
    async fn fetch(
        &self,
        store: &Arc<dyn ObjectStore>,
        identity: Identity,
        end: u64,
    ) -> Result<Arc<Vec<u8>>> {
        use std::hash::{Hash as _, Hasher as _};
        let mut hash = std::collections::hash_map::DefaultHasher::new();
        identity.hash(&mut hash);
        let stripe = usize::try_from(hash.finish() % u64::try_from(STRIPES)?)?;
        let _flight = self.flights[stripe].lock().await;
        if let Some(bytes) = self.get(&identity)? {
            return Ok(bytes);
        }
        let bytes = store.get_range(&Path::from(identity.0.clone()), identity.2..end).await?;
        ensure!(
            u64::try_from(bytes.len())? == end - identity.2,
            "cached pack range length mismatch"
        );
        let bytes = Arc::new(bytes.to_vec());
        let mut state =
            self.state.lock().map_err(|_| anyhow::anyhow!("range cache lock poisoned"))?;
        while state.bytes > self.bytes - bytes.len() || state.windows.len() >= self.entries {
            let oldest = state
                .windows
                .iter()
                .min_by_key(|(_, entry)| entry.accessed)
                .map(|(key, _)| key.clone())
                .context("range eviction without entries")?;
            let old = state.windows.remove(&oldest).context("range eviction lost entry")?;
            state.bytes -= old.bytes.len();
        }
        state.bytes += bytes.len();
        state.clock = state.clock.saturating_add(1);
        let accessed = state.clock;
        state.windows.insert(identity, Window { bytes: bytes.clone(), accessed });
        drop(state);
        Ok(bytes)
    }
}

#[cfg(test)]
mod tests;
