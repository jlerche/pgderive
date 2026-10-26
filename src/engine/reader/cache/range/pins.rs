//! Scoped read-ahead ownership, separate from long-lived cache admission.
use super::{Identity, Pack, Ranges, State, Window};
use anyhow::{Context as _, Result};
use std::{
    collections::BTreeMap,
    ops::Range,
    sync::{Arc, Mutex, Weak},
};

pub(super) struct Pinned {
    pub(super) windows: Mutex<BTreeMap<Identity, Window>>,
}
/// Keeps planned payloads alive independently of long-lived cache admission.
/// Each plan caps payload bytes and window count; identity and pin-map metadata
/// are additional bounded working memory, outside the weighted resident cache.
#[must_use = "hold the read-ahead guard through consumption of the planned rows"]
pub struct ReadGuard {
    pub(super) _pins: Vec<Arc<Pinned>>,
}
impl Ranges {
    pub(super) fn pin(&self, windows: BTreeMap<Identity, Window>) -> Result<Arc<Pinned>> {
        let pin = Arc::new(Pinned { windows: Mutex::new(windows) });
        let mut state =
            self.state.lock().map_err(|_| anyhow::anyhow!("range cache lock poisoned"))?;
        state.pins.retain(|pin| pin.strong_count() > 0);
        state.pins.push(Arc::downgrade(&pin));
        drop(state);
        Ok(pin)
    }
    pub(super) fn pinned(
        &self,
        pack: &Pack,
        requested: &Range<u64>,
    ) -> Result<Option<super::CachedRange>> {
        let found = {
            let mut state =
                self.state.lock().map_err(|_| anyhow::anyhow!("range cache lock poisoned"))?;
            state.pins.retain(|pin| pin.strong_count() > 0);
            state.pinned_window(pack, requested, false)?
        };
        let Some((identity, window)) = found else { return Ok(None) };
        self.retain(identity.clone(), window)?;
        let bytes = self
            .state
            .lock()
            .map_err(|_| anyhow::anyhow!("range cache lock poisoned"))?
            .get_or_cover(&identity)?
            .context("pinned read was not admitted")?;
        Ok(Some((identity.2, bytes)))
    }
}
impl State {
    pub(super) fn pinned_window(
        &self,
        pack: &Pack,
        requested: &Range<u64>,
        complete: bool,
    ) -> Result<Option<(Identity, Window)>> {
        for pin in self.pins.iter().filter_map(Weak::upgrade) {
            let windows =
                pin.windows.lock().map_err(|_| anyhow::anyhow!("pinned range lock poisoned"))?;
            for (identity, window) in windows.iter() {
                let end = identity.2 + u64::try_from(window.bytes.len())?;
                if identity.0 == pack.path
                    && identity.1 == pack.hash
                    && identity.3 == pack.bytes
                    && identity.2 <= requested.start
                    && requested.start < end
                    && (!complete || requested.end <= end)
                {
                    return Ok(Some((identity.clone(), window.clone())));
                }
            }
        }
        Ok(None)
    }
}
