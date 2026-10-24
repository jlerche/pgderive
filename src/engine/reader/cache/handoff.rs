//! One bounded generated-output workspace, shared by every sibling upload.
use super::BlockCache;
use anyhow::{Context, Result, ensure};
use std::sync::Arc;
impl BlockCache {
    /// Reuse a sibling's workspace; never multiply the cap by arrangement count.
    /// Input scans still peek at foreground blocks without admission or LRU changes.
    pub(crate) fn output(parent: Arc<Self>) -> Arc<Self> {
        if parent.staged {
            return parent;
        }
        let parent = parent.parent.clone().unwrap_or(parent);
        Arc::new(Self {
            bytes: parent.bytes,
            entries: parent.entries,
            encoded: parent.encoded.clone(),
            ranges: parent.ranges.clone(),
            parent: Some(parent),
            staged: true,
            state: Arc::default(),
        })
    }
    /// Generated pages are optional; avoid cloning them when retention is disabled
    /// or an oversized output has already abandoned its entire private workspace.
    pub(in crate::engine::reader) fn retains_output(&self) -> Result<bool> {
        if self.bytes == 0 || self.entries == 0 {
            return Ok(false);
        }
        let state = self.state.lock().map_err(|_| anyhow::anyhow!("cache lock poisoned"))?;
        Ok(!state.overflowed)
    }
    /// Promote only a complete fitting upload. Cache data alone cannot expose a
    /// reference, publish membership, advance progress or acknowledge source WAL.
    pub(crate) fn promote(&self) -> Result<()> {
        ensure!(self.staged, "output promotion requires a workspace");
        let parent = self.parent.as_ref().context("output workspace has no foreground cache")?;
        let mut state = self.state.lock().map_err(|_| anyhow::anyhow!("cache lock poisoned"))?;
        if state.overflowed {
            return Ok(());
        }
        let blocks = std::mem::take(&mut state.blocks);
        state.live.clear();
        state.stats.bytes = 0;
        state.stats.entries = 0;
        drop(state);
        let mut foreground =
            parent.state.lock().map_err(|_| anyhow::anyhow!("cache lock poisoned"))?;
        // Replace every duplicate before admission: older vectors may have larger
        // capacities than the generated copy. The staged complete-set charge is
        // then the exact charge used by the foreground admission decision.
        for identity in blocks.keys() {
            if let Some(old) = foreground.blocks.remove(identity) {
                foreground.stats.bytes -= old.charge;
            }
        }
        foreground.stats.entries = foreground.blocks.len();
        for (identity, entry) in blocks {
            foreground.admit(identity, entry.rows, entry.charge, (parent.bytes, parent.entries))?;
        }
        drop(foreground);
        Ok(())
    }
}

#[cfg(test)]
mod tests;
