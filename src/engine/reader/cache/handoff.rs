//! One bounded generated-output workspace, shared by every sibling upload.
use super::{BlockCache, Cached, Identity, State};
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
            index: parent.index.clone(),
            bytes: parent.bytes,
            entries: parent.entries,
            encoded: parent.encoded.clone(),
            ranges: parent.ranges.clone(),
            points: parent.points.clone(),
            filters: Arc::new(super::filter::Filters::new(parent.bytes, parent.entries)),
            parent: Some(parent),
            staged: true,
            state: Arc::default(),
        })
    }
    /// Generated pages are optional; avoid cloning them when retention is disabled
    /// or an output has filled its bounded private workspace.
    pub(in crate::engine::reader) fn retains_output(&self) -> Result<bool> {
        if self.bytes == 0 || self.entries == 0 {
            return Ok(false);
        }
        let state = self.state.lock().map_err(|_| anyhow::anyhow!("cache lock poisoned"))?;
        Ok(!state.overflowed)
    }
    /// Promote complete pages without evicting foreground-consumed pages. Cache data cannot expose a
    /// reference, publish membership, advance progress or acknowledge source WAL.
    pub(crate) fn promote(&self) -> Result<()> {
        ensure!(self.staged, "output promotion requires a workspace");
        let parent = self.parent.as_ref().context("output workspace has no foreground cache")?;
        let mut state = self.state.lock().map_err(|_| anyhow::anyhow!("cache lock poisoned"))?;
        let blocks = std::mem::take(&mut state.blocks);
        state.recency.clear();
        state.live.clear();
        state.stats.bytes = 0;
        state.stats.entries = 0;
        drop(state);
        let mut foreground =
            parent.state.lock().map_err(|_| anyhow::anyhow!("cache lock poisoned"))?;
        foreground.promote_generated(blocks, (parent.bytes, parent.entries))?;
        drop(foreground);
        self.filters.promote(&parent.filters)?;
        Ok(())
    }
}

impl State {
    fn promote_generated(
        &mut self,
        blocks: std::collections::BTreeMap<Identity, Cached>,
        limits: (usize, usize),
    ) -> Result<()> {
        let mut unused = Vec::new();
        for (identity, entry) in blocks {
            if entry.charge > limits.0 || limits.1 == 0 {
                continue;
            }
            let prior = self.blocks.get(&identity);
            let consumed = prior.is_some_and(|old| old.accessed > 0);
            let prior_charge = prior.map_or(0, |old| old.charge);
            let prior_entry = usize::from(prior.is_some());
            while self.stats.bytes - prior_charge > limits.0 - entry.charge
                || self.blocks.len() - prior_entry >= limits.1
            {
                // Timestamp zero denotes warming that no foreground get has
                // consumed. Scans peek without turning it into demand.
                let Some((0, oldest)) = self.recency.first() else { break };
                let oldest = oldest.clone();
                // A duplicate is accounted for separately by prior_charge.
                if oldest == identity {
                    break;
                }
                self.remove(oldest)?;
            }
            if self.stats.bytes - prior_charge > limits.0 - entry.charge
                || self.blocks.len() - prior_entry >= limits.1
            {
                continue;
            }
            if self.blocks.contains_key(&identity) {
                self.remove(identity.clone())?;
            }
            self.admit(identity.clone(), entry.rows, entry.charge, limits)?;
            if !consumed {
                unused.push(identity);
            }
        }
        // Current output must not evict its own earlier admissions. Mark only
        // after the complete bounded handoff, still under the same cache lock.
        for identity in unused {
            let entry = self.blocks.get_mut(&identity).context("generated page disappeared")?;
            self.recency.remove(&(entry.accessed, identity.clone()));
            entry.accessed = 0;
            self.recency.insert((0, identity));
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests;
