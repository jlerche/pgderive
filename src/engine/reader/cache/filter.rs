//! Disposable negative-only key synopses survive decoded page eviction.
use super::{Identity, Rows};
use crate::engine::reader::BatchData;
use anyhow::{Context as _, Result};
use std::sync::Arc;
use std::{
    collections::BTreeMap,
    hash::{Hash, Hasher},
    sync::Mutex,
};

#[derive(Debug, serde::Serialize)]
struct Metadata<K, V> {
    first: (K, V),
    last: (K, V),
    rows: usize,
    max_weight: u64,
}
#[derive(Debug, Clone)]
pub(super) struct Bloom {
    bits: Vec<u64>,
    metadata: Arc<Rows>,
    metadata_bytes: usize,
}
impl Bloom {
    pub(super) fn new<K: BatchData, V: BatchData>(
        rows: &[crate::engine::reader::Entry<K, V>],
    ) -> Result<Self> {
        let words = rows.len().checked_mul(10).context("filter size overflow")?.div_ceil(64).max(1);
        let metadata = Metadata {
            first: rows.first().context("empty filter page")?.0.clone(),
            last: rows.last().context("empty filter page")?.0.clone(),
            rows: rows.len(),
            max_weight: rows.iter().map(|(_, weight)| weight.unsigned_abs()).max().unwrap_or(0),
        };
        let metadata_bytes = std::mem::size_of_val(&metadata)
            .checked_add(serde_json::to_vec(&metadata)?.len())
            .context("filter metadata overflow")?;
        let mut bloom = Self { bits: vec![0; words], metadata: Arc::new(metadata), metadata_bytes };
        for ((key, _), _) in rows {
            for (word, mask) in bloom.positions(key)? {
                bloom.bits[word] |= mask;
            }
        }
        Ok(bloom)
    }
    fn positions(&self, key: &impl Hash) -> Result<[(usize, u64); 4]> {
        let mut hasher = std::hash::DefaultHasher::new();
        key.hash(&mut hasher);
        let hash = hasher.finish();
        let words = u64::try_from(self.bits.len())?;
        let mut result = [(0, 0); 4];
        for (rotation, slot) in [0, 13, 26, 39].into_iter().zip(&mut result) {
            let bit = hash.rotate_left(rotation);
            *slot = (usize::try_from((bit >> 6) % words)?, 1_u64 << (bit % 64));
        }
        Ok(result)
    }
    fn contains(&self, key: &impl Hash) -> Result<bool> {
        Ok(self.positions(key)?.into_iter().all(|(word, mask)| self.bits[word] & mask != 0))
    }
}
#[derive(Debug)]
struct Cached {
    bloom: Bloom,
    charge: usize,
    referenced: bool,
}
#[derive(Debug, Default)]
struct State {
    entries: BTreeMap<Arc<Identity>, Cached>,
    hand: Option<Arc<Identity>>,
    bytes: usize,
    rejected: u64,
    eviction_steps: u64,
}
#[derive(Debug)]
pub(super) struct Filters {
    bytes: usize,
    entries: usize,
    state: Mutex<State>,
}
impl Filters {
    pub(super) fn new(bytes: usize, entries: usize) -> Self {
        Self { bytes: bytes / 4, entries: entries.saturating_mul(16), state: Mutex::default() }
    }
    pub(super) const fn enabled(&self) -> bool {
        self.bytes > 0 && self.entries > 0
    }
    pub(super) fn contains<K: BatchData, V: BatchData>(
        &self,
        identity: &Identity,
        key: &K,
        block: &crate::engine::reader::format::Block<(K, V)>,
    ) -> Result<bool> {
        let mut state = self.state.lock().map_err(|_| anyhow::anyhow!("filter lock poisoned"))?;
        let present = match state.entries.get_mut(identity) {
            Some(entry) => {
                entry.referenced = true;
                match entry.bloom.metadata.downcast_ref::<Metadata<K, V>>() {
                    Some(metadata)
                        if metadata.first == block.first
                            && metadata.last == block.last
                            && metadata.rows == block.rows
                            && metadata.max_weight == block.max_weight =>
                    {
                        entry.bloom.contains(key)?
                    }
                    _ => true,
                }
            }
            None => true,
        };
        if !present {
            state.rejected = state.rejected.saturating_add(1);
        }
        drop(state);
        Ok(present)
    }
    pub(super) fn remember<K: BatchData, V: BatchData>(
        &self,
        identity: Identity,
        rows: &[crate::engine::reader::Entry<K, V>],
    ) -> Result<()> {
        if !self.enabled() {
            return Ok(());
        }
        {
            let state = self.state.lock().map_err(|_| anyhow::anyhow!("filter lock poisoned"))?;
            if state.entries.get(&identity).is_some_and(|entry| {
                entry.bloom.metadata.downcast_ref::<Metadata<K, V>>().is_some_and(|metadata| {
                    metadata.rows == rows.len()
                        && rows.first().is_some_and(|row| row.0 == metadata.first)
                        && rows.last().is_some_and(|row| row.0 == metadata.last)
                        && metadata.max_weight
                            == rows
                                .iter()
                                .map(|(_, weight)| weight.unsigned_abs())
                                .max()
                                .unwrap_or(0)
                })
            }) {
                return Ok(());
            }
        }
        self.insert(identity, Bloom::new(rows)?)
    }
    pub(super) fn insert(&self, identity: Identity, bloom: Bloom) -> Result<()> {
        let mut state = self.state.lock().map_err(|_| anyhow::anyhow!("filter lock poisoned"))?;
        let charge = std::mem::size_of::<(Identity, Cached)>()
            .checked_add(3 * std::mem::size_of::<usize>())
            .context("filter identity charge overflow")?
            .checked_add(identity.1.capacity())
            .and_then(|n| n.checked_add(identity.2.capacity()))
            .and_then(|n| n.checked_add(bloom.bits.capacity().checked_mul(8)?))
            .and_then(|n| n.checked_add(bloom.metadata_bytes))
            .context("filter cache charge overflow")?;
        if charge > self.bytes || self.entries == 0 {
            return Ok(());
        }
        if let Some((key, old)) = state.entries.remove_entry(&identity) {
            if state.hand.as_ref().is_some_and(|hand| hand == &key) {
                state.hand = None;
            }
            state.bytes -= old.charge;
        }
        while state.bytes > self.bytes - charge || state.entries.len() >= self.entries {
            evict(&mut state)?;
        }
        state.entries.insert(Arc::new(identity), Cached { bloom, charge, referenced: false });
        state.bytes += charge;
        drop(state);
        Ok(())
    }
    pub(super) fn promote(&self, parent: &Self) -> Result<()> {
        let entries = {
            let mut state =
                self.state.lock().map_err(|_| anyhow::anyhow!("filter lock poisoned"))?;
            state.bytes = 0;
            state.hand = None;
            std::mem::take(&mut state.entries)
        };
        for (identity, entry) in entries {
            parent.insert(Arc::unwrap_or_clone(identity), entry.bloom)?;
        }
        Ok(())
    }
    pub(super) fn stats(&self) -> Result<(usize, usize, u64, u64)> {
        let state = self.state.lock().map_err(|_| anyhow::anyhow!("filter lock poisoned"))?;
        Ok((state.bytes, state.entries.len(), state.rejected, state.eviction_steps))
    }
}

// CLOCK gives a hit a second chance without scanning the complete cache for
// every eviction or maintaining a separate recency tree on every key probe.
fn evict(state: &mut State) -> Result<()> {
    use std::ops::Bound::{Excluded, Unbounded};
    loop {
        state.eviction_steps = state.eviction_steps.saturating_add(1);
        let next = state.hand.as_ref().and_then(|hand| {
            state.entries.range::<Arc<Identity>, _>((Excluded(hand), Unbounded)).next()
        });
        let key = next
            .or_else(|| state.entries.first_key_value())
            .map(|(key, _)| key.clone())
            .context("filter eviction without entries")?;
        state.hand = Some(key.clone());
        let entry = state.entries.get_mut(&key).context("filter eviction lost entry")?;
        if entry.referenced {
            entry.referenced = false;
            continue;
        }
        let old = state.entries.remove(&key).context("filter eviction lost entry")?;
        state.bytes -= old.charge;
        state.hand = state
            .entries
            .range(..key)
            .next_back()
            .or_else(|| state.entries.last_key_value())
            .map(|(key, _)| key.clone());
        return Ok(());
    }
}

#[cfg(test)]
mod tests;
