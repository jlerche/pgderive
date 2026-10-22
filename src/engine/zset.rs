use super::weights::Accumulator;
use anyhow::{Context, Result};
use std::sync::{Arc, OnceLock};

/// A consolidated collection identified by complete tuples and signed weights.
#[derive(Debug, Clone)]
pub struct ZSet<T: Ord> {
    entries: Arc<Vec<(T, i64)>>,
    pub(super) sizing: Arc<OnceLock<super::batch_size::BatchSize>>,
}

impl<T: Ord> PartialEq for ZSet<T> {
    fn eq(&self, other: &Self) -> bool {
        self.entries == other.entries
    }
}
impl<T: Ord> Eq for ZSet<T> {}
impl<T: Ord> Default for ZSet<T> {
    fn default() -> Self {
        Self { entries: Arc::new(Vec::new()), sizing: Arc::new(OnceLock::new()) }
    }
}

impl<T: Ord + Clone> ZSet<T> {
    /// Consolidate updates, dropping zero weights.
    ///
    /// # Errors
    /// Returns an error on weight overflow.
    pub fn from_updates(updates: impl IntoIterator<Item = (T, i64)>) -> Result<Self> {
        let mut weights = Accumulator::default();
        for (tuple, weight) in updates {
            weights.add(tuple, weight);
        }
        Self::from_accumulator(weights)
    }

    /// Iterate in deterministic tuple order.
    pub fn iter(&self) -> impl Iterator<Item = (&T, &i64)> + Clone {
        self.entries.iter().map(|(tuple, weight)| (tuple, weight))
    }

    // Only finalized, sorted, unique, nonzero entries can reach cursor storage.
    pub(super) fn shared_entries(&self) -> Arc<Vec<(T, i64)>> {
        Arc::clone(&self.entries)
    }

    pub(crate) fn weight(&self, tuple: &T) -> i64 {
        self.entries
            .binary_search_by(|entry| entry.0.cmp(tuple))
            .map_or(0, |position| self.entries[position].1)
    }

    pub(crate) fn accumulate_weights(&self, delta: &Self, totals: &mut [num_bigint::BigInt]) {
        let mut position = 0;
        for ((tuple, _), total) in delta.iter().zip(totals) {
            position = super::search::advance(&self.entries, position, |row| row.0 < *tuple);
            if let Some((found, weight)) = self.entries.get(position)
                && found == tuple
            {
                *total += *weight;
            }
        }
    }

    /// Integrate one delta atomically, retaining the original state on error.
    ///
    /// # Errors
    /// Returns an error on weight overflow.
    pub fn apply(&mut self, delta: &Self) -> Result<()> {
        let mut prior = self.entries.iter().peekable();
        let mut next = Vec::with_capacity(self.entries.len());
        for (tuple, weight) in delta.iter() {
            while let Some(entry) = prior.next_if(|entry| &entry.0 < tuple) {
                next.push(entry.clone());
            }
            let weight = prior.next_if(|entry| &entry.0 == tuple).map_or(Ok(*weight), |entry| {
                entry.1.checked_add(*weight).context("Z-set weight overflow")
            })?;
            if weight != 0 {
                next.push((tuple.clone(), weight));
            }
        }
        next.extend(prior.cloned());
        self.entries = Arc::new(next);
        self.sizing = Arc::new(OnceLock::new());
        Ok(())
    }

    pub(super) fn from_accumulator(weights: Accumulator<T>) -> Result<Self> {
        Ok(Self {
            entries: Arc::new(weights.finish()?.into_iter().collect()),
            sizing: Arc::new(OnceLock::new()),
        })
    }
}
