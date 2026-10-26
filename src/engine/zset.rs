use super::weights::Accumulator;
use anyhow::{Context, Result, ensure};
use std::sync::{Arc, OnceLock};

/// A consolidated collection identified by complete tuples and signed weights.
#[derive(Debug, Clone)]
pub struct ZSet<T: Ord> {
    entries: Arc<Vec<(T, i64)>>,
    max_weight: u64,
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
        Self { entries: Arc::new(Vec::new()), max_weight: 0, sizing: Arc::new(OnceLock::new()) }
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

    /// Adopt already consolidated rows without rebuilding an exact accumulator.
    /// This boundary checks ordering and nonzero coefficients, never infers them.
    pub(crate) fn from_canonical(entries: Vec<(T, i64)>) -> Result<Self> {
        ensure!(entries.iter().all(|(_, weight)| *weight != 0), "zero canonical weight");
        ensure!(entries.windows(2).all(|pair| pair[0].0 < pair[1].0), "noncanonical batch order");
        Ok(Self::finalized(entries))
    }

    fn finalized(entries: Vec<(T, i64)>) -> Self {
        let max_weight = entries.iter().map(|(_, weight)| weight.unsigned_abs()).max().unwrap_or(0);
        Self { entries: Arc::new(entries), max_weight, sizing: Arc::new(OnceLock::new()) }
    }
    pub(crate) const fn max_weight(&self) -> u64 {
        self.max_weight
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
        let next = merge_canonical(&self.entries, &delta.entries)?;
        self.max_weight = next.iter().map(|(_, weight)| weight.unsigned_abs()).max().unwrap_or(0);
        self.entries = Arc::new(next);
        self.sizing = Arc::new(OnceLock::new());
        Ok(())
    }

    pub(super) fn from_accumulator(weights: Accumulator<T>) -> Result<Self> {
        Ok(Self::finalized(weights.finish()?))
    }
}

// Both inputs are already canonical: each complete identity occurs once per
// slice, so checked addition narrows exactly one final combined coefficient.
pub(super) fn merge_canonical<T: Ord + Clone>(
    prior: &[(T, i64)],
    delta: &[(T, i64)],
) -> Result<Vec<(T, i64)>> {
    let mut prior = prior.iter().peekable();
    let mut next = Vec::with_capacity(prior.len());
    for (tuple, weight) in delta {
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
    Ok(next)
}

#[cfg(test)]
mod tests {
    use super::merge_canonical;
    use crate::engine::{Batch, BatchBuilder};
    use anyhow::Result;
    #[test]
    fn canonical_merge_matches_exact_accumulation_for_signed_full_tuples() -> Result<()> {
        for before in [i64::MIN, -1, 0, 1, i64::MAX] {
            for delta in [i64::MIN, -1, 0, 1, i64::MAX] {
                let prior = Batch::from_updates([((1, 1), before), ((1, 2), -1), ((2, 3), 3)])?;
                let added = Batch::from_updates([((1, 1), delta), ((1, 2), 1), ((1, 4), -2)])?;
                let mut oracle = BatchBuilder::default();
                oracle.extend(&prior);
                oracle.extend(&added);
                let expected = oracle.finish();
                let actual = merge_canonical(&prior.shared_entries(), &added.shared_entries());
                assert_eq!(actual.is_ok(), expected.is_ok());
                if let (Ok(rows), Ok(expected)) = (actual, expected) {
                    assert_eq!(Batch::from_canonical(rows)?, expected);
                }
            }
        }
        Ok(())
    }
}
