use super::{ZSet, weights::Accumulator};
use anyhow::Result;

/// Full `(key, value)` identity arranged lexicographically; keys need not be unique.
pub type IndexedZSet<K, V> = ZSet<(K, V)>;

/// Immutable canonical logical batch: sorted full identities, nonzero i64 weights.
/// This owns memory; object-backed representations follow the same logical contract.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Batch<K: Ord, V: Ord> {
    collection: IndexedZSet<K, V>,
}

/// Exact, unfinished batch. Intermediate coefficients are not restricted to i64.
/// Physical grouping/merge boundaries do not finalize the logical collection.
#[derive(Debug, Clone)]
pub struct BatchBuilder<K: Ord, V: Ord> {
    weights: Accumulator<(K, V)>,
}

impl<K: Ord, V: Ord> Default for BatchBuilder<K, V> {
    fn default() -> Self {
        Self { weights: Accumulator::default() }
    }
}

impl<K: Ord + Clone, V: Ord + Clone> BatchBuilder<K, V> {
    /// Add an input coefficient, deferring narrowing until `finish`.
    pub fn push(&mut self, key: K, value: V, weight: i64) {
        self.weights.add((key, value), weight);
    }

    /// Merge unfinished partial work without premature representability checks.
    pub fn merge(&mut self, other: Self) {
        self.weights.merge(other.weights);
    }

    /// Include an already finalized input batch.
    pub fn extend(&mut self, batch: &Batch<K, V>) {
        for ((key, value), weight) in batch.iter() {
            self.push(key.clone(), value.clone(), *weight);
        }
    }

    /// Finalize one logical batch, consolidating and removing zero coefficients.
    ///
    /// # Errors
    /// Returns an error if a final coefficient is outside the i64 domain.
    pub fn finish(self) -> Result<Batch<K, V>> {
        Ok(Batch { collection: ZSet::from_accumulator(self.weights)? })
    }
}

impl<K: Ord + Clone, V: Ord + Clone> Batch<K, V> {
    /// Canonicalize a complete logical input batch.
    ///
    /// # Errors
    /// Returns an error if a final coefficient is outside the i64 domain.
    pub fn from_updates(updates: impl IntoIterator<Item = ((K, V), i64)>) -> Result<Self> {
        let mut builder = BatchBuilder::default();
        for ((key, value), weight) in updates {
            builder.push(key, value, weight);
        }
        builder.finish()
    }

    /// Read the canonical ordered full tuples and their multiplicities.
    pub fn iter(&self) -> impl Iterator<Item = (&(K, V), &i64)> + Clone {
        self.collection.iter()
    }
}

#[cfg(test)]
mod tests;
