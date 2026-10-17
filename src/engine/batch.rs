use super::{ZSet, weights::Accumulator};
use anyhow::Result;

/// Canonical lexicographically ordered full identities and signed i64 weights.
pub type Batch<K, V> = ZSet<(K, V)>;

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
        ZSet::from_accumulator(self.weights)
    }
}

#[cfg(test)]
mod tests;
