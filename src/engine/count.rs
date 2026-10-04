use super::{ZSet, weights::Accumulator};
use anyhow::Result;
use std::collections::BTreeMap;

/// Incremental sum of input multiplicities per group.
///
/// Each changed nonzero count is represented by one `(key, count)` output tuple
/// of weight +1, retracting the previous tuple with weight -1. Zero counts remove
/// the group. Signed input/counts are supported; SQL COUNT(*) equivalence assumes
/// valid nonnegative source bags. NULL group keys may be represented by Option.
#[derive(Debug, Clone)]
pub struct GroupedCount<K: Ord> {
    counts: BTreeMap<K, i64>,
}

impl<K: Ord> Default for GroupedCount<K> {
    fn default() -> Self {
        Self { counts: BTreeMap::new() }
    }
}

impl<K: Ord + Clone> GroupedCount<K> {
    /// Apply one complete transaction and emit consolidated count-row changes.
    /// Payloads keep full tuple identity upstream; counting sums their weights.
    ///
    /// # Errors
    /// Returns arithmetic overflow without changing the previously committed
    /// counts. Circuit time is owned by the enclosing transaction boundary.
    pub fn step<V: Ord + Clone>(&mut self, input: &ZSet<(K, V)>) -> Result<ZSet<(K, i64)>> {
        let mut totals = Accumulator::default();
        for (key, count) in &self.counts {
            totals.add(key.clone(), *count);
        }
        for ((key, _), weight) in input.iter() {
            totals.add(key.clone(), *weight);
        }
        let next = totals.finish()?;
        let keys = self.counts.keys().chain(next.keys()).collect::<std::collections::BTreeSet<_>>();
        let mut output = ZSet::default();
        for key in keys {
            let old = self.counts.get(key).copied().unwrap_or_default();
            let new = next.get(key).copied().unwrap_or_default();
            if old == new {
                continue;
            }
            if old != 0 {
                output.add((key.clone(), old), -1)?;
            }
            if new != 0 {
                output.add((key.clone(), new), 1)?;
            }
        }
        self.counts = next;
        Ok(output)
    }
}

#[cfg(test)]
mod tests;
