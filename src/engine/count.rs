use super::ZSet;
use anyhow::{Context, Result};
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
        let grouped = input.try_map(|(key, _)| Ok(key.clone()))?;
        let mut next = self.counts.clone();
        let mut output = ZSet::default();
        for (key, delta) in grouped.iter() {
            let old = self.counts.get(key).copied().unwrap_or_default();
            let new = old.checked_add(*delta).context("grouped count overflow")?;
            if old != 0 {
                output.add((key.clone(), old), -1)?;
            }
            if new == 0 {
                next.remove(key);
            } else {
                output.add((key.clone(), new), 1)?;
                next.insert(key.clone(), new);
            }
        }
        self.counts = next;
        Ok(output)
    }
}

#[cfg(test)]
mod tests;
