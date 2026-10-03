use anyhow::{Context, Result};
use std::collections::BTreeMap;

/// A consolidated collection identified by complete tuples and signed weights.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ZSet<T: Ord> {
    entries: BTreeMap<T, i64>,
}

impl<T: Ord> Default for ZSet<T> {
    fn default() -> Self {
        Self { entries: BTreeMap::new() }
    }
}

impl<T: Ord + Clone> ZSet<T> {
    /// Consolidate updates, dropping zero weights.
    ///
    /// # Errors
    /// Returns an error on weight overflow.
    pub fn from_updates(updates: impl IntoIterator<Item = (T, i64)>) -> Result<Self> {
        let mut result = Self::default();
        for (tuple, weight) in updates {
            result.add(tuple, weight)?;
        }
        Ok(result)
    }

    /// Iterate in deterministic tuple order.
    pub fn iter(&self) -> impl Iterator<Item = (&T, &i64)> {
        self.entries.iter()
    }

    /// Integrate one delta atomically, retaining the original state on error.
    ///
    /// # Errors
    /// Returns an error on weight overflow.
    pub fn apply(&mut self, delta: &Self) -> Result<()> {
        let mut next = self.clone();
        for (tuple, weight) in delta.iter() {
            next.add(tuple.clone(), *weight)?;
        }
        *self = next;
        Ok(())
    }

    pub(super) fn add(&mut self, tuple: T, weight: i64) -> Result<()> {
        let previous = self.entries.get(&tuple).copied().unwrap_or_default();
        let next = previous.checked_add(weight).context("Z-set weight overflow")?;
        if next == 0 {
            self.entries.remove(&tuple);
        } else {
            self.entries.insert(tuple, next);
        }
        Ok(())
    }
}
