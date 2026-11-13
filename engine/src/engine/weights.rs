use anyhow::{Context, Result};
use num_bigint::BigInt;
use std::collections::{BTreeMap, btree_map::Entry};

// Exact intermediates. Narrow only at a declared logical collection boundary.
#[derive(Debug, Clone)]
pub(super) struct Accumulator<T: Ord> {
    entries: BTreeMap<T, BigInt>,
}

impl<T: Ord> Default for Accumulator<T> {
    fn default() -> Self {
        Self { entries: BTreeMap::new() }
    }
}

impl<T: Ord> Accumulator<T> {
    pub(super) fn add(&mut self, tuple: T, weight: impl Into<BigInt>) {
        let weight = weight.into();
        match self.entries.entry(tuple) {
            Entry::Vacant(entry) => {
                entry.insert(weight);
            }
            Entry::Occupied(mut entry) => {
                *entry.get_mut() = std::mem::take(entry.get_mut()) + weight;
            }
        }
    }

    pub(super) fn merge(&mut self, other: Self) {
        for (tuple, weight) in other.entries {
            self.add(tuple, weight);
        }
    }

    pub(super) fn finish(self) -> Result<Vec<(T, i64)>> {
        self.entries
            .into_iter()
            .filter(|(_, weight)| *weight != BigInt::default())
            .map(|(tuple, weight)| {
                let weight =
                    i64::try_from(weight).context("finalized weight exceeds i64 domain")?;
                Ok((tuple, weight))
            })
            .collect()
    }
}
