use super::{ZSet, weights::Accumulator};
use anyhow::{Context, Result};
use num_bigint::BigInt;

/// Output of one successful transaction-ordered operator step.
#[derive(Debug)]
pub struct Step<K: Ord, L: Ord, R: Ord> {
    /// Monotonic logical tick, independent of source LSNs, starting at one.
    pub time: u64,
    /// Consolidated output changes for this tick.
    pub delta: ZSet<(K, L, R)>,
}

/// An equijoin over keyed full tuples, with signed multiplicities.
///
/// The key is navigation only: distinct values under one key remain distinct.
/// Both input deltas belong to the same transaction. This initial implementation
/// uses nested scans and cloned state; it is a semantic baseline, not a bounded
/// memory or indexed execution strategy.
#[derive(Debug, Clone)]
pub struct IncrementalJoin<K: Ord, L: Ord, R: Ord> {
    left: ZSet<(K, L)>,
    right: ZSet<(K, R)>,
    time: u64,
}

impl<K: Ord, L: Ord, R: Ord> Default for IncrementalJoin<K, L, R> {
    fn default() -> Self {
        Self { left: ZSet::default(), right: ZSet::default(), time: 0 }
    }
}

impl<K: Ord + Clone, L: Ord + Clone, R: Ord + Clone> IncrementalJoin<K, L, R> {
    /// Evaluate ΔL⋈R + L⋈ΔR + ΔL⋈ΔR, then atomically advance both inputs.
    /// Empty deltas still advance logical time.
    ///
    /// # Errors
    /// Returns weight or logical-time overflow; state and time remain unchanged.
    pub fn step(&mut self, left: &ZSet<(K, L)>, right: &ZSet<(K, R)>) -> Result<Step<K, L, R>> {
        let time = self.time.checked_add(1).context("logical time overflow")?;
        let mut delta = Accumulator::default();
        join_into(left, &self.right, &mut delta);
        join_into(&self.left, right, &mut delta);
        join_into(left, right, &mut delta);
        let delta = ZSet::from_accumulator(delta)?;
        let mut next_left = self.left.clone();
        let mut next_right = self.right.clone();
        next_left.apply(left)?;
        next_right.apply(right)?;
        self.left = next_left;
        self.right = next_right;
        self.time = time;
        Ok(Step { time, delta })
    }
}

fn join_into<K: Ord + Clone, L: Ord + Clone, R: Ord + Clone>(
    left: &ZSet<(K, L)>,
    right: &ZSet<(K, R)>,
    output: &mut Accumulator<(K, L, R)>,
) {
    for ((key, left), left_weight) in left.iter() {
        for ((right_key, right), right_weight) in right.iter() {
            if key != right_key {
                continue;
            }
            let weight = BigInt::from(*left_weight) * BigInt::from(*right_weight);
            output.add((key.clone(), left.clone(), right.clone()), weight);
        }
    }
}

#[cfg(test)]
mod tests;
