//! Install an uploaded immutable prefix without losing subsequently appended runs.
use super::{Run, TraceSnapshot};
use crate::engine::reader::BatchData;
use anyhow::{Context, Result, ensure};
use std::sync::Arc;

impl<K: BatchData, V: BatchData> TraceSnapshot<K, V> {
    pub(crate) fn rebase_flushed(&self, captured: &Self, flushed: &Self) -> Result<Self> {
        ensure!(
            captured.time <= self.time && flushed.time == captured.time && !flushed.has_memory(),
            "checkpoint prefix clock or storage mismatch"
        );
        let ticks = self.time.checked_sub(captured.time).context("checkpoint time underflow")?;
        ensure!(
            self.cohorts == captured.cohorts
                && captured.generation.checked_add(ticks) == Some(self.generation)
                && self.runs.len() >= captured.runs.len()
                && self.valid_suffix(captured),
            "checkpoint prefix is not a contiguous memory ancestor"
        );
        ensure!(
            self.runs.iter().zip(&captured.runs).all(|(current, prior)| same_run(current, prior)),
            "checkpoint prefix membership changed"
        );
        let mut runs = flushed.runs.clone();
        runs.extend_from_slice(&self.runs[captured.runs.len()..]);
        ensure!(runs.len() <= super::MAX_RUNS, "rebased checkpoint run limit exceeded");
        let generation = self.generation.checked_add(1).context("trace generation overflow")?;
        let mut rebased = Self::from_runs(runs, generation, self.time)?;
        rebased.cohorts.clone_from(&flushed.cohorts);
        rebased.spans[flushed.runs.len()..].copy_from_slice(&self.spans[captured.runs.len()..]);
        rebased.sealed_time = captured.time;
        rebased.logical_cache.clone_from(&self.logical_cache);
        Ok(rebased)
    }
}

fn same_run<K: BatchData, V: BatchData>(left: &Run<K, V>, right: &Run<K, V>) -> bool {
    match (left, right) {
        (Run::Memory(left), Run::Memory(right)) => Arc::ptr_eq(left, right),
        (Run::Object(left), Run::Object(right)) => left.reference() == right.reference(),
        _ => false,
    }
}

#[cfg(test)]
#[path = "tests/rebase.rs"]
mod tests;
