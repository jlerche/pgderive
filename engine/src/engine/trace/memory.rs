//! Binary-carry immutable memory runs, fenced at captured checkpoint boundaries.
use super::{Run, TraceSnapshot, memory_run_charge};
use crate::engine::reader::BatchData;
use anyhow::{Context, Result};
use std::sync::Arc;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) struct Span {
    pub(super) first: u64,
    pub(super) last: u64,
}
impl Span {
    pub(super) const fn new(time: u64) -> Self {
        Self { first: time, last: time }
    }
    const fn bounded_by(self, time: u64) -> bool {
        self.first <= self.last && self.last <= time
    }
    fn follows(self, prior: Self) -> bool {
        prior.last.checked_add(1) == Some(self.first)
    }
    const fn width(self) -> u64 {
        // Constructed only by extending ordered, nonempty spans.
        self.last - self.first + 1
    }
}
impl<K: BatchData, V: BatchData> TraceSnapshot<K, V> {
    pub(crate) fn unsealed_run_count(&self) -> usize {
        self.spans.iter().flatten().filter(|span| span.first > self.sealed_time).count()
    }
    pub(crate) fn seal_memory(&self) -> Self {
        let mut sealed = self.clone();
        sealed.sealed_time = sealed.time;
        sealed
    }
    pub(super) fn consolidate_memory(&mut self) -> Result<()> {
        while self.merge_memory_tail()? {}
        Ok(())
    }
    fn merge_memory_tail(&mut self) -> Result<bool> {
        let Some(first) = self.runs.len().checked_sub(2) else {
            return Ok(false);
        };
        let (Some(left_span), Some(right_span)) = (self.spans[first], self.spans[first + 1]) else {
            return Ok(false);
        };
        if left_span.first <= self.sealed_time
            || !right_span.follows(left_span)
            || left_span.width() != right_span.width()
        {
            return Ok(false);
        }
        let (Run::Memory(left), Run::Memory(right)) = (&self.runs[first], &self.runs[first + 1])
        else {
            return Ok(false);
        };
        let mut merged = left.as_ref().clone();
        if merged.apply(right).is_err() {
            // A memory subset may exceed i64 while the complete trace remains
            // representable through cancellation with retained objects. Keep
            // both exact runs; run/byte pressure can require checkpointing.
            return Ok(false);
        }
        let merged = Run::Memory(Arc::new(merged));
        let removed = memory_run_charge(&self.runs[first])?
            .checked_add(memory_run_charge(&self.runs[first + 1])?)
            .context("memtable charge overflow")?;
        let memory_bytes = self
            .memory_bytes
            .checked_sub(removed)
            .context("memtable charge underflow")?
            .checked_add(memory_run_charge(&merged)?)
            .context("memtable charge overflow")?;
        self.runs.truncate(first);
        self.spans.truncate(first);
        self.runs.push(merged);
        // Even a zero-net batch owns its complete logical interval.
        self.spans.push(Some(Span { first: left_span.first, last: right_span.last }));
        self.memory_bytes = memory_bytes;
        Ok(true)
    }
    pub(super) fn valid_suffix(&self, captured: &Self) -> bool {
        let mut previous = captured.time;
        for (run, span) in self.runs.iter().zip(&self.spans).skip(captured.runs.len()) {
            let Some(span) = span else { return false };
            if !matches!(run, Run::Memory(_))
                || previous.checked_add(1) != Some(span.first)
                || !span.bounded_by(self.time)
            {
                return false;
            }
            previous = span.last;
        }
        previous == self.time
    }
}

#[cfg(test)]
#[path = "tests/memory.rs"]
mod tests;
