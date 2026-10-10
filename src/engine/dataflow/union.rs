//! Synchronized signed bag addition, with complete-tick consolidation.
use super::TimedBatch;
use crate::engine::{
    Batch,
    execution::{Consolidator, Limits},
    reader::BatchData,
};
use anyhow::Result;
use std::borrow::Borrow;

/// Stateless UNION ALL over two identically typed delta edges.
pub struct Union;
impl Union {
    /// Add both complete inputs without advancing logical time or narrowing weights.
    ///
    /// # Errors
    /// Returns resource, scratch I/O or finalized coefficient errors.
    pub fn evaluate<K: BatchData, V: BatchData, I: Borrow<Batch<K, V>>>(
        &self,
        input: &TimedBatch<(I, I)>,
        limits: Limits,
    ) -> Result<TimedBatch<Batch<K, V>>> {
        let mut output = Consolidator::new(limits)?;
        for batch in [input.batch.0.borrow(), input.batch.1.borrow()] {
            limits.check_batch(batch)?;
            for (tuple, weight) in batch.iter() {
                output.add(tuple.clone(), *weight)?;
            }
        }
        Ok(TimedBatch { time: input.time, batch: output.finish_batch()? })
    }
}
