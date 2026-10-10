//! Pure bounded relational expansion over weighted full tuples.
use super::TimedBatch;
use crate::engine::{
    Batch,
    execution::{Consolidator, Limits},
    reader::BatchData,
};
use anyhow::Result;
use std::sync::Arc;
type Emit<'a, A, B> = dyn FnMut((A, B)) -> Result<()> + 'a;
type Mapper<K, V, A, B> = dyn for<'a> Fn(&K, &V, &mut Emit<'a, A, B>) -> Result<()> + Send + Sync;
/// Linear flat-map: every emitted identity carries the complete input weight.
/// Callbacks emit lazily; consolidation enforces per-tick fanout/spill budgets.
pub struct Expand<K: BatchData, V: BatchData, A: BatchData, B: BatchData> {
    map: Arc<Mapper<K, V, A, B>>,
}
impl<K: BatchData, V: BatchData, A: BatchData, B: BatchData> Expand<K, V, A, B> {
    /// Bind a deterministic pure expansion whose semantics bind the plan revision.
    pub fn new(
        map: impl for<'a> Fn(&K, &V, &mut Emit<'a, A, B>) -> Result<()> + Send + Sync + 'static,
    ) -> Self {
        Self { map: Arc::new(map) }
    }
    /// Preserve signed multiplicities and consolidate all expansion collisions.
    ///
    /// # Errors
    /// Returns callback, resource, scratch I/O or finalized coefficient errors.
    pub fn evaluate(
        &self,
        input: &TimedBatch<Batch<K, V>>,
        limits: Limits,
    ) -> Result<TimedBatch<Batch<A, B>>> {
        limits.check_batch(&input.batch)?;
        let mut output = Consolidator::new(limits)?;
        for ((key, value), weight) in input.batch.iter() {
            (self.map)(key, value, &mut |tuple| output.add(tuple, *weight))?;
        }
        Ok(TimedBatch { time: input.time, batch: output.finish_batch()? })
    }
}
