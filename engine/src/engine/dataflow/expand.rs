//! Pure bounded relational expansion over weighted full tuples.
use super::TimedBatch;
use crate::engine::{
    Batch,
    execution::{Consolidator, Limits, Ordered, SealedCursor},
    reader::BatchData,
};
use anyhow::Result;
use std::{borrow::Borrow, sync::Arc};
type Emit<'a, A, B> = dyn FnMut((A, B)) -> Result<()> + 'a;
type Mapper<K, V, A, B> = dyn for<'a> Fn(&K, &V, &mut Emit<'a, A, B>) -> Result<()> + Send + Sync;
/// Linear flat-map: every emitted identity carries the complete input weight.
/// Callbacks emit lazily; consolidation enforces per-tick fanout/spill budgets.
#[derive(Clone)]
pub struct Expand<K: BatchData, V: BatchData, A: BatchData, B: BatchData> {
    map: Arc<Mapper<K, V, A, B>>,
    ordered: bool,
}
impl<K: BatchData, V: BatchData, A: BatchData, B: BatchData> Expand<K, V, A, B> {
    /// Bind a deterministic pure expansion whose semantics bind the plan revision.
    pub fn new(
        map: impl for<'a> Fn(&K, &V, &mut Emit<'a, A, B>) -> Result<()> + Send + Sync + 'static,
    ) -> Self {
        Self { map: Arc::new(map), ordered: false }
    }
    /// Preserve signed multiplicities and consolidate all expansion collisions.
    ///
    /// # Errors
    /// Returns callback, resource, scratch I/O or finalized coefficient errors.
    pub fn evaluate<I: Borrow<Batch<K, V>>>(
        &self,
        input: &TimedBatch<I>,
        limits: Limits,
    ) -> Result<TimedBatch<Batch<A, B>>> {
        limits.check_batch(input.batch.borrow())?;
        if self.ordered {
            return self.evaluate_ordered(input, Ordered::new(limits)?);
        }
        let mut output = Consolidator::new(limits)?;
        for ((key, value), weight) in input.batch.borrow().iter() {
            (self.map)(key, value, &mut |tuple| output.add(tuple, *weight))?;
        }
        Ok(TimedBatch { time: input.time, batch: output.finish_batch()? })
    }
    fn evaluate_ordered<I: Borrow<Batch<K, V>>>(
        &self,
        input: &TimedBatch<I>,
        mut output: Ordered<A, B>,
    ) -> Result<TimedBatch<Batch<A, B>>> {
        for ((key, value), weight) in input.batch.borrow().iter() {
            if let Err(error) = (self.map)(key, value, &mut |tuple| output.add(tuple, *weight)) {
                return Err(output.failure(error));
            }
        }
        Ok(TimedBatch { time: input.time, batch: output.finish_batch()? })
    }
}

impl<K: BatchData, V: BatchData, A: BatchData, B: BatchData> Expand<K, V, A, B> {
    pub(crate) async fn evaluate_sealed(
        &self,
        input: &crate::engine::execution::Sealed<K, V>,
        limits: Limits,
    ) -> Result<crate::engine::execution::Sealed<A, B>> {
        self.evaluate_sealed_counted(input, limits, &mut 0).await
    }
    pub(crate) async fn evaluate_sealed_counted(
        &self,
        input: &crate::engine::execution::Sealed<K, V>,
        limits: Limits,
        contributions: &mut usize,
    ) -> Result<crate::engine::execution::Sealed<A, B>> {
        use crate::engine::reader::{BatchCursor as _, BatchReader as _};
        let mut cursor = input.cursor().await?;
        if self.ordered {
            return self
                .seal_ordered(cursor, Ordered::new(limits)?, limits.contributions, contributions)
                .await;
        }
        let mut output = Consolidator::new(limits)?;
        while let Some((key, value, weight)) = cursor.current() {
            (self.map)(key, value, &mut |tuple| {
                crate::engine::execution::charge_contribution(contributions, limits.contributions)?;
                output.add(tuple, weight)
            })?;
            cursor.advance().await?;
        }
        output.seal()
    }
    async fn seal_ordered(
        &self,
        mut cursor: SealedCursor<K, V>,
        mut output: Ordered<A, B>,
        limit: usize,
        contributions: &mut usize,
    ) -> Result<crate::engine::execution::Sealed<A, B>> {
        use crate::engine::reader::BatchCursor as _;
        while let Some((key, value, weight)) = cursor.current() {
            let mapped = (self.map)(key, value, &mut |tuple| {
                crate::engine::execution::charge_contribution(contributions, limit)?;
                output.add(tuple, weight)
            });
            if let Err(error) = mapped {
                return Err(output.failure(error));
            }
            if let Err(error) = cursor.advance().await {
                return Err(output.failure(error));
            }
        }
        output.seal()
    }
}

impl<Key: BatchData, Value: BatchData> Expand<Key, Value, Key, Value> {
    pub(super) fn filter(
        predicate: impl Fn(&Key, &Value) -> Result<bool> + Send + Sync + 'static,
    ) -> Self {
        Self {
            map: Arc::new(move |key, value, emit| {
                if predicate(key, value)? {
                    emit((key.clone(), value.clone()))?;
                }
                Ok(())
            }),
            ordered: true,
        }
    }
}
