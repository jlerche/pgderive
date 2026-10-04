use super::Stream;
use crate::engine::{Batch, BatchBuilder, reader::BatchData};
use anyhow::Result;
use std::sync::Arc;
type Mapper<K, V, A, B> = dyn Fn(&K, &V) -> Result<Option<(A, B)>> + Send + Sync;

/// Lifetime-bound pure projection/filter over a typed weighted edge.
pub struct Project<K: BatchData, V: BatchData, A: BatchData, B: BatchData> {
    map: Arc<Mapper<K, V, A, B>>,
}
impl<K: BatchData, V: BatchData, A: BatchData, B: BatchData> Project<K, V, A, B> {
    /// Bind deterministic semantics; None filters, Some projects full identity.
    pub fn new(map: impl Fn(&K, &V) -> Result<Option<(A, B)>> + Send + Sync + 'static) -> Self {
        Self { map: Arc::new(map) }
    }
    /// Preserve the tick and consolidate projected full-tuple collisions exactly.
    ///
    /// # Errors
    /// Returns callback errors or overflow of finalized projected coefficients.
    pub fn evaluate(&self, input: &Stream<Batch<K, V>>) -> Result<Stream<Batch<A, B>>> {
        let mut builder = BatchBuilder::default();
        for ((key, value), weight) in input.batch.iter() {
            if let Some((key, value)) = (self.map)(key, value)? {
                builder.push(key, value, *weight);
            }
        }
        Ok(Stream { time: input.time, batch: builder.finish()? })
    }
}
