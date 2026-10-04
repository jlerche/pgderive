use super::Stream;
use crate::engine::{
    Batch,
    reader::{BatchData, ObjectBatch},
    trace::{Run, TraceSnapshot},
};
use anyhow::{Result, ensure};
use object_store::ObjectStore;
use std::sync::Arc;

/// Bound immutable object writer for one typed arrangement in an acyclic graph.
pub struct Arrangement<K: BatchData, V: BatchData> {
    store: Arc<dyn ObjectStore>,
    schema: String,
    block_rows: usize,
    marker: std::marker::PhantomData<(K, V)>,
}
impl<K: BatchData, V: BatchData> Arrangement<K, V> {
    /// Bind storage and exact codec/type/ordering identity for this arrangement.
    ///
    /// # Errors
    /// Rejects an empty schema or zero block size.
    pub fn new(store: Arc<dyn ObjectStore>, schema: String, block_rows: usize) -> Result<Self> {
        ensure!(!schema.is_empty() && block_rows > 0, "invalid arrangement configuration");
        Ok(Self { store, schema, block_rows, marker: std::marker::PhantomData })
    }
    /// Create initial immutable state at logical time zero.
    #[must_use]
    pub const fn empty(&self) -> TraceSnapshot<K, V> {
        TraceSnapshot::empty()
    }
    async fn upload(&self, batch: &Batch<K, V>) -> Result<Run<K, V>> {
        let reference =
            ObjectBatch::write(self.store.clone(), batch, &self.schema, self.block_rows).await?;
        Ok(Run::Object(ObjectBatch::open(self.store.clone(), reference, &self.schema).await?))
    }
    /// Upload a complete delta and validate an unpublished next snapshot.
    ///
    /// # Errors
    /// Returns upload/read, tick or final state overflow errors.
    pub async fn stage(
        &self,
        prior: &TraceSnapshot<K, V>,
        delta: &Stream<Batch<K, V>>,
    ) -> Result<TraceSnapshot<K, V>> {
        prior.append(self.upload(&delta.batch).await?, delta.time).await
    }
    /// Upload equivalent consolidated state without advancing logical time.
    ///
    /// # Errors
    /// Returns read, upload or equivalence validation errors.
    pub async fn compact(&self, prior: &TraceSnapshot<K, V>) -> Result<TraceSnapshot<K, V>> {
        prior.replace(self.upload(&prior.materialize().await?).await?).await
    }
}
