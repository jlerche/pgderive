use super::Stream;
use crate::engine::{
    Batch,
    reader::{BatchData, BlockCache, ObjectBatch},
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
    cache: Arc<BlockCache>,
    marker: std::marker::PhantomData<(K, V)>,
}
impl<K: BatchData, V: BatchData> Arrangement<K, V> {
    /// Bind storage and exact codec/type/ordering identity for this arrangement.
    ///
    /// # Errors
    /// Rejects an empty schema or zero block size.
    pub fn new(store: Arc<dyn ObjectStore>, schema: String, block_rows: usize) -> Result<Self> {
        ensure!(!schema.is_empty() && block_rows > 0, "invalid arrangement configuration");
        Ok(Self {
            store,
            schema,
            block_rows,
            cache: Arc::new(BlockCache::new(0, 0)),
            marker: std::marker::PhantomData,
        })
    }
    /// Share a bounded block cache with other arrangements in the same query.
    #[must_use]
    pub fn with_cache(mut self, cache: Arc<BlockCache>) -> Self {
        self.cache = cache;
        self
    }
    /// Create initial immutable state at logical time zero.
    #[must_use]
    pub const fn empty(&self) -> TraceSnapshot<K, V> {
        TraceSnapshot::empty()
    }
    async fn upload(&self, batch: &Batch<K, V>) -> Result<Run<K, V>> {
        let reference =
            ObjectBatch::write(self.store.clone(), batch, &self.schema, self.block_rows).await?;
        Ok(Run::Object(
            ObjectBatch::open_with_cache(
                self.store.clone(),
                reference,
                &self.schema,
                self.cache.clone(),
            )
            .await?,
        ))
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
        prior.validate_delta(&delta.batch).await?;
        prior.append_validated(self.upload(&delta.batch).await?, delta.time)
    }
    /// Upload equivalent consolidated state without advancing logical time.
    ///
    /// # Errors
    /// Returns read, upload or equivalence validation errors.
    pub async fn compact(&self, prior: &TraceSnapshot<K, V>) -> Result<TraceSnapshot<K, V>> {
        prior.replace(self.upload(&prior.materialize().await?).await?).await
    }
}
