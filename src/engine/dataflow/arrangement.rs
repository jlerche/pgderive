use super::Stream;
use crate::engine::{
    Batch,
    reader::{BatchData, BatchReader, BlockCache, ObjectBatch, WriteLimits},
    trace::{Manifest, Run, TraceSnapshot},
};
use anyhow::{Result, ensure};
use object_store::ObjectStore;
use std::sync::Arc;

/// Bound immutable object writer for one typed arrangement in an acyclic graph.
#[derive(Clone)]
pub struct Arrangement<K: BatchData, V: BatchData> {
    store: Arc<dyn ObjectStore>,
    schema: String,
    namespace: Option<String>,
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
            namespace: None,
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
    pub(crate) fn with_namespace(mut self, namespace: &str) -> Self {
        self.namespace = Some(namespace.into());
        self
    }
    /// Create initial immutable state at logical time zero.
    #[must_use]
    pub const fn empty(&self) -> TraceSnapshot<K, V> {
        TraceSnapshot::empty()
    }
    /// Cold-validate and reopen complete recorded object membership.
    ///
    /// # Errors
    /// Returns manifest/schema, missing/corrupt object, or arithmetic failures.
    pub async fn reopen(&self, manifest: Manifest) -> Result<TraceSnapshot<K, V>> {
        TraceSnapshot::reopen(self.store.clone(), manifest, &self.schema, self.cache.clone()).await
    }
    async fn upload(&self, batch: &Batch<K, V>) -> Result<Run<K, V>> {
        let reference = match &self.namespace {
            Some(namespace) => {
                ObjectBatch::write_namespaced(
                    self.store.clone(),
                    batch,
                    &self.schema,
                    (namespace, self.block_rows),
                )
                .await?
            }
            None => {
                ObjectBatch::write(self.store.clone(), batch, &self.schema, self.block_rows).await?
            }
        };
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
        ensure!(prior.run_count() < 128, "arrangement run limit requires compaction");
        prior.validate_delta(&delta.batch).await?;
        prior.append_validated(self.upload(&delta.batch).await?, delta.time)
    }
    /// Upload equivalent consolidated state without advancing logical time.
    ///
    /// # Errors
    /// Returns read, upload or equivalence validation errors.
    pub async fn compact(&self, prior: &TraceSnapshot<K, V>) -> Result<TraceSnapshot<K, V>> {
        ensure!(prior.run_count() <= 128, "compaction input run limit exceeded");
        let mut cursor = prior.cursor().await?;
        let limits =
            WriteLimits { block_rows: self.block_rows.min(65_536), ..WriteLimits::default() };
        let references = match &self.namespace {
            Some(namespace) => {
                ObjectBatch::<K, V>::write_stream_namespaced(
                    self.store.clone(),
                    &mut cursor,
                    &self.schema,
                    (namespace, limits),
                )
                .await?
            }
            None => {
                ObjectBatch::<K, V>::write_stream(
                    self.store.clone(),
                    &mut cursor,
                    &self.schema,
                    limits,
                )
                .await?
            }
        };
        let mut runs = Vec::new();
        for reference in references {
            runs.push(Run::Object(
                ObjectBatch::open_with_cache(
                    self.store.clone(),
                    reference,
                    &self.schema,
                    self.cache.clone(),
                )
                .await?,
            ));
        }
        prior.replace_runs(runs).await
    }
}
