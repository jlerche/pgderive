use super::{
    BatchCursor, BatchData, BlockCache, Entry, ObjectBatch, ObjectRef, bounded,
    format::{Block, Index, validate},
};
use anyhow::{Context, Result, ensure};
use object_store::ObjectStore;
use std::sync::Arc;

// A point read decodes a complete block. Bound its ordinary JSON payload so cache
// misses do not deserialize thousands of unrelated wide rows. A single larger
// row remains valid up to the caller's hard block budget.
pub(super) const TARGET_BLOCK_BYTES: usize = 128 * 1024;

/// Fixed encoded-byte and entry budgets for streaming immutable state output.
///
/// Total state size may exceed these budgets; complete canonical identities are
/// split into nonoverlapping immutable roots without changing their weights.
#[derive(Debug, Clone, Copy)]
pub struct WriteLimits {
    /// Maximum encoded block bytes, at most the codec's decoded 8 MiB limit.
    pub block_bytes: usize,
    /// Maximum complete rows per block, at most 65,536.
    pub block_rows: usize,
    /// Maximum root/index bytes, at most the codec's decoded 8 MiB limit.
    pub index_bytes: usize,
    /// Maximum resident block fences before flushing a root.
    pub index_blocks: usize,
    /// Maximum coarse output roots retained in memory for atomic publication.
    pub output_roots: usize,
}
impl Default for WriteLimits {
    fn default() -> Self {
        Self {
            block_bytes: bounded::MAX_BYTES,
            block_rows: 4096,
            index_bytes: bounded::MAX_BYTES,
            index_blocks: 4096,
            output_roots: 128,
        }
    }
}
impl WriteLimits {
    fn validate(self) -> Result<Self> {
        ensure!(
            self.block_bytes > 2
                && self.block_bytes <= bounded::MAX_BYTES
                && self.block_rows > 0
                && self.block_rows <= 65_536
                && self.index_bytes > 2
                && self.index_bytes <= bounded::MAX_BYTES
                && self.index_blocks > 0
                && self.output_roots > 0,
            "invalid streaming write limits"
        );
        Ok(self)
    }
}
struct Writer<K: BatchData, V: BatchData> {
    store: Arc<dyn ObjectStore>,
    schema: String,
    limits: WriteLimits,
    rows: Vec<Entry<K, V>>,
    row_bytes: usize,
    blocks: Vec<Block<(K, V)>>,
    fence_bytes: usize,
    references: Vec<ObjectRef>,
    resident: Vec<ObjectBatch<K, V>>,
    cache: Option<Arc<BlockCache>>,
    previous: Option<(K, V)>,
}
impl<K: BatchData, V: BatchData> ObjectBatch<K, V> {
    /// Write into a unique upload namespace while returning globally readable refs.
    /// A namespace must never be reused after its durable upload reservation closes.
    ///
    /// # Errors
    /// Rejects invalid namespaces, noncanonical input, limits or object I/O failures.
    pub async fn write_stream_namespaced(
        store: Arc<dyn ObjectStore>,
        cursor: &mut impl BatchCursor<Key = K, Val = V>,
        schema: &str,
        target: (&str, WriteLimits),
    ) -> Result<Vec<ObjectRef>> {
        ensure!(super::object::valid_namespace(target.0), "invalid streaming upload namespace");
        let scoped: Arc<dyn ObjectStore> =
            Arc::new(object_store::prefix::PrefixStore::new(store, target.0));
        Self::write_stream(scoped, cursor, schema, target.1)
            .await?
            .into_iter()
            .map(|reference| reference.qualify(target.0))
            .collect()
    }
    /// Stream a canonical cursor into bounded blocks and nonoverlapping roots.
    /// No trace membership is published here. Exact consolidation and final i64
    /// representability belong to the logical input cursor, before physical splits.
    ///
    /// # Errors
    /// Rejects noncanonical rows, oversized individual rows/fences or GET/PUT failures.
    /// Any partial uploads remain unpublished; retry from a fresh pinned cursor.
    pub async fn write_stream(
        store: Arc<dyn ObjectStore>,
        cursor: &mut impl BatchCursor<Key = K, Val = V>,
        schema: &str,
        limits: WriteLimits,
    ) -> Result<Vec<ObjectRef>> {
        Ok(Self::stream_upload(store, cursor, schema, (None, limits), None).await?.0)
    }
    pub(crate) async fn write_stream_resident(
        store: Arc<dyn ObjectStore>,
        cursor: &mut impl BatchCursor<Key = K, Val = V>,
        schema: &str,
        target: (Option<&str>, WriteLimits),
        cache: Arc<BlockCache>,
    ) -> Result<Vec<Self>> {
        Ok(Self::stream_upload(store, cursor, schema, target, Some(cache)).await?.1)
    }
    async fn stream_upload(
        store: Arc<dyn ObjectStore>,
        cursor: &mut impl BatchCursor<Key = K, Val = V>,
        schema: &str,
        target: (Option<&str>, WriteLimits),
        cache: Option<Arc<BlockCache>>,
    ) -> Result<(Vec<ObjectRef>, Vec<Self>)> {
        let (namespace, limits) = target;
        if let Some(namespace) = namespace {
            ensure!(
                super::object::valid_namespace(namespace),
                "invalid streaming upload namespace"
            );
        }
        let scoped = super::object::scoped_store(&store, namespace);
        ensure!(!schema.is_empty(), "empty streaming batch schema");
        let limits = limits.validate()?;
        let mut writer = Writer {
            store: scoped,
            cache,
            schema: schema.into(),
            limits,
            rows: Vec::new(),
            row_bytes: 2,
            blocks: Vec::new(),
            fence_bytes: 0,
            references: Vec::new(),
            resident: Vec::new(),
            previous: None,
        };
        while cursor.current().is_some() {
            if let Some(span) = cursor.current_span() {
                ensure!(!span.is_empty(), "empty span at current tuple");
                for row in span {
                    writer.push(row.clone()).await?;
                }
                cursor.advance_span().await?;
            } else if let Some((key, value, weight)) = cursor.current() {
                writer.push(((key.clone(), value.clone()), weight)).await?;
                cursor.advance().await?;
            }
        }
        writer.flush_block().await?;
        writer.flush_root().await?;
        for batch in &mut writer.resident {
            if let Some(namespace) = namespace {
                batch.reference = batch.reference.clone().qualify(namespace)?;
                for block in Arc::make_mut(&mut batch.blocks) {
                    block.path = batch.reference.child_path(&block.path);
                }
                batch.store = store.clone();
            }
        }
        Ok((writer.references, writer.resident))
    }
}
impl<K: BatchData, V: BatchData> Writer<K, V> {
    async fn push(&mut self, row: Entry<K, V>) -> Result<()> {
        ensure!(
            row.1 != 0 && self.previous.as_ref().is_none_or(|previous| previous < &row.0),
            "noncanonical streaming input"
        );
        let bytes = bounded::encode(&row)?.len();
        ensure!(
            bytes.checked_add(2).is_some_and(|bytes| bytes <= self.limits.block_bytes),
            "streaming row exceeds block byte limit"
        );
        let comma = usize::from(!self.rows.is_empty());
        let next = self
            .row_bytes
            .checked_add(bytes)
            .and_then(|bytes| bytes.checked_add(comma))
            .context("streaming block size overflow")?;
        if self.rows.len() == self.limits.block_rows
            || (!self.rows.is_empty() && next > self.limits.block_bytes.min(TARGET_BLOCK_BYTES))
        {
            self.flush_block().await?;
        }
        self.row_bytes = self
            .row_bytes
            .checked_add(bytes)
            .and_then(|bytes| bytes.checked_add(usize::from(!self.rows.is_empty())))
            .context("streaming block size overflow")?;
        self.previous = Some(row.0.clone());
        self.rows.push(row);
        Ok(())
    }
    async fn flush_block(&mut self) -> Result<()> {
        if self.rows.is_empty() {
            return Ok(());
        }
        validate(&self.rows)?;
        let encoded = bounded::encode(&self.rows)?;
        ensure!(encoded.len() <= self.limits.block_bytes, "streaming block exceeds budget");
        let encoded_bytes = encoded.len();
        let (block, encoded) = super::format::encode_block::<(K, V), _>(&self.rows, &encoded)?;
        let bytes = bounded::encode(&block)?.len();
        let header =
            bounded::encode(&Index::<K, V> { schema: self.schema.clone(), blocks: Vec::new() })?
                .len();
        ensure!(
            header.checked_add(bytes).is_some_and(|bytes| bytes <= self.limits.index_bytes),
            "streaming fence exceeds index budget"
        );
        let total = header
            .checked_add(self.fence_bytes)
            .and_then(|total| total.checked_add(bytes))
            .and_then(|total| total.checked_add(self.blocks.len()))
            .context("streaming index size overflow")?;
        if self.blocks.len() == self.limits.index_blocks || total > self.limits.index_bytes {
            self.flush_root().await?;
        }
        bounded::put(&self.store, &block.path, encoded).await?;
        if let Some(cache) = &self.cache {
            cache.insert(
                &self.schema,
                block.hash.clone(),
                Arc::new(self.rows.clone()),
                encoded_bytes,
            )?;
        }
        self.fence_bytes =
            self.fence_bytes.checked_add(bytes).context("streaming fence bytes overflow")?;
        self.blocks.push(block);
        self.rows.clear();
        self.row_bytes = 2;
        Ok(())
    }
    async fn flush_root(&mut self) -> Result<()> {
        if self.blocks.is_empty() && !self.references.is_empty() {
            return Ok(());
        }
        ensure!(
            self.references.len() < self.limits.output_roots,
            "streaming output root limit exceeded"
        );
        let blocks = std::mem::take(&mut self.blocks);
        let reference =
            ObjectBatch::<K, V>::upload_index(&self.store, &self.schema, blocks.clone()).await?;
        self.references.push(reference.clone());
        if let Some(cache) = &self.cache {
            self.resident.push(ObjectBatch {
                store: self.store.clone(),
                reference,
                rows: super::format::row_count(&blocks)?,
                blocks: Arc::new(blocks),
                cache: cache.clone(),
            });
        }
        self.fence_bytes = 0;
        Ok(())
    }
}

#[cfg(test)]
#[path = "tests/streaming.rs"]
mod tests;
