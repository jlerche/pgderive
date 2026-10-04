use super::{
    BatchCursor, BatchData, Entry, ObjectBatch, ObjectRef, bounded,
    format::{Block, Index, hash, validate},
};
use anyhow::{Context, Result, ensure};
use object_store::ObjectStore;
use std::sync::Arc;

/// Fixed encoded-byte and entry budgets for streaming immutable state output.
///
/// Total state size may exceed these budgets; complete canonical identities are
/// split into nonoverlapping immutable roots without changing their weights.
#[derive(Debug, Clone, Copy)]
pub struct WriteLimits {
    /// Maximum encoded block bytes, at most the v2 codec's 8 MiB limit.
    pub block_bytes: usize,
    /// Maximum complete rows per block, at most 65,536.
    pub block_rows: usize,
    /// Maximum root/index bytes, at most the v2 codec's 8 MiB limit.
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
    /// Stream a canonical cursor into bounded blocks and nonoverlapping v2 roots.
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
        ensure!(!schema.is_empty(), "empty streaming batch schema");
        let limits = limits.validate()?;
        let mut writer = Writer {
            store,
            schema: schema.into(),
            limits,
            rows: Vec::new(),
            row_bytes: 2,
            blocks: Vec::new(),
            fence_bytes: 0,
            references: Vec::new(),
            previous: None,
        };
        while let Some((key, value, weight)) = cursor.current() {
            writer.push(((key.clone(), value.clone()), weight)).await?;
            cursor.advance().await?;
        }
        writer.flush_block().await?;
        writer.flush_root().await?;
        Ok(writer.references)
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
        if self.rows.len() == self.limits.block_rows || next > self.limits.block_bytes {
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
        let digest = hash(&encoded);
        let path = format!("pgderive/block-v2/{digest}");
        let block = Block {
            path: Some(path.clone()),
            first: self.rows.first().context("empty streaming block")?.0.clone(),
            last: self.rows.last().context("empty streaming block")?.0.clone(),
            offset: 0,
            length: u64::try_from(encoded.len())?,
            rows: self.rows.len(),
            hash: digest,
        };
        let bytes = bounded::encode(&block)?.len();
        let header = bounded::encode(&Index::<K, V> {
            version: 2,
            schema: self.schema.clone(),
            blocks: Vec::new(),
        })?
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
        bounded::put(&self.store, &path, encoded).await?;
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
        let reference = ObjectBatch::<K, V>::upload_index(
            &self.store,
            &self.schema,
            std::mem::take(&mut self.blocks),
        )
        .await?;
        self.references.push(reference);
        self.fence_bytes = 0;
        Ok(())
    }
}

#[cfg(test)]
#[path = "tests/streaming.rs"]
mod tests;
