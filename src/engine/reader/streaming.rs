use super::{
    BatchCursor, BatchData, BlockCache, Entry, ObjectBatch, ObjectRef, bounded,
    format::{Block, Pack, TARGET_OBJECT_BYTES, hash, header_bytes, pack_bytes, validate},
};
use anyhow::{Context, Result, ensure};
use object_store::ObjectStore;
use std::sync::Arc;

// A point read decodes a complete block. Bound its ordinary JSON payload so cache
// misses do not deserialize thousands of unrelated wide rows. A single larger
// row remains valid up to the caller's hard block budget.
pub(super) const TARGET_BLOCK_BYTES: usize = 16 * 1024;

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
    header_bytes: usize,
    payload: Vec<u8>,
    packs: Vec<Pack>,
    pack_bytes: usize,
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
            header_bytes: header_bytes(schema)?,
            payload: Vec::new(),
            packs: Vec::new(),
            pack_bytes: pack_bytes()?,
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
            crate::engine::execution::CapacityExceeded("streaming row exceeds block byte limit")
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
        let decoded = bounded::encode(&self.rows)?;
        ensure!(decoded.len() <= self.limits.block_bytes, "streaming block exceeds budget");
        let (mut block, encoded) = super::format::encode_block::<(K, V), _>(&self.rows, &decoded)?;
        let first_fence = self.fence_charge(&block)?;
        let next =
            self.payload.len().checked_add(encoded.len()).context("packed object size overflow")?;
        let new_pack = !self.payload.is_empty() && next > TARGET_OBJECT_BYTES;
        block.pack = self.packs.len() + usize::from(new_pack);
        block.offset = if new_pack { 0 } else { u64::try_from(self.payload.len())? };
        let mut bytes = self.fence_charge(&block)?;
        let minimum = self
            .header_bytes
            .checked_add(self.pack_bytes)
            .and_then(|n| n.checked_add(first_fence))
            .context("streaming fence size overflow")?;
        if minimum > self.limits.index_bytes {
            let mut single = block.clone();
            single.pack = 0;
            single.offset = 0;
            let digest = "0".repeat(64);
            let pack = Pack {
                path: format!("pgderive/pack/{digest}"),
                bytes: u64::try_from(bounded::MAX_BYTES)?,
                hash: digest,
            };
            let (root, packed) =
                super::index::build(&self.schema, self.limits.index_bytes, &[pack], &[single])?;
            ensure!(
                bounded::encode(&root)?.len() <= self.limits.index_bytes
                    && packed.len() <= self.limits.index_bytes,
                crate::engine::execution::CapacityExceeded("streaming fence exceeds index budget")
            );
        }
        if !self.blocks.is_empty()
            && (self.blocks.len() == self.limits.index_blocks
                || self.index_size(bytes, block.pack + 1)? > self.limits.index_bytes)
        {
            self.flush_root().await?;
            block.pack = 0;
            block.offset = 0;
            bytes = first_fence;
        } else if new_pack {
            self.flush_pack().await?;
        }
        self.payload.extend_from_slice(&encoded);
        // These immutable local computations cannot publish membership. Keeping
        // them in the bounded cache avoids pinning every page until the pack PUT.
        // Neither a reference nor durable progress escapes before both PUTs.
        if let Some(cache) = &self.cache
            && cache.retains_output()?
        {
            cache.insert(
                &self.schema,
                block.hash.clone(),
                Arc::new(self.rows.clone()),
                decoded.len(),
            )?;
        }
        self.fence_bytes =
            self.fence_bytes.checked_add(bytes).context("streaming fence bytes overflow")?;
        self.blocks.push(block);
        self.rows.clear();
        self.row_bytes = 2;
        Ok(())
    }
    fn fence_charge(&self, block: &Block<(K, V)>) -> Result<usize> {
        // Leaf payload plus repeated boundary fences through the bounded tree.
        bounded::encode(block)?
            .len()
            .checked_mul(3)
            .and_then(|bytes| bytes.checked_add(self.pack_bytes))
            .and_then(|bytes| bytes.checked_add(128))
            .context("index fence charge overflow")
    }
    fn index_size(&self, fence: usize, packs: usize) -> Result<usize> {
        packs
            .checked_mul(self.pack_bytes + 1)
            .and_then(|n| n.checked_add(self.header_bytes))
            .and_then(|n| n.checked_add(self.fence_bytes))
            .and_then(|n| n.checked_add(fence))
            .and_then(|n| n.checked_add(self.blocks.len()))
            .context("streaming index size overflow")
    }
    async fn flush_pack(&mut self) -> Result<()> {
        if self.payload.is_empty() {
            return Ok(());
        }
        let bytes = std::mem::take(&mut self.payload);
        let digest = hash(&bytes);
        let pack = Pack {
            path: format!("pgderive/pack/{digest}"),
            bytes: u64::try_from(bytes.len())?,
            hash: digest,
        };
        bounded::put(&self.store, &pack.path, bytes).await?;
        self.packs.push(pack);
        Ok(())
    }
    async fn flush_root(&mut self) -> Result<()> {
        if self.blocks.is_empty() && !self.references.is_empty() {
            return Ok(());
        }
        ensure!(
            self.references.len() < self.limits.output_roots,
            crate::engine::execution::CapacityExceeded("streaming output root limit exceeded")
        );
        self.flush_pack().await?;
        let packs = std::mem::take(&mut self.packs);
        let blocks = std::mem::take(&mut self.blocks);
        let (reference, mut index, head) = ObjectBatch::<K, V>::upload_index(
            &self.store,
            &self.schema,
            (packs.clone(), blocks.clone()),
            self.limits.index_bytes,
            self.cache.as_deref(),
        )
        .await?;
        self.references.push(reference.clone());
        if let Some(cache) = &self.cache {
            if index.pack.is_some()
                && let (Some(address), Some(head)) = (&index.node, &head)
            {
                cache.index.insert(
                    &address.hash,
                    address.length,
                    head.clone(),
                    bounded::encode(head.node())?.len(),
                )?;
            }
            let head = if index.pack.is_none() { head } else { None };
            index.inline = None;
            self.resident.push(ObjectBatch {
                store: self.store.clone(),
                reference,
                rows: super::format::row_count(&blocks)?,
                max_weight: blocks.iter().map(|block| block.max_weight).max().unwrap_or(0),
                index: Arc::new(index),
                head,
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
