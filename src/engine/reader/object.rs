use super::{
    BatchData, BatchReader, Cursor, Entry,
    format::{Block, Index, hash, validate},
};
use crate::engine::Batch;
use anyhow::{Context, Result, ensure};
use object_store::{ObjectStore, ObjectStoreExt, PutMode, PutOptions, path::Path};
use serde::{Deserialize, Serialize};
use std::sync::Arc;

/// Coarse immutable-object reference suitable for a trusted membership catalog.
/// Key/value fences stay in the object's index, not in `PostgreSQL`.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ObjectRef {
    path: String,
    bytes: u64,
    index_offset: u64,
    index_length: u64,
    index_hash: String,
    schema: String,
}

impl ObjectRef {
    /// Content-addressed object path.
    #[must_use]
    pub fn path(&self) -> &str {
        &self.path
    }
}

type Fences<K, V> = Arc<Vec<Block<(K, V)>>>;

/// Open immutable JSON-v1 batch with resident object-local fences and lazy blocks.
#[derive(Debug, Clone)]
pub struct ObjectBatch<K: BatchData, V: BatchData> {
    pub(super) store: Arc<dyn ObjectStore>,
    pub(super) reference: ObjectRef,
    pub(super) blocks: Fences<K, V>,
}

impl<K: BatchData, V: BatchData> ObjectBatch<K, V> {
    /// Encode and PUT a canonical batch without publishing trace membership.
    /// Schema identity must include the key/value types and ordering semantics.
    ///
    /// # Errors
    /// Returns encoding, identity, arithmetic, or object-store errors. Failed or
    /// uncertain PUTs never publish membership; callers may retry the same bytes.
    pub async fn write(
        store: Arc<dyn ObjectStore>,
        batch: &Batch<K, V>,
        schema: &str,
        block_rows: usize,
    ) -> Result<ObjectRef> {
        ensure!(!schema.is_empty() && block_rows > 0, "invalid batch schema or block size");
        let rows = batch.iter().map(|(tuple, weight)| (tuple.clone(), *weight)).collect::<Vec<_>>();
        validate(&rows)?;
        let mut bytes = Vec::new();
        let mut blocks = Vec::new();
        for chunk in rows.chunks(block_rows) {
            let encoded = serde_json::to_vec(chunk)?;
            blocks.push(Block {
                first: chunk.first().context("empty chunk")?.0.clone(),
                last: chunk.last().context("empty chunk")?.0.clone(),
                offset: u64::try_from(bytes.len())?,
                length: u64::try_from(encoded.len())?,
                rows: chunk.len(),
                hash: hash(&encoded),
            });
            bytes.extend(encoded);
        }
        let index = serde_json::to_vec(&Index { version: 1, schema: schema.to_owned(), blocks })?;
        let index_offset = u64::try_from(bytes.len())?;
        bytes.extend(&index);
        let reference = ObjectRef {
            path: format!("pgderive/batch-v1/{}", hash(&bytes)),
            bytes: u64::try_from(bytes.len())?,
            index_offset,
            index_length: u64::try_from(index.len())?,
            index_hash: hash(&index),
            schema: schema.to_owned(),
        };
        let path = Path::from(reference.path.clone());
        let options = PutOptions { mode: PutMode::Create, ..PutOptions::default() };
        match store.put_opts(&path, bytes.clone().into(), options).await {
            Ok(_) => {}
            Err(object_store::Error::AlreadyExists { .. }) => {
                ensure!(
                    store.get(&path).await?.bytes().await?.as_ref() == bytes,
                    "immutable object collision or corruption"
                );
            }
            Err(error) => return Err(error.into()),
        }
        Ok(reference)
    }

    /// Reopen from a trusted catalog reference, validating the index and schema.
    /// Per-block integrity is checked lazily before exposing rows.
    ///
    /// # Errors
    /// Returns missing-object, invalid schema/index, or object-store errors.
    pub async fn open(
        store: Arc<dyn ObjectStore>,
        reference: ObjectRef,
        schema: &str,
    ) -> Result<Self> {
        ensure!(reference.schema == schema, "batch schema mismatch");
        let end = reference
            .index_offset
            .checked_add(reference.index_length)
            .context("index range overflow")?;
        ensure!(end == reference.bytes && reference.index_length > 0, "invalid index range");
        let path = Path::from(reference.path.clone());
        ensure!(store.head(&path).await?.size == reference.bytes, "object size mismatch");
        let bytes = store.get_range(&path, reference.index_offset..end).await?;
        ensure!(hash(&bytes) == reference.index_hash, "object index checksum mismatch");
        let index: Index<K, V> = serde_json::from_slice(&bytes)?;
        ensure!(index.version == 1 && index.schema == schema, "unsupported object codec/schema");
        let mut offset = 0;
        for (ordinal, block) in index.blocks.iter().enumerate() {
            ensure!(
                block.offset == offset
                    && block.length > 0
                    && block.rows > 0
                    && block.first <= block.last,
                "invalid object block"
            );
            if ordinal > 0 {
                ensure!(index.blocks[ordinal - 1].last < block.first, "overlapping object fences");
            }
            offset = offset.checked_add(block.length).context("block range overflow")?;
        }
        ensure!(offset == reference.index_offset, "object block/index layout mismatch");
        Ok(Self { store, reference, blocks: Arc::new(index.blocks) })
    }

    pub(super) async fn read(&self, ordinal: usize) -> Result<Vec<Entry<K, V>>> {
        let block = self.blocks.get(ordinal).context("invalid block ordinal")?;
        let end = block.offset.checked_add(block.length).context("block range overflow")?;
        let bytes = self
            .store
            .get_range(&Path::from(self.reference.path.clone()), block.offset..end)
            .await?;
        ensure!(hash(&bytes) == block.hash, "object block checksum mismatch");
        let rows: Vec<Entry<K, V>> = serde_json::from_slice(&bytes)?;
        validate(&rows)?;
        ensure!(
            rows.len() == block.rows
                && rows.first().map(|row| &row.0) == Some(&block.first)
                && rows.last().map(|row| &row.0) == Some(&block.last),
            "block fences/count mismatch"
        );
        Ok(rows)
    }
}

impl<K: BatchData, V: BatchData> BatchReader for ObjectBatch<K, V> {
    type Key = K;
    type Val = V;
    type Cursor = Cursor<K, V>;
    async fn cursor(&self) -> Result<Cursor<K, V>> {
        Cursor::object(self.clone()).await
    }
}
