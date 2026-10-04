use super::{
    BatchData, BatchReader, BlockCache, Cursor, Entry,
    format::{Block, Index, hash, validate},
};
use crate::engine::Batch;
use anyhow::{Context, Result, ensure};
use object_store::{ObjectStore, ObjectStoreExt, path::Path};
use serde::{Deserialize, Serialize};
use std::sync::Arc;

/// Coarse immutable-object reference suitable for a trusted membership catalog.
/// Key/value fences stay in the object's index, not in `PostgreSQL`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ObjectRef {
    path: String,
    bytes: u64,
    index_offset: u64,
    index_length: u64,
    index_hash: String,
    schema: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    namespace: Option<String>,
}

impl ObjectRef {
    pub(crate) fn namespace(&self) -> Option<&str> {
        self.namespace.as_deref()
    }
    pub(crate) fn qualify(mut self, namespace: &str) -> Result<Self> {
        ensure!(
            valid_namespace(namespace) && self.namespace.is_none(),
            "invalid immutable upload namespace"
        );
        self.path = format!("{namespace}/{}", self.path);
        self.namespace = Some(namespace.into());
        Ok(self)
    }
    pub(super) fn validate_namespace(&self) -> Result<()> {
        if let Some(namespace) = &self.namespace {
            ensure!(
                valid_namespace(namespace)
                    && self.path == format!("{namespace}/pgderive/batch-v2/{}", self.index_hash),
                "invalid namespaced root identity"
            );
        }
        Ok(())
    }
    pub(super) fn child_path(&self, path: &str) -> String {
        self.namespace
            .as_ref()
            .map_or_else(|| path.to_owned(), |namespace| format!("{namespace}/{path}"))
    }
    /// Exact key/value codec and ordering identity.
    #[must_use]
    pub fn schema(&self) -> &str {
        &self.schema
    }
    pub(super) const fn index_offset(&self) -> u64 {
        self.index_offset
    }
    pub(super) async fn index_bytes(&self, store: &Arc<dyn ObjectStore>) -> Result<Vec<u8>> {
        self.validate_namespace()?;
        ensure!(
            self.index_length > 0 && self.index_length <= u64::try_from(super::bounded::MAX_BYTES)?,
            "invalid reachability index size"
        );
        let end = self
            .index_offset
            .checked_add(self.index_length)
            .context("reachability index range overflow")?;
        ensure!(
            end == self.bytes
                && store.head(&Path::from(self.path.clone())).await?.size == self.bytes,
            "reachability root size mismatch"
        );
        let bytes = store.get_range(&Path::from(self.path.clone()), self.index_offset..end).await?;
        ensure!(hash(&bytes) == self.index_hash, "reachability root checksum mismatch");
        Ok(bytes.to_vec())
    }
    /// Content-addressed object path.
    #[must_use]
    pub fn path(&self) -> &str {
        &self.path
    }
}

type Fences<K, V> = Arc<Vec<Block<(K, V)>>>;

/// Open immutable versioned JSON batch with resident object-local fences and lazy blocks.
#[derive(Debug, Clone)]
pub struct ObjectBatch<K: BatchData, V: BatchData> {
    pub(super) store: Arc<dyn ObjectStore>,
    pub(super) reference: ObjectRef,
    pub(super) blocks: Fences<K, V>,
    cache: Arc<BlockCache>,
}

impl<K: BatchData, V: BatchData> ObjectBatch<K, V> {
    /// Write in a unique upload namespace and return a globally readable root.
    /// Never reuse the namespace after its durable upload reservation closes.
    ///
    /// # Errors
    /// Rejects invalid namespaces, encoding limits or object I/O failures.
    pub async fn write_namespaced(
        store: Arc<dyn ObjectStore>,
        batch: &Batch<K, V>,
        schema: &str,
        target: (&str, usize),
    ) -> Result<ObjectRef> {
        ensure!(valid_namespace(target.0), "invalid batch upload namespace");
        let scoped: Arc<dyn ObjectStore> =
            Arc::new(object_store::prefix::PrefixStore::new(store, target.0));
        Self::write(scoped, batch, schema, target.1).await?.qualify(target.0)
    }
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
        let mut blocks = Vec::new();
        let mut fence_bytes = 0_usize;
        let mut rows = batch.iter();
        loop {
            let chunk = rows.by_ref().take(block_rows.min(65_536)).collect::<Vec<_>>();
            if chunk.is_empty() {
                break;
            }
            let encoded = super::bounded::encode(&chunk)?;
            let digest = hash(&encoded);
            let path = format!("pgderive/block-v2/{digest}");
            super::bounded::put(&store, &path, encoded.clone()).await?;
            let block = Block {
                path: Some(path),
                first: chunk.first().context("empty chunk")?.0.clone(),
                last: chunk.last().context("empty chunk")?.0.clone(),
                offset: 0,
                length: u64::try_from(encoded.len())?,
                rows: chunk.len(),
                hash: digest,
            };
            fence_bytes = fence_bytes
                .checked_add(super::bounded::encode(&block)?.len())
                .context("manifest size overflow")?;
            ensure!(fence_bytes <= super::bounded::MAX_BYTES, "manifest exceeds byte limit");
            blocks.push(block);
        }
        Self::upload_index(&store, schema, blocks).await
    }
    pub(super) async fn upload_index(
        store: &Arc<dyn ObjectStore>,
        schema: &str,
        blocks: Vec<Block<(K, V)>>,
    ) -> Result<ObjectRef> {
        let index =
            super::bounded::encode(&Index { version: 2, schema: schema.to_owned(), blocks })?;
        let reference = ObjectRef {
            path: format!("pgderive/batch-v2/{}", hash(&index)),
            bytes: u64::try_from(index.len())?,
            index_offset: 0,
            index_length: u64::try_from(index.len())?,
            index_hash: hash(&index),
            schema: schema.to_owned(),
            namespace: None,
        };
        super::bounded::put(store, &reference.path, index).await?;
        Ok(reference)
    }

    // Reachability scans use the same trusted size/hash boundary without decoding
    // application key/value types. They cannot infer liveness from object names alone.
    /// Reopen from a trusted catalog reference, validating the index and schema.
    /// Per-block integrity is checked lazily before exposing rows. Legacy v1
    /// objects are supported within the same 8 MiB block/index limit; larger
    /// legacy objects require an offline rewrite before this reader accepts them.
    ///
    /// # Errors
    /// Returns missing-object, invalid schema/index, or object-store errors.
    pub async fn open(
        store: Arc<dyn ObjectStore>,
        reference: ObjectRef,
        schema: &str,
    ) -> Result<Self> {
        Self::open_with_cache(store, reference, schema, Arc::new(BlockCache::new(0, 0))).await
    }
    /// Reopen with a shared bounded immutable block cache.
    ///
    /// # Errors
    /// Returns missing-object, invalid index/schema, or object-store failures.
    pub async fn open_with_cache(
        store: Arc<dyn ObjectStore>,
        reference: ObjectRef,
        schema: &str,
        cache: Arc<BlockCache>,
    ) -> Result<Self> {
        reference.validate_namespace()?;
        ensure!(
            reference.index_length <= u64::try_from(super::bounded::MAX_BYTES)?,
            "index exceeds byte limit"
        );
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
        let mut index: Index<K, V> = serde_json::from_slice(&bytes)?;
        ensure!(
            matches!(index.version, 1 | 2) && index.schema == schema,
            "unsupported object codec/schema"
        );
        ensure!(index.version == 1 || reference.index_offset == 0, "invalid manifest range");
        let mut offset = 0;
        for (ordinal, block) in index.blocks.iter().enumerate() {
            ensure!(
                block.offset == if index.version == 1 { offset } else { 0 }
                    && block.length > 0
                    && block.rows > 0
                    && (index.version == 1 || block.rows <= 65_536)
                    && block.first <= block.last,
                "invalid object block"
            );
            if ordinal > 0 {
                ensure!(index.blocks[ordinal - 1].last < block.first, "overlapping object fences");
            }
            ensure!(
                block.length <= u64::try_from(super::bounded::MAX_BYTES)?,
                "block exceeds byte limit"
            );
            ensure!(
                if index.version == 1 {
                    block.path.is_none()
                } else {
                    block.path.as_deref()
                        == Some(format!("pgderive/block-v2/{}", block.hash).as_str())
                },
                "invalid block address"
            );
            offset = offset.checked_add(block.length).context("block range overflow")?;
        }
        ensure!(
            index.version == 2 || offset == reference.index_offset,
            "object block/index layout mismatch"
        );
        if let Some(namespace) = &reference.namespace {
            ensure!(
                index.version == 2
                    && valid_namespace(namespace)
                    && reference.path.starts_with(&format!("{namespace}/pgderive/batch-v2/")),
                "invalid namespaced immutable root"
            );
        }
        if reference.namespace.is_some() {
            for block in &mut index.blocks {
                block.path = block.path.as_deref().map(|path| reference.child_path(path));
            }
        }
        Ok(Self { store, reference, blocks: Arc::new(index.blocks), cache })
    }

    /// Coarse trusted catalog reference for this immutable run.
    #[must_use]
    pub const fn reference(&self) -> &ObjectRef {
        &self.reference
    }
    pub(crate) fn share_cache(&mut self, cache: Arc<BlockCache>) {
        self.cache = cache;
    }
    /// Object containing a selected block; useful for integrity checks and reclamation.
    #[must_use]
    pub fn block_path(&self, ordinal: usize) -> Option<&str> {
        self.blocks
            .get(ordinal)
            .map(|block| block.path.as_deref().unwrap_or_else(|| self.reference.path()))
    }

    pub(super) fn key_block(&self, key: &K) -> Option<usize> {
        let ordinal = self.blocks.partition_point(|block| block.last.0 < *key);
        self.blocks.get(ordinal).filter(|block| block.first.0 <= *key).map(|_| ordinal)
    }

    pub(super) async fn read(&self, ordinal: usize) -> Result<Arc<Vec<Entry<K, V>>>> {
        let block = self.blocks.get(ordinal).context("invalid block ordinal")?;
        let end = block.offset.checked_add(block.length).context("block range overflow")?;
        let bytes = if let Some(bytes) = self.cache.get(&block.hash)? {
            bytes
        } else {
            let bytes = self
                .store
                .get_range(
                    &Path::from(block.path.as_ref().unwrap_or(&self.reference.path).clone()),
                    block.offset..end,
                )
                .await?;
            ensure!(hash(&bytes) == block.hash, "object block checksum mismatch");
            let bytes: Arc<[u8]> = Arc::from(bytes.as_ref());
            self.cache.insert(block.hash.clone(), bytes.clone())?;
            bytes
        };
        ensure!(hash(&bytes) == block.hash, "object block checksum mismatch");
        let rows: Vec<Entry<K, V>> = serde_json::from_slice(&bytes)?;
        validate(&rows)?;
        ensure!(
            rows.len() == block.rows
                && rows.first().map(|row| &row.0) == Some(&block.first)
                && rows.last().map(|row| &row.0) == Some(&block.last),
            "block fences/count mismatch"
        );
        Ok(Arc::new(rows))
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

pub(super) fn valid_namespace(namespace: &str) -> bool {
    namespace.strip_prefix("pgderive/upload-v1/").is_some_and(|suffix| {
        !suffix.is_empty()
            && suffix.len() <= 160
            && suffix.bytes().all(|byte| byte.is_ascii_alphanumeric() || byte == b'-')
    })
}
