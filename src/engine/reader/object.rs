use super::{
    BatchData, BatchReader, BlockCache, Cursor, Entry,
    format::{Block, Index, Pack, hash, validate},
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
    hash: String,
    schema: String,
    #[serde(deserialize_with = "Option::<String>::deserialize")]
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
    pub(super) fn validate_identity(&self) -> Result<()> {
        ensure!(
            self.hash.len() == 64
                && self
                    .hash
                    .bytes()
                    .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte)),
            "invalid immutable root hash"
        );
        if let Some(namespace) = &self.namespace {
            ensure!(valid_namespace(namespace), "invalid upload namespace");
        }
        ensure!(
            self.path == self.child_path(&format!("pgderive/batch/{}", self.hash)),
            "invalid immutable root identity"
        );
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
    pub(super) async fn index_bytes(&self, store: &Arc<dyn ObjectStore>) -> Result<Vec<u8>> {
        self.validate_identity()?;
        ensure!(
            self.bytes > 0 && self.bytes <= u64::try_from(super::bounded::MAX_BYTES)?,
            "invalid index byte limit"
        );
        let path = Path::from(self.path.clone());
        ensure!(store.head(&path).await?.size == self.bytes, "root size mismatch");
        let bytes = store.get_range(&path, 0..self.bytes).await?;
        ensure!(hash(&bytes) == self.hash, "root checksum mismatch");
        Ok(bytes.to_vec())
    }
    /// Content-addressed object path.
    #[must_use]
    pub fn path(&self) -> &str {
        &self.path
    }
}

type Fences<K, V> = Arc<Vec<Block<(K, V)>>>;

/// Open immutable JSON+Zstd batch with resident object-local fences and lazy blocks.
#[derive(Debug, Clone)]
pub struct ObjectBatch<K: BatchData, V: BatchData> {
    pub(super) store: Arc<dyn ObjectStore>,
    pub(super) reference: ObjectRef,
    pub(super) blocks: Fences<K, V>,
    pub(super) packs: Arc<Vec<Pack>>,
    pub(super) rows: usize,
    pub(super) max_weight: u64,
    pub(super) cache: Arc<BlockCache>,
}

pub struct WeightPage {
    pub(crate) ordinal: usize,
    pub(crate) range: std::ops::Range<usize>,
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
        Ok(Self::write_resident(
            store,
            batch,
            schema,
            (None, block_rows),
            Arc::new(BlockCache::new(0, 0)),
        )
        .await?
        .reference)
    }
    pub(super) async fn upload_index(
        store: &Arc<dyn ObjectStore>,
        schema: &str,
        packs: Vec<Pack>,
        blocks: Vec<Block<(K, V)>>,
        limit: usize,
    ) -> Result<ObjectRef> {
        let index = super::bounded::encode(&Index { schema: schema.to_owned(), packs, blocks })?;
        ensure!(index.len() <= limit, "streaming index exceeds byte budget");
        let reference = ObjectRef {
            path: format!("pgderive/batch/{}", hash(&index)),
            bytes: u64::try_from(index.len())?,
            hash: hash(&index),
            schema: schema.to_owned(),
            namespace: None,
        };
        super::bounded::put(store, &reference.path, index).await?;
        Ok(reference)
    }

    // Reachability scans use the same trusted size/hash boundary without decoding
    // application key/value types. They cannot infer liveness from object names alone.
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
        ensure!(reference.schema == schema, "batch schema mismatch");
        let bytes = reference.index_bytes(&store).await?;
        let mut index: Index<K, V> = serde_json::from_slice(&bytes)?;
        index.validate_storage()?;
        ensure!(index.schema == schema, "object schema mismatch");
        for (ordinal, block) in index.blocks.iter().enumerate() {
            ensure!(block.first <= block.last, "invalid block fences");
            if ordinal > 0 {
                ensure!(index.blocks[ordinal - 1].last < block.first, "overlapping object fences");
            }
        }
        for pack in &mut index.packs {
            pack.path = reference.child_path(&pack.path);
        }
        let rows = super::format::row_count(&index.blocks)?;
        let max_weight = index.blocks.iter().map(|block| block.max_weight).max().unwrap_or(0);
        Ok(Self {
            store,
            reference,
            blocks: Arc::new(index.blocks),
            packs: Arc::new(index.packs),
            rows,
            max_weight,
            cache,
        })
    }

    /// Coarse trusted catalog reference for this immutable run.
    #[must_use]
    pub const fn reference(&self) -> &ObjectRef {
        &self.reference
    }
    pub(crate) const fn max_weight(&self) -> u64 {
        self.max_weight
    }
    pub(crate) const fn row_count(&self) -> usize {
        self.rows
    }
    pub(crate) fn share_cache(&mut self, cache: Arc<BlockCache>) {
        self.cache = cache;
    }
    pub(crate) fn same_reader(&self, other: &Self) -> bool {
        self.reference == other.reference
            && Arc::ptr_eq(&self.store, &other.store)
            && Arc::ptr_eq(&self.cache, &other.cache)
    }

    /// Object path and exact compressed byte range containing a selected block.
    #[must_use]
    pub fn block_range(&self, ordinal: usize) -> Option<(&str, std::ops::Range<u64>)> {
        let block = self.blocks.get(ordinal)?;
        let data = self.packs.get(block.pack)?;
        let end = block.offset.checked_add(block.length)?;
        (end <= data.bytes).then_some((data.path.as_str(), block.offset..end))
    }

    pub(crate) async fn tuple_weight(&self, tuple: &(K, V)) -> Result<i64> {
        let ordinal = self.blocks.partition_point(|block| block.last < *tuple);
        let Some(_) = self.blocks.get(ordinal).filter(|block| block.first <= *tuple) else {
            return Ok(0);
        };
        if !self.cache.may_contain_key(&self.reference.schema, &self.blocks[ordinal], &tuple.0)? {
            return Ok(0);
        }
        let rows = self.read(ordinal).await?;
        Ok(rows.binary_search_by(|row| row.0.cmp(tuple)).ok().map_or(0, |index| rows[index].1))
    }
    pub(crate) fn next_weight_page(
        &self,
        tuples: &[&(K, V)],
        position: &mut usize,
    ) -> Result<Option<WeightPage>> {
        while *position < tuples.len() {
            let ordinal = self.blocks.partition_point(|block| block.last < *tuples[*position]);
            let Some(block) = self.blocks.get(ordinal) else { return Ok(None) };
            if block.first > *tuples[*position]
                || !self.may_contain_key(ordinal, &tuples[*position].0)?
            {
                *position += 1;
                continue;
            }
            let start = *position;
            *position += 1;
            while *position < tuples.len() && *tuples[*position] <= block.last {
                *position += 1;
            }
            return Ok(Some(WeightPage { ordinal, range: start..*position }));
        }
        Ok(None)
    }
    pub(crate) fn key_block(&self, key: &K) -> Option<usize> {
        let ordinal = self.blocks.partition_point(|block| block.last.0 < *key);
        self.blocks.get(ordinal).filter(|block| block.first.0 <= *key).map(|_| ordinal)
    }
    pub(crate) fn may_contain_key(&self, ordinal: usize, key: &K) -> Result<bool> {
        let block = self.blocks.get(ordinal).context("invalid filter block ordinal")?;
        self.cache.may_contain_key(&self.reference.schema, block, key)
    }

    pub(crate) fn reserve_prefetch(
        &self,
        ordinal: usize,
        plans: &mut Vec<super::ReadAhead>,
    ) -> Result<bool> {
        let block = self.blocks.get(ordinal).context("invalid prefetch block ordinal")?;
        if block.length > 256 * 1024 {
            return Ok(false);
        }
        if self.cache.peek::<K, V>(&self.reference.schema, &block.hash)?.is_some() {
            return Ok(true);
        }
        if let Some(cache) = &self.cache.encoded
            && cache.may_contain(&self.encoded_key(ordinal)?)
        {
            return Ok(true);
        }
        let index =
            if let Some(index) = plans.iter().position(|plan| plan.shares(&self.cache.ranges)) {
                index
            } else {
                let Some(plan) = super::ReadAhead::new(self.cache.ranges.clone())? else {
                    return Ok(false);
                };
                plans.push(plan);
                plans.len() - 1
            };
        let pack = self.packs.get(block.pack).context("missing prefetch pack")?;
        plans[index].reserve(
            &self.store,
            pack,
            block.offset
                ..block.offset.checked_add(block.length).context("prefetch range overflow")?,
        )
    }
    fn encoded_key(&self, ordinal: usize) -> Result<String> {
        let block = self.blocks.get(ordinal).context("invalid encoded block ordinal")?;
        Ok(format!("json-zstd-block:{}", block.hash))
    }
    async fn encoded(&self, ordinal: usize) -> Result<Vec<u8>> {
        let block = self.blocks.get(ordinal).context("invalid block ordinal")?;
        let (_, range) = self.block_range(ordinal).context("invalid packed block range")?;
        let key = self.encoded_key(ordinal)?;
        let cached = match &self.cache.encoded {
            Some(cache) => cache.get(&key, &block.hash).await,
            None => None,
        };
        let bytes = if let Some(bytes) = cached {
            bytes
        } else {
            let pack = self.packs.get(block.pack).context("missing data pack")?;
            let bytes = self.cache.ranges.read(&self.store, pack, range.clone()).await?;
            if hash(&bytes) != block.hash {
                self.cache.ranges.invalidate(pack, &range)?;
                anyhow::bail!("object block checksum mismatch");
            }
            if self.cache.admits_encoded()
                && let Some(cache) = &self.cache.encoded
            {
                cache.insert(key, bytes.clone());
            }
            bytes
        };
        ensure!(u64::try_from(bytes.len())? == block.length, "object range length mismatch");
        ensure!(hash(&bytes) == block.hash, "object block checksum mismatch");
        Ok(bytes)
    }
    pub(crate) async fn read(&self, ordinal: usize) -> Result<Arc<Vec<Entry<K, V>>>> {
        let block = self.blocks.get(ordinal).context("invalid block ordinal")?;

        let rows =
            if let Some(rows) = self.cache.get::<K, V>(&self.reference.schema, &block.hash)? {
                Self::check_rows(&rows, block)?;
                rows
            } else {
                let bytes = self.encoded(ordinal).await?;
                let decoded = super::compression::decode(&bytes)?;
                let mut rows: Vec<Entry<K, V>> = serde_json::from_slice(&decoded)?;
                validate(&rows)?;
                Self::check_rows(&rows, block)?;
                self.cache.record_decode(decoded.len(), rows.len())?;
                // JSON arrays do not carry a length hint. Do not retain their
                // geometric deserialization spare capacity in the bounded cache.
                rows.shrink_to_fit();
                let rows = Arc::new(rows);
                self.cache.insert(
                    &self.reference.schema,
                    block.hash.clone(),
                    rows.clone(),
                    decoded.len(),
                )?;
                rows
            };
        Ok(rows)
    }
    fn check_rows(rows: &[Entry<K, V>], block: &Block<(K, V)>) -> Result<()> {
        ensure!(
            rows.len() == block.rows
                && rows.first().map(|row| &row.0) == Some(&block.first)
                && rows.last().map(|row| &row.0) == Some(&block.last),
            "block fences/count mismatch"
        );
        ensure!(
            rows.iter().map(|(_, weight)| weight.unsigned_abs()).max().unwrap_or(0)
                == block.max_weight,
            "block weight bound mismatch"
        );
        Ok(())
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

pub fn valid_namespace(namespace: &str) -> bool {
    namespace.strip_prefix("pgderive/upload-v1/").is_some_and(|suffix| {
        !suffix.is_empty()
            && suffix.len() <= 160
            && suffix.bytes().all(|byte| byte.is_ascii_alphanumeric() || byte == b'-')
    })
}

#[cfg(test)]
#[path = "tests/codec.rs"]
mod tests;

pub(super) fn scoped_store(
    store: &Arc<dyn ObjectStore>,
    namespace: Option<&str>,
) -> Arc<dyn ObjectStore> {
    namespace.map_or_else(
        || store.clone(),
        |namespace| Arc::new(object_store::prefix::PrefixStore::new(store.clone(), namespace)),
    )
}
