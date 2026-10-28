use super::{
    BatchData, BatchReader, BlockCache, Cursor, Entry,
    format::{Block, Pack, hash, validate},
    index::{Address, Authenticated, Leaf, Node, Root},
};
use crate::engine::Batch;
use anyhow::{Context, Result, ensure};
use object_store::{ObjectStore, ObjectStoreExt, path::Path};
use serde::{Deserialize, Serialize};
use std::sync::Arc;
type IndexLeaf<K, V> = (usize, Arc<Node<(K, V)>>);

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
    #[serde(deserialize_with = "Option::<std::ops::Range<usize>>::deserialize")]
    pub(super) blocks: Option<std::ops::Range<usize>>,
}

impl ObjectRef {
    /// Whether two extents retain the same immutable physical object and dependencies.
    pub(crate) fn same_storage(&self, other: &Self) -> bool {
        self.path == other.path
            && self.bytes == other.bytes
            && self.hash == other.hash
            && self.schema == other.schema
            && self.namespace == other.namespace
    }
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
    pub(crate) fn validate_identity(&self) -> Result<()> {
        ensure!(
            self.blocks.as_ref().is_none_or(|range| range.start < range.end),
            "empty run extent"
        );
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

type Head<K, V> = Option<Arc<Authenticated<(K, V)>>>;
type IndexData<K, V> = (Vec<Pack>, Vec<Block<(K, V)>>);
type IndexUpload<K, V> = (ObjectRef, Root<(K, V)>, Head<K, V>);

/// Open immutable JSON+Zstd batch with paged object-local indexes and lazy blocks.
#[derive(Debug, Clone)]
pub struct ObjectBatch<K: BatchData, V: BatchData> {
    pub(super) store: Arc<dyn ObjectStore>,
    pub(super) reference: ObjectRef,
    pub(super) index: Arc<Root<(K, V)>>,
    pub(super) head: Head<K, V>,
    pub(super) rows: usize,
    pub(super) max_weight: u64,
    pub(super) cache: Arc<BlockCache>,
    pub(super) span: Option<Arc<super::slices::Span<K, V>>>,
}

#[derive(Clone)]
pub struct SelectedBlock<K: BatchData, V: BatchData> {
    pub(crate) ordinal: usize,
    owner: Arc<Root<(K, V)>>,
    leaf: Leaf<(K, V)>,
}

impl<K: BatchData, V: BatchData> SelectedBlock<K, V> {
    pub(crate) fn charge(&self) -> Result<usize> {
        super::bounded::encode(&self.leaf)?
            .len()
            .checked_mul(2)
            .and_then(|bytes| bytes.checked_add(std::mem::size_of::<Self>()))
            .context("selected index page charge overflow")
    }
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
        Ok(Self::write_resident(
            store,
            batch,
            schema,
            (Some(target.0), target.1),
            Arc::new(BlockCache::new(0, 0)),
        )
        .await?
        .reference)
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
        data: IndexData<K, V>,
        limit: usize,
        cache: Option<&BlockCache>,
    ) -> Result<IndexUpload<K, V>> {
        let (packs, blocks) = data;
        let (root, packed) = super::index::build(schema, limit, &packs, &blocks).await?;
        super::work::spend(0).await;
        let head = if let Some(node) = &root.inline {
            Some(Arc::new(node.clone()))
        } else if let Some(address) = &root.node {
            let start = usize::try_from(address.offset)?;
            let end = start
                .checked_add(usize::try_from(address.length)?)
                .context("index head range overflow")?;
            let bytes = packed.get(start..end).context("missing generated index head")?;
            super::work::spend(bytes.len()).await;
            let decoded = super::compression::decode(bytes)?;
            super::work::spend(decoded.len()).await;
            Some(Arc::new(serde_json::from_slice(&decoded)?))
        } else {
            None
        };
        let head = head
            .map(|node| {
                let address = root.node.as_ref().context("missing generated node address")?;
                let pack_bytes = root.pack.as_ref().map_or(address.length, |pack| pack.bytes);
                Ok::<_, anyhow::Error>(Arc::new(Authenticated::new(node, address, pack_bytes)?))
            })
            .transpose()?;
        if let Some(cache) = cache {
            super::index::warm(&cache.index, &root, &packed).await?;
        }
        if let Some(pack) = &root.pack {
            super::bounded::put(store, &pack.path, packed).await?;
        }
        super::work::spend(0).await;
        let index = super::bounded::encode(&root)?;
        super::work::spend(index.len()).await;
        ensure!(index.len() <= limit, "streaming index exceeds byte budget");
        let reference = ObjectRef {
            path: format!("pgderive/batch/{}", hash(&index)),
            bytes: u64::try_from(index.len())?,
            hash: hash(&index),
            schema: schema.to_owned(),
            namespace: None,
            blocks: None,
        };
        super::bounded::put(store, &reference.path, index).await?;
        Ok((reference, root, head))
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
        let mut index: Root<(K, V)> = serde_json::from_slice(&bytes)?;
        ensure!(index.schema == schema, "object schema mismatch");
        index.validate()?;
        if let Some(pack) = &index.pack {
            ensure!(
                pack.path == format!("pgderive/pack/{}", pack.hash),
                "qualified index pack path"
            );
        }
        let rows = index.node.as_ref().map_or(0, |node| node.rows);
        let max_weight = index.node.as_ref().map_or(0, |node| node.max_weight);
        let head = index
            .inline
            .take()
            .map(|node| {
                let address = index.node.as_ref().context("missing inline address")?;
                Ok::<_, anyhow::Error>(Arc::new(Authenticated::new(
                    Arc::new(node),
                    address,
                    address.length,
                )?))
            })
            .transpose()?;
        let selection = reference.blocks.clone();
        let mut batch = Self {
            store,
            reference,
            head,
            index: Arc::new(index),
            rows,
            max_weight,
            cache,
            span: None,
        };
        if let Some(address) = batch.index.node.clone() {
            batch.node(&address).await?;
        }
        if let Some(range) = selection {
            batch.install_span(range).await?;
        }
        Ok(batch)
    }

    /// Coarse trusted catalog reference for this immutable run.
    #[must_use]
    pub const fn reference(&self) -> &ObjectRef {
        &self.reference
    }
    pub(crate) const fn max_weight(&self) -> u64 {
        self.max_weight
    }
    pub(crate) fn key_bounds(&self) -> Option<(&K, &K)> {
        self.span.as_ref().map_or_else(
            || self.index.node.as_ref().map(|node| (&node.first.0, &node.last.0)),
            |span| Some((&span.first.0, &span.last.0)),
        )
    }
    pub(super) fn last_tuple(&self) -> Option<&(K, V)> {
        self.span.as_ref().map_or_else(
            || self.index.node.as_ref().map(|node| &node.last),
            |span| Some(&span.last),
        )
    }
    pub(crate) fn payload_bytes(&self) -> u64 {
        self.span.as_ref().map_or_else(
            || self.index.node.as_ref().map_or(0, |node| node.payload_bytes),
            |span| span.payload_bytes,
        )
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

    /// Estimated resident root and inline-node bytes; shared page cache is reported separately.
    /// Cloned readers share this metadata. This is a charge estimate, not allocator RSS.
    ///
    /// # Errors
    /// Returns an encoding or checked-size error.
    pub fn resident_index_bytes(&self) -> Result<usize> {
        let root = super::bounded::encode(self.index.as_ref())?.len();
        let root = root.checked_add(self.span_charge()?).context("span charge overflow")?;
        let head = self
            .head
            .as_ref()
            .map(|head| super::bounded::encode(head.node()).map(|bytes| bytes.len()))
            .transpose()?
            .unwrap_or(0);
        root.checked_add(head)
            .and_then(|bytes| bytes.checked_mul(2))
            .and_then(|bytes| bytes.checked_add(std::mem::size_of::<Self>()))
            .and_then(|bytes| bytes.checked_add(self.head.as_ref().map_or(0, |head| head.charge())))
            .context("resident index charge overflow")
    }
    pub(crate) fn block_count(&self) -> usize {
        self.span.as_ref().map_or_else(
            || self.index.node.as_ref().map_or(0, |node| node.blocks),
            |span| span.range.len(),
        )
    }
    pub(super) async fn node(&self, address: &Address<(K, V)>) -> Result<Arc<Node<(K, V)>>> {
        let pack_bytes = self.index.pack.as_ref().map_or(address.length, |pack| pack.bytes);
        if self.index.node.as_ref().is_some_and(|root| root.hash == address.hash)
            && let Some(node) = &self.head
        {
            return node.checked(address, pack_bytes);
        }
        let pack = self.index.pack.as_ref().context("missing index pack")?;
        if let Some(node) = self.cache.index.get(&address.hash, address.length)? {
            return node.checked(address, pack.bytes);
        }
        let _flight = self.cache.index.flight(&address.hash).await;
        if let Some(node) = self.cache.index.get(&address.hash, address.length)? {
            return node.checked(address, pack.bytes);
        }
        address.validate(pack.bytes)?;
        let mut qualified = pack.clone();
        qualified.path = self.reference.child_path(&pack.path);
        let range = address.offset..address.offset + address.length;
        let bytes = self
            .cache
            .index
            .read(
                &self.store,
                &qualified,
                super::index::FrameRead {
                    digest: &address.hash,
                    range,
                    read_ahead: self.span.is_none(),
                },
            )
            .await?;
        let decoded = super::compression::decode(&bytes)?;
        let node: Arc<Node<(K, V)>> = Arc::new(serde_json::from_slice(&decoded)?);
        let authenticated = Arc::new(Authenticated::new(node, address, pack.bytes)?);
        let node = authenticated.checked(address, pack.bytes)?;
        self.cache.index.insert(&address.hash, address.length, authenticated, decoded.len())?;
        Ok(node)
    }
    pub(super) async fn leaf(&self, ordinal: usize) -> Result<Leaf<(K, V)>> {
        if let Some(pages) = self.span.as_ref().and_then(|span| span.pages.as_ref()) {
            return pages.get(ordinal).cloned().context("invalid extent block ordinal");
        }
        let mut address = self.index.node.as_ref().context("empty object index")?.clone();
        ensure!(ordinal < self.block_count(), "invalid block ordinal");
        let mut relative = ordinal + self.span.as_ref().map_or(0, |span| span.range.start);
        loop {
            match self.node(&address).await?.as_ref() {
                Node::Leaf(leaves) => {
                    return leaves.get(relative).cloned().context("missing index leaf");
                }
                Node::Branch(children) => {
                    address = super::index::ordinal_child(children, &mut relative)?;
                }
            }
        }
    }
    pub(super) async fn seek_block(
        &self,
        before: impl Fn(&(K, V)) -> bool + Send + Sync,
    ) -> Result<usize> {
        if let Some(pages) = self.span.as_ref().and_then(|span| span.pages.as_ref()) {
            return Ok(pages.partition_point(|leaf| before(&leaf.block.last)));
        }
        let Some((ordinal, node)) = self.seek_leaf(&before).await? else {
            return Ok(self.block_count());
        };
        let Node::Leaf(leaves) = node.as_ref() else { anyhow::bail!("expected index leaf") };
        Ok(self.relative_block(ordinal + leaves.partition_point(|leaf| before(&leaf.block.last))))
    }
    async fn seek_leaf(
        &self,
        before: &(impl Fn(&(K, V)) -> bool + Sync),
    ) -> Result<Option<IndexLeaf<K, V>>> {
        let Some(root) = self.index.node.as_ref() else { return Ok(None) };
        if before(&root.last) {
            return Ok(None);
        }
        let mut address = root.clone();
        let mut ordinal = 0;
        loop {
            let node = self.node(&address).await?;
            match node.as_ref() {
                Node::Leaf(_) => return Ok(Some((ordinal, node))),
                Node::Branch(children) => {
                    let Some(child) = super::index::seek_child(children, before, &mut ordinal)
                    else {
                        return Ok(None);
                    };
                    address = child;
                }
            }
        }
    }

    /// Object path and exact compressed byte range containing a selected block.
    ///
    /// # Errors
    /// Returns authenticated index-page I/O or validation errors.
    pub async fn block_range(
        &self,
        ordinal: usize,
    ) -> Result<Option<(String, std::ops::Range<u64>)>> {
        if ordinal >= self.block_count() {
            return Ok(None);
        }
        let leaf = self.leaf(ordinal).await?;
        let end =
            leaf.block.offset.checked_add(leaf.block.length).context("block range overflow")?;
        Ok(Some((leaf.pack.path.clone(), leaf.block.offset..end)))
    }

    pub(crate) async fn tuple_weight(&self, tuple: &(K, V)) -> Result<i64> {
        if self.index.node.as_ref().is_none_or(|root| tuple < &root.first || tuple > &root.last) {
            return Ok(0);
        }
        let ordinal = self.seek_block(|last| last < tuple).await?;
        if ordinal >= self.block_count() {
            return Ok(0);
        }
        let leaf = self.leaf(ordinal).await?;
        if leaf.block.first > *tuple {
            return Ok(0);
        }
        if !self.cache.may_contain_key(&self.reference.schema, &leaf.block, &tuple.0)? {
            return Ok(0);
        }
        let rows = self.read_leaf(&leaf).await?;
        Ok(rows.binary_search_by(|row| row.0.cmp(tuple)).ok().map_or(0, |index| rows[index].1))
    }
    pub(crate) async fn next_weight_page(
        &self,
        tuples: &[&(K, V)],
        position: &mut usize,
    ) -> Result<Option<WeightPage>> {
        let Some(root) = self.index.node.as_ref() else {
            return Ok(None);
        };
        let remaining = tuples.get(*position..).context("invalid weight-page position")?;
        *position += remaining.partition_point(|tuple| *tuple < &root.first);
        while *position < tuples.len() {
            if tuples[*position] > &root.last {
                return Ok(None);
            }
            let ordinal = self.seek_block(|last| last < tuples[*position]).await?;
            if ordinal >= self.block_count() {
                return Ok(None);
            }
            let leaf = self.leaf(ordinal).await?;
            let block = &leaf.block;
            if block.first > *tuples[*position]
                || !self.cache.may_contain_key(
                    &self.reference.schema,
                    block,
                    &tuples[*position].0,
                )?
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
    pub(crate) const fn block_cache(&self) -> &Arc<BlockCache> {
        &self.cache
    }
    pub(crate) async fn key_page(&self, key: &K) -> Result<Option<SelectedBlock<K, V>>> {
        if let Some(span) = &self.span
            && let Some(pages) = &span.pages
        {
            return self.key_leaf(pages, span.range.start, key).await;
        }
        if self.index.node.as_ref().is_none_or(|root| key < &root.first.0 || key > &root.last.0) {
            return Ok(None);
        }
        let Some((ordinal, node)) = self.seek_leaf(&|last| last.0 < *key).await? else {
            return Ok(None);
        };
        let Node::Leaf(leaves) = node.as_ref() else { anyhow::bail!("expected index leaf") };
        self.key_leaf(leaves, ordinal, key).await
    }

    async fn key_leaf(
        &self,
        leaves: &[Leaf<(K, V)>],
        ordinal: usize,
        key: &K,
    ) -> Result<Option<SelectedBlock<K, V>>> {
        let position = leaves.partition_point(|leaf| leaf.block.last.0 < *key);
        let absolute = ordinal + position;
        let relative = self.relative_block(absolute);
        if relative >= self.block_count() {
            return Ok(None);
        }
        let leaf = if self.span.as_ref().is_some_and(|span| absolute < span.range.start) {
            self.leaf(0).await?
        } else {
            let Some(leaf) = leaves.get(position) else { return Ok(None) };
            leaf.clone()
        };
        Ok((leaf.block.first.0 <= *key && *key <= leaf.block.last.0).then(|| SelectedBlock {
            ordinal: relative,
            owner: self.index.clone(),
            leaf,
        }))
    }
    pub(crate) fn validate_page(&self, page: &SelectedBlock<K, V>) -> Result<()> {
        ensure!(
            Arc::ptr_eq(&self.index, &page.owner),
            "selected page belongs to a different reader"
        );
        Ok(())
    }
    pub(crate) fn may_contain_page(&self, page: &SelectedBlock<K, V>, key: &K) -> Result<bool> {
        self.validate_page(page)?;
        let present = self.cache.may_contain_key(&self.reference.schema, &page.leaf.block, key)?;
        #[cfg(test)]
        if present {
            self.cache.ranges.record_probe(
                &self.reference.schema,
                &page.leaf.pack.path,
                &page.leaf,
                key,
            )?;
        }
        Ok(present)
    }
    pub(crate) fn reserve_page(
        &self,
        page: &SelectedBlock<K, V>,
        plans: &mut Vec<super::ReadAhead>,
    ) -> Result<bool> {
        self.validate_page(page)?;
        self.reserve_leaf(&page.leaf, plans)
    }
    pub(crate) async fn read_page(
        &self,
        page: &SelectedBlock<K, V>,
    ) -> Result<Arc<Vec<Entry<K, V>>>> {
        self.validate_page(page)?;
        self.read_leaf(&page.leaf).await
    }
    fn reserve_leaf(&self, leaf: &Leaf<(K, V)>, plans: &mut Vec<super::ReadAhead>) -> Result<bool> {
        let block = &leaf.block;
        if !leaf.dense_pack || block.length > 256 * 1024 {
            return Ok(false);
        }
        if self.cache.peek::<K, V>(&self.reference.schema, &block.hash)?.is_some() {
            return Ok(true);
        }
        if let Some(cache) = &self.cache.encoded
            && cache.may_contain(&Self::encoded_key(block))
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
        plans[index].reserve(
            &self.store,
            &leaf.pack,
            block.offset
                ..block.offset.checked_add(block.length).context("prefetch range overflow")?,
        )
    }
    fn encoded_key(block: &Block<(K, V)>) -> String {
        format!("json-zstd-block:{}", block.hash)
    }
    async fn encoded(&self, leaf: &Leaf<(K, V)>, read_ahead: bool) -> Result<Vec<u8>> {
        let block = &leaf.block;
        let range =
            block.offset..block.offset.checked_add(block.length).context("block range overflow")?;
        let key = Self::encoded_key(block);
        let cached = match &self.cache.encoded {
            Some(cache) => cache.get(&key, &block.hash).await,
            None => None,
        };
        let bytes = if let Some(bytes) = cached {
            bytes
        } else {
            let bytes = if read_ahead {
                self.cache.ranges.read(&self.store, &leaf.pack, range.clone()).await?
            } else {
                self.cache.ranges.read_exact(&self.store, &leaf.pack, range.clone()).await?
            };
            if hash(&bytes) != block.hash {
                self.cache.ranges.invalidate(&leaf.pack, &range)?;
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
        let leaf = self.leaf(ordinal).await?;
        self.read_leaf(&leaf).await
    }
    pub(super) async fn read_leaf(&self, leaf: &Leaf<(K, V)>) -> Result<Arc<Vec<Entry<K, V>>>> {
        self.read_leaf_with(leaf, self.span.is_none()).await
    }
    pub(super) async fn read_leaf_exact(
        &self,
        leaf: &Leaf<(K, V)>,
    ) -> Result<Arc<Vec<Entry<K, V>>>> {
        self.read_leaf_with(leaf, false).await
    }
    async fn read_leaf_with(
        &self,
        leaf: &Leaf<(K, V)>,
        read_ahead: bool,
    ) -> Result<Arc<Vec<Entry<K, V>>>> {
        let block = &leaf.block;

        let rows =
            if let Some(rows) = self.cache.get::<K, V>(&self.reference.schema, &block.hash)? {
                Self::check_rows(&rows, block)?;
                rows
            } else {
                let bytes = self.encoded(leaf, read_ahead && leaf.dense_pack).await?;
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

#[cfg(test)]
impl<K: BatchData, V: BatchData> ObjectBatch<K, V> {
    pub(super) async fn test_blocks(&self) -> Result<Vec<Block<(K, V)>>> {
        let mut blocks = Vec::new();
        for ordinal in 0..self.block_count() {
            blocks.push(self.leaf(ordinal).await?.block);
        }
        Ok(blocks)
    }
    pub(super) async fn test_packs(&self) -> Result<Vec<Pack>> {
        let mut packs = std::collections::BTreeMap::new();
        for ordinal in 0..self.block_count() {
            let leaf = self.leaf(ordinal).await?;
            packs.insert(leaf.block.pack, leaf.pack);
        }
        Ok(packs.into_values().collect())
    }
    pub(super) fn test_block_mut(&mut self, ordinal: usize) -> Result<&mut Block<(K, V)>> {
        let head = self.head.as_mut().context("missing test index head")?;
        let Node::Leaf(leaves) = Arc::make_mut(head).test_node_mut() else {
            anyhow::bail!("test requires inline leaf");
        };
        Ok(&mut leaves.get_mut(ordinal).context("missing test leaf")?.block)
    }
}

#[cfg(test)]
#[path = "tests/locations.rs"]
mod locations;
