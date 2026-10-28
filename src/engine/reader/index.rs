//! Authenticated, range-readable immutable index trees.
use super::{
    BatchData, bounded,
    format::{Block, Pack, hash},
};
use anyhow::{Context as _, Result, ensure};
use serde::{Deserialize, Serialize};

mod authenticated;
mod frames;
pub(super) use authenticated::Authenticated;

// Keep cold navigation nodes small enough to admit under bounded metadata caches.
pub const FANOUT: usize = 16;
pub(super) const INLINE_BYTES: usize = 16 * 1024;
pub(super) const PAGE_BYTES: usize = bounded::MAX_BYTES;

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct Address<T> {
    pub(super) offset: u64,
    pub(super) length: u64,
    pub(super) hash: String,
    pub(super) first: T,
    pub(super) last: T,
    pub(super) blocks: usize,
    pub(super) rows: usize,
    pub(super) payload_bytes: u64,
    pub(super) max_weight: u64,
    pub(super) depth: usize,
}
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct Leaf<T> {
    pub(super) dense_pack: bool,
    pub(super) block: Block<T>,
    pub(super) pack: Pack,
}
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) enum Node<T> {
    Leaf(Vec<Leaf<T>>),
    Branch(Vec<Address<T>>),
}
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct Root<T> {
    pub(super) schema: String,
    pub(super) pack: Option<Pack>,
    pub(super) node: Option<Address<T>>,
    pub(super) inline: Option<Node<T>>,
}

impl<T> Address<T> {
    pub(super) fn validate(&self, bytes: u64) -> Result<()> {
        ensure!(
            self.blocks > 0
                && self.payload_bytes > 0
                && self.rows >= self.blocks
                && self.max_weight > 0
                && self.max_weight <= i64::MIN.unsigned_abs()
                && self.depth <= 16,
            "invalid index node statistics"
        );
        ensure!(
            self.length > 0
                && self.length <= u64::try_from(PAGE_BYTES)?
                && self.offset.checked_add(self.length).is_some_and(|end| end <= bytes)
                && super::format::valid_hash(&self.hash),
            "invalid index node address"
        );
        Ok(())
    }
}
impl<T: Ord + Clone> Node<T> {
    fn summary(&self) -> Result<Address<T>> {
        let mut payload_bytes = 0_u64;
        let (first, last, blocks, rows, max_weight, depth) = match self {
            Self::Leaf(leaves) => {
                let first = leaves.first().context("empty index leaf")?;
                let last = leaves.last().context("empty index leaf")?;
                ensure!(leaves.len() <= FANOUT, "index leaf fanout exceeded");
                let mut rows = 0_usize;
                let mut max_weight = 0;
                for leaf in leaves {
                    leaf.block.validate_address()?;
                    leaf.pack.validate()?;
                    ensure!(
                        leaf.block.first <= leaf.block.last
                            && leaf
                                .block
                                .offset
                                .checked_add(leaf.block.length)
                                .is_some_and(|end| end <= leaf.pack.bytes),
                        "invalid index leaf block"
                    );
                    payload_bytes = payload_bytes
                        .checked_add(leaf.block.length)
                        .context("index payload byte overflow")?;
                    rows = rows.checked_add(leaf.block.rows).context("index row count overflow")?;
                    max_weight = max_weight.max(leaf.block.max_weight);
                }
                ensure!(
                    leaves.windows(2).all(|pair| pair[0].block.last < pair[1].block.first),
                    "overlapping index leaf fences"
                );
                (
                    first.block.first.clone(),
                    last.block.last.clone(),
                    leaves.len(),
                    rows,
                    max_weight,
                    0,
                )
            }
            Self::Branch(children) => {
                let first = children.first().context("empty index branch")?;
                let last = children.last().context("empty index branch")?;
                ensure!(
                    children.len() >= 2 && children.len() <= FANOUT,
                    "invalid index branch fanout"
                );
                ensure!(
                    children.windows(2).all(|pair| pair[0].last < pair[1].first),
                    "overlapping index branch fences"
                );
                let mut blocks = 0_usize;
                let mut rows = 0_usize;
                let mut max_weight = 0;
                for child in children {
                    ensure!(child.depth == first.depth, "uneven index tree depth");
                    blocks =
                        blocks.checked_add(child.blocks).context("index block count overflow")?;
                    payload_bytes = payload_bytes
                        .checked_add(child.payload_bytes)
                        .context("index payload byte overflow")?;
                    rows = rows.checked_add(child.rows).context("index row count overflow")?;
                    max_weight = max_weight.max(child.max_weight);
                }
                (first.first.clone(), last.last.clone(), blocks, rows, max_weight, first.depth + 1)
            }
        };
        Ok(Address {
            offset: 0,
            length: 0,
            hash: String::new(),
            first,
            last,
            blocks,
            rows,
            payload_bytes,
            max_weight,
            depth,
        })
    }
    pub(super) fn validate(&self, address: &Address<T>, pack_bytes: u64) -> Result<()> {
        self.validate_structure(address, pack_bytes)?;
        let actual = self.summary()?;
        ensure!(
            actual.first == address.first
                && actual.last == address.last
                && actual.blocks == address.blocks
                && actual.rows == address.rows
                && actual.payload_bytes == address.payload_bytes
                && actual.max_weight == address.max_weight
                && actual.depth == address.depth,
            "index node summary mismatch"
        );
        if let Self::Branch(children) = self {
            for child in children {
                child.validate(address.offset)?;
            }
        }
        Ok(())
    }
}

async fn append<T: Ord + Clone + Serialize + Send + Sync>(
    node: &Node<T>,
    bytes: &mut Vec<u8>,
) -> Result<Address<T>> {
    super::work::spend(0).await;
    let decoded = bounded::encode(node)?;
    super::work::spend(decoded.len()).await;
    let encoded = super::compression::compress(&decoded)?;
    ensure!(
        decoded.len() <= PAGE_BYTES,
        crate::engine::execution::CapacityExceeded("index node exceeds page byte limit")
    );
    let mut address = node.summary()?;
    address.offset = u64::try_from(bytes.len())?;
    address.length = u64::try_from(encoded.len())?;
    address.hash = hash(&encoded);
    ensure!(
        bytes.len().checked_add(encoded.len()).is_some_and(|n| n <= bounded::MAX_BYTES),
        crate::engine::execution::CapacityExceeded("index pack exceeds byte limit")
    );
    bytes.extend(encoded);
    Ok(address)
}

// Sparse shared packs use exact reads; mostly live packs retain scan coalescing.
fn dense_packs<T>(packs: &[Pack], blocks: &[Block<T>]) -> Result<Vec<bool>> {
    let mut bytes = vec![0_u64; packs.len()];
    for block in blocks {
        let count = bytes.get_mut(block.pack).context("missing coverage pack")?;
        *count = count.checked_add(block.length).context("pack coverage overflow")?;
    }
    Ok(bytes
        .iter()
        .zip(packs)
        .map(|(bytes, pack)| {
            crate::engine::execution::service::coverage(pack.bytes, *bytes);
            *bytes >= pack.bytes.div_ceil(2)
        })
        .collect())
}

type PackedIndex<K, V> = (Root<(K, V)>, Vec<u8>);

pub(super) async fn build<K: BatchData, V: BatchData>(
    schema: &str,
    limit: usize,
    packs: &[Pack],
    blocks: &[Block<(K, V)>],
) -> Result<PackedIndex<K, V>> {
    if blocks.is_empty() {
        return Ok((
            Root { schema: schema.into(), pack: None, node: None, inline: None },
            Vec::new(),
        ));
    }
    let dense = dense_packs(packs, blocks)?;
    let mut bytes = Vec::new();
    let mut layer = Vec::new();
    for chunk in blocks.chunks(FANOUT) {
        super::work::spend(0).await;
        let leaves = chunk
            .iter()
            .map(|block| {
                Ok(Leaf {
                    dense_pack: *dense.get(block.pack).context("missing pack coverage")?,
                    block: block.clone(),
                    pack: packs.get(block.pack).context("missing index data pack")?.clone(),
                })
            })
            .collect::<Result<Vec<_>>>()?;
        layer.push(append(&Node::Leaf(leaves), &mut bytes).await?);
    }
    while layer.len() > 1 {
        layer = append_level(&layer, &mut bytes).await?;
    }
    if blocks.len() <= FANOUT {
        super::work::spend(bytes.len()).await;
        let decoded = super::compression::decode(&bytes)?;
        let mut inline_address = layer.last().cloned().context("missing inline index address")?;
        super::work::spend(decoded.len()).await;
        let inline: Node<(K, V)> = serde_json::from_slice(&decoded)?;
        (inline_address.length, inline_address.hash) = inline_identity(&inline)?;
        let candidate = Root {
            schema: schema.into(),
            pack: None,
            node: Some(inline_address),
            inline: Some(inline),
        };
        if decoded.len() <= INLINE_BYTES && bounded::encode(&candidate)?.len() <= limit {
            return Ok((candidate, Vec::new()));
        }
    }
    super::work::spend(bytes.len()).await;
    let digest = hash(&bytes);
    let pack = Pack {
        path: format!("pgderive/pack/{digest}"),
        bytes: u64::try_from(bytes.len())?,
        hash: digest,
    };
    Ok((Root { schema: schema.into(), pack: Some(pack), node: layer.pop(), inline: None }, bytes))
}

#[derive(Debug)]
pub(super) struct Cache {
    state: std::sync::Mutex<CacheState>,
    bytes: usize,
    entries: usize,
    frames: frames::Frames,
    scan: Option<super::cache::range::Ranges>,
    metrics: std::sync::Arc<super::cache::metrics::Metrics>,
    flights: [tokio::sync::Mutex<()>; 16],
    parent: Option<std::sync::Arc<Self>>,
}
#[derive(Debug, Default)]
struct CacheState {
    entries: std::collections::BTreeMap<(String, u64, std::any::TypeId), Cached>,
    bytes: usize,
    clock: u64,
}
#[derive(Clone, Debug)]
struct Cached {
    node: std::sync::Arc<dyn std::any::Any + Send + Sync>,
    charge: usize,
    accessed: u64,
}
pub(super) struct Stats {
    pub(super) bytes: usize,
    pub(super) entries: usize,
    pub(super) encoded_bytes: usize,
    pub(super) encoded_entries: usize,
    pub(super) reads: u64,
    pub(super) read_bytes: u64,
    pub(super) requested_bytes: u64,
}
pub(super) struct FrameRead<'a> {
    pub(super) digest: &'a str,
    pub(super) range: std::ops::Range<u64>,
    pub(super) read_ahead: bool,
}
impl Cache {
    pub(super) fn new(bytes: usize, entries: usize) -> Self {
        Self {
            bytes: bytes / 4,
            entries,
            state: std::sync::Mutex::default(),
            frames: frames::Frames::new(
                bytes - bytes / 4,
                ((bytes - bytes / 4) / 1024).max(entries),
            ),
            scan: None,
            metrics: std::sync::Arc::default(),
            flights: std::array::from_fn(|_| tokio::sync::Mutex::new(())),
            parent: None,
        }
    }
    pub(super) fn scan(parent: std::sync::Arc<Self>) -> Self {
        let parent = parent.parent.clone().unwrap_or(parent);
        Self {
            scan: Some(super::cache::range::Ranges::scan(parent.metrics.clone())),
            metrics: parent.metrics.clone(),
            parent: Some(parent),
            bytes: 256 * 1024,
            entries: 17,
            ..Self::new(0, 0)
        }
    }
    pub(super) fn stats(&self) -> Result<Stats> {
        let state = self.state.lock().map_err(|_| anyhow::anyhow!("index cache lock poisoned"))?;
        let retained = (state.bytes, state.entries.len());
        drop(state);
        let (encoded_bytes, encoded_entries) = if let Some(scan) = &self.scan {
            let (bytes, entries, _) = scan.snapshot()?;
            (bytes, entries)
        } else {
            self.frames.stats()
        };
        let io = self.metrics.total_io();
        Ok(Stats {
            bytes: retained.0,
            entries: retained.1,
            encoded_bytes,
            encoded_entries,
            reads: io.gets,
            read_bytes: io.fetched_bytes,
            requested_bytes: io.requested_bytes,
        })
    }
    pub(super) async fn read(
        &self,
        store: &std::sync::Arc<dyn object_store::ObjectStore>,
        pack: &Pack,
        frame: FrameRead<'_>,
    ) -> Result<std::sync::Arc<Vec<u8>>> {
        use object_store::ObjectStoreExt as _;
        let FrameRead { digest, range, read_ahead } = frame;
        ensure!(range.start < range.end, "empty index frame range");
        ensure!(range.end <= pack.bytes, "index frame range exceeds pack");
        let length = range.end - range.start;
        if let Some(scan) = &self.scan {
            let bytes = if read_ahead {
                scan.read(store, pack, range.clone()).await?
            } else {
                scan.read_exact(store, pack, range.clone()).await?
            };
            if u64::try_from(bytes.len())? != length || hash(&bytes) != digest {
                scan.invalidate(pack, &range)?;
                anyhow::bail!("index frame checksum/length mismatch");
            }
            return Ok(std::sync::Arc::new(bytes));
        }
        self.metrics.requested(false, length);
        if let Some(bytes) = self.frames.get(digest, length) {
            return Ok(bytes);
        }
        self.metrics.fetching(false);
        let bytes =
            store.get_range(&object_store::path::Path::from(pack.path.clone()), range).await?;
        self.metrics.fetched(false, u64::try_from(bytes.len())?);
        let bytes = std::sync::Arc::new(bytes.to_vec());
        self.frames.insert(digest, length, bytes.clone())?;
        Ok(bytes)
    }
    #[cfg(test)]
    pub(super) fn clear_encoded(&self) {
        self.frames.clear();
    }
    pub(super) async fn flight(&self, hash: &str) -> tokio::sync::MutexGuard<'_, ()> {
        use std::hash::{Hash as _, Hasher as _};
        let mut hasher = std::collections::hash_map::DefaultHasher::new();
        hash.hash(&mut hasher);
        let stripe = hasher.finish().to_le_bytes()[0] & 15;
        self.flights[usize::from(stripe)].lock().await
    }
    pub(super) fn get<T: Ord + Clone + Serialize + Send + Sync + 'static>(
        &self,
        hash: &str,
        length: u64,
    ) -> Result<Option<std::sync::Arc<Authenticated<T>>>> {
        if let Some(parent) = &self.parent
            && let Some(entry) = parent.peek::<T>(hash, length)?
        {
            let node = entry
                .node
                .clone()
                .downcast()
                .map_err(|_| anyhow::anyhow!("index cache type mismatch"))?;
            self.retain(hash, length, std::any::TypeId::of::<T>(), entry)?;
            return Ok(Some(node));
        }
        let mut state =
            self.state.lock().map_err(|_| anyhow::anyhow!("index cache lock poisoned"))?;
        state.clock = state.clock.saturating_add(1);
        let clock = state.clock;
        let found = state.entries.get_mut(&(hash.into(), length, std::any::TypeId::of::<T>())).map(
            |entry| {
                entry.accessed = clock;
                entry.node.clone()
            },
        );
        drop(state);
        found
            .map(|node| node.downcast().map_err(|_| anyhow::anyhow!("index cache type mismatch")))
            .transpose()
    }
    fn peek<T: Send + Sync + 'static>(&self, hash: &str, length: u64) -> Result<Option<Cached>> {
        let state = self.state.lock().map_err(|_| anyhow::anyhow!("index cache lock poisoned"))?;
        let found = state.entries.get(&(hash.into(), length, std::any::TypeId::of::<T>())).cloned();
        drop(state);
        Ok(found)
    }
    pub(super) fn insert<T: Ord + Clone + Serialize + Send + Sync + 'static>(
        &self,
        hash: &str,
        length: u64,
        node: std::sync::Arc<Authenticated<T>>,
        encoded: usize,
    ) -> Result<()> {
        let slots = match node.node() {
            Node::Leaf(entries) => entries.capacity().checked_mul(std::mem::size_of::<Leaf<T>>()),
            Node::Branch(entries) => {
                entries.capacity().checked_mul(std::mem::size_of::<Address<T>>())
            }
        }
        .context("index cache slot charge overflow")?;
        let charge = encoded
            .checked_mul(2)
            .and_then(|n| n.checked_add(slots))
            .and_then(|n| n.checked_add(node.charge()))
            .context("index cache charge overflow")?;
        self.retain(hash, length, std::any::TypeId::of::<T>(), Cached { node, charge, accessed: 0 })
    }
    fn retain(&self, hash: &str, length: u64, kind: std::any::TypeId, entry: Cached) -> Result<()> {
        let Cached { node, charge, .. } = entry;
        if self.entries == 0 || charge > self.bytes {
            return Ok(());
        }
        let mut state =
            self.state.lock().map_err(|_| anyhow::anyhow!("index cache lock poisoned"))?;
        let identity = (hash.into(), length, kind);
        state.clock = state.clock.saturating_add(1);
        let clock = state.clock;
        if let Some(entry) = state.entries.get_mut(&identity) {
            entry.accessed = clock;
            return Ok(());
        }
        while state.bytes > self.bytes - charge || state.entries.len() >= self.entries {
            let oldest = state
                .entries
                .iter()
                .min_by_key(|(_, entry)| entry.accessed)
                .map(|(identity, _)| identity.clone())
                .context("empty index cache eviction")?;
            let removed = state.entries.remove(&oldest).context("missing index cache eviction")?;
            state.bytes -= removed.charge;
        }
        state.clock = state.clock.saturating_add(1);
        let accessed = state.clock;
        state.entries.insert(identity, Cached { node, charge, accessed });
        state.bytes += charge;
        drop(state);
        Ok(())
    }
}
impl<T: Ord + Clone + Serialize> Root<T> {
    pub(super) fn validate(&self) -> Result<()> {
        match (&self.pack, &self.node, &self.inline) {
            (None, None, None) => Ok(()),
            (None, Some(address), Some(node)) => {
                ensure!(
                    address.offset == 0 && address.length <= u64::try_from(INLINE_BYTES)?,
                    "invalid inline index size"
                );
                let (length, digest) = inline_identity(node)?;
                ensure!(
                    length == address.length && digest == address.hash,
                    "inline index checksum mismatch"
                );
                node.validate(address, address.length)
            }
            (Some(pack), Some(node), None) => {
                pack.validate()?;
                node.validate(pack.bytes)?;
                ensure!(
                    node.first <= node.last
                        && node.offset.checked_add(node.length) == Some(pack.bytes),
                    "invalid index root range"
                );
                Ok(())
            }
            _ => anyhow::bail!("incomplete immutable index root"),
        }
    }
}

impl<T> Node<T> {
    pub(super) fn validate_structure(&self, address: &Address<T>, pack_bytes: u64) -> Result<()> {
        address.validate(pack_bytes)?;
        let mut rows = 0_usize;
        let mut blocks = 0_usize;
        let mut max_weight = 0_u64;
        let mut payload_bytes = 0_u64;
        match self {
            Self::Leaf(leaves) => {
                ensure!(
                    !leaves.is_empty() && leaves.len() <= FANOUT && address.depth == 0,
                    "invalid index leaf structure"
                );
                for leaf in leaves {
                    leaf.block.validate_address()?;
                    leaf.pack.validate()?;
                    ensure!(
                        leaf.block
                            .offset
                            .checked_add(leaf.block.length)
                            .is_some_and(|end| end <= leaf.pack.bytes),
                        "index leaf exceeds data pack"
                    );
                    payload_bytes = payload_bytes
                        .checked_add(leaf.block.length)
                        .context("index payload byte overflow")?;
                    rows = rows.checked_add(leaf.block.rows).context("index row count overflow")?;
                    blocks += 1;
                    max_weight = max_weight.max(leaf.block.max_weight);
                }
            }
            Self::Branch(children) => {
                ensure!(
                    children.len() >= 2 && children.len() <= FANOUT,
                    "invalid index branch structure"
                );
                let mut end = 0;
                for child in children {
                    child.validate(address.offset)?;
                    ensure!(
                        child.depth.checked_add(1) == Some(address.depth) && child.offset >= end,
                        "invalid index child order/depth"
                    );
                    end = child
                        .offset
                        .checked_add(child.length)
                        .context("index child range overflow")?;
                    payload_bytes = payload_bytes
                        .checked_add(child.payload_bytes)
                        .context("index payload byte overflow")?;
                    rows = rows.checked_add(child.rows).context("index row count overflow")?;
                    blocks =
                        blocks.checked_add(child.blocks).context("index block count overflow")?;
                    max_weight = max_weight.max(child.max_weight);
                }
            }
        }
        ensure!(
            rows == address.rows
                && blocks == address.blocks
                && max_weight == address.max_weight
                && payload_bytes == address.payload_bytes,
            "index structure statistics mismatch"
        );
        Ok(())
    }
}

pub(super) async fn warm<K: BatchData, V: BatchData>(
    cache: &Cache,
    root: &Root<(K, V)>,
    packed: &[u8],
) -> Result<()> {
    if root.inline.is_some() {
        return Ok(());
    }
    let mut pending = root.node.clone().into_iter().collect::<Vec<_>>();
    while let Some(address) = pending.pop() {
        super::work::spend(0).await;
        let start = usize::try_from(address.offset)?;
        let end = start
            .checked_add(usize::try_from(address.length)?)
            .context("generated index range overflow")?;
        let bytes = packed.get(start..end).context("missing generated index node")?;
        super::work::spend(bytes.len()).await;
        cache.frames.insert(&address.hash, address.length, std::sync::Arc::new(bytes.to_vec()))?;
        if address.depth == 0 {
            continue;
        }
        let decoded = super::compression::decode(bytes)?;
        super::work::spend(decoded.len()).await;
        let node: std::sync::Arc<Node<(K, V)>> =
            std::sync::Arc::new(serde_json::from_slice(&decoded)?);
        if let Node::Branch(children) = node.as_ref() {
            pending.extend(children.iter().cloned());
        }
        let pack_bytes = root.pack.as_ref().context("missing generated index pack")?.bytes;
        let node = std::sync::Arc::new(Authenticated::new(node, &address, pack_bytes)?);
        cache.insert(&address.hash, address.length, node, decoded.len())?;
    }
    Ok(())
}

pub(super) fn ordinal_child<T: Clone>(
    children: &[Address<T>],
    relative: &mut usize,
) -> Result<Address<T>> {
    for child in children {
        if *relative < child.blocks {
            return Ok(child.clone());
        }
        *relative -= child.blocks;
    }
    anyhow::bail!("missing ordinal index child")
}
pub(super) fn seek_child<T: Clone>(
    children: &[Address<T>],
    before: &impl Fn(&T) -> bool,
    ordinal: &mut usize,
) -> Option<Address<T>> {
    let position = children.partition_point(|child| before(&child.last));
    *ordinal += children[..position].iter().map(|child| child.blocks).sum::<usize>();
    children.get(position).cloned()
}

async fn append_level<T: Ord + Clone + Serialize + Send + Sync>(
    layer: &[Address<T>],
    bytes: &mut Vec<u8>,
) -> Result<Vec<Address<T>>> {
    let groups = layer.len().div_ceil(FANOUT);
    let width = layer.len() / groups;
    let remainder = layer.len() % groups;
    let mut next = Vec::new();
    let mut offset = 0;
    for group in 0..groups {
        let end = offset + width + usize::from(group < remainder);
        super::work::spend(0).await;
        next.push(append(&Node::Branch(layer[offset..end].to_vec()), bytes).await?);
        offset = end;
    }
    Ok(next)
}

pub(super) fn inline_identity(node: &impl Serialize) -> Result<(u64, String)> {
    // GC has no application tuple types. Canonical JSON object order makes the
    // inline identity identical for typed nodes and their opaque JSON views.
    let bytes = bounded::encode(&serde_json::to_value(node)?)?;
    Ok((u64::try_from(bytes.len())?, hash(&bytes)))
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn scan_retains_parent_navigation_after_foreground_eviction() -> Result<()> {
        let parent = std::sync::Arc::new(Cache::new(16384, 1));
        let child = |value| Address {
            offset: value,
            length: 1,
            hash: "0".repeat(64),
            first: value,
            last: value,
            blocks: 1,
            rows: 1,
            payload_bytes: 1,
            max_weight: 1,
            depth: 0,
        };
        let raw = std::sync::Arc::new(Node::Branch(vec![child(0), child(1)]));
        let mut address = raw.summary()?;
        address.offset = 2;
        address.length = 1;
        address.hash = "0".repeat(64);
        let node = std::sync::Arc::new(Authenticated::new(raw, &address, 3)?);
        parent.insert("old", 1, node.clone(), 32)?;
        let scan = Cache::scan(parent.clone());
        let initial = scan.get::<u64>("old", 1)?.context("missing shared navigation")?;
        assert!(std::sync::Arc::ptr_eq(&initial, &node));
        parent.insert("new", 1, node.clone(), 32)?;
        assert!(parent.get::<u64>("old", 1)?.is_none());
        let retained = scan.get::<u64>("old", 1)?.context("lost private navigation")?;
        assert!(std::sync::Arc::ptr_eq(&retained, &node));
        let stats = scan.stats()?;
        assert_eq!(stats.entries, 1);
        assert!(stats.bytes <= scan.bytes);
        assert_eq!(stats.reads, 0);
        Ok(())
    }
    #[tokio::test]
    async fn every_branch_is_balanced_including_a_singleton_tail() -> Result<()> {
        for count in [2_usize, 32, 33, 1025] {
            let layer = (0..count)
                .map(|ordinal| {
                    Ok(Address {
                        offset: u64::try_from(ordinal)?,
                        length: 1,
                        hash: "0".repeat(64),
                        first: ordinal,
                        last: ordinal,
                        blocks: 1,
                        rows: 1,
                        payload_bytes: 1,
                        max_weight: 1,
                        depth: 0,
                    })
                })
                .collect::<Result<Vec<_>>>()?;
            let mut bytes = vec![0; count];
            let parents = append_level(&layer, &mut bytes).await?;
            assert_eq!(parents.iter().map(|parent| parent.blocks).sum::<usize>(), count);
            assert!(parents.iter().all(|parent| (2..=FANOUT).contains(&parent.blocks)));
        }
        Ok(())
    }
    #[test]
    fn authenticated_branch_structure_rejects_duplicate_children_and_false_certificates()
    -> Result<()> {
        let child = Address {
            offset: 0,
            length: 1,
            hash: "0".repeat(64),
            first: 0,
            last: 0,
            blocks: 1,
            rows: 1,
            payload_bytes: 1,
            max_weight: 1,
            depth: 0,
        };
        let mut other = child.clone();
        other.offset = 1;
        other.first = 1;
        other.last = 1;
        let parent = Address {
            offset: 2,
            length: 1,
            hash: "0".repeat(64),
            first: 0,
            last: 1,
            blocks: 2,
            rows: 2,
            payload_bytes: 2,
            max_weight: 1,
            depth: 1,
        };
        Node::Branch(vec![child.clone(), other.clone()]).validate(&parent, 3)?;
        assert!(
            Node::Branch(vec![child.clone(), child.clone()])
                .validate_structure(&parent, 3)
                .is_err()
        );
        other.depth = 1;
        assert!(
            Node::Branch(vec![child.clone(), other.clone()])
                .validate_structure(&parent, 3)
                .is_err()
        );
        other.depth = 0;
        other.rows = 2;
        assert!(Node::Branch(vec![child, other]).validate_structure(&parent, 3).is_err());
        Ok(())
    }
}
