use super::{
    ObjectRef,
    format::hash,
    index::{Address, Node, Root},
};
use anyhow::{Context as _, Result, ensure};
use object_store::{ObjectStore, ObjectStoreExt as _, path::Path};
use std::{collections::BTreeSet, sync::Arc};

impl ObjectRef {
    /// Authenticate the complete index tree and discover immutable storage dependencies.
    /// A failed read forbids physical sweep.
    ///
    /// # Errors
    /// Rejects missing/corrupt roots, incompatible indexes or invalid child addresses.
    pub async fn dependencies(&self, store: &Arc<dyn ObjectStore>) -> Result<Vec<String>> {
        let bytes = self.index_bytes(store).await?;
        let root: Root<serde_json::Value> = serde_json::from_slice(&bytes)?;
        ensure!(root.schema == self.schema(), "invalid reachability root schema");
        if let Some(inline) = root.inline {
            ensure!(root.pack.is_none(), "inline index retains external pack");
            return self
                .inline_dependencies(inline, &root.node.context("inline index lacks summary")?);
        }
        let (pack, node) = match (root.pack, root.node) {
            (None, None) => return Ok(Vec::new()),
            (Some(pack), Some(node)) => (pack, node),
            _ => anyhow::bail!("incomplete reachability root"),
        };
        pack.validate()?;
        ensure!(
            node.offset.checked_add(node.length) == Some(pack.bytes),
            "invalid reachability root range"
        );
        let path = self.child_path(&pack.path);
        let packed = store.get_range(&Path::from(path.clone()), 0..pack.bytes).await?;
        ensure!(
            u64::try_from(packed.len())? == pack.bytes && hash(&packed) == pack.hash,
            "reachability index pack checksum/length mismatch"
        );
        let mut dependencies = BTreeSet::from([path]);
        let mut pending = vec![node];
        let mut examined = 0_u64;
        while let Some(address) = pending.pop() {
            examined =
                examined.checked_add(address.length).context("index traversal byte overflow")?;
            ensure!(examined <= pack.bytes, "index traversal exceeds physical metadata");
            address.validate(pack.bytes)?;
            let start = usize::try_from(address.offset)?;
            let end = start
                .checked_add(usize::try_from(address.length)?)
                .context("reachability node range overflow")?;
            let bytes = packed.get(start..end).context("reachability node exceeds packed index")?;
            ensure!(hash(bytes) == address.hash, "reachability index page checksum mismatch");
            let node: Node<serde_json::Value> =
                serde_json::from_slice(&super::compression::decode(bytes)?)?;
            node.validate_structure(&address, pack.bytes)?;
            match node {
                Node::Branch(children) => pending.extend(children),
                Node::Leaf(leaves) => dependencies
                    .extend(leaves.into_iter().map(|leaf| self.child_path(&leaf.pack.path))),
            }
        }
        ensure!(examined == pack.bytes, "index contains unreachable metadata bytes");
        Ok(dependencies.into_iter().collect())
    }
    fn inline_dependencies(
        &self,
        inline: Node<serde_json::Value>,
        address: &Address<serde_json::Value>,
    ) -> Result<Vec<String>> {
        let (length, digest) = super::index::inline_identity(&inline)?;
        ensure!(
            address.offset == 0
                && address.length <= 16 * 1024
                && length == address.length
                && digest == address.hash,
            "invalid inline index identity"
        );
        inline.validate_structure(address, address.length)?;
        let Node::Leaf(leaves) = inline else {
            anyhow::bail!("inline index is not a leaf");
        };
        Ok(leaves
            .into_iter()
            .map(|leaf| self.child_path(&leaf.pack.path))
            .collect::<BTreeSet<_>>()
            .into_iter()
            .collect())
    }
}
