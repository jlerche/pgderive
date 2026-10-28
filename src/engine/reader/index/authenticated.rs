//! Validated immutable nodes retain intrinsic checks across caller contexts.
use super::{Address, Node};
use anyhow::{Context as _, Result, ensure};
use serde::Serialize;
use std::sync::Arc;

#[derive(Debug, Clone)]
pub(in crate::engine::reader) struct Authenticated<T> {
    node: Arc<Node<T>>,
    summary: Address<T>,
    child_end: u64,
    charge: usize,
}
impl<T: Ord + Clone + Serialize> Authenticated<T> {
    // Callers authenticate encoded content (or inline root identity) first.
    pub(in crate::engine::reader) fn new(
        node: Arc<Node<T>>,
        address: &Address<T>,
        pack_bytes: u64,
    ) -> Result<Self> {
        node.validate(address, pack_bytes)?;
        let child_end = match node.as_ref() {
            Node::Leaf(_) => 0,
            Node::Branch(children) => children.iter().try_fold(0, |end, child| {
                Ok::<_, anyhow::Error>(end.max(
                    child.offset.checked_add(child.length).context("index child end overflow")?,
                ))
            })?,
        };
        let charge = super::bounded::encode(address)?
            .len()
            .checked_mul(2)
            .and_then(|bytes| bytes.checked_add(std::mem::size_of::<Self>()))
            .context("authenticated index charge overflow")?;
        Ok(Self { node, summary: address.clone(), child_end, charge })
    }
    // Content identity alone does not authenticate a parent's summary or placement.
    pub(in crate::engine::reader) fn checked(
        &self,
        address: &Address<T>,
        pack_bytes: u64,
    ) -> Result<Arc<Node<T>>> {
        address.validate(pack_bytes)?;
        let expected = &self.summary;
        ensure!(
            address.hash == expected.hash
                && address.length == expected.length
                && address.first == expected.first
                && address.last == expected.last
                && address.blocks == expected.blocks
                && address.rows == expected.rows
                && address.payload_bytes == expected.payload_bytes
                && address.max_weight == expected.max_weight
                && address.depth == expected.depth,
            "authenticated index context mismatch"
        );
        ensure!(self.child_end <= address.offset, "index children exceed caller offset");
        Ok(self.node.clone())
    }
    pub(in crate::engine::reader) fn node(&self) -> &Node<T> {
        &self.node
    }
    pub(in crate::engine::reader) const fn charge(&self) -> usize {
        self.charge
    }
    #[cfg(test)]
    pub(in crate::engine::reader) fn test_node_mut(&mut self) -> &mut Node<T> {
        Arc::make_mut(&mut self.node)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    type Branch = (Arc<Node<u64>>, Address<u64>);
    fn branch() -> Result<Branch> {
        let child = |key| Address {
            offset: key,
            length: 1,
            hash: "0".repeat(64),
            first: key,
            last: key,
            blocks: 1,
            rows: 1,
            payload_bytes: 1,
            max_weight: 1,
            depth: 0,
        };
        let node = Arc::new(Node::Branch(vec![child(0), child(1)]));
        let mut address = node.summary()?.into_owned();
        address.offset = 2;
        address.length = 1;
        address.hash = "1".repeat(64);
        Ok((node, address))
    }
    #[test]
    fn reuse_checks_every_parent_claim_and_relocated_child_bounds() -> Result<()> {
        let (node, address) = branch()?;
        let authenticated = Authenticated::new(node.clone(), &address, 3)?;
        for _ in 0..100 {
            assert!(Arc::ptr_eq(&node, &authenticated.checked(&address, 3)?));
        }
        let mut missing = serde_json::to_value(&address)?;
        missing.as_object_mut().context("address object")?.remove("payload_bytes");
        assert!(serde_json::from_value::<Address<u64>>(missing).is_err());
        let mut forged = address.clone();
        forged.payload_bytes += 1;
        assert!(node.validate(&forged, 3).is_err());
        assert!(node.validate_structure(&forged, 3).is_err());
        let mut overflow = node.as_ref().clone();
        if let Node::Branch(children) = &mut overflow {
            children[0].payload_bytes = u64::MAX;
        }
        assert!(overflow.summary().is_err());
        assert!(overflow.validate_structure(&address, 3).is_err());
        let mut relocated = address.clone();
        relocated.offset = 10;
        assert!(authenticated.checked(&relocated, 11).is_ok());
        relocated.offset = 1;
        assert!(authenticated.checked(&relocated, 11).is_err());
        assert!(authenticated.checked(&address, 2).is_err());
        for field in 0..9 {
            let mut wrong = address.clone();
            match field {
                0 => wrong.first += 1,
                1 => wrong.last += 1,
                2 => wrong.rows += 1,
                3 => wrong.blocks += 1,
                4 => wrong.depth += 1,
                5 => wrong.max_weight += 1,
                6 => wrong.length += 1,
                7 => wrong.payload_bytes += 1,
                _ => wrong.hash = "2".repeat(64),
            }
            assert!(authenticated.checked(&wrong, 100).is_err());
        }
        Ok(())
    }
    #[test]
    fn certification_rejects_malformed_intrinsics_before_cache_admission() -> Result<()> {
        let (mut node, address) = branch()?;
        let Node::Branch(children) = Arc::make_mut(&mut node) else {
            anyhow::bail!("expected branch fixture");
        };
        children[0].last = 10;
        assert!(Authenticated::new(node, &address, 3).is_err());
        Ok(())
    }
}
