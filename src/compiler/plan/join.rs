//! Logical joins decoded from their execution scaffolding for all rewrite passes.
use super::graph::Graph;
use crate::compiler::{ColumnRef, relational::Node};
use anyhow::Result;
use std::collections::BTreeSet;

pub(super) struct Side {
    pub(super) id: String,
    pub(super) input: String,
    pub(super) keys: Vec<ColumnRef>,
}
impl Side {
    fn at(graph: &Graph<'_>, id: &str) -> Result<Self> {
        let (input, keys) = match graph.node(id)? {
            Node::EquijoinKey { input, keys, .. } => (input, keys.clone()),
            Node::CrossKey { input, .. } => (input, Vec::new()),
            Node::LeftJoinInput { input, spec, .. } => (input, spec.keys.clone()),
            Node::RightPresence { input, .. } | Node::RightCounts { input, .. } => {
                return Self::at(graph, input);
            }
            _ => anyhow::bail!("missing logical join input"),
        };
        Ok(Self { id: id.into(), input: input.clone(), keys })
    }
    pub(super) fn columns(&self, graph: &Graph<'_>) -> Result<BTreeSet<String>> {
        graph.columns(&self.input)
    }
}

pub(super) struct Join {
    pub(super) node: Node,
    pub(super) left: Side,
    pub(super) right: Side,
}
impl Join {
    pub(super) fn at(graph: &Graph<'_>, id: &str) -> Result<Option<Self>> {
        let node = graph.node(id)?;
        let (left, right) = match node {
            Node::Join { left, right, .. } => (left, right),
            Node::LeftJoin { input, .. }
            | Node::Existence { input, .. }
            | Node::Mark { input, .. } => {
                let Node::Union { left, right, .. } = graph.node(input)? else {
                    anyhow::bail!("missing tagged join union")
                };
                (left, right)
            }
            _ => return Ok(None),
        };
        Ok(Some(Self {
            node: node.clone(),
            left: Side::at(graph, left)?,
            right: Side::at(graph, right)?,
        }))
    }
    /// Follow one uniquely owned input through its count/tag execution scaffolding.
    pub(super) fn above(graph: &Graph<'_>, key: &str) -> Result<Option<(Self, bool)>> {
        let mut edge = key;
        while let Some(owner) = graph.only_consumer(edge) {
            if matches!(
                owner,
                Node::RightPresence { .. } | Node::RightCounts { .. } | Node::Union { .. }
            ) {
                edge = owner.id();
                continue;
            }
            let Some(join) = Self::at(graph, owner.id())? else { return Ok(None) };
            if join.left.id == key {
                return Ok(Some((join, true)));
            }
            if join.right.id == key {
                return Ok(Some((join, false)));
            }
            return Ok(None);
        }
        Ok(None)
    }
}
