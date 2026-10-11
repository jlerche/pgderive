//! Logical row uniqueness for positional SQL windows, separate from bag identity.
use crate::{
    compiler::{
        partition::Mode,
        relational::{Node, Relational, field},
    },
    source::Contract,
};
use anyhow::{Context, Result};
use std::collections::{BTreeMap, BTreeSet};
type Identity = Option<BTreeSet<String>>;
pub(super) fn keys(ir: &Relational, contract: &Contract) -> Result<Identity> {
    let mut identities = BTreeMap::<String, Identity>::new();
    let mut result = None;
    for node in &ir.nodes {
        let (id, identity) = infer(node, ir, contract, &identities)?;
        result.clone_from(&identity);
        identities.insert(id.to_owned(), identity);
    }
    Ok(result)
}
fn prior(input: &str, identities: &BTreeMap<String, Identity>) -> Result<Identity> {
    identities.get(input).cloned().context("missing occurrence input")
}
fn infer<'a>(
    node: &'a Node,
    ir: &Relational,
    contract: &Contract,
    identities: &BTreeMap<String, Identity>,
) -> Result<(&'a str, Identity)> {
    let (id, identity) = match node {
        Node::Source { id, source } => {
            let source_spec = ir.sources.get(*source).context("missing occurrence source")?;
            let relation = contract
                .relations
                .iter()
                .find(|relation| {
                    relation.schema == source_spec.schema && relation.table == source_spec.table
                })
                .context("missing occurrence relation")?;
            let keys = relation
                .columns
                .iter()
                .filter(|column| column.primary)
                .map(|column| field(*source, &column.name))
                .collect::<BTreeSet<_>>();
            (id, (!keys.is_empty()).then_some(keys))
        }
        Node::Rebind { id, input, fields } => {
            let remapped = prior(input, identities)?.and_then(|keys| {
                keys.into_iter()
                    .map(|key| {
                        fields
                            .iter()
                            .find(|binding| binding.input.name == key)
                            .map(|binding| binding.output.name.clone())
                    })
                    .collect()
            });
            (id, remapped)
        }
        Node::Join { id, left, right } => {
            let combined = prior(left, identities)?.zip(prior(right, identities)?).and_then(
                |(mut left, right)| {
                    if !left.is_disjoint(&right) {
                        return None;
                    }
                    left.extend(right);
                    Some(left)
                },
            );
            (id, combined)
        }
        Node::Partition { id, input, spec } | Node::Statistics { id, input, spec } => {
            let identity = if let Mode::Grouped { keys } = &spec.mode {
                Some(keys.iter().map(|column| column.name.clone()).collect())
            } else {
                prior(input, identities)?
            };
            (id, identity)
        }
        Node::Expand { id, input, .. } => {
            let identity = prior(input, identities)?
                .filter(|keys| !keys.contains(crate::compiler::expansion::FIELD))
                .map(|mut keys| {
                    keys.insert(crate::compiler::expansion::FIELD.into());
                    keys
                });
            (id, identity)
        }
        Node::Union { id, .. }
        | Node::Lookup { id, .. }
        | Node::DistinctInput { id, .. }
        | Node::DistinctCounts { id, .. }
        | Node::DistinctMembership { id, .. } => (id, None),
        Node::Filter { id, input, .. }
        | Node::Map { id, input, .. }
        | Node::KeyBy { id, input, .. }
        | Node::CrossKey { id, input }
        | Node::EquijoinKey { id, input, .. }
        | Node::LookupInput { id, input, .. }
        | Node::Project { id, input }
        | Node::Output { id, input }
        | Node::PartitionBy { id, input, .. }
        | Node::Finalize { id, input, .. } => (id, prior(input, identities)?),
    };
    Ok((id, identity))
}
