//! Source-specific uniqueness proofs; never inferred from generic DBSP keys.
use crate::{
    compiler::{
        partition::{Function, Mode},
        relational::{Node, Relational, field},
    },
    source::Contract,
};
use anyhow::{Context, Result};
use std::collections::{BTreeMap, BTreeSet};

/// Only production snapshot/committed-CDC execution may use these proofs.
/// A complete source PK retained in the row proves tuple multiplicity is 0/1,
/// even when navigation keys change. Rebind may discard other columns safely.
/// Unrecognized operators terminate the proof rather than weakening validation.
pub(super) fn statistics(contract: &Contract, ir: &Relational) -> Result<BTreeSet<String>> {
    contract.validate()?;
    let mut keys = BTreeMap::<String, BTreeSet<String>>::new();
    let mut certified = BTreeSet::new();
    for node in &ir.nodes {
        let (id, identity) = match node {
            Node::Source { id, source } => (id, Some(source_key(contract, ir, *source)?)),
            Node::Filter { id, input, .. }
            | Node::Map { id, input, .. }
            | Node::PartitionBy { id, input, .. }
            | Node::CrossKey { id, input }
            | Node::EquijoinKey { id, input, .. } => (id, keys.get(input).cloned()),
            Node::Rebind { id, input, fields } => {
                let mapped = keys.get(input).and_then(|identity| {
                    identity
                        .iter()
                        .map(|name| {
                            fields
                                .iter()
                                .find(|binding| &binding.input.name == name)
                                .map(|binding| binding.output.name.clone())
                        })
                        .collect::<Option<BTreeSet<_>>>()
                        .filter(|mapped| mapped.len() == identity.len())
                });
                (id, mapped)
            }
            Node::Statistics { id, input, spec } => {
                if keys.contains_key(input)
                    && matches!(spec.mode, Mode::Grouped { .. })
                    && spec.aggregates.iter().all(|aggregate| {
                        matches!(
                            aggregate.function,
                            Function::Count | Function::Sum | Function::Average
                        ) && aggregate.argument.as_ref().is_none_or(|argument| argument.oid != 1700)
                    })
                {
                    certified.insert(id.clone());
                }
                continue;
            }
            _ => continue,
        };
        if let Some(identity) = identity {
            keys.insert(id.clone(), identity);
        }
    }
    Ok(certified)
}

fn source_key(contract: &Contract, ir: &Relational, ordinal: usize) -> Result<BTreeSet<String>> {
    let source = ir.sources.get(ordinal).context("missing source-key relation")?;
    let relation = contract
        .relations
        .iter()
        .find(|relation| relation.schema == source.schema && relation.table == source.table)
        .context("source-key relation differs from contract")?;
    Ok(relation
        .columns
        .iter()
        .filter(|column| column.primary)
        .map(|column| field(ordinal, &column.name))
        .collect())
}
