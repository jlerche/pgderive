//! Resolve the labels exposed by an independently bound SQL scope.
use super::bind as bind_inner;
use crate::{
    compiler::{
        ColumnRef,
        projection::OutputColumn,
        relational::{Binding, Node, Relational, field},
    },
    source::Contract,
};
use anyhow::{Context, Result, ensure};
pub(super) fn resolve_output(
    name: &crate::compiler::parser::Name,
    alias: &str,
    columns: &[OutputColumn],
) -> Result<ColumnRef> {
    let label = match name.0.as_slice() {
        [label] => label,
        [qualifier, label] if qualifier == alias => label,
        _ => anyhow::bail!("unknown derived qualifier"),
    };
    let mut candidates = columns.iter().filter(|column| column.label == *label);
    let candidate = candidates.next().context("unknown derived column")?;
    ensure!(candidates.next().is_none(), "ambiguous derived column");
    Ok(candidate.column.clone())
}

pub(super) struct Scope {
    pub(super) alias: String,
    pub(super) ir: Relational,
}
pub(super) fn scope(
    (parsed, alias): (Box<crate::compiler::parser::Parsed>, String),
    prefix: &str,
    offset: usize,
    contract: &Contract,
) -> Result<Scope> {
    let compiled = bind_inner(*parsed, contract)?;
    let mut ir = compiled.relational;
    let Some(Node::Output { input, .. }) = ir.nodes.pop() else {
        anyhow::bail!("scoped join requires final relational Output");
    };
    for node in &mut ir.nodes {
        node.rewrite(&|id| format!("{prefix}{id}"), &|edge| format!("{prefix}{edge}"));
    }
    let fields = ir
        .output
        .columns
        .iter()
        .enumerate()
        .map(|(index, output)| {
            let mut target = output.column.clone();
            target.name = format!("@{prefix}{index}");
            Binding { input: output.column.clone(), output: target }
        })
        .collect::<Vec<_>>();
    ir.output.columns = ir
        .output
        .columns
        .into_iter()
        .zip(&fields)
        .map(|(column, field)| OutputColumn { column: field.output.clone(), label: column.label })
        .collect();
    ir.nodes.push(Node::Rebind {
        id: format!("{prefix}visible"),
        input: format!("{prefix}{input}"),
        fields,
    });
    adapt_sources(&mut ir, offset, contract)?;
    Ok(Scope { alias, ir })
}
fn adapt_sources(ir: &mut Relational, offset: usize, contract: &Contract) -> Result<()> {
    let sources = ir
        .nodes
        .iter()
        .enumerate()
        .filter_map(|(position, node)| {
            if let Node::Source { id, source } = node {
                Some((position, id.clone(), *source))
            } else {
                None
            }
        })
        .collect::<Vec<_>>();
    // Reverse insertion keeps recorded source positions valid.
    for (position, id, local) in sources.into_iter().rev() {
        let global = local.checked_add(offset).context("source index overflow")?;
        let Node::Source { source, .. } = &mut ir.nodes[position] else {
            anyhow::bail!("missing scoped source")
        };
        *source = global;
        if global == local {
            continue;
        }
        let source = ir.sources.get(local).context("missing scoped source metadata")?;
        let relation = contract
            .relations
            .iter()
            .find(|relation| relation.schema == source.schema && relation.table == source.table)
            .context("unknown scoped source")?;
        let fields = relation
            .columns
            .iter()
            .map(|column| {
                let input = ColumnRef {
                    name: field(global, &column.name),
                    right: false,
                    oid: column.oid,
                    nullable: column.nullable,
                };
                let mut output = input.clone();
                output.name = field(local, &column.name);
                Binding { input, output }
            })
            .collect();
        // Nested remaps strictly increase source indices; bind both boundaries into the ID.
        let mapped = format!("{id}_local_{global}_{local}");
        for node in &mut ir.nodes {
            node.rewrite(&|id| id.into(), &|edge| {
                if edge == id { mapped.clone() } else { edge.into() }
            });
        }
        ir.nodes.insert(position + 1, Node::Rebind { id: mapped, input: id, fields });
    }
    Ok(())
}
