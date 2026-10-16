//! Resolve derived output labels, then filter after the inner partition.
use super::{Compiled, bind};
use crate::{
    compiler::{
        ColumnRef, Predicate, parser::derived::Derived, projection::OutputColumn, relational::Node,
    },
    source::Contract,
};
use anyhow::{Context, Result, ensure};
pub(super) fn bind_derived(parsed: Derived, contract: &Contract) -> Result<Compiled> {
    let mut compiled = bind(*parsed.inner, contract)?;
    let relational = &mut compiled.relational;
    ensure!(
        relational
            .output
            .mapping
            .as_ref()
            .is_none_or(crate::compiler::output::Output::native_rendering),
        "derived scope cannot consume unfinalized aggregate encoding"
    );
    let resolve = |name: &crate::compiler::parser::Name| {
        resolve_output(name, &parsed.alias, &relational.output.columns)
    };
    let predicate = parsed
        .predicate
        .map(|value| crate::compiler::expression::bind(value, &resolve).map(Predicate::new))
        .transpose()?;
    let mut transforms = Vec::new();
    let columns = parsed
        .columns
        .into_iter()
        .map(|(name, label, transform)| {
            transforms.push(transform);
            Ok(OutputColumn { column: resolve(&name)?, label })
        })
        .collect::<Result<Vec<_>>>()?;
    let Some(Node::Output { id, input }) = relational.nodes.pop() else {
        anyhow::bail!("derived query requires final Output node");
    };
    let input = if let Some(predicate) = predicate {
        super::topk::specialize(relational, &input, &predicate);
        let count =
            relational.nodes.iter().filter(|node| matches!(node, Node::Filter { .. })).count();
        let filter = if count == 0 { "qualified".into() } else { format!("qualified_{count}") };
        relational.nodes.push(Node::Filter { id: filter.clone(), input, predicate });
        filter
    } else {
        input
    };
    relational.nodes.push(Node::Output { id, input });
    relational.output.mapping = super::output_mapping(transforms, &columns)?;
    relational.output.columns = columns;
    Ok(compiled)
}

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
