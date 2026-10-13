//! Resolve derived output labels, then filter after the inner partition.
use super::{Compiled, bind};
use crate::{
    compiler::{
        ColumnRef, Predicate, Program, parser::derived::Derived, projection::OutputColumn,
        relational::Node,
    },
    source::Contract,
};
use anyhow::{Context, Result, ensure};
pub(super) fn bind_derived(parsed: Derived, contract: &Contract) -> Result<Compiled> {
    let mut compiled = bind(*parsed.inner, contract)?;
    compose_join(&mut compiled)?;
    let Program::Relational { relational } = &mut compiled.program else {
        anyhow::bail!("derived query requires relational lowering");
    };
    ensure!(
        relational.output.terminal.is_none(),
        "derived scope cannot consume sink-deferred expressions"
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
        anyhow::bail!("derived query requires terminal output node");
    };
    let (input, suffix) = if let Some(predicate) = predicate {
        let optimized = super::topk::specialize(relational, &input, &predicate);
        if optimized {
            compiled
                .revision
                .as_mut()
                .context("missing compiler revision")?
                .push_str(":pg-ordered-occurrence-topk-v1");
        }
        let count =
            relational.nodes.iter().filter(|node| matches!(node, Node::Filter { .. })).count();
        let filter = if count == 0 { "qualified".into() } else { format!("qualified_{count}") };
        relational.nodes.push(Node::Filter { id: filter.clone(), input, predicate });
        (filter, ":derived-scope-filter-v1")
    } else {
        (input, ":derived-scope-projection-v1")
    };
    relational.nodes.push(Node::Output { id, input });
    relational.output.terminal = super::terminal(transforms, &columns)?;
    relational.output.columns = columns;
    let revision = compiled.revision.as_mut().context("missing inner compiler revision")?;
    revision.push_str(suffix);
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

/// Retain joined full tuples internally; only exposed labels enter name resolution.
pub(super) fn compose_join(compiled: &mut Compiled) -> Result<()> {
    let Program::Relational { relational } = &mut compiled.program else {
        return Ok(());
    };
    let Some(Node::Project { id, input }) = relational.nodes.last() else {
        return Ok(());
    };
    ensure!(
        relational.nodes.iter().any(|node| matches!(node, Node::Join { .. })),
        "composable projection requires a joined input"
    );
    ensure!(
        relational.output.terminal.is_none(),
        "derived scope cannot consume sink-deferred expressions"
    );
    let id = id.clone();
    let mut input = input.clone();
    relational.nodes.pop();
    if let Some(expression) = compiled.predicates.take() {
        let filter = "joined_source_filter".to_string();
        relational.nodes.push(Node::Filter {
            id: filter.clone(),
            input,
            predicate: Predicate::new(expression),
        });
        input = filter;
    }
    relational.nodes.push(Node::Output { id, input });
    compiled
        .revision
        .as_mut()
        .context("missing join compiler revision")?
        .push_str(":joined-derived-full-tuple-v1");
    Ok(())
}
