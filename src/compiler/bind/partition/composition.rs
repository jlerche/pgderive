//! Compose typed SQL scopes without merging their state or predicate placement.
use super::{Body, Stage, build};
use crate::{
    compiler::{
        Compiled, Predicate,
        parser::Parsed,
        relational::{Node, Relational},
    },
    source::Contract,
};
use anyhow::{Context, Result, ensure};
pub(super) fn bind(
    inner: Parsed,
    alias: &str,
    body: &Body,
    contract: &Contract,
) -> Result<Compiled> {
    let mut compiled = crate::compiler::bind::bind(inner, contract)?;
    let relational = &mut compiled.relational;
    let resolve = |name: &crate::compiler::parser::Name| {
        crate::compiler::bind::derived::resolve_output(name, alias, &relational.output.columns)
    };
    let mut outer = stage(relational, body, &resolve, contract)?;
    compose(relational, &mut outer)?;
    Ok(compiled)
}
pub(super) fn stage(
    relational: &Relational,
    body: &Body,
    resolve: &impl Fn(&crate::compiler::parser::Name) -> Result<crate::compiler::ColumnRef>,
    contract: &Contract,
) -> Result<Relational> {
    let occurrence = crate::compiler::properties::occurrence(relational, contract)?;
    let known_occurrence = occurrence.unique_rows();

    let compiled = build(
        body,
        resolve,
        Stage {
            sources: Vec::new(),
            occurrence_keys: &occurrence,
            aggregate_base: aggregate_base(relational)?,
            scalar_scope: Some(relational.nodes.len()),
        },
    )?;
    validate_occurrences(known_occurrence, &compiled.relational)?;
    Ok(compiled.relational)
}
fn aggregate_base(relational: &Relational) -> Result<usize> {
    let mut base = 0;
    for node in &relational.nodes {
        if let Node::TopK { spec, .. } = node {
            let index: usize = spec
                .field
                .strip_prefix("@aggregate_")
                .context("invalid top-k aggregate field")?
                .parse()?;
            base = base.max(index.checked_add(1).context("aggregate field overflow")?);
        }
        if let Node::Partition { spec, .. }
        | Node::Statistics { spec, .. }
        | Node::OrderedFinalize { spec, .. }
        | Node::PrefixWindow { spec, .. }
        | Node::Finalize { spec, .. } = node
        {
            for value in &spec.aggregates {
                let index: usize = value
                    .field
                    .strip_prefix("@aggregate_")
                    .context("invalid aggregate field")?
                    .parse()?;
                base = base.max(index.checked_add(1).context("aggregate field overflow")?);
            }
        }
    }
    Ok(base)
}
fn validate_occurrences(known: bool, outer: &Relational) -> Result<bool> {
    use crate::compiler::partition::Mode;
    let positional = outer.nodes.iter().any(|node| matches!(node, Node::Partition { spec, .. }
        if matches!(spec.mode, Mode::Rows { .. }) || spec.aggregates.iter().any(|value| value.function.occurrence_order())));
    ensure!(
        !positional || known,
        "derived positional windows require proven logical occurrence identity"
    );
    Ok(positional)
}
pub(in crate::compiler::bind) fn lower_source_filter(
    relational: &mut Relational,
    expression: Option<crate::compiler::expression::Expr>,
) -> Result<()> {
    let Some(expression) = expression else {
        return Ok(());
    };
    let position = relational
        .nodes
        .iter()
        .position(|node| matches!(node, Node::Source { source: 0, .. }))
        .context("missing predicate source")?;
    let Node::Source { id, .. } = &relational.nodes[position] else {
        anyhow::bail!("invalid predicate source");
    };
    let source = id.clone();
    let filter = "scoped_source_filter";
    for node in &mut relational.nodes {
        node.rewrite(&|id| id.into(), &|input| {
            if input == source { filter.into() } else { input.into() }
        });
    }
    relational.nodes.insert(
        position + 1,
        Node::Filter { id: filter.into(), input: source, predicate: Predicate::new(expression) },
    );
    Ok(())
}
pub(super) fn compose(inner: &mut Relational, outer: &mut Relational) -> Result<()> {
    let Some(Node::Output { input, .. }) = inner.nodes.pop() else {
        anyhow::bail!("missing inner output");
    };
    for node in &outer.nodes {
        if let Node::Filter { input: source, predicate, .. } = node
            && source == "source"
        {
            super::super::topk::specialize(inner, &input, predicate);
        }
    }
    let prefix = format!("stage_{}_", inner.nodes.len());
    for mut node in std::mem::take(&mut outer.nodes) {
        if matches!(node, Node::Source { .. }) {
            continue;
        }
        node.rewrite(
            &|id| if id == "project" { id.into() } else { format!("{prefix}{id}") },
            &|edge| if edge == "source" { input.clone() } else { format!("{prefix}{edge}") },
        );
        inner.nodes.push(node);
    }
    inner.output = outer.output.clone();
    Ok(())
}
