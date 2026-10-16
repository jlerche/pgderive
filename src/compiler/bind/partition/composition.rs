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
    ensure!(
        relational
            .output
            .mapping
            .as_ref()
            .is_none_or(crate::compiler::output::Output::native_rendering),
        "derived scope cannot consume unfinalized aggregate encoding"
    );
    let source = relational.sources.first().context("missing native source")?;
    let relation = contract
        .relations
        .iter()
        .find(|relation| relation.schema == source.schema && relation.table == source.table)
        .context("missing native source contract")?;
    let occurrence = super::occurrence::keys(relational, contract)?;
    let known_occurrence = occurrence.is_some();
    let occurrence_keys = occurrence.unwrap_or_default().into_iter().collect::<Vec<_>>();
    let resolve = |name: &crate::compiler::parser::Name| {
        crate::compiler::bind::derived::resolve_output(name, alias, &relational.output.columns)
    };
    let mut outer = build(
        body,
        &resolve,
        Stage {
            relation,
            occurrence_keys: &occurrence_keys,
            aggregate_base: aggregate_base(relational)?,
            scalar_scope: Some(relational.nodes.len()),
        },
    )?;
    let stage = &mut outer.relational;
    validate_occurrences(known_occurrence, stage)?;
    compose(relational, stage)?;
    Ok(compiled)
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
        rewrite(node, &|id| id.into(), &|input| {
            if input == source { filter.into() } else { input.into() }
        });
    }
    relational.nodes.insert(
        position + 1,
        Node::Filter { id: filter.into(), input: source, predicate: Predicate::new(expression) },
    );
    Ok(())
}
fn compose(inner: &mut Relational, outer: &mut Relational) -> Result<()> {
    let Some(Node::Output { input, .. }) = inner.nodes.pop() else {
        anyhow::bail!("missing inner output");
    };
    let prefix = format!("stage_{}_", inner.nodes.len());
    for mut node in std::mem::take(&mut outer.nodes) {
        if matches!(node, Node::Source { .. }) {
            continue;
        }
        rewrite(
            &mut node,
            &|id| if id == "project" { id.into() } else { format!("{prefix}{id}") },
            &|edge| if edge == "source" { input.clone() } else { format!("{prefix}{edge}") },
        );
        inner.nodes.push(node);
    }
    inner.output = outer.output.clone();
    Ok(())
}
pub(in crate::compiler::bind) fn rewrite(
    node: &mut Node,
    rename: &impl Fn(&str) -> String,
    edge: &impl Fn(&str) -> String,
) {
    match node {
        Node::Source { id, .. } => *id = rename(id),
        Node::Join { id, left, right } | Node::Union { id, left, right } => {
            *id = rename(id);
            *left = edge(left);
            *right = edge(right);
        }
        Node::Filter { id, input, .. }
        | Node::Expand { id, input, .. }
        | Node::Map { id, input, .. }
        | Node::Rebind { id, input, .. }
        | Node::CrossKey { id, input }
        | Node::EquijoinKey { id, input, .. }
        | Node::LeftJoinInput { id, input, .. }
        | Node::LeftJoin { id, input, .. }
        | Node::SetDistinct { id, input, .. }
        | Node::Existence { id, input, .. }
        | Node::LookupInput { id, input, .. }
        | Node::Lookup { id, input, .. }
        | Node::Output { id, input }
        | Node::PartitionBy { id, input, .. }
        | Node::Partition { id, input, .. }
        | Node::Statistics { id, input, .. }
        | Node::DistinctInput { id, input, .. }
        | Node::DistinctCounts { id, input, .. }
        | Node::DistinctMembership { id, input, .. }
        | Node::OrderedInput { id, input, .. }
        | Node::OrderedFirst { id, input, .. }
        | Node::TopKInput { id, input, .. }
        | Node::TopK { id, input, .. }
        | Node::OrderedGroup { id, input }
        | Node::OrderedFinalize { id, input, .. }
        | Node::Finalize { id, input, .. } => {
            *id = rename(id);
            *input = edge(input);
        }
    }
}
