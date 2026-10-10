//! Compose typed SQL scopes without merging their state or predicate placement.
use super::{Body, Stage, build};
use crate::{
    compiler::{
        Compiled, Predicate, Program,
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
    let Program::Relational { relational } = &mut compiled.program else {
        anyhow::bail!("derived partition requires relational lowering");
    };
    ensure!(
        relational.output.terminal.is_none(),
        "derived scope cannot consume sink-deferred expressions"
    );
    let source = relational.sources.first().context("missing native source")?;
    let relation = contract
        .relations
        .iter()
        .find(|relation| relation.schema == source.schema && relation.table == source.table)
        .context("missing native source contract")?;
    let resolve = |name: &crate::compiler::parser::Name| {
        crate::compiler::bind::derived::resolve_output(name, alias, &relational.output.columns)
    };
    let mut outer = build(
        body,
        &resolve,
        Stage {
            relation,
            aggregate_base: aggregate_base(relational)?,
            scalar_scope: Some(relational.nodes.len()),
        },
    )?;
    let Program::Relational { relational: stage } = &mut outer.program else {
        anyhow::bail!("missing partition stage");
    };
    validate_occurrences(relational, stage)?;
    lower_source_filter(relational, compiled.predicates.take())?;
    compose(relational, stage, outer.predicates.take())?;
    let revision = compiled.revision.as_mut().context("missing inner compiler revision")?;
    revision.push_str(":derived-partition-composition-v1:");
    revision.push_str(outer.revision.as_deref().context("missing stage revision")?);
    Ok(compiled)
}
fn aggregate_base(relational: &Relational) -> Result<usize> {
    let mut base = 0;
    for node in &relational.nodes {
        if let Node::Partition { spec, .. }
        | Node::Statistics { spec, .. }
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
fn validate_occurrences(inner: &Relational, outer: &Relational) -> Result<()> {
    use crate::compiler::partition::Mode;
    let positional = outer.nodes.iter().any(|node| matches!(node, Node::Partition { spec, .. }
        if matches!(spec.mode, Mode::Rows { .. }) || spec.aggregates.iter().any(|value| value.function.occurrence_order())));
    ensure!(
        !positional
            || !inner.nodes.iter().any(|node| matches!(
                node,
                Node::Expand { .. }
                    | Node::Partition {
                        spec: crate::compiler::partition::Partition {
                            mode: Mode::Grouped { .. },
                            ..
                        },
                        ..
                    }
                    | Node::Statistics { .. }
            )),
        "derived positional windows require preserved native occurrence identity"
    );
    Ok(())
}
fn lower_source_filter(
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
fn compose(
    inner: &mut Relational,
    outer: &mut Relational,
    expression: Option<crate::compiler::expression::Expr>,
) -> Result<()> {
    let Some(Node::Output { input, .. }) = inner.nodes.pop() else {
        anyhow::bail!("missing inner output");
    };
    let prefix = format!("stage_{}_", inner.nodes.len());
    let input = if let Some(expression) = expression {
        let id = format!("{prefix}where");
        inner.nodes.push(Node::Filter {
            id: id.clone(),
            input,
            predicate: Predicate::new(expression),
        });
        id
    } else {
        input
    };
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
fn rewrite(node: &mut Node, rename: &impl Fn(&str) -> String, edge: &impl Fn(&str) -> String) {
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
        | Node::KeyBy { id, input, .. }
        | Node::LookupInput { id, input, .. }
        | Node::Lookup { id, input, .. }
        | Node::Project { id, input }
        | Node::Output { id, input }
        | Node::PartitionBy { id, input, .. }
        | Node::Partition { id, input, .. }
        | Node::Statistics { id, input, .. }
        | Node::Finalize { id, input, .. } => {
            *id = rename(id);
            *input = edge(input);
        }
    }
}
