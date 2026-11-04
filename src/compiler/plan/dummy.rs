//! A scope with a constant false or NULL filter reads nothing, as a dummy relation does.
use super::{graph::Graph, join::Join, not_null};
use crate::compiler::{
    Predicate,
    expression::Expr,
    relational::{Node, ZERO_LIMIT, barrier},
};
use anyhow::{Context, Result};
use std::collections::BTreeSet;

pub(super) fn run(graph: &mut Graph<'_>, defer: bool) -> Result<bool> {
    let mut changed = false;
    let computed =
        if defer { super::prune::computed_origin(graph, false) } else { BTreeSet::new() };
    for id in graph.ids(|node| matches!(node, Node::Filter { .. })) {
        changed |= filter(graph, &id, &computed)?;
    }
    for id in graph.ids(|node| matches!(node, Node::LeftJoin { .. })) {
        changed |= join(graph, &id)?;
    }
    Ok(changed)
}
fn folded(value: &Expr) -> Expr {
    value.fold().unwrap_or_else(|_| value.clone())
}
fn never_true(value: &Expr) -> bool {
    value.is_constant() && matches!(value.constant_truth(), Ok(truth) if truth != Some(true))
}
fn filter(graph: &mut Graph<'_>, id: &str, computed: &BTreeSet<String>) -> Result<bool> {
    let Node::Filter { input, predicate, .. } = graph.node(id)? else { return Ok(false) };
    let (input, values) = (input.clone(), predicate.clone().conjuncts());
    let mut folded = Vec::with_capacity(values.len());
    for value in &values {
        // What a computed column will substitute decides whether this can fold.
        if value.read_names().is_disjoint(computed) {
            folded.push(not_null::simplified(graph, &input, self::folded(value))?);
        } else {
            folded.push(value.clone());
        }
    }
    if folded.iter().any(never_true) {
        let scope = highest(graph, id)?;
        return empty_sources(graph, &scope);
    }
    let kept = folded.into_iter().filter(|value| !constant_true(value)).collect::<Vec<_>>();
    if kept == values {
        return Ok(false);
    }
    graph.keep(id, kept)?;
    Ok(true)
}
/// A constant false ON leaves nothing to match, so the nullable side is never read; a FULL
/// join keeps both sides, so only the constant remains of its condition.
fn join(graph: &mut Graph<'_>, id: &str) -> Result<bool> {
    let Some(join) = Join::at(graph, id)? else { return Ok(false) };
    let Node::LeftJoin { spec, .. } = join.node else { return Ok(false) };
    let Some(values) = spec.predicate.map(Predicate::conjuncts) else { return Ok(false) };
    if !values.iter().map(folded).any(|value| never_true(&value)) {
        return Ok(false);
    }
    if spec.full {
        let kept = values.iter().filter_map(|value| {
            value.fold().map_or_else(
                |_| Some(value.clone()),
                |folded| never_true(&folded).then_some(folded),
            )
        });
        let kept = kept.collect::<Vec<_>>();
        if kept == values {
            return Ok(false);
        }
        let Node::LeftJoin { spec, .. } = graph.node_mut(id)? else {
            anyhow::bail!("missing left join")
        };
        spec.predicate = Predicate::from_conjuncts(kept);
        return Ok(true);
    }
    empty_sources(graph, &join.right.input)
}
fn constant_true(value: &Expr) -> bool {
    value.is_constant() && matches!(value.constant_truth(), Ok(Some(true)))
}
/// Climb while an empty input makes the consumer empty too.
fn highest(graph: &Graph<'_>, id: &str) -> Result<String> {
    let mut at = id.to_owned();
    while let Some(next) = empties(graph, &at)? {
        at = next;
    }
    Ok(at)
}
fn empties(graph: &Graph<'_>, edge: &str) -> Result<Option<String>> {
    let Some(consumer) = graph.only_consumer(edge) else { return Ok(None) };
    let id = consumer.id();
    Ok(match consumer {
        Node::Filter { .. }
        | Node::Map { .. }
        | Node::EquijoinKey { .. }
        | Node::CrossKey { .. }
        | Node::Join { .. }
        | Node::SetTag { .. }
        | Node::Statistics { .. }
        | Node::Finalize { .. }
        | Node::Partition { .. }
        | Node::SetDistinct { .. }
        | Node::TopKInput { .. }
        | Node::TopK { .. }
        | Node::RightCounts { .. }
        | Node::Sorted { .. }
        | Node::RightPresence { .. } => Some(id.to_owned()),
        Node::PartitionBy { keys, .. } if !keys.is_empty() => Some(id.to_owned()),
        Node::Rebind { .. } if !barrier(id) => Some(id.to_owned()),
        Node::LeftJoinInput { .. } => tagged(graph, id, edge)?,
        Node::Union { left, right, .. } => tagged(graph, id, edge)?.or({
            let other = if left == edge { right } else { left };
            if emits_nothing(graph, other)? { Some(id.to_owned()) } else { None }
        }),
        _ => None,
    })
}
/// The preserved input of an outer join, or either input of a semijoin, empties the join.
fn tagged(graph: &Graph<'_>, id: &str, edge: &str) -> Result<Option<String>> {
    let union = match graph.node(id)? {
        Node::Union { id, .. } => id.as_str(),
        Node::LeftJoinInput { .. } => match graph.only_consumer(id) {
            Some(Node::Union { id, .. }) => id.as_str(),
            Some(Node::RightCounts { .. }) => return Ok(Some(id.to_owned())),
            _ => return Ok(None),
        },
        _ => return Ok(None),
    };
    let Some(owner) = graph.only_consumer(union) else { return Ok(None) };
    let Some(join) = Join::at(graph, owner.id())? else { return Ok(None) };
    let from_left = join.left.id == id || join.left.id == edge;
    let owner = join.node.id().to_owned();
    let emptied = match join.node {
        Node::LeftJoin { spec, .. } => !spec.full && from_left,
        Node::Existence { anti, .. } => from_left || !anti,
        Node::Mark { spec, .. } => from_left || spec.semijoin,
        _ => false,
    };
    Ok(emptied.then_some(owner))
}
fn silenced(graph: &Graph<'_>, source: &str) -> bool {
    graph.consumers(source).all(|node| matches!(node, Node::Empty { .. } | Node::Constant { .. }))
}
/// A constant emits its rows whether or not its source is silenced, so its scope is not empty.
fn emits_nothing(graph: &Graph<'_>, edge: &str) -> Result<bool> {
    let ancestors = graph.ancestors(edge)?;
    if ancestors.iter().any(|node| matches!(node, Node::Constant { .. })) {
        return Ok(false);
    }
    Ok(ancestors
        .iter()
        .filter(|node| matches!(node, Node::Source { .. }))
        .all(|node| silenced(graph, node.id())))
}
fn empty_sources(graph: &mut Graph<'_>, scope: &str) -> Result<bool> {
    let mut changed = false;
    for source in graph.sources(scope)? {
        if silenced(graph, &source) {
            continue;
        }
        let id = graph.fresh("empty");
        redirect_readers(graph, &source, &id);
        graph.ir.nodes.push(Node::Empty { id, input: source });
        changed = true;
    }
    Ok(changed)
}
/// A constant only needs its source to tick, so it keeps the source itself.
fn redirect_readers(graph: &mut Graph<'_>, source: &str, id: &str) {
    let rename = |edge: &str| if edge == source { id.to_owned() } else { edge.to_owned() };
    for node in graph.ir.nodes.iter_mut().filter(|node| !matches!(node, Node::Constant { .. })) {
        node.rewrite(&|id| id.into(), &rename);
    }
}
/// Fold what survived the rewrites; a failure here is the planning error `PostgreSQL` raises.
pub(super) fn fold_all(graph: &mut Graph<'_>) -> Result<()> {
    for node in &mut graph.ir.nodes {
        fold_node(node)?;
    }
    Ok(())
}
pub(super) fn fold_node(node: &mut Node) -> Result<()> {
    fold_condition(node)?;
    match node {
        Node::Map { computed, .. } => {
            for value in computed {
                value.expression = fold_computed(value)?;
            }
        }
        Node::Partition { spec, .. }
        | Node::Statistics { spec, .. }
        | Node::Finalize { spec, .. }
        | Node::OrderedFinalize { spec, .. }
        | Node::OrderedFirst { spec, .. }
        | Node::OrderedInput { spec, .. }
        | Node::DistinctInput { spec, .. }
        | Node::DistinctCounts { spec, .. }
        | Node::DistinctMembership { spec, .. }
        | Node::PrefixInput { spec, .. }
        | Node::PrefixStatistics { spec, .. }
        | Node::PrefixWindow { spec, .. } => {
            for aggregate in &mut spec.aggregates {
                fold_optional(&mut aggregate.filter)?;
                if let Some(expression) = &mut aggregate.expression {
                    *expression = expression.fold()?;
                }
            }
        }
        _ => {}
    }
    Ok(())
}
/// Preserve the separately planned aggregate argument even when FILTER folds to false.
pub(super) fn fold_computed(
    value: &crate::compiler::scalar::Computed,
) -> Result<crate::compiler::typed::Typed> {
    if value.column.name.starts_with(AGGREGATE_INPUT) {
        value.expression.fold_filtered()?;
    }
    value.expression.fold()
}
const AGGREGATE_INPUT: &str = "@aggregate_input_";
/// Conditions are planned even in a scope whose rows are never asked for.
pub(super) fn fold_condition(node: &mut Node) -> Result<()> {
    match node {
        Node::Filter { predicate, .. } => *predicate = predicate.fold()?,
        Node::LeftJoin { spec, .. } => fold_optional(&mut spec.predicate)?,
        Node::Mark { spec, .. } => {
            spec.comparison = spec.comparison.fold()?;
            fold_optional(&mut spec.correlation)?;
        }
        _ => {}
    }
    Ok(())
}
fn fold_optional(value: &mut Option<Predicate>) -> Result<()> {
    if let Some(predicate) = value {
        *predicate = predicate.fold()?;
    }
    Ok(())
}
/// LIMIT 0 was only planned; nothing beneath it runs or keeps state.
pub(super) fn discard_zero_limits(graph: &mut Graph<'_>) -> Result<()> {
    for id in graph.ids(|node| node.id().ends_with(ZERO_LIMIT)) {
        let Node::Filter { input, .. } = graph.node(&id)? else { continue };
        let input = input.clone();
        let source =
            graph.sources(&input)?.into_iter().next().context("LIMIT 0 reads no source")?;
        let empty = graph.fresh("empty");
        graph.ir.nodes.push(Node::Empty { id: empty.clone(), input: source });
        let Node::Filter { input, .. } = graph.node_mut(&id)? else { continue };
        *input = empty;
    }
    Ok(())
}
