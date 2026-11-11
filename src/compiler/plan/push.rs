//! Move filters to the lowest scope `PostgreSQL` would run them at.
use super::{
    graph::Graph,
    join::{Join, Side},
    scope::Scopes,
};
use crate::compiler::{
    ColumnRef, Predicate,
    expression::Expr,
    relational::{Binding, Node},
    scalar::Computed,
    setop::Kind as SetKind,
    subquery::Mark,
    typed::{self, Typed},
};
use anyhow::Result;
use std::collections::{BTreeMap, BTreeSet};

pub(super) fn run(graph: &mut Graph<'_>) -> Result<bool> {
    let mut changed = false;
    for id in graph.ids(|node| matches!(node, Node::Filter { .. })) {
        changed |= filter(graph, &id)?;
    }
    for id in graph.ids(|node| matches!(node, Node::LeftJoin { spec, .. } if !spec.full)) {
        changed |= join_condition(graph, &id)?;
    }
    Ok(changed)
}
fn filter(graph: &mut Graph<'_>, id: &str) -> Result<bool> {
    let Some(Node::Filter { input, .. }) = graph.ir.nodes.iter().find(|node| node.id() == id)
    else {
        return Ok(false);
    };
    let input = input.clone();
    if graph.only_consumer(&input).is_none() {
        return Ok(false);
    }
    match graph.node(&input)?.clone() {
        Node::Filter { input: below, predicate, .. } => merge(graph, id, &input, &below, predicate),
        Node::Map { input: below, computed, .. } => {
            let replace = |column: &ColumnRef| computed_value(&computed, column);
            below_node(graph, id, &input, &below, &|value| {
                value.replaced(&replace).filter(super::bounded)
            })
        }
        Node::Rebind { input: below, fields, .. }
            if !crate::compiler::relational::barrier(&input) =>
        {
            let replace = |column: &ColumnRef| field_input(&fields, column);
            below_node(graph, id, &input, &below, &|value| value.replaced(&replace))
        }
        Node::SetTag { input: below, .. } | Node::Sorted { input: below, .. } => {
            below_node(graph, id, &input, &below, &|value| Some(value.clone()))
        }
        Node::PartitionBy { input: below, keys, .. } => {
            below_keys(graph, id, (&input, &below), &keys)
        }
        Node::Join { .. } | Node::LeftJoin { .. } | Node::Existence { .. } | Node::Mark { .. } => {
            join_filter(graph, id, &input)
        }
        Node::Union { left, right, .. } => union(graph, id, &input, (&left, &right)),
        Node::SetOperation { input: below, spec, .. } => {
            if spec.kind == SetKind::Except {
                return Ok(false);
            }
            below_keys(graph, id, (&input, &below), &spec.columns)
        }
        Node::SetDistinct { .. }
        | Node::Partition { .. }
        | Node::Statistics { .. }
        | Node::Finalize { .. }
        | Node::OrderedFinalize { .. }
        | Node::PrefixWindow { .. } => grouped(graph, id, &input),
        _ => Ok(false),
    }
}
fn computed_value(computed: &[Computed], column: &ColumnRef) -> Option<Typed> {
    computed
        .iter()
        .find(|value| value.column.name == column.name)
        .map(|value| value.expression.clone())
}
fn field_input(fields: &[Binding], column: &ColumnRef) -> Option<Typed> {
    fields
        .iter()
        .find(|field| field.output.name == column.name)
        .map(|field| Typed::column(field.input.clone()))
}
fn conjuncts(graph: &Graph<'_>, id: &str) -> Result<Vec<Expr>> {
    let Node::Filter { predicate, .. } = graph.node(id)? else {
        anyhow::bail!("missing plan filter")
    };
    Ok(predicate.clone().conjuncts())
}
fn merge(
    graph: &mut Graph<'_>,
    id: &str,
    child: &str,
    below: &str,
    inner: Predicate,
) -> Result<bool> {
    let mut values = inner.conjuncts();
    values.extend(conjuncts(graph, id)?);
    graph.remove(child)?;
    let Node::Filter { input, .. } = graph.node_mut(id)? else {
        anyhow::bail!("missing plan filter")
    };
    *input = below.into();
    graph.keep(id, values)?;
    Ok(true)
}
type Rewrite<'a> = dyn Fn(&Expr) -> Option<Expr> + 'a;
fn below_node(
    graph: &mut Graph<'_>,
    id: &str,
    child: &str,
    below: &str,
    rewrite: &Rewrite<'_>,
) -> Result<bool> {
    let (mut moved, mut kept) = (Vec::new(), Vec::new());
    let mut proven = BTreeSet::new();
    for value in conjuncts(graph, id)? {
        match rewrite(&value) {
            Some(rewritten) if !value.read_names().is_empty() => {
                proven.extend(Predicate::new(value).non_null_columns());
                moved.push(rewritten);
            }
            _ => kept.push(value),
        }
    }
    prove_non_null(graph, child, &proven)?;
    let moved = match Expr::And(moved).until_false() {
        Expr::And(values) => values,
        value => vec![value],
    };
    finish(graph, id, kept, vec![(child, below, moved)])
}
/// A filter moved below keeps its `IS NOT NULL` fact on the column it was written against.
fn prove_non_null(graph: &mut Graph<'_>, child: &str, proven: &BTreeSet<String>) -> Result<()> {
    match graph.node_mut(child)? {
        Node::Map { computed, .. } => {
            for value in computed.iter_mut().filter(|value| proven.contains(&value.column.name)) {
                value.column.nullable = false;
            }
        }
        Node::Rebind { fields, .. } => {
            for field in fields.iter_mut().filter(|field| proven.contains(&field.output.name)) {
                field.output.nullable = false;
            }
        }
        _ => {}
    }
    Ok(())
}
fn below_keys(
    graph: &mut Graph<'_>,
    id: &str,
    edges: (&str, &str),
    keys: &[ColumnRef],
) -> Result<bool> {
    let names = keys.iter().map(|key| key.name.clone()).collect::<BTreeSet<_>>();
    let (moved, kept) = split(conjuncts(graph, id)?, &names);
    finish(graph, id, kept, vec![(edges.0, edges.1, moved)])
}
/// Move the conjuncts that read only `names`; a constant conjunct stays where it was written.
fn split(values: Vec<Expr>, names: &BTreeSet<String>) -> (Vec<Expr>, Vec<Expr>) {
    values.into_iter().partition(|value| {
        let reads = value.read_names();
        !reads.is_empty() && reads.is_subset(names)
    })
}
type Placement<'a> = (&'a str, &'a str, Vec<Expr>);
fn finish(
    graph: &mut Graph<'_>,
    id: &str,
    kept: Vec<Expr>,
    placements: Vec<Placement<'_>>,
) -> Result<bool> {
    if placements.iter().all(|(_, _, values)| values.is_empty()) {
        return Ok(false);
    }
    for (consumer, edge, values) in placements {
        graph.filter_edge(consumer, edge, values)?;
    }
    graph.keep(id, kept)?;
    Ok(true)
}
fn join_filter(graph: &mut Graph<'_>, id: &str, input: &str) -> Result<bool> {
    let Some(join) = Join::at(graph, input)? else { return Ok(false) };
    let derive = matches!(join.node, Node::Join { .. } | Node::LeftJoin { .. });
    let sides = match &join.node {
        Node::Join { .. } => vec![&join.left, &join.right],
        Node::LeftJoin { spec, .. } if !spec.full => vec![&join.left],
        Node::Mark { spec, .. } if !spec.semijoin => return subplan(graph, id, &join.left, spec),
        Node::Existence { .. } | Node::Mark { .. } => vec![&join.left],
        _ => return Ok(false),
    };
    let mut kept = conjuncts(graph, id)?;
    let mut lower = Vec::new();
    for side in sides {
        let columns = side.columns(graph)?;
        let (moved, rest) = split(kept, &columns);
        kept = rest;
        lower.push((side, columns, moved));
    }
    // Derive restrictions only from conjuncts left above every eligible input.
    let mut placements = Vec::new();
    for (side, columns, mut moved) in lower {
        if derive {
            implied_restrictions(graph, &kept, &columns, &mut moved);
        }
        placements.push((side.id.as_str(), side.input.as_str(), moved));
    }
    finish(graph, id, kept, placements)
}
/// An OR over a join implies a restriction when every arm restricts the same input.
fn implied_restrictions(
    graph: &mut Graph<'_>,
    values: &[Expr],
    columns: &BTreeSet<String>,
    into: &mut Vec<Expr>,
) {
    for value in values {
        if let Some(restriction) = value.or_restriction(columns)
            && graph.derived.insert(restriction.clone())
        {
            into.push(restriction);
        }
    }
}
/// A membership test that stays a filter orders by cost with the conjuncts beside it, unless
/// a join below separates them onto different inputs.
fn subplan(graph: &mut Graph<'_>, id: &str, left: &Side, mark: &Mark) -> Result<bool> {
    let columns = left.columns(graph)?;
    let probe = probe_expression(graph, &left.input, &mark.probe);
    let threshold = typed::OPERATOR_COST.saturating_add(probe.cost());
    let mut reads = BTreeMap::new();
    probe.reads(&mut reads);
    let probed = reads.into_keys().collect();
    let sides = join_sides(graph, &left.input)?;
    let (mut lower, mut kept) = (Vec::new(), Vec::new());
    for value in conjuncts(graph, id)? {
        let names = value.read_names();
        let local = !names.is_empty() && names.is_subset(&columns);
        if local && (value.cost() <= threshold || separated(&names, &probed, &sides)) {
            lower.push(value);
        } else {
            kept.push(value);
        }
    }
    finish(graph, id, kept, vec![(&left.id, &left.input, lower)])
}
pub(super) fn probe_expression(graph: &Graph<'_>, edge: &str, probe: &ColumnRef) -> Typed {
    let mut at = edge;
    while let Ok(node) = graph.node(at) {
        match node {
            Node::Map { input, computed, .. } => {
                if let Some(value) = computed_value(computed, probe) {
                    return value;
                }
                at = input;
            }
            Node::Filter { input, .. } => at = input,
            _ => break,
        }
    }
    Typed::column(probe.clone())
}
fn join_sides(graph: &Graph<'_>, edge: &str) -> Result<Vec<BTreeSet<String>>> {
    Ok(match graph.node(edge)? {
        Node::Map { input, .. } | Node::Filter { input, .. } => join_sides(graph, input)?,
        Node::Join { .. } | Node::LeftJoin { .. } => {
            let Some(join) = Join::at(graph, edge)? else { return Ok(Vec::new()) };
            vec![join.left.columns(graph)?, join.right.columns(graph)?]
        }
        _ => Vec::new(),
    })
}
fn separated(
    names: &BTreeSet<String>,
    probed: &BTreeSet<String>,
    sides: &[BTreeSet<String>],
) -> bool {
    sides.iter().any(|side| names.is_subset(side) && !probed.is_subset(side))
}
fn union(graph: &mut Graph<'_>, id: &str, child: &str, arms: (&str, &str)) -> Result<bool> {
    let scopes = Scopes::new(graph, [arms.0, arms.1])?;
    let (left, right) = (scopes.at(arms.0)?, scopes.at(arms.1)?);
    if left.except || right.except {
        return Ok(false);
    }
    let values = conjuncts(graph, id)?;
    let (moved, kept): (Vec<_>, Vec<_>) = values.into_iter().partition(|value| {
        let names = value.read_names();
        !names.is_empty() && names.is_disjoint(&left.coerced) && names.is_disjoint(&right.coerced)
    });
    let placements = vec![(child, arms.0, moved.clone()), (child, arms.1, moved)];
    finish(graph, id, kept, placements)
}
/// A grouped or windowed scope admits a filter over the keys that form its partitions.
fn grouped(graph: &mut Graph<'_>, id: &str, child: &str) -> Result<bool> {
    let common = match graph.node(child)? {
        Node::Partition { spec, .. } | Node::PrefixWindow { spec, .. } => {
            spec.previous_windows.as_ref().map(|previous| previous.common_keys.clone())
        }
        _ => None,
    };
    let mut at = child.to_owned();
    loop {
        let next = match graph.node(&at)? {
            Node::PartitionBy { input, keys, .. } => {
                let input = input.clone();
                let mut keys = keys.clone();
                if let Some(common) = &common {
                    keys.retain(|key| common.contains(&key.name));
                }
                return below_keys(graph, id, (&at, &input), &keys);
            }
            Node::SetDistinct { input, .. }
            | Node::Partition { input, .. }
            | Node::Statistics { input, .. }
            | Node::Finalize { input, .. }
            | Node::OrderedFinalize { input, .. }
            | Node::OrderedGroup { input, .. }
            | Node::OrderedFirst { input, .. }
            | Node::OrderedInput { input, .. }
            | Node::PrefixInput { input, .. } => input.clone(),
            Node::Union { left, .. } | Node::PrefixWindow { left, .. } => left.clone(),
            _ => return Ok(false),
        };
        at = next;
    }
}
fn join_condition(graph: &mut Graph<'_>, id: &str) -> Result<bool> {
    let Some(join) = Join::at(graph, id)? else { return Ok(false) };
    let Node::LeftJoin { spec, .. } = join.node else { return Ok(false) };
    let Some(predicate) = spec.predicate else { return Ok(false) };
    let right = join.right;
    let (lower, kept) = split(predicate.conjuncts(), &right.columns(graph)?);
    if lower.is_empty() {
        return Ok(false);
    }
    graph.filter_edge(&right.id, &right.input, lower)?;
    let Node::LeftJoin { spec, .. } = graph.node_mut(id)? else {
        anyhow::bail!("missing left join")
    };
    spec.predicate = Predicate::from_conjuncts(kept);
    Ok(true)
}
