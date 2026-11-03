//! Move filters to the lowest scope `PostgreSQL` would run them at.
use super::graph::Graph;
use crate::compiler::{
    ColumnRef, Predicate,
    expression::Expr,
    relational::{Binding, Node},
    scalar::Computed,
    setop::Kind,
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
    if graph.consumers(&input).len() != 1 {
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
        Node::Join { left, right, .. } => inner_join(graph, id, (&left, &right)),
        Node::LeftJoin { input: union, spec, .. } => {
            if spec.full {
                return Ok(false);
            }
            outer_join(graph, id, &union)
        }
        Node::Existence { input: union, .. } => semi_join(graph, id, &union),
        Node::Mark { input: union, spec, .. } => {
            if spec.semijoin {
                semi_join(graph, id, &union)
            } else {
                subplan(graph, id, &union, &spec)
            }
        }
        Node::Union { left, right, .. } => union(graph, id, &input, (&left, &right)),
        Node::SetOperation { input: below, spec, .. } => {
            if spec.kind == Kind::Except {
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
struct Side {
    id: String,
    input: String,
    columns: BTreeSet<String>,
}
fn side(graph: &Graph<'_>, id: &str) -> Result<Side> {
    let (Node::EquijoinKey { input, .. }
    | Node::CrossKey { input, .. }
    | Node::LeftJoinInput { input, .. }) = graph.node(id)?
    else {
        anyhow::bail!("missing join input")
    };
    Ok(Side { id: id.into(), input: input.clone(), columns: graph.columns(input)? })
}
fn tagged(graph: &Graph<'_>, union: &str) -> Result<(Side, Side)> {
    let Node::Union { left, right, .. } = graph.node(union)? else {
        anyhow::bail!("missing tagged join union")
    };
    Ok((side(graph, left)?, side(graph, right)?))
}
fn inner_join(graph: &mut Graph<'_>, id: &str, keys: (&str, &str)) -> Result<bool> {
    let (left, right) = (side(graph, keys.0)?, side(graph, keys.1)?);
    let (mut lower_left, rest) = split(conjuncts(graph, id)?, &left.columns);
    let (mut lower_right, kept) = split(rest, &right.columns);
    implied_restrictions(graph, &kept, &left.columns, &mut lower_left);
    implied_restrictions(graph, &kept, &right.columns, &mut lower_right);
    finish(
        graph,
        id,
        kept,
        vec![(&left.id, &left.input, lower_left), (&right.id, &right.input, lower_right)],
    )
}
fn outer_join(graph: &mut Graph<'_>, id: &str, union: &str) -> Result<bool> {
    let (left, _) = tagged(graph, union)?;
    let (mut lower, kept) = split(conjuncts(graph, id)?, &left.columns);
    implied_restrictions(graph, &kept, &left.columns, &mut lower);
    finish(graph, id, kept, vec![(&left.id, &left.input, lower)])
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
fn left_input(graph: &Graph<'_>, union: &str) -> Result<Side> {
    let Node::Union { left, .. } = graph.node(union)? else {
        anyhow::bail!("missing tagged join union")
    };
    side(graph, left)
}
fn semi_join(graph: &mut Graph<'_>, id: &str, union: &str) -> Result<bool> {
    let left = left_input(graph, union)?;
    let (lower, kept) = split(conjuncts(graph, id)?, &left.columns);
    finish(graph, id, kept, vec![(&left.id, &left.input, lower)])
}
/// A membership test that stays a filter orders by cost with the conjuncts beside it, unless
/// a join below separates them onto different inputs.
fn subplan(graph: &mut Graph<'_>, id: &str, union: &str, mark: &Mark) -> Result<bool> {
    let left = left_input(graph, union)?;
    let probe = probe_expression(graph, &left.input, &mark.probe);
    let threshold = typed::OPERATOR_COST.saturating_add(probe.cost());
    let mut reads = BTreeMap::new();
    probe.reads(&mut reads);
    let probed = reads.into_keys().collect();
    let sides = join_sides(graph, &left.input)?;
    let (mut lower, mut kept) = (Vec::new(), Vec::new());
    for value in conjuncts(graph, id)? {
        let names = value.read_names();
        let local = !names.is_empty() && names.is_subset(&left.columns);
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
        Node::Join { left, right, .. } => vec![graph.columns(left)?, graph.columns(right)?],
        Node::LeftJoin { input, .. } => {
            let (left, right) = tagged(graph, input)?;
            vec![left.columns, right.columns]
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
    if has_except(graph, arms.0)? || has_except(graph, arms.1)? {
        return Ok(false);
    }
    let values = conjuncts(graph, id)?;
    let (moved, kept): (Vec<_>, Vec<_>) = values.into_iter().partition(|value| {
        let names = value.read_names();
        !names.is_empty()
            && !names.iter().any(|name| {
                coerced(graph, arms.0, name).unwrap_or(true)
                    || coerced(graph, arms.1, name).unwrap_or(true)
            })
    });
    let placements = vec![(child, arms.0, moved.clone()), (child, arms.1, moved)];
    finish(graph, id, kept, placements)
}
/// `compare_tlist_datatypes`: a set-operation column an arm reaches through a cast is unsafe.
pub(super) fn coerced(graph: &Graph<'_>, edge: &str, name: &str) -> Result<bool> {
    Ok(match graph.node(edge)? {
        Node::SetTag { input, .. } | Node::PartitionBy { input, .. } => {
            coerced(graph, input, name)?
        }
        Node::Rebind { input, fields, .. } => {
            let field = fields.iter().find(|field| field.output.name == name);
            // A cast the query wrote already gave the arm the common type.
            let aligned = field.filter(|field| field.input.name.contains("_cast_"));
            let probe = aligned.map(|field| probe_expression(graph, input, &field.input));
            probe.is_some_and(|value| value.casts_type())
        }
        _ => false,
    })
}
pub(super) fn coerced_rebind(graph: &Graph<'_>, id: &str) -> Result<bool> {
    let Node::Rebind { fields, .. } = graph.node(id)? else { return Ok(false) };
    if !fields.iter().any(|field| field.output.name.starts_with("@union_")) {
        return Ok(false);
    }
    for field in fields {
        if coerced(graph, id, &field.output.name)? {
            return Ok(true);
        }
    }
    Ok(false)
}
/// `recurse_pushdown_safe` refuses a set-operation tree that contains EXCEPT anywhere.
fn has_except(graph: &Graph<'_>, edge: &str) -> Result<bool> {
    Ok(match graph.node(edge)? {
        Node::SetOperation { spec, input, .. } => {
            spec.kind == Kind::Except || has_except(graph, input)?
        }
        Node::Union { left, right, .. } => has_except(graph, left)? || has_except(graph, right)?,
        Node::Rebind { input, .. }
        | Node::Map { input, .. }
        | Node::SetTag { input, .. }
        | Node::PartitionBy { input, .. }
        | Node::Sorted { input, .. }
        | Node::SetDistinct { input, .. } => has_except(graph, input)?,
        _ => false,
    })
}
/// A grouped or windowed scope admits a filter over the keys that form its partitions.
fn grouped(graph: &mut Graph<'_>, id: &str, child: &str) -> Result<bool> {
    let mut at = child.to_owned();
    loop {
        let next = match graph.node(&at)? {
            Node::PartitionBy { input, keys, .. } => {
                let (keyed, input, keys) = (at.clone(), input.clone(), keys.clone());
                return below_keys(graph, id, (&keyed, &input), &keys);
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
    let Node::LeftJoin { input, spec, .. } = graph.node(id)? else { return Ok(false) };
    let Some(predicate) = spec.predicate.clone() else { return Ok(false) };
    let input = input.clone();
    let (_, right) = tagged(graph, &input)?;
    let (lower, kept) = split(predicate.conjuncts(), &right.columns);
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
