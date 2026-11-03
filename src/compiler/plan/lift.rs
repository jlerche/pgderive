//! Lift computed outputs above joins, where `PostgreSQL` evaluates flattened target lists.
use super::graph::Graph;
use crate::compiler::{
    ColumnRef,
    relational::{Binding, Node, barrier},
    scalar::Computed,
    typed::Typed,
};
use anyhow::{Context, Result};
use std::collections::{BTreeMap, BTreeSet};

pub(super) fn run(graph: &mut Graph<'_>) -> Result<bool> {
    let mut changed = false;
    for id in graph.ids(|node| matches!(node, Node::Map { .. })) {
        changed |= lift(graph, &id)?;
    }
    Ok(changed)
}
fn lift(graph: &mut Graph<'_>, id: &str) -> Result<bool> {
    let Some(Node::Map { input, computed, .. }) =
        graph.ir.nodes.iter().find(|node| node.id() == id).cloned()
    else {
        return Ok(false);
    };
    let map = Lower { id: id.to_owned(), input, computed };
    match graph.only_consumer(id).cloned() {
        Some(Node::Rebind { id: rebind, fields, input: _ }) if ascends(graph, &rebind) => {
            through_rebind(graph, map, &rebind, fields)
        }
        Some(Node::Map { id: upper, computed, .. }) => merge(graph, &map, &upper, computed),
        Some(Node::EquijoinKey { id: key, keys, .. }) => over_join(graph, &map, &key, &keys),
        Some(Node::CrossKey { id: key, .. }) => over_join(graph, &map, &key, &[]),
        Some(Node::LeftJoinInput { id: key, spec, .. }) => over_tagged(graph, &map, &key, &spec),
        _ => Ok(false),
    }
}
struct Lower {
    id: String,
    input: String,
    computed: Vec<Computed>,
}
/// A rename above a join input is worth crossing; one above anything else gains nothing.
fn ascends(graph: &Graph<'_>, rebind: &str) -> bool {
    !barrier(rebind)
        && graph.only_consumer(rebind).is_some_and(|node| {
            matches!(
                node,
                Node::EquijoinKey { .. }
                    | Node::CrossKey { .. }
                    | Node::LeftJoinInput { .. }
                    | Node::Rebind { .. }
                    | Node::Map { .. }
            )
        })
}
fn through_rebind(
    graph: &mut Graph<'_>,
    map: Lower,
    rebind: &str,
    fields: Vec<Binding>,
) -> Result<bool> {
    let (mut kept, mut lifted) = (Vec::new(), Vec::new());
    for field in fields {
        let value = map.computed.iter().find(|value| value.column.name == field.input.name);
        match value {
            Some(value) => lifted.push((value.expression.clone(), field.output)),
            None => kept.push(field),
        }
    }
    if lifted.is_empty() {
        return Ok(false);
    }
    let mut reads = BTreeMap::new();
    for (value, _) in &lifted {
        value.reads(&mut reads);
    }
    let mut names = kept
        .iter()
        .map(|field| (field.input.name.clone(), field.output.clone()))
        .collect::<BTreeMap<_, _>>();
    for (name, column) in reads {
        names.entry(name).or_insert_with_key(|name| {
            let mut output = column.clone();
            output.name = format!("@{rebind}:{name}");
            kept.push(Binding { input: column, output: output.clone() });
            output
        });
    }
    let replace = |column: &ColumnRef| names.get(&column.name).cloned().map(Typed::column);
    let computed = lifted
        .into_iter()
        .map(|(value, column)| Some(Computed { column, expression: value.replaced(&replace)? }))
        .collect::<Option<Vec<_>>>();
    let Some(computed) = computed else { return Ok(false) };
    graph.remove(&map.id)?;
    let below = graph.fresh("rebind");
    let Node::Rebind { fields, id, input } = graph.node_mut(rebind)? else {
        anyhow::bail!("missing plan rebind")
    };
    *fields = kept;
    id.clone_from(&below);
    *input = map.input;
    graph.ir.nodes.push(Node::Map { id: rebind.into(), input: below, computed });
    Ok(true)
}
fn merge(graph: &mut Graph<'_>, map: &Lower, upper: &str, computed: Vec<Computed>) -> Result<bool> {
    let replace = |column: &ColumnRef| {
        map.computed
            .iter()
            .find(|value| value.column.name == column.name)
            .map(|value| value.expression.clone())
    };
    let merged = computed
        .into_iter()
        .map(|value| {
            let expression = value.expression.replaced(&replace).filter(super::bounded)?;
            Some(Computed { column: value.column, expression })
        })
        .collect::<Option<Vec<_>>>();
    let Some(mut merged) = merged else { return Ok(false) };
    for value in &map.computed {
        if !merged.iter().any(|prior| prior.column.name == value.column.name) {
            merged.push(value.clone());
        }
    }
    graph.remove(&map.id)?;
    let Node::Map { input, computed, .. } = graph.node_mut(upper)? else {
        anyhow::bail!("missing upper plan map")
    };
    input.clone_from(&map.input);
    *computed = merged;
    Ok(true)
}
/// Keep what `pinned` names below; the remainder is what can be evaluated above the join.
fn divide(map: &Lower, pinned: &BTreeSet<String>) -> (Vec<Computed>, Vec<Computed>) {
    map.computed.iter().cloned().partition(|value| pinned.contains(&value.column.name))
}
fn settle(graph: &mut Graph<'_>, map: &Lower, low: Vec<Computed>) -> Result<()> {
    if low.is_empty() {
        graph.remove(&map.id)?;
        graph.redirect(&map.id, &map.input);
        return Ok(());
    }
    let Node::Map { computed, .. } = graph.node_mut(&map.id)? else {
        anyhow::bail!("missing plan map")
    };
    *computed = low;
    Ok(())
}
fn rename(graph: &mut Graph<'_>, join: &str) -> Result<String> {
    let below = graph.fresh("join");
    let renamed = below.clone();
    graph.node_mut(join)?.rewrite(&move |_| renamed.clone(), &|edge| edge.into());
    Ok(below)
}
fn over_join(graph: &mut Graph<'_>, map: &Lower, key: &str, keys: &[ColumnRef]) -> Result<bool> {
    let Some(Node::Join { id: join, .. }) = graph.only_consumer(key).cloned() else {
        return Ok(false);
    };
    let pinned = keys.iter().map(|key| key.name.clone()).collect();
    let (low, high) = divide(map, &pinned);
    if high.is_empty() {
        return Ok(false);
    }
    settle(graph, map, low)?;
    let below = rename(graph, &join)?;
    graph.ir.nodes.push(Node::Map { id: join, input: below, computed: high });
    Ok(true)
}
fn over_tagged(
    graph: &mut Graph<'_>,
    map: &Lower,
    key: &str,
    spec: &crate::compiler::left_join::Input,
) -> Result<bool> {
    let Some(Node::Union { id: union, .. }) = graph.only_consumer(key).cloned() else {
        return Ok(false);
    };
    let Some(owner) = graph.only_consumer(&union).cloned() else { return Ok(false) };
    let mut pinned = spec.keys.iter().map(|key| key.name.clone()).collect::<BTreeSet<_>>();
    let join = owner.id().to_owned();
    let (full, reads_condition) = match &owner {
        Node::LeftJoin { spec, .. } => {
            pinned.extend(spec.predicate.clone().map_or_else(BTreeSet::new, |value| {
                value
                    .conjuncts()
                    .iter()
                    .flat_map(crate::compiler::expression::Expr::read_names)
                    .collect()
            }));
            (spec.full, true)
        }
        Node::Existence { .. } => (false, false),
        Node::Mark { spec: mark, .. } => {
            pinned.insert(mark.probe.name.clone());
            pinned.insert(mark.value.name.clone());
            (false, false)
        }
        _ => return Ok(false),
    };
    if !spec.left && !reads_condition {
        return Ok(false);
    }
    let nullable = full || !spec.left;
    let (mut low, high) = divide(map, &pinned);
    let mut lifted = Vec::new();
    for value in high {
        if nullable && !null_extends(&value.expression) {
            low.push(value);
        } else {
            lifted.push(value);
        }
    }
    if lifted.is_empty() {
        return Ok(false);
    }
    settle(graph, map, low)?;
    adjust_lists(graph, &join, spec.left, &mut lifted, nullable)?;
    let below = rename(graph, &join)?;
    graph.ir.nodes.push(Node::Map { id: join, input: below, computed: lifted });
    Ok(true)
}
/// Above a nullable input a lifted output must read NULL for null-extended rows, which takes
/// a strict expression that reads a column of that input.
fn null_extends(value: &Typed) -> bool {
    let mut reads = BTreeMap::new();
    value.reads(&mut reads);
    value.is_strict() && !reads.is_empty()
}
/// The null-extension lists must name what the lifted expressions read instead of their results.
fn adjust_lists(
    graph: &mut Graph<'_>,
    join: &str,
    left: bool,
    lifted: &mut [Computed],
    nullable: bool,
) -> Result<()> {
    let mut reads = BTreeMap::new();
    for value in lifted.iter() {
        value.expression.reads(&mut reads);
    }
    for value in lifted.iter_mut().filter(|_| nullable) {
        value.column.nullable = true;
    }
    let Node::LeftJoin { spec, .. } = graph.node_mut(join).context("missing plan join")? else {
        return Ok(());
    };
    let list = if left { &mut spec.left } else { &mut spec.right };
    list.retain(|column| !lifted.iter().any(|value| value.column.name == column.name));
    for column in reads.into_values() {
        if !list.iter().any(|prior| prior.name == column.name) {
            list.push(column);
        }
    }
    Ok(())
}
