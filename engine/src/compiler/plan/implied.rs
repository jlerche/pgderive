//! Carry `column = constant` across equijoin keys, as equivalence classes do.
use super::{
    graph::Graph,
    join::{Join, Side},
};
use crate::compiler::{
    expression::Expr,
    relational::{Node, barrier},
    typed::Typed,
};
use anyhow::Result;
use std::collections::BTreeMap;

type Constants = BTreeMap<String, Vec<Typed>>;
pub(super) fn run(graph: &mut Graph<'_>) -> Result<bool> {
    let mut changed = false;
    for id in graph.ids(|node| {
        matches!(
            node,
            Node::Join { .. } | Node::LeftJoin { .. } | Node::Existence { .. } | Node::Mark { .. }
        )
    }) {
        let Some(mut join) = Join::at(graph, &id)? else { continue };
        match join.node {
            Node::Join { .. } => {
                changed |= carry(graph, &join.left, &join.right)?;
                changed |= carry(graph, &join.right, &join.left)?;
            }
            Node::LeftJoin { spec, .. } if !spec.full => {
                changed |= carry(graph, &join.left, &join.right)?;
            }
            Node::Existence { .. } => changed |= carry(graph, &join.left, &join.right)?,
            Node::Mark { spec, .. } if spec.semijoin => {
                join.left.keys = vec![spec.probe];
                join.right.keys = vec![spec.value];
                changed |= carry(graph, &join.left, &join.right)?;
            }
            _ => {}
        }
    }
    Ok(changed)
}
/// Equivalence classes span an operator family, not one exact type.
const fn same_family(left: u32, right: u32) -> bool {
    left == right
        || matches!((left, right), (20 | 21 | 23, 20 | 21 | 23))
        || matches!((left, right), (25 | 1043, 25 | 1043))
}
fn carry(graph: &mut Graph<'_>, from: &Side, to: &Side) -> Result<bool> {
    let mut constants = Constants::new();
    collect(graph, &from.input, &mut constants)?;
    let mut derived = Vec::new();
    for (source, target) in from.keys.iter().zip(&to.keys) {
        if !same_family(source.oid, target.oid) {
            continue;
        }
        for constant in constants.get(&source.name).into_iter().flatten() {
            let value = Expr::equals(target.clone(), constant.clone());
            if graph.derived.insert(value.clone()) {
                derived.push(value);
            }
        }
    }
    if derived.is_empty() {
        return Ok(false);
    }
    // Attach all inferred conjuncts together: a first insertion replaces this input edge.
    graph.filter_edge(&to.id, &to.input, derived)?;
    Ok(true)
}
/// Constants that hold for every row an edge emits; a nullable side's do not hold above it.
fn collect(graph: &Graph<'_>, edge: &str, out: &mut Constants) -> Result<()> {
    match graph.node(edge)? {
        Node::Filter { input, predicate, .. } => {
            for value in predicate.clone().conjuncts() {
                if let Some((column, constant)) = value.constant_equality() {
                    out.entry(column.name.clone()).or_default().push(constant.clone());
                }
            }
            collect(graph, input, out)
        }
        Node::Map { input, .. }
        | Node::EquijoinKey { input, .. }
        | Node::CrossKey { input, .. }
        | Node::LeftJoinInput { input, .. } => collect(graph, input, out),
        Node::Rebind { id, input, fields } if !barrier(id) => {
            let mut below = Constants::new();
            collect(graph, input, &mut below)?;
            for field in fields {
                if let Some(values) = below.get(&field.input.name) {
                    out.entry(field.output.name.clone())
                        .or_default()
                        .extend(values.iter().cloned());
                }
            }
            Ok(())
        }
        Node::Join { left, right, .. } => {
            collect(graph, left, out)?;
            collect(graph, right, out)
        }
        Node::LeftJoin { spec, .. } if !spec.full => preserved_left(graph, edge, out),
        Node::Existence { .. } | Node::Mark { .. } => preserved_left(graph, edge, out),
        _ => Ok(()),
    }
}
fn preserved_left(graph: &Graph<'_>, edge: &str, out: &mut Constants) -> Result<()> {
    let Some(join) = Join::at(graph, edge)? else { return Ok(()) };
    collect(graph, &join.left.input, out)
}
