//! Carry `column = constant` across equijoin keys, as equivalence classes do.
use super::graph::Graph;
use crate::compiler::{
    ColumnRef,
    expression::Expr,
    relational::{Node, barrier},
    typed::Typed,
};
use anyhow::Result;
use std::collections::BTreeMap;

type Constants = BTreeMap<String, Vec<Typed>>;
type Carry = ((String, ColumnRef), Target);
struct Target {
    key: String,
    input: String,
    column: ColumnRef,
}

pub(super) fn run(graph: &mut Graph<'_>) -> Result<bool> {
    let mut changed = false;
    for id in graph.ids(|node| {
        matches!(
            node,
            Node::Join { .. } | Node::LeftJoin { .. } | Node::Existence { .. } | Node::Mark { .. }
        )
    }) {
        for (from, to) in pairs(graph, &id)? {
            changed |= carry(graph, &from, &to)?;
        }
    }
    Ok(changed)
}
/// Each source column's edge and the input the same-valued column can be filtered at.
fn pairs(graph: &Graph<'_>, id: &str) -> Result<Vec<Carry>> {
    let mut out = Vec::new();
    match graph.node(id)? {
        Node::Join { left, right, .. } => {
            let (a, b) = (side(graph, left)?, side(graph, right)?);
            add(&mut out, &a, &b);
            add(&mut out, &b, &a);
        }
        Node::LeftJoin { input, spec, .. } if !spec.full => {
            let (a, b) = tagged(graph, input)?;
            add(&mut out, &a, &b);
        }
        Node::Existence { input, .. } => {
            let (a, b) = tagged(graph, input)?;
            add(&mut out, &a, &b);
        }
        Node::Mark { input, spec, .. } if spec.semijoin => {
            let (mut a, mut b) = tagged(graph, input)?;
            a.2 = vec![spec.probe.clone()];
            b.2 = vec![spec.value.clone()];
            add(&mut out, &a, &b);
        }
        _ => {}
    }
    Ok(out)
}
type Side = (String, String, Vec<ColumnRef>);
fn side(graph: &Graph<'_>, id: &str) -> Result<Side> {
    Ok(match graph.node(id)? {
        Node::EquijoinKey { input, keys, .. } => (id.to_owned(), input.clone(), keys.clone()),
        Node::LeftJoinInput { input, spec, .. } => {
            (id.to_owned(), input.clone(), spec.keys.clone())
        }
        Node::RightPresence { input, .. } | Node::RightCounts { input, .. } => side(graph, input)?,
        _ => (id.to_owned(), String::new(), Vec::new()),
    })
}
fn tagged(graph: &Graph<'_>, union: &str) -> Result<(Side, Side)> {
    let Node::Union { left, right, .. } = graph.node(union)? else {
        anyhow::bail!("missing tagged join union")
    };
    Ok((side(graph, left)?, side(graph, right)?))
}
fn add(out: &mut Vec<Carry>, from: &Side, to: &Side) {
    for (source, target) in from.2.iter().zip(&to.2) {
        if same_family(source.oid, target.oid) {
            out.push((
                (from.1.clone(), source.clone()),
                Target { key: to.0.clone(), input: to.1.clone(), column: target.clone() },
            ));
        }
    }
}
/// Equivalence classes span an operator family, not one exact type.
const fn same_family(left: u32, right: u32) -> bool {
    left == right
        || matches!((left, right), (20 | 21 | 23, 20 | 21 | 23))
        || matches!((left, right), (25 | 1043, 25 | 1043))
}
fn carry(graph: &mut Graph<'_>, from: &(String, ColumnRef), to: &Target) -> Result<bool> {
    let mut constants = Constants::new();
    collect(graph, &from.0, &mut constants)?;
    let mut derived = Vec::new();
    for constant in constants.remove(&from.1.name).unwrap_or_default() {
        let value = Expr::equals(to.column.clone(), constant);
        if graph.derived.insert(value.clone()) {
            derived.push(value);
        }
    }
    if derived.is_empty() {
        return Ok(false);
    }
    graph.filter_edge(&to.key, &to.input, derived)?;
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
        Node::LeftJoin { input, spec, .. } if !spec.full => tagged_left(graph, input, out),
        Node::Existence { input, .. } | Node::Mark { input, .. } => tagged_left(graph, input, out),
        _ => Ok(()),
    }
}
fn tagged_left(graph: &Graph<'_>, union: &str, out: &mut Constants) -> Result<()> {
    let Node::Union { left, .. } = graph.node(union)? else { return Ok(()) };
    collect(graph, left, out)
}
