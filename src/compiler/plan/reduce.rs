//! Reduce outer joins whose null-extended rows a stricter consumer always rejects.
use super::{graph::Graph, join::Join};
use crate::compiler::{
    Predicate,
    expression::Expr,
    relational::{Node, barrier},
};
use anyhow::{Context, Result};
use std::collections::BTreeSet;

type Names = BTreeSet<String>;
/// What the consumers above reject when NULL: columns, and ORs each of whose arms rejects a NULL
/// somewhere, which `find_nonnullable_rels` proves per relation rather than per column.
#[derive(Clone, Default)]
struct Required {
    columns: Names,
    any: Vec<Vec<Names>>,
}
impl Required {
    fn add(&mut self, predicate: &Predicate) {
        self.columns.extend(predicate.strict_columns());
        self.any.extend(predicate.clone().conjuncts().iter().filter_map(Expr::strict_arms));
    }
    fn map(&self, apply: &impl Fn(&Names) -> Names) -> Self {
        Self {
            columns: apply(&self.columns),
            any: self.any.iter().map(|arms| arms.iter().map(apply).collect()).collect(),
        }
    }
    fn rejects(&self, columns: &Names) -> bool {
        !self.columns.is_disjoint(columns)
            || self.any.iter().any(|arms| arms.iter().all(|arm| !arm.is_disjoint(columns)))
    }
}

pub(super) fn run(graph: &mut Graph<'_>) -> Result<bool> {
    let output = graph.ids(|node| matches!(node, Node::Output { .. }));
    let root = output.last().context("missing plan output")?.clone();
    visit(graph, &root, &Required::default())
}
fn visit(graph: &mut Graph<'_>, id: &str, required: &Required) -> Result<bool> {
    if let Some(join) = Join::at(graph, id)? {
        return match &join.node {
            Node::LeftJoin { .. } => left_join(graph, &join, required),
            Node::Join { .. } => {
                let mut changed = false;
                for side in [&join.left, &join.right] {
                    let mut local = required.clone();
                    local.columns.extend(side.keys.iter().map(|key| key.name.clone()));
                    changed |= visit(graph, &side.input, &local)?;
                }
                Ok(changed)
            }
            _ => Ok(visit(graph, &join.left.input, required)?
                | visit(graph, &join.right.input, &Required::default())?),
        };
    }
    match graph.node(id)?.clone() {
        Node::Filter { input, predicate, .. } => {
            let mut required = required.clone();
            required.add(&predicate);
            visit(graph, &input, &required)
        }
        Node::Map { input, computed, .. } => {
            let required = required.map(&|names| {
                let mut names = names.clone();
                for value in &computed {
                    if names.remove(&value.column.name) {
                        value.expression.strict_columns(&mut names);
                    }
                }
                names
            });
            visit(graph, &input, &required)
        }
        Node::Rebind { input, fields, .. } if !barrier(id) => {
            let renamed = required.map(&|names| {
                let read = fields.iter().filter(|field| names.contains(&field.output.name));
                read.map(|field| field.input.name.clone()).collect()
            });
            visit(graph, &input, &renamed)
        }
        Node::EquijoinKey { input, keys, .. } => {
            let mut required = required.clone();
            required.columns.extend(keys.into_iter().map(|key| key.name));
            visit(graph, &input, &required)
        }
        Node::CrossKey { input, .. } => visit(graph, &input, required),
        _ => unrelated(graph, id),
    }
}
fn unrelated(graph: &mut Graph<'_>, id: &str) -> Result<bool> {
    let inputs = graph.node(id)?.inputs().into_iter().map(str::to_owned).collect::<Vec<_>>();
    let mut changed = false;
    for input in inputs {
        changed |= visit(graph, &input, &Required::default())?;
    }
    Ok(changed)
}
fn names(columns: &[crate::compiler::ColumnRef]) -> Names {
    columns.iter().map(|column| column.name.clone()).collect()
}
fn left_join(graph: &mut Graph<'_>, join: &Join, required: &Required) -> Result<bool> {
    let Node::LeftJoin { id, input: union, spec } = &join.node else {
        anyhow::bail!("missing outer join")
    };
    let (left, right) =
        (required.rejects(&names(&spec.left)), required.rejects(&names(&spec.right)));
    if spec.scalar.is_none() && right && (!spec.full || left) {
        inner(graph, id, union, &join.left.id, &join.right.id)?;
        visit(graph, id, required)?;
        return Ok(true);
    }
    if spec.full && (left || right) {
        if right {
            swap(graph, id, union)?;
        } else {
            let Node::LeftJoin { spec, .. } = graph.node_mut(id)? else {
                anyhow::bail!("missing left join")
            };
            spec.full = false;
        }
        visit(graph, id, required)?;
        return Ok(true);
    }
    if spec.full {
        return Ok(visit(graph, &join.left.input, &Required::default())?
            | visit(graph, &join.right.input, &Required::default())?);
    }
    let changed = visit(graph, &join.left.input, required)?;
    let mut local = Required { columns: names(&join.right.keys), any: Vec::new() };
    if let Some(predicate) = &spec.predicate {
        local.add(predicate);
    }
    Ok(changed | visit(graph, &join.right.input, &local)?)
}
fn swap(graph: &mut Graph<'_>, id: &str, input: &str) -> Result<()> {
    let Node::Union { left, right, .. } = graph.node_mut(input)? else {
        anyhow::bail!("missing tagged join union")
    };
    std::mem::swap(left, right);
    let edges = [left.clone(), right.clone()];
    for edge in edges {
        let Node::LeftJoinInput { spec, .. } = graph.node_mut(&edge)? else {
            anyhow::bail!("missing tagged join input")
        };
        spec.left = !spec.left;
    }
    let Node::LeftJoin { spec, .. } = graph.node_mut(id)? else {
        anyhow::bail!("missing left join")
    };
    std::mem::swap(&mut spec.left, &mut spec.right);
    spec.full = false;
    Ok(())
}
fn inner(graph: &mut Graph<'_>, id: &str, union: &str, left: &str, right: &str) -> Result<()> {
    for edge in [left, right] {
        let Node::LeftJoinInput { id, input, spec } = graph.remove(edge)? else {
            anyhow::bail!("missing tagged join input")
        };
        graph.ir.nodes.push(if spec.keys.is_empty() {
            Node::CrossKey { id, input }
        } else {
            Node::EquijoinKey { id, input, keys: spec.keys }
        });
    }
    graph.remove(union)?;
    let Node::LeftJoin { spec, .. } = graph.remove(id)? else { anyhow::bail!("missing left join") };
    let (left, right) = (left.to_owned(), right.to_owned());
    if let Some(predicate) = spec.predicate {
        let join = graph.fresh("inner");
        graph.ir.nodes.push(Node::Join { id: join.clone(), left, right });
        graph.ir.nodes.push(Node::Filter { id: id.into(), input: join, predicate });
    } else {
        graph.ir.nodes.push(Node::Join { id: id.into(), left, right });
    }
    Ok(())
}
