//! Reduce outer joins whose null-extended rows a stricter consumer always rejects.
use super::graph::Graph;
use crate::compiler::{
    Predicate,
    expression::Expr,
    left_join::Spec,
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
    let before = outer_joins(graph);
    visit(graph, &root, &Required::default())?;
    Ok(outer_joins(graph) != before)
}
fn outer_joins(graph: &Graph<'_>) -> Vec<String> {
    graph
        .ids(|node| matches!(node, Node::LeftJoin { spec, .. } if spec.full))
        .into_iter()
        .chain(graph.ids(|node| matches!(node, Node::LeftJoin { .. })))
        .collect()
}
fn visit(graph: &mut Graph<'_>, id: &str, required: &Required) -> Result<()> {
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
        Node::Join { left, right, .. } => {
            visit(graph, &left, required)?;
            visit(graph, &right, required)
        }
        Node::LeftJoin { input, spec, .. } => left_join(graph, id, &input, &spec, required),
        Node::Existence { input, .. } | Node::Mark { input, .. } => {
            let Node::Union { left, right, .. } = graph.node(&input)?.clone() else {
                return unrelated(graph, id);
            };
            let below = tagged_input(graph, &left)?;
            visit(graph, &below, required)?;
            let below = tagged_input(graph, &right);
            visit(graph, &below?, &Required::default())
        }
        _ => unrelated(graph, id),
    }
}
fn unrelated(graph: &mut Graph<'_>, id: &str) -> Result<()> {
    let inputs = graph.node(id)?.inputs().into_iter().map(str::to_owned).collect::<Vec<_>>();
    for input in inputs {
        visit(graph, &input, &Required::default())?;
    }
    Ok(())
}
fn tagged_input(graph: &Graph<'_>, id: &str) -> Result<String> {
    match graph.node(id)? {
        Node::LeftJoinInput { input, .. }
        | Node::RightPresence { input, .. }
        | Node::RightCounts { input, .. } => {
            let input = input.clone();
            if matches!(graph.node(&input)?, Node::RightCounts { .. } | Node::LeftJoinInput { .. })
            {
                return tagged_input(graph, &input);
            }
            Ok(input)
        }
        _ => anyhow::bail!("missing tagged join input"),
    }
}
fn names(columns: &[crate::compiler::ColumnRef]) -> Names {
    columns.iter().map(|column| column.name.clone()).collect()
}
fn left_join(
    graph: &mut Graph<'_>,
    id: &str,
    input: &str,
    spec: &Spec,
    required: &Required,
) -> Result<()> {
    let strict = |columns: &[crate::compiler::ColumnRef]| required.rejects(&names(columns));
    let (left, right) = (strict(&spec.left), strict(&spec.right));
    let Node::Union { left: left_edge, right: right_edge, .. } = graph.node(input)?.clone() else {
        anyhow::bail!("missing tagged join union")
    };
    match (spec.full, left, right) {
        (false, _, true) | (true, true, true) => {
            inner(graph, id, input, &left_edge, &right_edge)?;
            return visit(graph, id, required);
        }
        (true, true, false) => set_preserved(graph, id)?,
        (true, false, true) => swap(graph, id, input)?,
        _ => {}
    }
    let Node::LeftJoin { input, spec, .. } = graph.node(id)?.clone() else {
        anyhow::bail!("missing left join")
    };
    let Node::Union { left: left_edge, right: right_edge, .. } = graph.node(&input)?.clone() else {
        anyhow::bail!("missing tagged join union")
    };
    let (below_left, below_right) =
        (tagged_input(graph, &left_edge)?, tagged_input(graph, &right_edge)?);
    if spec.full {
        visit(graph, &below_left, &Required::default())?;
        return visit(graph, &below_right, &Required::default());
    }
    visit(graph, &below_left, required)?;
    let Node::LeftJoinInput { spec: keys, .. } = graph.node(&right_edge)? else {
        anyhow::bail!("missing tagged join input")
    };
    let mut local = Required { columns: names(&keys.keys), any: Vec::new() };
    if let Some(predicate) = &spec.predicate {
        local.add(predicate);
    }
    visit(graph, &below_right, &local)
}
fn set_preserved(graph: &mut Graph<'_>, id: &str) -> Result<()> {
    let Node::LeftJoin { spec, .. } = graph.node_mut(id)? else {
        anyhow::bail!("missing left join")
    };
    spec.full = false;
    Ok(())
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
