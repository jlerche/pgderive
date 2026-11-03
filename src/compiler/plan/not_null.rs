//! `restriction_is_always_true`: a NULL test on a NOT NULL base column is already decided.
use super::graph::Graph;
use crate::compiler::{
    ColumnRef,
    expression::Expr,
    relational::{Node, barrier},
};
use anyhow::Result;

pub(super) fn simplified(graph: &Graph<'_>, edge: &str, value: Expr) -> Result<Expr> {
    match decided(graph, edge, &value)? {
        Some(true) => return Ok(Expr::And(Vec::new())),
        Some(false) => return Ok(Expr::Or(Vec::new())),
        None => {}
    }
    let Expr::Or(arms) = &value else { return Ok(value) };
    let mut all_false = true;
    for arm in arms {
        match decided(graph, edge, arm)? {
            Some(true) => return Ok(Expr::And(Vec::new())),
            Some(false) => {}
            None => all_false = false,
        }
    }
    Ok(if all_false { Expr::Or(Vec::new()) } else { value })
}
fn decided(graph: &Graph<'_>, edge: &str, value: &Expr) -> Result<Option<bool>> {
    let Expr::Null(tested, not) = value else { return Ok(None) };
    let Some(column) = tested.as_column() else { return Ok(None) };
    Ok(base_not_null(graph, edge, column)?.then_some(*not))
}
/// The column reaches this edge from a NOT NULL base column, with no outer join nulling it.
fn base_not_null(graph: &Graph<'_>, edge: &str, column: &ColumnRef) -> Result<bool> {
    match graph.node(edge)? {
        Node::Source { .. } => Ok(!column.nullable),
        Node::Filter { input, .. }
        | Node::Empty { input, .. }
        | Node::EquijoinKey { input, .. }
        | Node::CrossKey { input, .. } => base_not_null(graph, input, column),
        Node::Map { input, computed, .. } => {
            if computed.iter().any(|value| value.column.name == column.name) {
                return Ok(false);
            }
            base_not_null(graph, input, column)
        }
        // Above an aligned set-operation arm the filter still has a fact to leave on its way down.
        Node::Rebind { fields, .. }
            if fields.iter().any(|field| field.output.name.starts_with("@union_")) =>
        {
            Ok(false)
        }
        Node::Rebind { id, input, fields } if !barrier(id) => {
            let field = fields.iter().find(|field| field.output.name == column.name);
            field.map_or(Ok(false), |field| base_not_null(graph, input, &field.input))
        }
        Node::Join { left, right, .. } => {
            let side = if graph.columns(left)?.contains(&column.name) { left } else { right };
            base_not_null(graph, side, column)
        }
        Node::LeftJoin { input, spec, .. } if !spec.full => {
            let Node::Union { left, .. } = graph.node(input)? else { return Ok(false) };
            let Node::LeftJoinInput { input, .. } = graph.node(left)? else { return Ok(false) };
            if !graph.columns(input)?.contains(&column.name) {
                return Ok(false);
            }
            base_not_null(graph, input, column)
        }
        _ => Ok(false),
    }
}
