//! Bind new expression semantics only when the resolved plan uses them.
use super::{Compiled, Program, expression::Expr, relational::Node, scalar::Native};
use anyhow::{Context, Result};
pub(super) fn bind(compiled: &mut Compiled) -> Result<()> {
    let gap = compiled.predicates.as_ref().is_some_and(Expr::has_gap)
        || match &compiled.program {
            Program::Relational { relational } => relational.nodes.iter().any(node_gap),
            _ => false,
        };
    if gap {
        compiled
            .revision
            .as_mut()
            .context("missing gap compiler revision")?
            .push_str(":pg-timestamp-gap-v1");
    }
    if let Program::Relational { relational } = &compiled.program {
        for (used, suffix) in [
            (relational.nodes.iter().any(node_offset), ":pg-fixed-time-offset-v1"),
            (relational.nodes.iter().any(node_temporal_extrema), ":pg-native-temporal-extrema-v1"),
        ] {
            if used {
                compiled
                    .revision
                    .as_mut()
                    .context("missing scalar compiler revision")?
                    .push_str(suffix);
            }
        }
    }
    Ok(())
}
fn node_gap(node: &Node) -> bool {
    match node {
        Node::Filter { predicate, .. } => predicate.has_gap(),
        Node::Map { computed, .. } => computed.iter().any(|value| match &value.expression {
            Native::Case { case } => case.has_gap(),
            Native::Bin(_) | Native::Offset { .. } => false,
        }),
        Node::Partition { spec, .. }
        | Node::Statistics { spec, .. }
        | Node::Finalize { spec, .. } => spec
            .aggregates
            .iter()
            .any(|value| value.filter.as_ref().is_some_and(super::Predicate::has_gap)),
        _ => false,
    }
}

fn node_offset(node: &Node) -> bool {
    matches!(node, Node::Map { computed, .. } if computed.iter().any(|value| matches!(value.expression, Native::Offset { .. })))
}
fn node_temporal_extrema(node: &Node) -> bool {
    match node {
        Node::Partition { spec, .. }
        | Node::Statistics { spec, .. }
        | Node::Finalize { spec, .. } => spec.aggregates.iter().any(|value| {
            matches!(
                value.function,
                super::partition::Function::Min | super::partition::Function::Max
            ) && value.argument.as_ref().is_some_and(|arg| matches!(arg.oid, 1114 | 1184))
        }),
        _ => false,
    }
}
