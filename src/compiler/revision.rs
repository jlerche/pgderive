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
    Ok(())
}
fn node_gap(node: &Node) -> bool {
    match node {
        Node::Filter { predicate, .. } => predicate.has_gap(),
        Node::Map { computed, .. } => computed.iter().any(|value| match &value.expression {
            Native::Case { case } => case.has_gap(),
            Native::Bin(_) => false,
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
