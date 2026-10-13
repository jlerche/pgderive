//! Bind new expression semantics only when the resolved plan uses them.
use super::{Compiled, Program, expression::Expr, relational::Node, scalar::Native};
use anyhow::{Context, Result};
pub(super) fn bind(compiled: &mut Compiled) -> Result<()> {
    let terminal = match &compiled.program {
        Program::Projection { projection } => projection.terminal.as_ref(),
        Program::Relational { relational } => relational.output.terminal.as_ref(),
        Program::Grouped { .. } => None,
    };
    if terminal.is_some_and(crate::catalog::Terminal::has_array) {
        compiled
            .revision
            .as_mut()
            .context("missing terminal compiler revision")?
            .push_str(":pg-c-array-element-v1");
    }
    if terminal.is_some_and(crate::catalog::Terminal::has_numeric_multiply) {
        compiled
            .revision
            .as_mut()
            .context("missing decimal compiler revision")?
            .push_str(":pg-terminal-decimal-multiply-v1");
    }
    if terminal.is_some_and(crate::catalog::Terminal::has_timestamp_format) {
        compiled
            .revision
            .as_mut()
            .context("missing timestamp format revision")?
            .push_str(":pg-terminal-utc-calendar-format-v1");
    }
    let text = compiled.predicates.as_ref().is_some_and(Expr::has_text_equality)
        || match &compiled.program {
            Program::Relational { relational } => relational.nodes.iter().any(node_text),
            _ => false,
        };
    if text {
        compiled
            .revision
            .as_mut()
            .context("missing text compiler revision")?
            .push_str(":pg-deterministic-text-equality-v1");
    }
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
    if compiled.predicates.as_ref().is_some_and(Expr::has_typed)
        || matches!(&compiled.program, Program::Relational { relational } if relational.nodes.iter().any(node_typed))
    {
        compiled
            .revision
            .as_mut()
            .context("missing predicate compiler revision")?
            .push_str(":pg-checked-integral-predicate-v1");
    }
    relational_revisions(compiled)?;
    Ok(())
}
fn node_gap(node: &Node) -> bool {
    match node {
        Node::Filter { predicate, .. } => predicate.has_gap(),
        Node::Map { computed, .. } => computed.iter().any(|value| match &value.expression {
            Native::Case { case } => case.has_gap(),
            Native::Typed { typed } => typed.has_gap(),
            Native::Bin(_) | Native::Offset { .. } | Native::Remainder { .. } => false,
        }),
        Node::Partition { spec, .. }
        | Node::Statistics { spec, .. }
        | Node::OrderedFinalize { spec, .. }
        | Node::Finalize { spec, .. } => spec
            .aggregates
            .iter()
            .any(|value| value.filter.as_ref().is_some_and(super::Predicate::has_gap)),
        _ => false,
    }
}

fn node_text(node: &Node) -> bool {
    match node {
        Node::Filter { predicate, .. } => predicate.has_text_equality(),
        Node::Map { computed, .. } => computed.iter().any(|value| match &value.expression {
            Native::Case { case } => case.has_text_equality(),
            Native::Typed { typed } => typed.has_text_equality(),
            Native::Bin(_) | Native::Offset { .. } | Native::Remainder { .. } => false,
        }),
        Node::Partition { spec, .. }
        | Node::Statistics { spec, .. }
        | Node::OrderedFinalize { spec, .. }
        | Node::Finalize { spec, .. } => spec
            .aggregates
            .iter()
            .any(|value| value.filter.as_ref().is_some_and(super::Predicate::has_text_equality)),
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
        | Node::OrderedFinalize { spec, .. }
        | Node::Finalize { spec, .. } => spec.aggregates.iter().any(|value| {
            matches!(
                value.function,
                super::partition::Function::Min | super::partition::Function::Max
            ) && value.argument.as_ref().is_some_and(|arg| matches!(arg.oid, 1114 | 1184))
        }),
        _ => false,
    }
}

fn node_distinct(node: &Node) -> bool {
    matches!(node, Node::Partition { spec, .. } | Node::DistinctCounts { spec, .. } if spec.aggregates.iter().any(|value| matches!(value.function, super::partition::Function::CountDistinct)))
}

fn node_text_key(node: &Node) -> bool {
    matches!(node, Node::PartitionBy { keys, .. } if keys.iter().any(|key| matches!(key.oid, 25 | 1043)))
}

fn relational_revisions(compiled: &mut Compiled) -> Result<()> {
    if let Program::Relational { relational } = &compiled.program {
        for (used, suffix) in [
            (relational.nodes.iter().any(|node| matches!(node, Node::Map { computed, .. } if computed.iter().any(|value| matches!(value.expression, Native::Typed { .. })))), ":pg-checked-integral-scalar-v1"),
            (
                relational.nodes.iter().any(|node| matches!(node, Node::OrderedFirst { .. })),
                ":pg-ordered-full-identity-extrema-v1",
            ),
            (
                relational
                    .nodes
                    .iter()
                    .any(|node| matches!(node, Node::Partition { .. } | Node::Lookup { .. })),
                ":retained-partition-output-v1",
            ),
            (relational.nodes.iter().any(node_text_key), ":pg-deterministic-text-partition-v1"),
            (relational.nodes.iter().any(node_distinct), ":pg-integral-distinct-count-v1"),
            (
                relational.nodes.iter().any(|node| matches!(node, Node::DistinctCounts { .. })),
                ":pg-exact-distinct-membership-v1",
            ),
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

fn node_typed(node: &Node) -> bool {
    match node {
        Node::Filter { predicate, .. } => predicate.has_typed(),
        Node::Partition { spec, .. }
        | Node::Statistics { spec, .. }
        | Node::OrderedFinalize { spec, .. }
        | Node::Finalize { spec, .. } => spec
            .aggregates
            .iter()
            .any(|value| value.filter.as_ref().is_some_and(super::Predicate::has_typed)),
        _ => false,
    }
}
