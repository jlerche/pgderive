//! Bind new expression semantics only when the resolved plan uses them.
use super::{Compiled, Program, expression::Expr, relational::Node, scalar::Native};
use anyhow::{Context, Result};
pub(super) fn bind(compiled: &mut Compiled) -> Result<()> {
    let terminal = match &compiled.program {
        Program::Projection { projection } => projection.terminal.as_ref(),
        Program::Relational { relational } => relational.output.terminal.as_ref(),
        Program::Grouped { .. } => None,
    };
    let nodes = match &compiled.program {
        Program::Relational { relational } => relational.nodes.as_slice(),
        _ => &[],
    };
    let predicate = |check: fn(&Expr) -> bool, include_map| {
        compiled.predicates.as_ref().is_some_and(check)
            || nodes.iter().any(|node| node_predicates(node, check, include_map))
    };
    for (used, suffix, context) in [
        (
            terminal.is_some_and(crate::catalog::Terminal::has_array),
            ":pg-c-array-element-v1",
            "missing terminal compiler revision",
        ),
        (
            terminal.is_some_and(crate::catalog::Terminal::has_numeric_multiply),
            ":pg-terminal-decimal-multiply-v1",
            "missing decimal compiler revision",
        ),
        (
            terminal.is_some_and(crate::catalog::Terminal::has_timestamp_format),
            ":pg-terminal-utc-calendar-format-v1",
            "missing timestamp format revision",
        ),
        (
            predicate(Expr::has_text_equality, true),
            ":pg-deterministic-text-equality-v1",
            "missing text compiler revision",
        ),
        (predicate(Expr::has_gap, true), ":pg-timestamp-gap-v1", "missing gap compiler revision"),
        (
            predicate(Expr::has_typed, false),
            ":pg-checked-integral-predicate-v1",
            "missing predicate compiler revision",
        ),
        (
            compiled.predicates.as_ref().is_some_and(Expr::has_numeric)
                || nodes.iter().any(|node| match node {
                    Node::Map { computed, .. } => {
                        computed.iter().any(|value| value.column.oid == 1700)
                    }
                    Node::Filter { predicate, .. } => predicate.matches(Expr::has_numeric),
                    _ => false,
                }),
            ":pg-circuit-numeric-bigint-scale-4096-v1",
            "missing numeric compiler revision",
        ),
    ] {
        if used {
            compiled.revision.as_mut().context(context)?.push_str(suffix);
        }
    }
    relational_revisions(compiled)
}
fn node_predicates(node: &Node, check: fn(&Expr) -> bool, include_map: bool) -> bool {
    match node {
        Node::Filter { predicate, .. } => predicate.matches(check),
        Node::Map { computed, .. } if include_map => {
            computed.iter().any(|value| match &value.expression {
                Native::Case { case } => case.predicates(check),
                Native::Typed { typed } => typed.predicates(check),
                Native::Bin(_) | Native::Offset { .. } | Native::Remainder { .. } => false,
            })
        }
        Node::Partition { spec, .. }
        | Node::Statistics { spec, .. }
        | Node::OrderedFinalize { spec, .. }
        | Node::Finalize { spec, .. } => spec
            .aggregates
            .iter()
            .any(|value| value.filter.as_ref().is_some_and(|filter| filter.matches(check))),
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
