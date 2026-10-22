//! Prefix RANGE aggregates use durable ordered rows and binary-prefix summaries.
use crate::compiler::{
    ColumnRef,
    partition::{Function, Mode, Partition},
    relational::Node,
    scalar::Computed,
};
pub(super) fn supports(spec: &Partition) -> bool {
    let Mode::Peers { order, frame, groups: false } = &spec.mode else { return false };
    let [order] = order.as_slice() else { return false };
    frame.start.is_none()
        && frame.end == Some(0)
        && !order.descending
        && !order.column.nullable
        && matches!(order.column.oid, 20 | 21 | 23)
        && !spec.aggregates.is_empty()
        && spec.aggregates.len() <= 64
        && spec.aggregates.iter().all(|value| {
            matches!(value.function, Function::Count)
                || matches!(value.function, Function::Sum | Function::Average)
                    && value.argument.as_ref().is_some_and(|arg| matches!(arg.oid, 20 | 21 | 23))
        })
}
pub(super) fn nodes(keys: Vec<ColumnRef>, spec: Partition, computed: Vec<Computed>) -> Vec<Node> {
    let mut nodes = super::input_nodes(keys, computed);
    nodes.extend([
        Node::PrefixInput {
            id: "prefix_input".into(),
            input: "partition_input".into(),
            spec: spec.clone(),
        },
        Node::PrefixStatistics {
            id: "prefix_statistics".into(),
            input: "prefix_input".into(),
            spec: spec.clone(),
        },
        Node::PrefixWindow {
            id: "partition".into(),
            left: "prefix_input".into(),
            right: "prefix_statistics".into(),
            spec,
        },
        Node::Output { id: "project".into(), input: "partition".into() },
    ]);
    nodes
}
