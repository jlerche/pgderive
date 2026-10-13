//! Grouped extrema use an explicitly ordered full-tuple input arrangement.
use crate::compiler::{
    ColumnRef,
    partition::{Function, Mode, Partition},
    relational::Node,
    scalar::Computed,
};
pub(super) fn supports(spec: &Partition) -> bool {
    matches!(spec.mode, Mode::Grouped { .. })
        && !spec.aggregates.is_empty()
        && spec
            .aggregates
            .iter()
            .all(|value| matches!(value.function, Function::Min | Function::Max))
}
pub(super) fn nodes(keys: Vec<ColumnRef>, spec: Partition, computed: Vec<Computed>) -> Vec<Node> {
    let mapped = !computed.is_empty();
    let mut nodes = vec![Node::Source { id: "source".into(), source: 0 }];
    if mapped {
        nodes.push(Node::Map { id: "mapped".into(), input: "source".into(), computed });
    }
    nodes.extend([
        Node::PartitionBy {
            id: "partition_input".into(),
            input: if mapped { "mapped" } else { "source" }.into(),
            keys,
        },
        Node::OrderedInput {
            id: "ordered_input".into(),
            input: "partition_input".into(),
            spec: spec.clone(),
        },
        Node::OrderedFirst {
            id: "ordered_first".into(),
            input: "ordered_input".into(),
            spec: spec.clone(),
        },
        Node::OrderedGroup { id: "ordered_group".into(), input: "ordered_first".into() },
        Node::OrderedFinalize { id: "partition".into(), input: "ordered_group".into(), spec },
        Node::Output { id: "project".into(), input: "partition".into() },
    ]);
    nodes
}
