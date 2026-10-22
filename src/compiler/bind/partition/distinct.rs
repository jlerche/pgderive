//! Lower exact grouped DISTINCT through sparse counts and membership deltas.
use crate::compiler::{
    ColumnRef,
    partition::{Function, Mode, Partition},
    relational::Node,
    scalar::Computed,
};
pub(super) fn supports(spec: &Partition) -> bool {
    matches!(spec.mode, Mode::Grouped { .. })
        && spec
            .aggregates
            .iter()
            .any(|aggregate| matches!(aggregate.function, Function::CountDistinct))
        && spec.aggregates.iter().all(|aggregate| {
            matches!(
                aggregate.function,
                Function::Count | Function::CountDistinct | Function::Sum | Function::Average
            )
        })
}
pub(super) fn nodes(keys: Vec<ColumnRef>, spec: Partition, computed: Vec<Computed>) -> Vec<Node> {
    let mut nodes = super::input_nodes(keys, computed);
    nodes.extend([
        Node::DistinctInput {
            id: "distinct_input".into(),
            input: "partition_input".into(),
            spec: spec.clone(),
        },
        Node::DistinctCounts {
            id: "distinct_counts".into(),
            input: "distinct_input".into(),
            spec: spec.clone(),
        },
        Node::DistinctMembership {
            id: "distinct_membership".into(),
            input: "distinct_counts".into(),
            spec: spec.clone(),
        },
        Node::Union {
            id: "statistics_input".into(),
            left: "partition_input".into(),
            right: "distinct_membership".into(),
        },
        Node::Statistics {
            id: "statistics".into(),
            input: "statistics_input".into(),
            spec: spec.clone(),
        },
        Node::Finalize { id: "partition".into(), input: "statistics".into(), spec },
        Node::Output { id: "project".into(), input: "partition".into() },
    ]);
    nodes
}
