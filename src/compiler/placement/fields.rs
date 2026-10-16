//! Exact scalar lineage through row-preserving nodes. Stateful encodings remain opaque.
use super::{Consumer, ConsumerKind, Stage};
use crate::compiler::{ColumnRef, Compiled, relational::Node};
use std::collections::{BTreeMap, BTreeSet};

type Uses = BTreeMap<String, BTreeSet<Consumer>>;

pub(super) fn consumers(compiled: &Compiled, stages: &[Stage]) -> Uses {
    let nodes = &compiled.relational.nodes;
    let indices: BTreeMap<_, _> =
        stages.iter().enumerate().map(|(i, stage)| (stage.id.as_str(), i)).collect();
    let mut uses = Uses::new();
    for (index, node) in nodes.iter().enumerate() {
        let (columns, kind) = match node {
            Node::Map { computed, .. } => (
                computed.iter().flat_map(|value| value.expression.columns()).collect(),
                ConsumerKind::IntermediateComputation,
            ),
            Node::Filter { predicate, .. } => (predicate.columns(), ConsumerKind::Predicate),
            Node::EquijoinKey { keys, .. } => (keys.clone(), ConsumerKind::Join),
            Node::LeftJoinInput { spec, .. } => (spec.keys.clone(), ConsumerKind::Join),
            Node::LookupInput { spec, .. } => (spec.keys.clone(), ConsumerKind::Join),
            Node::TopKInput { spec, .. } => (
                spec.order.iter().map(|order| order.column.clone()).collect(),
                ConsumerKind::Ordering,
            ),
            Node::Partition { spec, .. }
            | Node::Statistics { spec, .. }
            | Node::OrderedInput { spec, .. }
            | Node::DistinctInput { spec, .. } => {
                (partition_columns(spec), ConsumerKind::AggregateOrWindow)
            }
            Node::PartitionBy { keys, .. } => (keys.clone(), ConsumerKind::GroupOrPartition),
            Node::SetDistinct { columns, .. } => (columns.clone(), ConsumerKind::SetOperation),
            Node::Output { .. } => (
                compiled.output().columns.iter().map(|value| value.column.clone()).collect(),
                ConsumerKind::Output,
            ),
            _ => continue,
        };
        let consumer = Consumer { node: stages[index].id.clone(), kind };
        if let Some(input) = stages[index].inputs.first() {
            for column in columns {
                trace(
                    indices[input.as_str()],
                    column,
                    &consumer,
                    &mut uses,
                    &Graph { nodes, stages, indices: &indices },
                );
            }
        }
    }
    uses
}

struct Graph<'a> {
    nodes: &'a [Node],
    stages: &'a [Stage],
    indices: &'a BTreeMap<&'a str, usize>,
}

fn trace(index: usize, column: ColumnRef, consumer: &Consumer, uses: &mut Uses, graph: &Graph<'_>) {
    let mut pending = vec![(index, column.name)];
    let mut visited = BTreeSet::new();
    while let Some((index, column)) = pending.pop() {
        if !visited.insert((index, column.clone())) {
            continue;
        }
        let stage = &graph.stages[index];
        let columns = match &graph.nodes[index] {
            Node::Map { computed, .. } => {
                if let Some(value) = computed.iter().find(|value| value.column.name == column) {
                    uses.entry(format!("local:{}:{}", stage.id, column))
                        .or_default()
                        .insert(consumer.clone());
                    value.expression.columns().into_iter().map(|column| column.name).collect()
                } else {
                    vec![column]
                }
            }
            Node::Finalize { spec, .. } => super::finalizers::operands(spec, &column),
            Node::Statistics { spec, .. } => super::finalizers::measurements(spec, &column)
                .into_iter()
                .map(|column| column.name)
                .collect(),
            Node::Rebind { fields, .. } => fields
                .iter()
                .filter(|binding| binding.output.name == column)
                .map(|binding| binding.input.name.clone())
                .collect(),
            Node::Filter { .. }
            | Node::CrossKey { .. }
            | Node::EquijoinKey { .. }
            | Node::PartitionBy { .. }
            | Node::Output { .. } => vec![column],
            // Do not infer field identity through other packed aggregate/window/join state.
            _ => continue,
        };
        if let Some(input) = stage.inputs.first() {
            pending
                .extend(columns.into_iter().map(|column| (graph.indices[input.as_str()], column)));
        }
    }
}

fn partition_columns(spec: &crate::compiler::partition::Partition) -> Vec<ColumnRef> {
    use crate::compiler::{navigation, partition::Mode};
    let mut columns = match &spec.mode {
        Mode::Grouped { keys } => keys.clone(),
        Mode::Rows { order, .. } | Mode::Ranking { order } | Mode::Peers { order, .. } => {
            order.iter().map(|order| order.column.clone()).collect()
        }
    };
    for aggregate in &spec.aggregates {
        columns.extend(aggregate.argument.clone());
        if let Some(filter) = &aggregate.filter {
            columns.extend(filter.columns());
        }
        if let Some(navigation) = &aggregate.navigation {
            if let navigation::Offset::Column(column) = &navigation.offset {
                columns.push(column.clone());
            }
            if let navigation::Default::Column(column) = &navigation.default {
                columns.push(column.clone());
            }
        }
    }
    columns
}
