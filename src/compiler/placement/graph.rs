use super::{Consumer, ConsumerKind, Stage};
use crate::compiler::relational::Node;
use anyhow::{Result, ensure};
use std::collections::{BTreeMap, BTreeSet};

type Declaration<'a> = (&'a str, Vec<&'a str>, Option<ConsumerKind>);
fn declaration(node: &Node) -> Declaration<'_> {
    use ConsumerKind as C;
    match node {
        Node::Source { id, .. } => (id, vec![], None),
        Node::Map { id, input, .. }
        | Node::Rebind { id, input, .. }
        | Node::CrossKey { id, input }
        | Node::Output { id, input } => (id, vec![input], None),
        Node::Expand { id, input, .. } => (id, vec![input], Some(C::IntermediateComputation)),
        Node::Filter { id, input, .. } => (id, vec![input], Some(C::Predicate)),
        Node::Join { id, left, right } => (id, vec![left, right], Some(C::Join)),
        Node::Union { id, left, right } => (id, vec![left, right], Some(C::SetOperation)),
        Node::EquijoinKey { id, input, .. }
        | Node::LeftJoinInput { id, input, .. }
        | Node::LeftJoin { id, input, .. }
        | Node::LookupInput { id, input, .. }
        | Node::Lookup { id, input, .. } => (id, vec![input], Some(C::Join)),
        Node::PartitionBy { id, input, .. } => (id, vec![input], Some(C::GroupOrPartition)),
        Node::SetDistinct { id, input, .. }
        | Node::Existence { id, input, .. }
        | Node::DistinctInput { id, input, .. }
        | Node::DistinctCounts { id, input, .. }
        | Node::DistinctMembership { id, input, .. } => (id, vec![input], Some(C::SetOperation)),
        Node::Partition { id, input, .. }
        | Node::Statistics { id, input, .. }
        | Node::Finalize { id, input, .. } => (id, vec![input], Some(C::AggregateOrWindow)),
        Node::TopKInput { id, input, .. }
        | Node::TopK { id, input, .. }
        | Node::OrderedInput { id, input, .. }
        | Node::OrderedFirst { id, input, .. }
        | Node::OrderedGroup { id, input }
        | Node::OrderedFinalize { id, input, .. } => (id, vec![input], Some(C::Ordering)),
    }
}

pub(super) fn stages(nodes: &[Node]) -> Result<Vec<Stage>> {
    let mut stages = Vec::new();
    let mut indices = BTreeMap::new();
    let mut seeds = Vec::new();
    for node in nodes {
        let (id, inputs, consumer) = declaration(node);
        ensure!(!indices.contains_key(id), "duplicate placement stage");
        ensure!(
            inputs.iter().all(|input| indices.contains_key(input)),
            "placement stage input missing or cyclic"
        );
        if let Some(kind) = consumer {
            seeds.push((stages.len(), Consumer { node: id.into(), kind }));
        }
        // A map needs its operands before it can execute. Its own result can
        // remain a candidate when no later maintenance operator consumes it.
        if matches!(node, Node::Map { .. }) {
            for input in &inputs {
                seeds.push((
                    indices[input],
                    Consumer { node: id.into(), kind: ConsumerKind::IntermediateComputation },
                ));
            }
        }
        indices.insert(id, stages.len());
        stages.push(Stage {
            id: id.into(),
            inputs: inputs.into_iter().map(str::to_owned).collect(),
            maintenance_consumers: Vec::new(),
        });
    }
    for (root, consumer) in seeds {
        mark(root, &consumer, &indices, &mut stages);
    }
    for stage in &mut stages {
        stage.maintenance_consumers.sort();
        stage.maintenance_consumers.dedup();
    }
    Ok(stages)
}

fn mark(root: usize, consumer: &Consumer, indices: &BTreeMap<&str, usize>, stages: &mut [Stage]) {
    let mut pending = vec![root];
    let mut visited = BTreeSet::new();
    while let Some(index) = pending.pop() {
        if !visited.insert(index) {
            continue;
        }
        let stage = &mut stages[index];
        stage.maintenance_consumers.push(consumer.clone());
        pending.extend(stage.inputs.iter().map(|input| indices[input.as_str()]));
    }
}
