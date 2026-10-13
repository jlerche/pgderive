//! Specialize a sole-consumer `ROW_NUMBER` partition with a proven bounded cutoff.
use crate::compiler::{
    Predicate,
    partition::{Function, Mode},
    relational::{Node, Relational},
    topk::Spec,
};
pub(super) fn specialize(ir: &mut Relational, input: &str, predicate: &Predicate) -> bool {
    let Some(Node::Partition { id, input: parent, spec }) = ir.nodes.last() else { return false };
    if id != input || spec.aggregates.len() != 1 {
        return false;
    }
    let aggregate = &spec.aggregates[0];
    if !matches!(aggregate.function, Function::RowNumber) {
        return false;
    }
    let Mode::Ranking { order } = &spec.mode else {
        return false;
    };
    let Some(limit) = predicate.row_number_cutoff(&aggregate.field) else {
        return false;
    };
    let spec = Spec { order: order.clone(), field: aggregate.field.clone(), limit };
    let id = id.clone();
    let parent = parent.clone();
    let indexed = format!("{id}_topk_input");
    ir.nodes.pop();
    ir.nodes.push(Node::TopKInput { id: indexed.clone(), input: parent, spec: spec.clone() });
    ir.nodes.push(Node::TopK { id, input: indexed, spec });
    true
}
