//! Integral distinct aggregation over retained positive full-tuple bags.
use super::aggregate::{Contribution, Linear, contribution};
use crate::{
    compiler::partition::{Aggregate, Function},
    engine::Batch,
    transaction::Row,
};
use anyhow::Result;
use num_bigint::BigInt;
use std::collections::BTreeSet;

pub(super) fn evaluate(
    aggregate: &Aggregate,
    bag: &Batch<(), Row>,
    work: &mut crate::engine::dataflow::PartitionWork,
) -> Result<Option<String>> {
    work.charge(bag.iter().count())?;
    let mut values = BTreeSet::new();
    let mut state = Linear::default();
    for (((), row), _) in bag.iter() {
        if let Some(Contribution::Value(value)) = contribution(aggregate, row)? {
            let value = value.parse::<i64>()?;
            if values.insert(value) {
                state.count += 1;
                if !matches!(aggregate.function, Function::CountDistinct) {
                    state.sum += BigInt::from(value);
                }
            }
        }
    }
    state.result(aggregate)
}
