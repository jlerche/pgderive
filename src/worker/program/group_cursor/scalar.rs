//! A keyed scalar join emits a cardinality marker for evaluation at the SQL read site.
use super::{Key, Spec, is_left, pair, unmatched};
use crate::{
    compiler::ColumnRef,
    engine::{
        dataflow::{Group, GroupCursor, GroupInput, PartitionWork},
        execution::Consolidator,
    },
    transaction::Row,
};
use anyhow::Result;
use num_bigint::BigInt;
use std::sync::Arc;

#[derive(PartialEq, Eq)]
enum Scalar {
    Empty,
    One(Row),
    Multiple,
}

async fn read(mut cursor: GroupCursor<Key, Row>, work: &mut PartitionWork) -> Result<Scalar> {
    let mut value = Scalar::Empty;
    while let Some((row, weight)) = cursor.next(work).await? {
        if !is_left(&row)? {
            if weight > 1 || value != Scalar::Empty {
                return Ok(Scalar::Multiple);
            }
            value = Scalar::One(row);
        }
    }
    Ok(value)
}

fn output(spec: &Spec, marker: &ColumnRef, row: &Row, value: &Scalar) -> Result<Row> {
    let mut output = match value {
        Scalar::One(other) => pair(row, other)?,
        Scalar::Empty | Scalar::Multiple => unmatched(spec, row)?,
    };
    output.insert(
        marker.name.clone(),
        Some(if *value == Scalar::Multiple { "t" } else { "f" }.into()),
    );
    Ok(output)
}

pub(super) fn operator(spec: &Spec, marker: &ColumnRef) -> Group<Key, Row> {
    let spec = Arc::new((spec.clone(), marker.clone()));
    Group::new(move |input, work| {
        let spec = Arc::clone(&spec);
        Box::pin(async move { evaluate((&spec.0, &spec.1), input, work).await })
    })
}

async fn evaluate(
    (spec, marker): (&Spec, &ColumnRef),
    input: GroupInput<'_, Key, Row>,
    work: &mut PartitionWork,
) -> Result<crate::engine::Batch<(), Row>> {
    let before = input.before().await?;
    let after = input.after().await?;
    let old = read(before.clone(), work).await?;
    let new = read(after.clone(), work).await?;
    let mut delta = Consolidator::new(input.limits)?;
    if old == new {
        for (((), row), weight) in input.delta.iter() {
            if is_left(row)? {
                delta.add(((), output(spec, marker, row, &new)?), *weight)?;
            }
        }
    } else {
        for (mut cursor, value, sign) in [(before, old, -1), (after, new, 1)] {
            while let Some((row, weight)) = cursor.next(work).await? {
                if is_left(&row)? {
                    delta.add(
                        ((), output(spec, marker, &row, &value)?),
                        BigInt::from(weight) * sign,
                    )?;
                }
            }
        }
    }
    delta.finish_batch()
}
