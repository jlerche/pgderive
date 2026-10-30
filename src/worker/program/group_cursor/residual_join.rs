//! Differentiate LEFT ON matches per full left identity, before NULL extension.
use super::{is_left, unmatched};
use crate::{
    compiler::left_join::Spec,
    engine::{
        dataflow::{Group, GroupCursor, PartitionWork},
        execution::Consolidator,
    },
    transaction::Row,
    worker::program::{
        left_join::{matching, null_extend},
        relational::Key,
    },
};
use anyhow::Result;
use num_bigint::BigInt;
pub(super) fn operator(spec: &Spec) -> Group<Key, Row> {
    let spec = spec.clone();
    Group::new(move |input, work| {
        let spec = spec.clone();
        Box::pin(async move {
            let mut output = Consolidator::new(input.limits)?;
            for (cursor, sign) in [(input.before().await?, -1), (input.after().await?, 1)] {
                emit(&spec, cursor, sign, &mut output, work).await?;
            }
            output.finish_batch()
        })
    })
}
async fn emit(
    spec: &Spec,
    mut cursor: GroupCursor<Key, Row>,
    sign: i64,
    output: &mut Consolidator<((), Row)>,
    work: &mut PartitionWork,
) -> Result<()> {
    let start = cursor.clone();
    while let Some((left, weight)) = cursor.next(work).await? {
        if !is_left(&left)? {
            continue;
        }
        let mut right = start.clone();
        let mut matched = false;
        while let Some((row, count)) = right.next(work).await? {
            if is_left(&row)? {
                continue;
            }
            work.charge(1)?;
            if let Some(result) = matching(spec, &left, &row)? {
                matched = true;
                output.add(((), result), BigInt::from(weight) * count * sign)?;
            }
        }
        if !matched {
            output.add(((), unmatched(spec, &left)?), BigInt::from(weight) * sign)?;
        }
    }
    if spec.full {
        emit_right(spec, start, sign, output, work).await?;
    }
    Ok(())
}

async fn emit_right(
    spec: &Spec,
    mut cursor: GroupCursor<Key, Row>,
    sign: i64,
    output: &mut Consolidator<((), Row)>,
    work: &mut PartitionWork,
) -> Result<()> {
    let start = cursor.clone();
    while let Some((right, weight)) = cursor.next(work).await? {
        if is_left(&right)? {
            continue;
        }
        let mut left = start.clone();
        let mut matched = false;
        while let Some((row, _)) = left.next(work).await? {
            if !is_left(&row)? {
                continue;
            }
            work.charge(1)?;
            if matching(spec, &row, &right)?.is_some() {
                matched = true;
                break;
            }
        }
        if !matched {
            output.add(((), null_extend(&spec.left, &right)?), BigInt::from(weight) * sign)?;
        }
    }
    Ok(())
}
