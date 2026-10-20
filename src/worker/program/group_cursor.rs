//! SQL membership and left-join differentiation using bounded input deltas and cursors.
use super::{left_join::stripped, relational::Key};
use crate::{
    compiler::{ColumnRef, left_join::Spec},
    engine::{
        Batch,
        dataflow::{Group, GroupCursor, GroupInput, PartitionWork},
        execution::Consolidator,
    },
    transaction::Row,
};
use anyhow::{Context, Result, ensure};
use num_bigint::BigInt;
const SIDE: &str = "\0left_join_side";
fn is_left(row: &Row) -> Result<bool> {
    match row.get(SIDE).and_then(Option::as_deref) {
        Some("left") => Ok(true),
        Some("right") => Ok(false),
        _ => anyhow::bail!("invalid group input side"),
    }
}
fn project(columns: &[ColumnRef], row: &Row) -> Result<Row> {
    columns
        .iter()
        .map(|column| {
            Ok((
                column.name.clone(),
                row.get(&column.name).context("missing DISTINCT output")?.clone(),
            ))
        })
        .collect()
}
pub(super) fn distinct(columns: &[ColumnRef]) -> Group<Key, Row> {
    let columns = columns.to_vec();
    Group::new(move |input, work| {
        let columns = columns.clone();
        Box::pin(async move {
            let mut output = Vec::new();
            for (mut cursor, sign) in [(input.before().await?, -1), (input.after().await?, 1)] {
                if let Some((row, _)) = cursor.next(work).await? {
                    output.push((((), project(&columns, &row)?), sign));
                }
            }
            Batch::from_updates(output)
        })
    })
}
async fn present(mut cursor: GroupCursor<Key, Row>, work: &mut PartitionWork) -> Result<bool> {
    let Some((row, _)) = cursor.next(work).await? else { return Ok(false) };
    if is_left(&row)? {
        return Ok(false);
    }
    super::membership_counts::validate_presence(&row)?;
    Ok(true)
}
async fn emit_left(
    mut cursor: GroupCursor<Key, Row>,
    sign: i64,
    output: &mut Consolidator<((), Row)>,
    work: &mut PartitionWork,
) -> Result<()> {
    while let Some((row, weight)) = cursor.next(work).await? {
        if is_left(&row)? {
            output.add(((), stripped(&row)), BigInt::from(weight) * sign)?;
        }
    }
    Ok(())
}
pub(super) fn existence(anti: bool) -> Group<Key, Row> {
    Group::new(move |input, work| {
        Box::pin(async move {
            let old = present(input.before().await?, work).await? != anti;
            let new = present(input.after().await?, work).await? != anti;
            let mut output = Consolidator::new(input.limits)?;
            match (old, new) {
                (true, true) => emit_delta(input.delta, None, &mut output)?,
                (true, false) => emit_left(input.before().await?, -1, &mut output, work).await?,
                (false, true) => emit_left(input.after().await?, 1, &mut output, work).await?,
                (false, false) => {}
            }
            output.finish_batch()
        })
    })
}
fn pair(left: &Row, right: &Row) -> Result<Row> {
    let mut result = stripped(left);
    for (field, value) in stripped(right) {
        ensure!(result.insert(field, value).is_none(), "left join field collision");
    }
    Ok(result)
}
async fn products(
    input: &GroupInput<'_, Key, Row>,
    after: bool,
    changes: &[(&Row, i64)],
    output: &mut Consolidator<((), Row)>,
    work: &mut PartitionWork,
) -> Result<bool> {
    let mut cursor = if after { input.after().await? } else { input.before().await? };
    let mut present = false;
    while let Some((row, weight)) = cursor.next(work).await? {
        let left = is_left(&row)?;
        present |= !left;
        if left == after {
            continue;
        }
        for (changed, coefficient) in changes {
            work.charge(1)?;
            let result = if left { pair(&row, changed)? } else { pair(changed, &row)? };
            output.add(((), result), BigInt::from(weight) * coefficient)?;
        }
    }
    Ok(present)
}
fn unmatched(spec: &Spec, row: &Row) -> Result<Row> {
    let mut result = stripped(row);
    for column in &spec.right {
        ensure!(result.insert(column.name.clone(), None).is_none(), "left join field collision");
    }
    Ok(result)
}
async fn emit_unmatched(
    spec: &Spec,
    mut cursor: GroupCursor<Key, Row>,
    sign: i64,
    output: &mut Consolidator<((), Row)>,
    work: &mut PartitionWork,
) -> Result<()> {
    while let Some((row, weight)) = cursor.next(work).await? {
        if is_left(&row)? {
            output.add(((), unmatched(spec, &row)?), BigInt::from(weight) * sign)?;
        }
    }
    Ok(())
}
pub(super) fn left_join(spec: &Spec) -> Group<Key, Row> {
    let spec = spec.clone();
    Group::new(move |input, work| {
        let spec = spec.clone();
        Box::pin(async move {
            let mut output = Consolidator::new(input.limits)?;
            let (left, right) = split(input.delta)?;
            let old = products(&input, false, &right, &mut output, work).await?;
            let new = products(&input, true, &left, &mut output, work).await?;
            match (old, new) {
                (false, false) => emit_delta(input.delta, Some(&spec), &mut output)?,
                (false, true) => {
                    emit_unmatched(&spec, input.before().await?, -1, &mut output, work).await?;
                }
                (true, false) => {
                    emit_unmatched(&spec, input.after().await?, 1, &mut output, work).await?;
                }
                (true, true) => {}
            }
            output.finish_batch()
        })
    })
}

fn emit_delta(
    delta: &Batch<(), Row>,
    spec: Option<&Spec>,
    output: &mut Consolidator<((), Row)>,
) -> Result<()> {
    for (((), row), weight) in delta.iter() {
        if !is_left(row)? {
            continue;
        }
        let result = spec.map_or_else(|| Ok(stripped(row)), |spec| unmatched(spec, row))?;
        output.add(((), result), *weight)?;
    }
    Ok(())
}

type Rows<'a> = Vec<(&'a Row, i64)>;
fn split(delta: &Batch<(), Row>) -> Result<(Rows<'_>, Rows<'_>)> {
    let mut left = Vec::new();
    let mut right = Vec::new();
    for (((), row), weight) in delta.iter() {
        if is_left(row)? {
            left.push((row, *weight));
        } else {
            right.push((row, *weight));
        }
    }
    Ok((left, right))
}

#[cfg(test)]
mod tests;
