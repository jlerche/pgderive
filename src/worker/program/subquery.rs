//! SQL IN is a three-valued mark, not a multiplicity-changing semijoin.
use super::{
    left_join::{pair, stripped},
    relational::Key,
};
use crate::{
    compiler::subquery::Mark,
    engine::{
        dataflow::{Group, GroupCursor, PartitionWork},
        execution::Consolidator,
    },
    transaction::Row,
};
use anyhow::{Context, Result};
use num_bigint::BigInt;
const SIDE: &str = "\0left_join_side";
fn left(row: &Row) -> Result<bool> {
    match row.get(SIDE).and_then(Option::as_deref) {
        Some("left") => Ok(true),
        Some("right") => Ok(false),
        _ => anyhow::bail!("invalid IN marker input side"),
    }
}
pub(super) fn mark(spec: &Mark) -> Group<Key, Row> {
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
    spec: &Mark,
    mut cursor: GroupCursor<Key, Row>,
    sign: i64,
    output: &mut Consolidator<((), Row)>,
    work: &mut PartitionWork,
) -> Result<()> {
    let start = cursor.clone();
    while let Some((row, weight)) = cursor.next(work).await? {
        if !left(&row)? {
            continue;
        }
        let value = membership(spec, &row, start.clone(), work).await?;
        let mut row = stripped(&row);
        row.insert(
            spec.output.name.clone(),
            value.map(|value| if value { "t" } else { "f" }.into()),
        );
        output.add(((), row), BigInt::from(weight) * sign)?;
    }
    Ok(())
}
async fn membership(
    spec: &Mark,
    probe: &Row,
    mut right: GroupCursor<Key, Row>,
    work: &mut PartitionWork,
) -> Result<Option<bool>> {
    let null_probe = probe.get(&spec.probe.name).context("missing IN probe")?.is_none();
    let mut seen = false;
    let mut null_value = false;
    while let Some((row, _)) = right.next(work).await? {
        if left(&row)? {
            continue;
        }
        let joined = pair(probe, &row)?;
        if !spec.correlation.as_ref().map_or(Ok(true), |condition| condition.qualifies(&joined))? {
            continue;
        }
        seen = true;
        null_value |= row.get(&spec.value.name).context("missing IN value")?.is_none();
        if spec.comparison.qualifies(&joined)? {
            return Ok(Some(true));
        }
    }
    Ok(if seen && (null_probe || null_value) { None } else { Some(false) })
}
