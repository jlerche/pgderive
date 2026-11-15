//! SQL subquery comparisons preserve outer multiplicity and produce three-valued marks.
use super::{
    left_join::{pair, stripped},
    relational::Key,
};
use crate::{
    compiler::{ColumnRef, subquery::Mark},
    engine::{
        dataflow::{Group, GroupCursor, PartitionWork},
        execution::{CapacityExceeded, Consolidator, Limits},
    },
    transaction::Row,
};
use anyhow::{Context, Result, ensure};
use num_bigint::BigInt;
use std::{collections::BTreeSet, sync::Arc};
const SIDE: &str = "\0left_join_side";
const VALUE_OVERHEAD: u64 = 64;
fn left(row: &Row) -> Result<bool> {
    match row.get(SIDE).and_then(Option::as_deref) {
        Some("left") => Ok(true),
        Some("right") => Ok(false),
        _ => anyhow::bail!("invalid IN marker input side"),
    }
}
struct RightValues {
    seen: bool,
    null_value: bool,
    values: BTreeSet<String>,
}
const fn indexed(spec: &Mark) -> bool {
    // Component-preserving interval encodings can differ for equal durations.
    spec.skip.is_none()
        && spec.probe.oid != 1186
        && spec.comparison.is_equality()
        && spec.correlation.is_none()
        && (spec.probe.oid == spec.value.oid
            || (numeric_family(spec.probe.oid) && numeric_family(spec.value.oid))
            || (matches!(spec.probe.oid, 25 | 1043) && matches!(spec.value.oid, 25 | 1043)))
}
const fn numeric_family(oid: u32) -> bool {
    matches!(oid, 20 | 21 | 23 | 1700)
}
fn value_key(column: &ColumnRef, row: &Row) -> Result<Option<String>> {
    if !numeric_family(column.oid) {
        return crate::compiler::native_key(column, row);
    }
    row.get(&column.name)
        .context("missing IN comparison value")?
        .as_deref()
        .map(|text| Ok(crate::numeric::Numeric::parse(text)?.key()))
        .transpose()
}
async fn right_values(
    spec: &Mark,
    mut cursor: GroupCursor<Key, Row>,
    work: &mut PartitionWork,
    limits: Limits,
) -> Result<RightValues> {
    let mut set = RightValues { seen: false, null_value: false, values: BTreeSet::new() };
    let mut bytes = 0_u64;
    while let Some((row, _)) = cursor.next(work).await? {
        if left(&row)? {
            continue;
        }
        set.seen = true;
        let Some(key) = value_key(&spec.value, &row)? else {
            set.null_value = true;
            continue;
        };
        let length = u64::try_from(key.len())?.saturating_add(VALUE_OVERHEAD);
        if set.values.insert(key) {
            bytes = bytes.saturating_add(length);
            ensure!(
                bytes <= limits.output_bytes,
                CapacityExceeded("IN value set exceeds resident limit")
            );
        }
    }
    Ok(set)
}
fn indexed_membership(spec: &Mark, probe: &Row, set: &RightValues) -> Result<Option<bool>> {
    if !set.seen {
        return Ok(Some(false));
    }
    let Some(key) = value_key(&spec.probe, probe)? else { return Ok(None) };
    Ok(if set.values.contains(&key) {
        Some(true)
    } else if set.null_value {
        None
    } else {
        Some(false)
    })
}
pub(super) fn mark(spec: &Mark) -> Group<Key, Row> {
    let spec = Arc::new(spec.clone());
    Group::new(move |input, work| {
        let spec = Arc::clone(&spec);
        Box::pin(async move {
            let mut output = Consolidator::new(input.limits)?;
            for (cursor, sign) in [(input.before().await?, -1), (input.after().await?, 1)] {
                let set = if indexed(&spec) {
                    Some(right_values(&spec, cursor.clone(), work, input.limits).await?)
                } else {
                    None
                };
                emit(&spec, set.as_ref(), (cursor, sign, input.limits), &mut output, work).await?;
            }
            output.finish_batch()
        })
    })
}
async fn emit(
    spec: &Mark,
    set: Option<&RightValues>,
    state: (GroupCursor<Key, Row>, i64, Limits),
    output: &mut Consolidator<((), Row)>,
    work: &mut PartitionWork,
) -> Result<()> {
    let (mut cursor, sign, limits) = state;
    let start = cursor.clone();
    while let Some((row, weight)) = cursor.next(work).await? {
        if !left(&row)? {
            continue;
        }
        let value = match set {
            Some(set) => indexed_membership(spec, &row, set)?,
            None => membership(spec, &row, start.clone(), work, limits).await?,
        };
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
    limits: Limits,
) -> Result<Option<bool>> {
    let mut seen = DistinctKeys::default();
    let mut unknown = false;
    let mut skip = spec.skip.map_or(0, std::num::NonZeroUsize::get);
    while let Some((row, weight)) = right.next(work).await? {
        if left(&row)? {
            continue;
        }
        let joined = pair(probe, &row)?;
        if !spec.correlation.as_ref().map_or(Ok(true), |condition| condition.qualifies(&joined))? {
            continue;
        }
        match spec.comparison.truth((&joined, &joined))? {
            Some(true) if skip == 0 && spec.distinct.is_empty() => return Ok(Some(true)),
            Some(true) => {
                let count = if spec.distinct.is_empty() {
                    usize::try_from(weight)?
                } else {
                    usize::from(seen.insert(&spec.distinct, &row, limits)?)
                };
                if count > skip {
                    return Ok(Some(true));
                }
                skip -= count;
            }
            Some(false) => {}
            None => unknown = true,
        }
    }
    Ok((!unknown).then_some(false))
}

#[derive(Default)]
struct DistinctKeys {
    values: BTreeSet<Vec<Option<String>>>,
    bytes: u64,
}

impl DistinctKeys {
    fn insert(&mut self, columns: &[ColumnRef], row: &Row, limits: Limits) -> Result<bool> {
        let key =
            columns.iter().map(|column| group_value(column, row)).collect::<Result<Vec<_>>>()?;
        let bytes = key.iter().try_fold(VALUE_OVERHEAD, |bytes, value| -> Result<u64> {
            let size = value.as_ref().map_or(0, String::capacity);
            Ok(bytes.saturating_add(VALUE_OVERHEAD).saturating_add(u64::try_from(size)?))
        })?;
        if !self.values.insert(key) {
            return Ok(false);
        }
        self.bytes = self.bytes.saturating_add(bytes);
        ensure!(
            self.bytes <= limits.output_bytes,
            CapacityExceeded("EXISTS group keys exceed resident limit")
        );
        Ok(true)
    }
}

fn group_value(column: &ColumnRef, row: &Row) -> Result<Option<String>> {
    if column.oid == 1186 {
        return row
            .get(&column.name)
            .context("missing EXISTS interval key")?
            .as_deref()
            .map(|value| {
                let interval = crate::temporal::Interval::parse(value)?;
                Ok(serde_json::to_string(&interval.duration_key())?)
            })
            .transpose();
    }
    crate::compiler::native_key(column, row)
}
