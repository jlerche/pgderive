//! Exact LIMIT-one predecessor selection over affected tagged source bags.
use super::relational::Key;
use crate::{
    compiler::{
        ColumnRef,
        lookup::{Input, Lookup, SIDE},
    },
    engine::{
        Batch,
        dataflow::{PartitionWork, Project},
        execution::{Consolidator, Limits},
    },
    transaction::Row,
};
use anyhow::{Context, Result, ensure};
pub(super) fn input(spec: &Input) -> Project<Key, Row, Key, Row> {
    let spec = spec.clone();
    Project::new(move |_: &Key, row: &Row| {
        let keys = spec
            .keys
            .iter()
            .map(|column| {
                serde_json::to_string(row.get(&column.name).context("missing lookup key")?)
                    .map_err(Into::into)
            })
            .collect::<Result<_>>()?;
        let mut row = row.clone();
        ensure!(
            row.insert(SIDE.into(), Some(spec.side.to_string())).is_none(),
            "duplicate lookup tag"
        );
        Ok(Some((keys, row)))
    })
}
struct Candidate<'a> {
    sort: Vec<Option<i64>>,
    bound: Option<i64>,
    row: &'a Row,
}
pub(super) fn evaluate(
    spec: &Lookup,
    rows: &Batch<(), Row>,
    limits: Limits,
    work: &mut PartitionWork,
) -> Result<Batch<(), Row>> {
    let mut right = candidates(spec, rows, limits, work)?;
    right.sort_by(|left, right| super::partition::compare(&left.sort, &right.sort, &spec.order));
    let mut outputs = Consolidator::new(limits)?;
    for (((), left), weight) in rows.iter() {
        work.charge(1)?;
        if side(left)? != "0" {
            continue;
        }
        let selected = select(spec, left, &right, work)?;
        let mut row = left.clone();
        row.remove(SIDE);
        for field in &spec.right_fields {
            let value = selected
                .map(|right| right.get(field).context("missing right lookup field").cloned())
                .transpose()?
                .flatten();
            ensure!(row.insert(field.clone(), value).is_none(), "duplicate lookup output field");
        }
        // LIMIT 1 selects one occurrence; right multiplicity does not multiply output.
        outputs.add(((), row), *weight)?;
    }
    outputs.finish_batch()
}
fn side(row: &Row) -> Result<&str> {
    let side = row.get(SIDE).and_then(Option::as_deref).context("missing lookup tag")?;
    ensure!(matches!(side, "0" | "1"), "invalid lookup tag");
    Ok(side)
}
fn candidates<'a>(
    spec: &Lookup,
    rows: &'a Batch<(), Row>,
    limits: Limits,
    work: &mut PartitionWork,
) -> Result<Vec<Candidate<'a>>> {
    let mut right = Vec::new();
    let mut bytes = 0_u64;
    for (((), row), weight) in rows.iter() {
        work.charge(1)?;
        ensure!(*weight > 0, "negative lookup multiplicity");
        if side(row)? != "1" {
            continue;
        }
        work.charge(spec.order.len())?;
        let sort =
            spec.order.iter().map(|order| value(&order.column, row)).collect::<Result<Vec<_>>>()?;
        bytes = bytes
            .checked_add(u64::try_from(crate::engine::execution::record_size(
                &sort,
                limits.record_bytes,
            )?)?)
            .context("lookup sort byte overflow")?;
        ensure!(
            bytes <= limits.output_bytes && right.len() < limits.output_entries,
            "lookup sort budget exceeded"
        );
        right.push(Candidate { sort, bound: value(&spec.right_bound, row)?, row });
    }
    Ok(right)
}
fn select<'a>(
    spec: &Lookup,
    left: &Row,
    right: &[Candidate<'a>],
    work: &mut PartitionWork,
) -> Result<Option<&'a Row>> {
    for key in &spec.left_keys {
        if left.get(&key.name).context("missing left lookup key")?.is_none() {
            return Ok(None);
        }
    }
    let Some(bound) = value(&spec.left_bound, left)? else { return Ok(None) };
    for candidate in right {
        work.charge(1)?;
        if candidate.bound.is_some_and(|value| value < bound || spec.inclusive && value == bound) {
            return Ok(Some(candidate.row));
        }
    }
    Ok(None)
}
fn value(column: &ColumnRef, row: &Row) -> Result<Option<i64>> {
    row.get(&column.name)
        .context("missing lookup ordered value")?
        .as_deref()
        .map(|value| match column.oid {
            1114 | 1184 => Ok(crate::temporal::Timestamp::parse(value, column.oid)?.sort_value()),
            _ => value.parse::<i64>().map_err(Into::into),
        })
        .transpose()
}
