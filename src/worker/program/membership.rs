//! Retained full-tuple DISTINCT and existence partition transformations.
use super::relational::Key;
use crate::{
    compiler::ColumnRef,
    engine::{
        Batch,
        dataflow::{Partition, PartitionWork},
        execution::{Consolidator, Limits},
    },
    transaction::Row,
};
use anyhow::{Context, Result, ensure};
const SIDE: &str = "\0left_join_side";
pub(super) fn distinct(columns: &[ColumnRef]) -> Partition<Key, Row> {
    let incremental = columns.to_vec();
    let columns = columns.to_vec();
    Partition::new_zero_preserving(move |_: &Key, rows: &Batch<(), Row>, work| {
        for (((), _), weight) in rows.iter() {
            work.charge(1)?;
            ensure!(*weight > 0, "negative DISTINCT membership");
        }
        // Navigation partitions projected values, but the retained input keeps
        // every complete row. Presence needs no narrowed occurrence total.
        let (((), first), _) = rows.iter().next().context("empty DISTINCT partition")?;
        let projected = columns
            .iter()
            .map(|column| {
                Ok((
                    column.name.clone(),
                    first.get(&column.name).context("missing DISTINCT output")?.clone(),
                ))
            })
            .collect::<Result<Row>>()?;
        Batch::from_updates([(((), projected), 1)])
    })
    .with_incremental(move |_, before, after, _, work| {
        let mut changes = Vec::new();
        for (bag, sign) in [(before, -1), (after, 1)] {
            work.charge(1)?;
            if let Some((((), row), _)) = bag.iter().next() {
                let row = incremental
                    .iter()
                    .map(|column| {
                        Ok((
                            column.name.clone(),
                            row.get(&column.name).context("missing DISTINCT output")?.clone(),
                        ))
                    })
                    .collect::<Result<Row>>()?;
                changes.push((((), row), sign));
            }
        }
        Batch::from_updates(changes)
    })
}
pub(super) fn existence(anti: bool, limits: Limits) -> Partition<Key, Row> {
    Partition::new_zero_preserving(move |_: &Key, rows: &Batch<(), Row>, work| {
        let mut present = false;
        for (((), row), weight) in rows.iter() {
            work.charge(1)?;
            ensure!(*weight > 0, "negative existence membership");
            match row.get(SIDE).and_then(Option::as_deref) {
                Some("right") => present = true,
                Some("left") => {}
                _ => anyhow::bail!("invalid existence input tag"),
            }
        }
        let mut output = Consolidator::new(limits)?;
        for (((), row), weight) in rows.iter() {
            work.charge(1)?;
            if row.get(SIDE).and_then(Option::as_deref) == Some("left") && present != anti {
                let mut row = row.clone();
                row.remove(SIDE);
                output.add(((), row), *weight)?;
            }
        }
        output.finish_batch()
    })
    .with_incremental(move |_, before, after, delta, work| {
        let old = present(before, work)? != anti;
        let new = present(after, work)? != anti;
        let (bag, sign) = match (old, new) {
            (true, true) => (delta, 1),
            (true, false) => (before, -1),
            (false, true) => (after, 1),
            (false, false) => return Batch::from_updates([]),
        };
        let mut output = Consolidator::new(limits)?;
        for (((), row), weight) in bag.iter() {
            work.charge(1)?;
            if row.get(SIDE).and_then(Option::as_deref) == Some("left") {
                let mut row = row.clone();
                row.remove(SIDE);
                output.add(((), row), num_bigint::BigInt::from(*weight) * sign)?;
            }
        }
        output.finish_batch()
    })
}

fn present(bag: &Batch<(), Row>, work: &mut PartitionWork) -> Result<bool> {
    for (((), row), _) in bag.iter() {
        work.charge(1)?;
        match row.get(SIDE).and_then(Option::as_deref) {
            Some("right") => return Ok(true),
            Some("left") => {}
            _ => anyhow::bail!("invalid existence input tag"),
        }
    }
    Ok(false)
}
