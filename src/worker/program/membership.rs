//! Retained full-tuple DISTINCT and existence partition transformations.
use super::relational::Key;
use crate::{
    compiler::ColumnRef,
    engine::{
        Batch,
        dataflow::Partition,
        execution::{Consolidator, Limits},
    },
    transaction::Row,
};
use anyhow::{Context, Result, ensure};
const SIDE: &str = "\0left_join_side";
pub(super) fn distinct(columns: &[ColumnRef]) -> Partition<Key, Row> {
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
}
