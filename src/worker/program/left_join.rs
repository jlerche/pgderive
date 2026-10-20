//! Left equijoin differentiated over complete tagged input partitions.
use super::relational::Key;
use crate::{
    compiler::left_join::{Input, Spec},
    engine::{
        Batch,
        dataflow::{Partition, Project},
        execution::{Consolidator, Limits},
    },
    transaction::Row,
};
use anyhow::{Context, Result, ensure};
use num_bigint::BigInt;
const SIDE: &str = "\0left_join_side";
pub(super) fn input(spec: &Input) -> Project<Key, Row, Key, Row> {
    let spec = spec.clone();
    Project::new(move |_: &Key, row: &Row| {
        ensure!(!row.contains_key(SIDE), "left join internal field collision");
        let components = spec
            .keys
            .iter()
            .map(|column| crate::compiler::native_key(column, row))
            .collect::<Result<Vec<_>>>()?;
        let key = if components.iter().any(Option::is_none) {
            vec!["null".into(), if spec.left { "left".into() } else { "right".into() }]
        } else {
            let mut key = vec!["match".into()];
            key.extend(
                components.into_iter().collect::<Option<Vec<_>>>().context("missing join key")?,
            );
            key
        };
        let mut tagged = row.clone();
        tagged.insert(SIDE.into(), Some(if spec.left { "left".into() } else { "right".into() }));
        Ok(Some((key, tagged)))
    })
}
pub(super) fn operator(spec: &Spec, limits: Limits) -> Partition<Key, Row> {
    let incremental = spec.clone();
    let spec = spec.clone();
    Partition::new_zero_preserving(move |_: &Key, rows: &Batch<(), Row>, work| {
        let mut left = Vec::new();
        let mut right = Vec::new();
        for (((), row), weight) in rows.iter() {
            work.charge(1)?;
            ensure!(*weight > 0, "negative left join input membership");
            match row.get(SIDE).and_then(Option::as_deref) {
                Some("left") => left.push((row, *weight)),
                Some("right") => right.push((row, *weight)),
                _ => anyhow::bail!("invalid left join input tag"),
            }
        }
        let mut output = Consolidator::new(limits)?;
        for (row, weight) in left {
            if right.is_empty() {
                work.charge(1)?;
                let mut result = stripped(row);
                for column in &spec.right {
                    ensure!(
                        result.insert(column.name.clone(), None).is_none(),
                        "left join field collision"
                    );
                }
                output.add(((), result), weight)?;
                continue;
            }
            for (other, multiplicity) in &right {
                work.charge(1)?;
                let mut result = stripped(row);
                for (field, value) in stripped(other) {
                    ensure!(result.insert(field, value).is_none(), "left join field collision");
                }
                output.add(((), result), BigInt::from(weight) * BigInt::from(*multiplicity))?;
            }
        }
        output.finish_batch()
    })
    .with_incremental(move |_, before, after, delta, work| {
        super::join_delta::evaluate((&incremental, limits), before, after, delta, work)
    })
}
pub(super) fn stripped(row: &Row) -> Row {
    row.iter()
        .filter(|(field, _)| field.as_str() != SIDE)
        .map(|(field, value)| (field.clone(), value.clone()))
        .collect()
}
