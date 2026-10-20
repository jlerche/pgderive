//! Direct differentiation of tagged left joins; arrangements keep their old codecs.
use super::left_join::stripped;
use crate::{
    compiler::left_join::Spec,
    engine::{
        Batch,
        dataflow::PartitionWork,
        execution::{Consolidator, Limits},
    },
    transaction::Row,
};
use anyhow::{Result, ensure};
use num_bigint::BigInt;
const SIDE: &str = "\0left_join_side";
type Rows<'a> = Vec<(&'a Row, i64)>;
fn split<'a>(bag: &'a Batch<(), Row>, work: &mut PartitionWork) -> Result<(Rows<'a>, Rows<'a>)> {
    let mut left = Vec::new();
    let mut right = Vec::new();
    for (((), row), weight) in bag.iter() {
        work.charge(1)?;
        match row.get(SIDE).and_then(Option::as_deref) {
            Some("left") => left.push((row, *weight)),
            Some("right") => right.push((row, *weight)),
            _ => anyhow::bail!("invalid left join input tag"),
        }
    }
    Ok((left, right))
}
fn products(
    left: &Rows<'_>,
    right: &Rows<'_>,
    output: &mut Consolidator<((), Row)>,
    work: &mut PartitionWork,
) -> Result<()> {
    for (row, weight) in left {
        for (other, multiplicity) in right {
            work.charge(1)?;
            let mut result = stripped(row);
            for (field, value) in stripped(other) {
                ensure!(result.insert(field, value).is_none(), "left join field collision");
            }
            output.add(((), result), BigInt::from(*weight) * multiplicity)?;
        }
    }
    Ok(())
}
fn unmatched(
    spec: &Spec,
    rows: &Rows<'_>,
    sign: i64,
    output: &mut Consolidator<((), Row)>,
    work: &mut PartitionWork,
) -> Result<()> {
    for (row, weight) in rows {
        work.charge(1)?;
        let mut result = stripped(row);
        for column in &spec.right {
            ensure!(
                result.insert(column.name.clone(), None).is_none(),
                "left join field collision"
            );
        }
        output.add(((), result), BigInt::from(*weight) * sign)?;
    }
    Ok(())
}
pub(super) fn evaluate(
    context: (&Spec, Limits),
    before: &Batch<(), Row>,
    after: &Batch<(), Row>,
    delta: &Batch<(), Row>,
    work: &mut PartitionWork,
) -> Result<Batch<(), Row>> {
    let (spec, limits) = context;
    let (old_left, old_right) = split(before, work)?;
    let (new_left, new_right) = split(after, work)?;
    let (left_delta, right_delta) = split(delta, work)?;
    let mut output = Consolidator::new(limits)?;
    // dL * R_new + L_old * dR includes the simultaneous-input cross term once.
    products(&left_delta, &new_right, &mut output, work)?;
    products(&old_left, &right_delta, &mut output, work)?;
    match (old_right.is_empty(), new_right.is_empty()) {
        (true, true) => unmatched(spec, &left_delta, 1, &mut output, work)?,
        (true, false) => unmatched(spec, &old_left, -1, &mut output, work)?,
        (false, true) => unmatched(spec, &new_left, 1, &mut output, work)?,
        (false, false) => {}
    }
    output.finish_batch()
}

#[cfg(test)]
mod tests;
