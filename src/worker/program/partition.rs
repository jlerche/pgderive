use crate::{
    compiler::{
        ColumnRef,
        partition::{Aggregate, Frame, Function, Mode, Order, Partition},
    },
    engine::{
        Batch,
        execution::{Consolidator, Limits},
    },
    transaction::Row,
};
use anyhow::{Context, Result, ensure};
use num_bigint::BigInt;
use std::cmp::Ordering;
type Bag = Batch<(), Row>;
type Ordered = (Vec<Option<i64>>, Row);
pub(super) fn evaluate(
    spec: &Partition,
    bag: &Bag,
    context: (Limits, &mut crate::engine::dataflow::PartitionWork),
) -> Result<Bag> {
    let (limits, work) = context;
    match &spec.mode {
        Mode::Grouped { keys } => grouped(spec, bag, keys, work),
        Mode::Rows { order, frame } => rows(spec, bag, order, frame, (limits, work)),
        Mode::Ranking { order } => ranks(spec, bag, order, (limits, work)),
    }
}
fn grouped(
    spec: &Partition,
    bag: &Bag,
    keys: &[ColumnRef],
    work: &mut crate::engine::dataflow::PartitionWork,
) -> Result<Bag> {
    let Some((((), first), _)) = bag.iter().next() else {
        return Batch::from_updates([]);
    };
    let mut output = keys
        .iter()
        .map(|key| {
            Ok((key.name.clone(), first.get(&key.name).context("missing grouping field")?.clone()))
        })
        .collect::<Result<Row>>()?;
    for aggregate in &spec.aggregates {
        output.insert(aggregate.field.clone(), aggregate_value(aggregate, bag, work)?);
    }
    Batch::from_updates([(((), output), 1)])
}
fn aggregate_value(
    aggregate: &Aggregate,
    bag: &Bag,
    work: &mut crate::engine::dataflow::PartitionWork,
) -> Result<Option<String>> {
    work.charge(bag.iter().count())?;
    let mut count = BigInt::from(0);
    let mut sum = BigInt::from(0);
    let mut extremum = None;
    for (((), row), weight) in bag.iter() {
        if aggregate
            .filter
            .as_ref()
            .map(|predicate| predicate.qualifies(row))
            .transpose()?
            .is_some_and(|qualifies| !qualifies)
        {
            continue;
        }
        let value = aggregate
            .argument
            .as_ref()
            .map(|argument| row.get(&argument.name).context("missing aggregate input"))
            .transpose()?;
        if value.is_some_and(Option::is_none) {
            continue;
        }
        count += *weight;
        if !matches!(aggregate.function, Function::Count) {
            let integer: i64 = value
                .and_then(|value| value.as_ref())
                .context("missing integral measure")?
                .parse()?;
            sum += BigInt::from(integer) * weight;
            extremum = Some(extremum.map_or(integer, |prior: i64| {
                if matches!(aggregate.function, Function::Min) {
                    prior.min(integer)
                } else {
                    prior.max(integer)
                }
            }));
        }
    }
    match aggregate.function {
        Function::Count => Ok(Some(i64::try_from(count).context("COUNT overflow")?.to_string())),
        Function::Sum | Function::Average if count == BigInt::from(0) => Ok(None),
        Function::Average => {
            let count = i64::try_from(count).context("AVG non-NULL count overflow")?;
            if aggregate.argument.as_ref().is_some_and(|arg| arg.oid != 20) {
                i64::try_from(&sum).context("AVG(int2/int4) sum overflow")?;
            }
            Ok(Some(format!("{sum}/{count}")))
        }
        Function::Sum if aggregate.argument.as_ref().is_some_and(|arg| arg.oid == 20) => {
            Ok(Some(sum.to_string()))
        }
        Function::Sum => {
            Ok(Some(i64::try_from(sum).context("SUM(int2/int4) overflow")?.to_string()))
        }
        Function::Min | Function::Max => Ok(extremum.map(|value| value.to_string())),
        Function::Rank | Function::DenseRank | Function::RowNumber => {
            anyhow::bail!("ranking requires ordered evaluator")
        }
    }
}
fn ordered(
    bag: &Bag,
    order: &[Order],
    limits: Limits,
    work: &mut crate::engine::dataflow::PartitionWork,
) -> Result<Vec<Ordered>> {
    let mut rows = Vec::new();
    let mut bytes = 0_u64;
    for (((), row), weight) in bag.iter() {
        let keys = order
            .iter()
            .map(|key| {
                row.get(&key.column.name)
                    .context("missing sort field")?
                    .as_ref()
                    .map(|value| match key.column.oid {
                        1114 | 1184 => {
                            Ok(crate::temporal::Timestamp::parse(value, key.column.oid)?
                                .sort_value())
                        }
                        _ => value.parse::<i64>().map_err(Into::into),
                    })
                    .transpose()
            })
            .collect::<Result<Vec<_>>>()?;
        let weight = usize::try_from(*weight).context("negative window multiplicity")?;
        ensure!(
            rows.len().checked_add(weight).is_some_and(
                |count| count <= limits.contributions && count <= limits.output_entries
            ),
            "window occurrence limit exceeded"
        );
        let row_bytes = u64::try_from(crate::engine::execution::record_size(
            &(row, &keys),
            limits.record_bytes,
        )?)?;
        bytes = bytes
            .checked_add(
                row_bytes.checked_mul(u64::try_from(weight)?).context("window byte overflow")?,
            )
            .context("window byte overflow")?;
        ensure!(bytes <= limits.output_bytes, "window occurrence byte limit exceeded");
        work.charge(weight)?;
        for _ in 0..weight {
            rows.push((keys.clone(), row.clone()));
        }
    }
    rows.sort_by(|left, right| compare(&left.0, &right.0, order));
    Ok(rows)
}
fn compare(left: &[Option<i64>], right: &[Option<i64>], order: &[Order]) -> Ordering {
    for ((left, right), spec) in left.iter().zip(right).zip(order) {
        let comparison = match (left, right) {
            (None, None) => Ordering::Equal,
            (None, Some(_)) => {
                if spec.nulls_first {
                    Ordering::Less
                } else {
                    Ordering::Greater
                }
            }
            (Some(_), None) => {
                if spec.nulls_first {
                    Ordering::Greater
                } else {
                    Ordering::Less
                }
            }
            (Some(left), Some(right)) => {
                let comparison = left.cmp(right);
                if spec.descending { comparison.reverse() } else { comparison }
            }
        };
        if comparison != Ordering::Equal {
            return comparison;
        }
    }
    Ordering::Equal
}
fn rows(
    spec: &Partition,
    bag: &Bag,
    order: &[Order],
    frame: &Frame,
    context: (Limits, &mut crate::engine::dataflow::PartitionWork),
) -> Result<Bag> {
    let (limits, work) = context;
    let ordered = ordered(bag, order, limits, work)?;
    let mut updates = Consolidator::new(limits)?;
    for (index, (_, row)) in ordered.iter().enumerate() {
        let (start, end) = bounds(frame, index, ordered.len())?;
        work.charge(end.saturating_sub(start))?;
        let frame = Batch::from_updates(
            ordered[start.min(end)..end].iter().map(|(_, row)| (((), row.clone()), 1)),
        )?;
        limits.check_batch(&frame)?;
        let mut output = row.clone();
        for aggregate in &spec.aggregates {
            output.insert(aggregate.field.clone(), aggregate_value(aggregate, &frame, work)?);
        }
        updates.add(((), output), 1)?;
    }
    updates.finish_batch()
}
fn bounds(frame: &Frame, index: usize, length: usize) -> Result<(usize, usize)> {
    let index = i64::try_from(index)?;
    let length = i64::try_from(length)?;
    let start = frame.start.map_or(0, |offset| (index + offset).clamp(0, length));
    let end = frame.end.map_or(length, |offset| (index + offset + 1).clamp(0, length));
    Ok((usize::try_from(start)?, usize::try_from(end)?))
}

fn ranks(
    spec: &Partition,
    bag: &Bag,
    order: &[Order],
    context: (Limits, &mut crate::engine::dataflow::PartitionWork),
) -> Result<Bag> {
    let (limits, work) = context;
    let ordered = ordered(bag, order, limits, work)?;
    let mut updates = Consolidator::new(limits)?;
    let mut rank = 1_i64;
    let mut dense = 0_i64;
    for (index, (keys, row)) in ordered.iter().enumerate() {
        let position = i64::try_from(index)?.checked_add(1).context("ROW_NUMBER overflow")?;
        if index == 0 || compare(keys, &ordered[index - 1].0, order) != Ordering::Equal {
            rank = position;
            dense = dense.checked_add(1).context("DENSE_RANK overflow")?;
        }
        let mut output = row.clone();
        for function in &spec.aggregates {
            work.charge(1)?;
            let value = match function.function {
                Function::Rank => rank,
                Function::DenseRank => dense,
                Function::RowNumber => position,
                _ => anyhow::bail!("aggregate requires framed evaluator"),
            };
            output.insert(function.field.clone(), Some(value.to_string()));
        }
        updates.add(((), output), 1)?;
    }
    updates.finish_batch()
}
