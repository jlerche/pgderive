mod sliding;
use super::aggregate::{Linear, contribution};
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
use sliding::Frames;
use std::cmp::Ordering;
type Bag = Batch<(), Row>;
pub(super) type Ordered = (Vec<Option<i64>>, Row);
pub(super) fn evaluate(
    spec: &Partition,
    bag: &Bag,
    context: (Limits, &mut crate::engine::dataflow::PartitionWork),
) -> Result<Bag> {
    let (limits, work) = context;
    match &spec.mode {
        Mode::Grouped { keys } => grouped(spec, bag, keys, work),
        Mode::Ranking { order } => ranks(spec, bag, order, (limits, work)),
        Mode::Rows { order, frame } | Mode::Peers { order, frame, .. } => {
            framed(spec, bag, (order, frame), (limits, work), Frames::new(spec))
        }
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
    if matches!(aggregate.function, Function::CountDistinct) {
        super::distinct::evaluate(aggregate, bag, work)
    } else {
        ordinary_value(aggregate, bag, work)
    }
}

fn ordinary_value(
    aggregate: &Aggregate,
    bag: &Bag,
    work: &mut crate::engine::dataflow::PartitionWork,
) -> Result<Option<String>> {
    if aggregate.argument.as_ref().is_some_and(|arg| arg.oid == 1700) {
        return numeric_value(aggregate, bag, work);
    }
    work.charge(bag.iter().count())?;
    if matches!(aggregate.function, Function::Count | Function::Sum | Function::Average) {
        let mut state = Linear::default();
        for (((), row), weight) in bag.iter() {
            state.observe(aggregate, row, *weight)?;
        }
        return state.result(aggregate);
    }
    let mut extremum = None;
    for (((), row), _) in bag.iter() {
        let Some(super::aggregate::Contribution::Value(text)) = contribution(aggregate, row)?
        else {
            continue;
        };
        let argument = aggregate.argument.as_ref().context("missing aggregate argument")?;
        let temporal = matches!(argument.oid, 1114 | 1184);
        let (value, canonical) = if temporal {
            let timestamp = crate::temporal::Timestamp::parse(&text, argument.oid)?;
            (timestamp.sort_value(), timestamp.text(argument.oid)?)
        } else {
            let value = text.parse::<i64>()?;
            (value, value.to_string())
        };
        update_extremum(&mut extremum, value, &canonical, &aggregate.function);
    }
    Ok(extremum.map(|(_, text)| text))
}

fn update_extremum(
    extremum: &mut Option<(i64, String)>,
    value: i64,
    text: &str,
    function: &Function,
) {
    if extremum.as_ref().is_none_or(|(prior, _)| {
        if matches!(function, Function::Min) { value < *prior } else { value > *prior }
    }) {
        *extremum = Some((value, text.into()));
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
                    .map(|value| super::order::value(value, key.column.oid))
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
pub(super) fn compare(left: &[Option<i64>], right: &[Option<i64>], order: &[Order]) -> Ordering {
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
                Function::Rank => Some(rank.to_string()),
                Function::DenseRank => Some(dense.to_string()),
                Function::RowNumber => Some(position.to_string()),
                Function::Lag | Function::Lead => super::navigation::evaluate(
                    function,
                    function.navigation.as_ref().context("missing navigation specification")?,
                    &ordered,
                    index,
                    matches!(function.function, Function::Lead),
                )?,
                _ => anyhow::bail!("aggregate requires framed evaluator"),
            };
            output.insert(function.field.clone(), value);
        }
        updates.add(((), output), 1)?;
    }
    updates.finish_batch()
}

fn framed_fields(
    spec: &Partition,
    frame: &[Ordered],
    context: (Limits, &mut crate::engine::dataflow::PartitionWork),
) -> Result<Row> {
    let (limits, work) = context;
    work.charge(frame.len())?;
    let frame = Batch::from_updates(frame.iter().map(|(_, row)| (((), row.clone()), 1)))?;
    limits.check_batch(&frame)?;
    let mut output = Row::new();
    for aggregate in &spec.aggregates {
        output.insert(aggregate.field.clone(), aggregate_value(aggregate, &frame, work)?);
    }
    Ok(output)
}
fn framed(
    spec: &Partition,
    bag: &Bag,
    ordering: (&[Order], &Frame),
    context: (Limits, &mut crate::engine::dataflow::PartitionWork),
    mut frames: Frames,
) -> Result<Bag> {
    let (order, frame) = ordering;
    let (limits, work) = context;
    let ordered = ordered(bag, order, limits, work)?;
    let peers = matches!(spec.mode, Mode::Peers { .. });
    let mut groups: Vec<(usize, usize)> = Vec::new();
    for (index, (keys, _)) in ordered.iter().enumerate() {
        if !peers {
            groups.push((index, index + 1));
            continue;
        }
        work.charge(1)?;
        if index == 0 || compare(keys, &ordered[index - 1].0, order) != Ordering::Equal {
            groups.push((index, index + 1));
        } else if let Some(group) = groups.last_mut() {
            group.1 = index + 1;
        }
    }
    let mut updates = Consolidator::new(limits)?;
    for (index, (first, last)) in groups.iter().enumerate() {
        let (start, end) = bounds(frame, index, groups.len())?;
        let end_row = if end == 0 { 0 } else { groups[end - 1].1 };
        let start_row = if start >= end { end_row } else { groups[start].0 };
        // Every member of a peer group has the same RANGE/GROUPS frame.
        let fields = frames.fields(spec, &ordered, start_row..end_row, (limits, work))?;
        for (_, row) in &ordered[*first..*last] {
            if peers {
                work.charge(fields.len())?;
            }
            let mut output = row.clone();
            output.extend(fields.clone());
            updates.add(((), output), 1)?;
        }
    }
    updates.finish_batch()
}

fn numeric_value(
    aggregate: &Aggregate,
    bag: &Bag,
    work: &mut crate::engine::dataflow::PartitionWork,
) -> Result<Option<String>> {
    use crate::numeric::Numeric;
    work.charge(bag.iter().count())?;
    let mut sum = Numeric::parse("0")?;
    let mut count = BigInt::from(0);
    let mut extremum: Option<Numeric> = None;
    for (((), row), weight) in bag.iter() {
        let Some(super::aggregate::Contribution::Value(text)) = contribution(aggregate, row)?
        else {
            continue;
        };
        ensure!(*weight > 0, "negative numeric aggregate occurrence");
        count += *weight;
        let value = Numeric::parse(&text)?;
        match aggregate.function {
            Function::Count => {}
            Function::Sum => {
                sum =
                    sum.binary(&value.binary(&Numeric::parse(&weight.to_string())?, "*")?, "+")?;
            }
            Function::Min | Function::Max => {
                if extremum.as_ref().is_none_or(|prior| {
                    if matches!(aggregate.function, Function::Min) {
                        value < *prior
                    } else {
                        value > *prior
                    }
                }) {
                    extremum = Some(value);
                }
            }
            _ => anyhow::bail!("unsupported numeric aggregate"),
        }
    }
    match aggregate.function {
        Function::Count => Ok(Some(i64::try_from(count).context("COUNT overflow")?.to_string())),
        Function::Sum => Ok((count != BigInt::from(0)).then(|| sum.text())),
        Function::Min | Function::Max => Ok(extremum.map(|v| v.text())),
        _ => anyhow::bail!("unsupported numeric aggregate"),
    }
}

#[cfg(test)]
mod tests;

#[cfg(test)]
pub(in crate::worker) fn evaluate_scan(
    spec: &Partition,
    bag: &Bag,
    context: (Limits, &mut crate::engine::dataflow::PartitionWork),
) -> Result<Bag> {
    match &spec.mode {
        Mode::Rows { order, frame } | Mode::Peers { order, frame, .. } => {
            framed(spec, bag, (order, frame), context, Frames::scan())
        }
        _ => evaluate(spec, bag, context),
    }
}
