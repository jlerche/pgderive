mod frame_value;
mod range;
mod ranking;
mod selection;
mod sliding;
use super::aggregate::{Linear, contribution};
use crate::{
    compiler::{
        ColumnRef,
        partition::{
            Aggregate, Exclusion, Frame, Function, Mode, Order, Partition, PreviousWindows,
        },
    },
    engine::{
        Batch,
        execution::{Consolidator, Limits},
    },
    transaction::Row,
};
use anyhow::{Context, Result, ensure};
use num_bigint::BigInt;
use selection::Selection;
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
        Mode::Ranking { order } => ranking::evaluate(spec, bag, order, (limits, work)),
        _ => framed(spec, bag, (limits, work), Frames::new(spec)),
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
    if aggregate.function.distinct_measure() {
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
    if aggregate.function.boolean()
        || matches!(aggregate.function, Function::Count | Function::Sum | Function::Average)
    {
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
    previous: Option<&PreviousWindows>,
    context: (Limits, &mut crate::engine::dataflow::PartitionWork),
) -> Result<Vec<Ordered>> {
    let (limits, work) = context;
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
    rows.sort_by(|left, right| {
        compare(&left.0, &right.0, order).then_with(|| {
            previous.map_or(Ordering::Equal, |previous| {
                let original =
                    |(name, _): &(&String, &Option<String>)| !previous.fields.contains(*name);
                left.1.iter().filter(original).cmp(right.1.iter().filter(original))
            })
        })
    });
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

fn framed_fields(
    spec: &Partition,
    frame: Selection<'_>,
    context: (Limits, &mut crate::engine::dataflow::PartitionWork),
) -> Result<Row> {
    let mut aggregates =
        spec.aggregates.iter().filter(|value| !value.function.frame_value()).peekable();
    if aggregates.peek().is_none() {
        return Ok(Row::new());
    }
    let (limits, work) = context;
    work.charge(frame.len())?;
    let frame = Batch::from_updates(frame.iter().map(|(_, row)| (((), row.clone()), 1)))?;
    limits.check_batch(&frame)?;
    let mut output = Row::new();
    for aggregate in aggregates {
        output.insert(aggregate.field.clone(), aggregate_value(aggregate, &frame, work)?);
    }
    Ok(output)
}
fn framed(
    spec: &Partition,
    bag: &Bag,
    context: (Limits, &mut crate::engine::dataflow::PartitionWork),
    mut frames: Frames,
) -> Result<Bag> {
    let (order, frame, mut ranges) = match &spec.mode {
        Mode::Rows { order, frame } | Mode::Peers { order, frame, .. } => {
            (order, Some(frame), None)
        }
        Mode::Range { order, frame } => (order, None, Some(range::Cursor::integral(frame))),
        Mode::TemporalRange { order, frame } => {
            (order, None, Some(range::Cursor::temporal(frame)?))
        }
        _ => anyhow::bail!("expected framed window"),
    };
    let (limits, work) = context;
    let ordered = ordered(bag, order, spec.previous_windows.as_ref(), (limits, work))?;
    let peers = !matches!(spec.mode, Mode::Rows { .. });
    let exclusion = spec.mode.exclusion();
    let groups = frame_groups(&ordered, order, peers, work)?;
    let mut positional_peers = 0..0;
    let values: Vec<_> =
        spec.aggregates.iter().filter(|value| value.function.frame_value()).collect();
    let mut updates = Consolidator::new(limits)?;
    for (index, group) in groups.iter().enumerate() {
        let range = if let Some(ranges) = &mut ranges {
            let [key] = order.as_slice() else {
                anyhow::bail!("RANGE offsets require one ordering key")
            };
            ranges.bounds(&ordered, key, group.start, work)?
        } else {
            let (start, end) =
                bounds(frame.context("missing positional frame")?, index, groups.len())?;
            let end = if end == 0 { 0 } else { groups[end - 1].end };
            let start = groups.get(start).map_or(end, |group| group.start);
            start.min(end)..end
        };
        let peer_group = if !peers && exclusion.uses_peers() {
            if !positional_peers.contains(&group.start) {
                positional_peers = group.start..peer_end(&ordered, order, group.start, work)?;
            }
            &positional_peers
        } else {
            group
        };
        let shared = if exclusion.is_none()
            || (exclusion == Exclusion::Current && !range.contains(&group.start))
            || (exclusion == Exclusion::Ties
                && (peer_group.end <= range.start
                    || peer_group.start >= range.end
                    || peer_group.len() == 1))
        {
            Some(frames.fields(spec, &ordered, range.clone(), (limits, work))?)
        } else if exclusion == Exclusion::Group {
            let selection = Selection::new(&ordered, &range, peer_group, group.start, exclusion);
            Some(framed_fields(spec, selection, (limits, work))?)
        } else {
            None
        };
        for current in group.clone() {
            let row = &ordered[current].1;
            let selection = Selection::new(&ordered, &range, peer_group, current, exclusion);
            let output =
                frame_output(spec, &values, row, (selection, shared.as_ref()), (limits, work))?;
            updates.add(((), output), 1)?;
        }
    }
    updates.finish_batch()
}

fn frame_output(
    spec: &Partition,
    values: &[&Aggregate],
    row: &Row,
    (selection, shared): (Selection<'_>, Option<&Row>),
    (limits, work): (Limits, &mut crate::engine::dataflow::PartitionWork),
) -> Result<Row> {
    let mut output = row.clone();
    if let Some(fields) = shared {
        if !matches!(spec.mode, Mode::Rows { .. }) {
            work.charge(fields.len())?;
        }
        output.extend(fields.clone());
    } else {
        output.extend(framed_fields(spec, selection, (limits, work))?);
    }
    for value in values {
        work.charge(1)?;
        let result = frame_value::evaluate(value, selection, row)?;
        output.insert(value.field.clone(), result);
    }
    Ok(output)
}

fn frame_groups(
    rows: &[Ordered],
    order: &[Order],
    peers: bool,
    work: &mut crate::engine::dataflow::PartitionWork,
) -> Result<Vec<std::ops::Range<usize>>> {
    if !peers {
        return Ok((0..rows.len()).map(|index| index..index + 1).collect());
    }
    let mut groups = Vec::new();
    let mut start = 0;
    while start < rows.len() {
        let end = peer_end(rows, order, start, work)?;
        groups.push(start..end);
        start = end;
    }
    Ok(groups)
}

fn peer_end(
    rows: &[Ordered],
    order: &[Order],
    first: usize,
    work: &mut crate::engine::dataflow::PartitionWork,
) -> Result<usize> {
    for index in first + 1..rows.len() {
        work.charge(1)?;
        if compare(&rows[first].0, &rows[index].0, order) != Ordering::Equal {
            return Ok(index);
        }
    }
    Ok(rows.len())
}

fn numeric_value(
    aggregate: &Aggregate,
    bag: &Bag,
    work: &mut crate::engine::dataflow::PartitionWork,
) -> Result<Option<String>> {
    use crate::numeric::Numeric;
    work.charge(bag.iter().count())?;
    let mut sum = crate::numeric::sum::Sum::default();
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
            Function::Sum | Function::Average => {
                sum.add_scaled(&crate::numeric::sum::Sum::measure(&text)?, *weight);
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
        Function::Sum => sum.result(&count),
        Function::Average => sum.average_ratio(&count),
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
        Mode::Grouped { .. } | Mode::Ranking { .. } => evaluate(spec, bag, context),
        _ => framed(spec, bag, context, Frames::scan()),
    }
}
