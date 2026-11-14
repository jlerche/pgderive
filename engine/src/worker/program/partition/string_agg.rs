//! Build an ordered text result from a complete affected partition.
use super::{Bag, selection::Selection};
use crate::{
    compiler::{
        Predicate, TEXT_OUTPUT_LIMIT,
        partition::{SEED, StringAgg},
    },
    engine::{
        dataflow::PartitionWork,
        execution::{CapacityExceeded, Limits},
    },
    transaction::Row,
    worker::program::topk::codec::append_scalar,
};
use anyhow::{Context, Result, ensure};
use std::collections::{BTreeMap, btree_map::Entry};

#[derive(PartialEq, Eq, PartialOrd, Ord)]
struct Input {
    order: String,
    value: String,
    delimiter: Option<String>,
}

pub(super) fn evaluate(
    spec: &StringAgg,
    filter: Option<&Predicate>,
    bag: &Bag,
    (limits, work): (Limits, &mut PartitionWork),
) -> Result<Option<String>> {
    let mut ordered = BTreeMap::<Input, usize>::new();
    let mut remaining = limits.scratch_bytes;
    for (((), row), weight) in bag.iter() {
        work.charge(1)?;
        if row.contains_key(SEED)
            || filter.map(|filter| filter.qualifies(row)).transpose()? == Some(false)
        {
            continue;
        }
        work.charge(spec.order.len().saturating_add(2))?;
        if let Some(input) = inputs(spec, row)? {
            insert(&mut ordered, input, usize::try_from(*weight)?, &mut remaining)?;
        }
    }
    let mut output = None;
    for (input, count) in ordered {
        // In DISTINCT mode every sort key is one of the two arguments, so
        // equal pairs also share the same token and occupy one map entry.
        let count = if spec.distinct { 1 } else { count };
        append(&mut output, &input, count, work)?;
    }
    Ok(output)
}

// Window frames already enumerate occurrences in SQL order, including
// frame exclusions. Never consolidate that sequence into a bag.
pub(super) fn frame(
    spec: &StringAgg,
    filter: Option<&Predicate>,
    frame: Selection<'_>,
    work: &mut PartitionWork,
) -> Result<Option<String>> {
    let mut output = None;
    for (_, row) in frame.iter() {
        work.charge(1)?;
        if filter.map(|filter| filter.qualifies(row)).transpose()? == Some(false) {
            continue;
        }
        work.charge(2)?;
        if let Some(input) = inputs(spec, row)? {
            append(&mut output, &input, 1, work)?;
        }
    }
    Ok(output)
}

fn insert(
    ordered: &mut BTreeMap<Input, usize>,
    input: Input,
    weight: usize,
    remaining: &mut u64,
) -> Result<()> {
    let entry = match ordered.entry(input) {
        Entry::Occupied(entry) => entry.into_mut(),
        Entry::Vacant(entry) => {
            let key = entry.key();
            let bytes = key
                .order
                .capacity()
                .saturating_add(key.value.capacity())
                .saturating_add(key.delimiter.as_ref().map_or(0, String::capacity))
                .saturating_add(std::mem::size_of::<(Input, usize)>());
            *remaining = remaining
                .checked_sub(u64::try_from(bytes)?)
                .context(CapacityExceeded("STRING_AGG ordering scratch limit exceeded"))?;
            entry.insert(0)
        }
    };
    *entry = entry.checked_add(weight).context("STRING_AGG multiplicity overflow")?;
    Ok(())
}

fn inputs(spec: &StringAgg, row: &Row) -> Result<Option<Input>> {
    let value = spec.value.evaluate(row)?;
    let delimiter = spec.delimiter.evaluate(row)?;
    let mut order = String::new();
    for key in &spec.order {
        let value = key.expression.evaluate(row)?;
        append_scalar(
            &mut order,
            key.expression.oid(),
            value.as_deref(),
            key.descending,
            key.nulls_first,
        )?;
    }
    Ok(value.map(|value| Input { order, value, delimiter }))
}

fn append(
    output: &mut Option<String>,
    input: &Input,
    count: usize,
    work: &mut PartitionWork,
) -> Result<()> {
    if count == 0 {
        return Ok(());
    }
    let delimiters = if output.is_some() { count } else { count - 1 };
    let delimiter = input.delimiter.as_deref().unwrap_or("");
    let prior = output.as_ref().map_or(0, String::len);
    let size = input
        .value
        .len()
        .checked_mul(count)
        .and_then(|values| delimiter.len().checked_mul(delimiters)?.checked_add(values))
        .and_then(|bytes| prior.checked_add(bytes))
        .filter(|size| *size <= TEXT_OUTPUT_LIMIT)
        .context("STRING_AGG output exceeds 16 MiB engine budget")?;
    let output = output.get_or_insert_with(String::new);
    if size == prior {
        return Ok(());
    }
    work.charge(count)?;
    output.reserve(size - prior);
    for index in 0..count {
        if index > 0 || delimiters == count {
            output.push_str(delimiter);
        }
        output.push_str(&input.value);
    }
    ensure!(output.len() == size, "STRING_AGG output size mismatch");
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::{Input, PartitionWork, TEXT_OUTPUT_LIMIT, append};
    use anyhow::Result;

    #[test]
    fn output_budget_counts_utf8_bytes_and_omits_first_delimiter() -> Result<()> {
        let mut work = PartitionWork::new(TEXT_OUTPUT_LIMIT);
        let input = Input {
            order: String::new(),
            value: "é".repeat(TEXT_OUTPUT_LIMIT / 2),
            delimiter: Some("!".into()),
        };
        let mut result = None;
        append(&mut result, &input, 1, &mut work)?;
        assert_eq!(result.as_ref().map(String::len), Some(TEXT_OUTPUT_LIMIT));
        assert!(append(&mut result, &input, 1, &mut work).is_err());
        assert_eq!(result.as_ref().map(String::len), Some(TEXT_OUTPUT_LIMIT));
        Ok(())
    }
}
