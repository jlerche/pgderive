//! Integral distinct aggregation over retained positive full-tuple bags.
use crate::{compiler::partition::Aggregate, engine::Batch, transaction::Row};
use anyhow::{Context, Result};
use std::collections::BTreeSet;
pub(super) fn evaluate(
    aggregate: &Aggregate,
    bag: &Batch<(), Row>,
    work: &mut crate::engine::dataflow::PartitionWork,
) -> Result<Option<String>> {
    work.charge(bag.iter().count())?;
    let argument = aggregate.argument.as_ref().context("missing distinct input")?;
    let mut values = BTreeSet::new();
    for (((), row), _) in bag.iter() {
        if aggregate
            .filter
            .as_ref()
            .map(|filter| filter.qualifies(row))
            .transpose()?
            .is_some_and(|qualifies| !qualifies)
        {
            continue;
        }
        if let Some(value) = row.get(&argument.name).context("missing distinct measure")? {
            values.insert(value.parse::<i64>()?);
        }
    }
    Ok(Some(i64::try_from(values.len()).context("COUNT DISTINCT overflow")?.to_string()))
}
