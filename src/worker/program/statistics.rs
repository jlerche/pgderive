//! SQL sufficient statistics codec and terminal aggregate finalization.
use super::{aggregate::Linear, relational::Key};
use crate::{
    compiler::partition::{
        Function, Mode, Partition,
        statistics::{count, field, sum, width},
    },
    engine::{
        dataflow::{Project, Statistics},
        value::{Decoded, NativeBudget},
    },
    transaction::Row,
};
use anyhow::{Context, Result, ensure};
use num_bigint::BigInt;
use std::{collections::BTreeMap, sync::Arc};
pub(super) fn operator(spec: &Partition, budget: Arc<NativeBudget>) -> Statistics<Key, Row> {
    let width = width(spec.aggregates.len());
    let codec = super::native::Integers::new("sql-linear-statistics-v1", width, budget.clone());
    let encoded_codec = codec.clone();
    let measured = spec.clone();
    let encoded = spec.clone();
    Statistics::with_shared_decode(
        width,
        move |_, row| measure(&measured, row),
        move |row| decode(row, width, &codec),
        move |key, values| encode(&encoded, key, values, (&encoded_codec, &budget)),
    )
}
fn measure(spec: &Partition, row: &Row) -> Result<Vec<BigInt>> {
    if let Some(values) = super::distinct_statistics::measure(spec, row)? {
        return Ok(values);
    }
    let mut values = vec![BigInt::from(1)];
    for aggregate in &spec.aggregates {
        if matches!(aggregate.function, Function::CountDistinct) {
            values.extend([BigInt::from(0), BigInt::from(0)]);
            continue;
        }
        let mut state = Linear::default();
        state.observe(aggregate, row, 1)?;
        values.extend([state.count, state.sum]);
    }
    Ok(values)
}
fn decode(
    row: &Row,
    width: usize,
    codec: &super::native::Integers,
) -> Result<Decoded<Vec<BigInt>>> {
    codec.decode(row, |fields| {
        (0..width)
            .map(|index| {
                fields
                    .get(&field(index))
                    .and_then(Option::as_deref)
                    .context("missing persisted statistic")?
                    .parse()
                    .map_err(Into::into)
            })
            .collect()
    })
}

fn validate(values: &[BigInt]) -> Result<bool> {
    let rows = values.first().context("missing statistics row count")?;
    ensure!(rows >= &BigInt::from(0), "negative statistics group multiplicity");
    for pair in values[1..].chunks_exact(2) {
        let count = &pair[0];
        let sum = &pair[1];
        ensure!(count >= &BigInt::from(0) && count <= rows, "invalid statistics non-NULL count");
        ensure!(
            count != &BigInt::from(0) || sum == &BigInt::from(0),
            "statistics sum without contributions"
        );
    }
    Ok(rows != &BigInt::from(0))
}
fn encode(
    spec: &Partition,
    key: &Key,
    values: &[BigInt],
    native: (&super::native::Integers, &Arc<NativeBudget>),
) -> Result<Option<Row>> {
    if !validate(values)? {
        return Ok(None);
    }
    let Mode::Grouped { keys } = &spec.mode else {
        anyhow::bail!("statistics require grouped mode");
    };
    ensure!(keys.len() == key.len(), "statistics grouping key width mismatch");
    let fields = super::keys::values(key, native.1)?;
    let row = keys
        .iter()
        .zip(fields.iter())
        .map(|(column, value)| (column.name.clone(), value.clone()))
        .collect::<BTreeMap<String, Option<String>>>();
    let mut row: Row = row.into();
    for (index, value) in values.iter().enumerate() {
        row.insert(field(index), Some(value.to_string()));
    }
    Ok(Some(native.0.retain(row, values)?))
}

pub(super) fn finalize(spec: &Partition, budget: Arc<NativeBudget>) -> Project<Key, Row, Key, Row> {
    let spec = spec.clone();
    let codec = super::native::Integers::new(
        "sql-linear-statistics-v1",
        width(spec.aggregates.len()),
        budget,
    );
    Project::new(move |key: &Key, row: &Row| {
        let values = decode(row, width(spec.aggregates.len()), &codec)?;
        ensure!(validate(&values)?, "empty retained statistics");
        let Mode::Grouped { keys } = &spec.mode else {
            anyhow::bail!("statistics require grouped mode");
        };
        let mut output = keys
            .iter()
            .map(|column| {
                Ok((
                    column.name.clone(),
                    row.get(&column.name).context("missing statistics key")?.clone(),
                ))
            })
            .collect::<Result<Row>>()?;
        for (index, aggregate) in spec.aggregates.iter().enumerate() {
            let count = &values[count(index)];
            let sum = &values[sum(index)];
            let value = Linear { count: count.clone(), sum: sum.clone() }.result(aggregate)?;
            output.insert(aggregate.field.clone(), value);
        }
        Ok(Some((key.clone(), output)))
    })
}

#[cfg(test)]
mod tests;
