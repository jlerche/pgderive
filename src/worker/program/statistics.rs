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
mod scalar;
use scalar::Scalar;
pub(super) fn operator(spec: &Partition, budget: Arc<NativeBudget>) -> Statistics<Key, Row> {
    let width = width(spec.aggregates.len());
    let codec = super::native::Vector::new("sql-additive-statistics-v2", width, budget.clone());
    let encoded_codec = codec.clone();
    let measured = spec.clone();
    let encoded = spec.clone();
    Statistics::with_algebra(
        width,
        move |_, row| measure(&measured, row),
        move |row| decode(row, width, &codec),
        move |key, values| encode(&encoded, key, values, (&encoded_codec, &budget)),
    )
}
fn measure(spec: &Partition, row: &Row) -> Result<Vec<Scalar>> {
    if let Some(values) = super::distinct_statistics::measure(spec, row)? {
        return Ok(values.into_iter().map(Scalar::Integer).collect());
    }
    let mut values = vec![Scalar::Integer(1.into())];
    for aggregate in &spec.aggregates {
        if matches!(aggregate.function, Function::CountDistinct) {
            values.extend([Scalar::default(), Scalar::default()]);
            continue;
        }
        if scalar::numeric(aggregate) {
            let value = match super::aggregate::contribution(aggregate, row)? {
                Some(super::aggregate::Contribution::Value(text)) => {
                    Some(crate::numeric::sum::Sum::measure(&text)?)
                }
                _ => None,
            };
            values.push(Scalar::Integer(i32::from(value.is_some()).into()));
            values.push(Scalar::Numeric(value.unwrap_or_default()));
            continue;
        }
        let mut state = Linear::default();
        state.observe(aggregate, row, 1)?;
        values.extend([Scalar::Integer(state.count), Scalar::Integer(state.sum)]);
    }
    Ok(values)
}
pub(super) fn measure_integers(spec: &Partition, row: &Row) -> Result<Vec<BigInt>> {
    measure(spec, row)?.into_iter().map(|value| value.integer().cloned()).collect()
}
fn decode(row: &Row, width: usize, codec: &super::native::Vector) -> Result<Decoded<Vec<Scalar>>> {
    codec.decode(row, |row| scalar::decode_fields(row, width))
}
pub(super) fn decode_fields(row: &Row, width: usize, missing: &'static str) -> Result<Vec<BigInt>> {
    (0..width)
        .map(|index| {
            row.get(&field(index))
                .and_then(Option::as_deref)
                .context(missing)?
                .parse()
                .map_err(Into::into)
        })
        .collect()
}
pub(super) fn encode_fields(values: &[BigInt]) -> impl Iterator<Item = (String, Option<String>)> {
    values.iter().enumerate().map(|(index, value)| (field(index), Some(value.to_string())))
}

fn encode(
    spec: &Partition,
    key: &Key,
    values: &[Scalar],
    native: (&super::native::Vector, &Arc<NativeBudget>),
) -> Result<Option<Row>> {
    if !scalar::validate(values)? {
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
    let values = scalar::canonical(values)?;
    row.extend(scalar::encode_fields(&values)?);
    Ok(Some(native.0.retain(row, &values)?))
}

pub(super) fn finalize(spec: &Partition, budget: Arc<NativeBudget>) -> Project<Key, Row, Key, Row> {
    let spec = spec.clone();
    let codec = super::native::Vector::new(
        "sql-additive-statistics-v2",
        width(spec.aggregates.len()),
        budget,
    );
    Project::new(move |key: &Key, row: &Row| {
        let values = decode(row, width(spec.aggregates.len()), &codec)?;
        ensure!(scalar::validate(&values)?, "empty retained statistics");
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
            let value = match sum {
                Scalar::Numeric(sum) if scalar::numeric(aggregate) => {
                    sum.result(count.integer()?)?
                }
                Scalar::Integer(sum) if !scalar::numeric(aggregate) => {
                    Linear { count: count.integer()?.clone(), sum: sum.clone() }
                        .result(aggregate)?
                }
                _ => anyhow::bail!("statistics scalar domain mismatch"),
            };
            output.insert(aggregate.field.clone(), value);
        }
        Ok(Some((key.clone(), output)))
    })
}

#[cfg(test)]
mod tests;
