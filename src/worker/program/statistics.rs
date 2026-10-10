//! SQL sufficient statistics codec and terminal aggregate finalization.
use super::relational::Key;
use crate::{
    compiler::partition::{Function, Mode, Partition},
    engine::dataflow::{Project, Statistics},
    transaction::Row,
};
use anyhow::{Context, Result, ensure};
use num_bigint::BigInt;
pub(super) fn operator(spec: &Partition) -> Statistics<Key, Row> {
    let width = 1 + spec.aggregates.len() * 2;
    let measured = spec.clone();
    let encoded = spec.clone();
    Statistics::new(
        width,
        move |row| measure(&measured, row),
        move |row| decode(row, width),
        move |key, values| encode(&encoded, key, values),
    )
}
fn measure(spec: &Partition, row: &Row) -> Result<Vec<BigInt>> {
    let mut values = vec![BigInt::from(1)];
    for aggregate in &spec.aggregates {
        let qualifies = aggregate
            .filter
            .as_ref()
            .map(|filter| filter.qualifies(row))
            .transpose()?
            .unwrap_or(true);
        let argument = aggregate
            .argument
            .as_ref()
            .map(|column| row.get(&column.name).context("missing statistics input"))
            .transpose()?;
        let present = qualifies && argument.is_none_or(Option::is_some);
        values.push(BigInt::from(u8::from(present)));
        let sum = if present && !matches!(aggregate.function, Function::Count) {
            BigInt::from(
                argument
                    .and_then(Option::as_deref)
                    .context("missing statistics measure")?
                    .parse::<i64>()?,
            )
        } else {
            BigInt::from(0)
        };
        values.push(sum);
    }
    Ok(values)
}
fn field(index: usize) -> String {
    format!("@statistic_{index}")
}
fn decode(row: &Row, width: usize) -> Result<Vec<BigInt>> {
    (0..width)
        .map(|index| {
            row.get(&field(index))
                .and_then(Option::as_deref)
                .context("missing persisted statistic")?
                .parse()
                .map_err(Into::into)
        })
        .collect()
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
fn encode(spec: &Partition, key: &Key, values: &[BigInt]) -> Result<Option<Row>> {
    if !validate(values)? {
        return Ok(None);
    }
    let Mode::Grouped { keys } = &spec.mode else {
        anyhow::bail!("statistics require grouped mode");
    };
    ensure!(keys.len() == key.len(), "statistics grouping key width mismatch");
    let mut row = keys
        .iter()
        .zip(key)
        .map(|(column, value)| Ok((column.name.clone(), serde_json::from_str(value)?)))
        .collect::<Result<Row>>()?;
    for (index, value) in values.iter().enumerate() {
        row.insert(field(index), Some(value.to_string()));
    }
    Ok(Some(row))
}
pub(super) fn finalize(spec: &Partition) -> Project<Key, Row, Key, Row> {
    let spec = spec.clone();
    Project::new(move |key: &Key, row: &Row| {
        let values = decode(row, 1 + spec.aggregates.len() * 2)?;
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
            let count = &values[1 + index * 2];
            let sum = &values[2 + index * 2];
            let value = match aggregate.function {
                Function::Count => {
                    Some(i64::try_from(count).context("COUNT overflow")?.to_string())
                }
                Function::Sum | Function::Average if count == &BigInt::from(0) => None,
                Function::Sum
                    if aggregate.argument.as_ref().is_some_and(|column| column.oid == 20) =>
                {
                    Some(sum.to_string())
                }
                Function::Sum => {
                    Some(i64::try_from(sum).context("SUM(int2/int4) overflow")?.to_string())
                }
                Function::Average => {
                    let count = i64::try_from(count).context("AVG non-NULL count overflow")?;
                    if aggregate.argument.as_ref().is_some_and(|column| column.oid != 20) {
                        i64::try_from(sum).context("AVG(int2/int4) sum overflow")?;
                    }
                    Some(format!("{sum}/{count}"))
                }
                Function::CountDistinct
                | Function::Min
                | Function::Max
                | Function::Rank
                | Function::DenseRank
                | Function::RowNumber
                | Function::Lag
                | Function::Lead => {
                    anyhow::bail!("nonlinear aggregate requires partition evaluator")
                }
            };
            output.insert(aggregate.field.clone(), value);
        }
        Ok(Some((key.clone(), output)))
    })
}
