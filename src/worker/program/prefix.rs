//! Durable integral prefix RANGE windows, with ordered data and additive trie nodes.
mod codec;
mod evaluate;
mod rows;
use super::relational::Key;
use crate::engine::value::NativeBudget;
use crate::{
    compiler::partition::{Mode, Partition},
    engine::dataflow::{Binary, Expand, IndexedStatistics, Project, Statistics},
    transaction::Row,
};
use anyhow::{Context, Result, ensure};
use codec::{decode, encode, measure, node, path, width};
use std::sync::Arc;
fn order(spec: &Partition, row: &Row) -> Result<u64> {
    let Mode::Peers { order, .. } = &spec.mode else {
        anyhow::bail!("prefix window requires peer mode")
    };
    let [order] = order.as_slice() else {
        anyhow::bail!("prefix window requires one order column")
    };
    let value = row
        .get(&order.column.name)
        .and_then(Option::as_deref)
        .context("NULL prefix window ordering")?
        .parse::<i64>()?;
    Ok(super::order::biased(value, false))
}
fn split(key: &Key) -> Result<(Key, u64)> {
    let mut base = key.clone();
    let token = base.pop().context("missing prefix ordering token")?;
    let value = u64::from_str_radix(&token, 16)?;
    ensure!(token == format!("{value:016x}"), "noncanonical prefix ordering token");
    Ok((base, value))
}
pub(super) fn input(spec: &Partition) -> Project<Key, Row, Key, Row> {
    let spec = spec.clone();
    Project::new(move |key: &Key, row: &Row| {
        let mut key = key.clone();
        key.push(format!("{:016x}", order(&spec, row)?));
        Ok(Some((key, row.clone())))
    })
}
pub(super) fn statistics(
    spec: &Partition,
    budget: Arc<NativeBudget>,
) -> IndexedStatistics<Key, Row> {
    let spec = spec.clone();
    let size = width(&spec);
    let native = super::native::Vector::new("sql-prefix-statistics-v1", size, budget);
    let expanded = native.clone();
    let measured = native.clone();
    let decoded = native.clone();
    let expand = Expand::new(move |key: &Key, row: &Row, emit| {
        let (base, value) = split(key)?;
        ensure!(value == order(&spec, row)?, "prefix ordering disagrees with retained row");
        let values = measure(&spec, row)?;
        let payload = expanded.retain(encode(&values), &values)?;
        for prefix in path(value) {
            emit((node(&base, prefix), payload.clone()))?;
        }
        Ok(())
    });
    let stats = Statistics::with_algebra(
        size,
        move |_, row| measured.decode(row, |row| decode(row, size)).map(|values| values.to_vec()),
        move |row| decoded.decode(row, |row| decode(row, size)),
        move |_, values| {
            codec::validate(values)?;
            (values[0] != 0.into()).then(|| native.retain(encode(values), values)).transpose()
        },
    );
    IndexedStatistics::new(expand, stats)
}
pub(super) fn window(spec: &Partition, budget: Arc<NativeBudget>) -> Binary<Key, Row> {
    let native = super::native::Vector::new("sql-prefix-statistics-v1", width(spec), budget);
    let spec = spec.clone();
    Binary::new(move |input, work| {
        let spec = spec.clone();
        let native = native.clone();
        Box::pin(async move { evaluate::evaluate(&spec, input, work, &native).await })
    })
}

#[cfg(test)]
mod codec_tests;
